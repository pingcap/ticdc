# Native Lake Sink: Final Design and E2E Results

Last updated: 2026-10-26.

Status: experimental incremental sink; controlled single-capture operation with
an exclusively owned target. This is the only design and acceptance document for
this feature. Historical proposals and separate review/test reports are removed.

## 1. Purpose and scope

Replace the intermediate `TiCDC -> Canal JSON -> object storage -> bend-cdc ->
Lake` path with a native sink:

```text
Existing TiCDC dispatcher and DDL barriers
  -> native typed-row adapter
  -> shared source admission + bounded memory/disk spool
  -> fixed table lanes
  -> retained raw INSERT -> target MERGE -> raw compaction
  -> source completion callback
```

The correctness reference is bend-cdc's TiDB incremental merge path at
`b6481621021e82aee659ad88e8e6c00f6dd11906`. Migrate its key, raw/MERGE, type and
schema-epoch semantics rather than introduce another general CDC framework.
The upstream integration base is TiCDC `b9179182f`. These are source comparison
identifiers, not deployment versions.

### Supported first-version behavior

- Incremental INSERT/UPDATE/DELETE for ordinary non-partitioned tables.
- Explicit primary keys, TiCDC-selected non-null unique keys, composite source
  keys, and an explicit single-column `conflict-key` override.
- Lazy target/raw table creation, parameter-bound batched writes, retained
  tombstones, strict version ordering, target MERGE and historical compaction.
- CREATE DATABASE, CREATE/RECOVER TABLE, DROP TABLE, TRUNCATE and a conservative
  subset of ADD/DROP/RENAME/CHANGE/MODIFY COLUMN.
- Literal defaults, nullability changes and supported widening conversions.
- Durable pending/applied DDL recovery and old-event/DDL replay fences.

### Explicit boundaries

- One controlled capture and a statically exclusive target. `exclusive=true` is
  an operator assertion, **not** a distributed lock or an automatic HA protocol.
- Reject partition dispatchers, across-node table splitting, syncpoints,
  active-active replication, redo, low-latency mode, column selectors, target
  routing and table-level transaction atomicity.
- RENAME TABLE, including multi-table rename, and DROP DATABASE are explicitly
  skipped with warnings after draining old DML. They preserve target objects;
  this is not successful target-side DDL translation. Subsequent DML uses its
  current source name and may create a new projection without moving old rows.
- No Dumpling, CSV, COPY INTO, full snapshot orchestration or historical empty
  table discovery. Users independently prepare a consistent starting state.
- No stream, persistent DML ledger, batch manifest, consumption witness, new
  collector credit protocol, progress migration or distributed quota service.

## 2. Integration and implementation map

Core logic is owned by [the Lake sink package](../../downstreamadapter/sink/lake).
Existing-code changes are limited to scheme/factory/type registration,
schema-store requirements, error classification, metrics and shared spool byte
accounting. The dispatcher/collector framework is not redesigned.

| Component | Responsibility |
|---|---|
| [sink.go](../../downstreamadapter/sink/lake/sink.go) | Lifecycle, shared source admission, DDL barriers and callbacks |
| [native.go](../../downstreamadapter/sink/lake/native.go), [row.go](../../downstreamadapter/sink/lake/row.go) | Authoritative source schema and typed-value normalization |
| [lane.go](../../downstreamadapter/sink/lake/lane.go), [completion.go](../../downstreamadapter/sink/lake/completion.go) | Incremental fragments, bounded spool, batching and event completion |
| [writer.go](../../downstreamadapter/sink/lake/writer.go), [sql.go](../../downstreamadapter/sink/lake/sql.go) | Serial raw/MERGE/compaction and version ordering |
| [client.go](../../downstreamadapter/sink/lake/client.go) | Bound Query API requests and matching terminal-state evidence |
| [schema.go](../../downstreamadapter/sink/lake/schema.go) | Durable schema plans, reconciliation and recovery |
| [local.py](../../tests/integration_tests/lake_native/local.py) | Owned local backend/source/capture E2E lifecycle |

Public names are Lake: `lake://`, `lake+http://`, `LakeSinkType`, Lake errors,
`ticdc_lake_*` metrics and `integration_test_lake`. Backend wire headers and the
compatible backend image keep their actual identifiers. The optional fixed
bend-cdc baseline overlay also retains that module's real API/driver names;
those identifiers are not new public sink names.

## 3. Native events, identities and values

Keep `outputRawChangeEvent=false`, matching the fixed cloud-storage input path.
The existing scanner splits relevant PK/UK changes into DELETE and delayed
INSERT. Preserve that order, including opposite actions with the same key and
commit TSO. Commit TSO is not a unique row/event identity. An unexpected unsplit
UPDATE changing the effective conflict identity fails closed instead of leaving
an old key behind.

Use authoritative `TableInfo` and native `CommitTs`. Native rows do not pass
through Canal ordinal/Latin-1 decoding a second time. Normalize once into an
owned immutable payload, action and canonical key representation:

- Resolve a source PK or non-null unique key using TiCDC's existing helper.
- Composite keys use a canonical named key/value representation; SQL joins use
  every effective key column. Null/missing identities are rejected.
- `conflict-key` is a feed-wide single-column override, not a source index. It
  can replace no key or a single native key, but not a composite key. The operator
  guarantees unique, non-null, stable values and must not change it on an
  existing projection without reinitialization.
- **A truly keyless source also requires `force-replicate = true` in the TiCDC
  replica configuration** (`force_replicate` in the API). `conflict-key` alone
  cannot make the upstream emit an otherwise ineligible table.

Type mapping preserves signed/unsigned integer widths, exact decimals up to the
supported precision, numeric-label ENUM and SET labels, BIT(64), binary bytes,
JSON documents, SQL NULL versus JSON null and microsecond temporal values.
Binary is hex-encoded once and projected using `unhex`. JSON uses a separate
projection so SQL NULL is not confused with a JSON document containing `null`.
TIMESTAMP converts the configured source timezone to UTC; DATETIME retains wall
clock values; TIME/duration uses text. Unsupported or invalid values fail closed.
Visible STORED generated values become ordinary nullable columns. Invisible
VIRTUAL generated columns are not materialized or recreated as expressions.

## 4. Admission, batching and bounded buffering

### Shared admission

`source-bytes` and `source-events` are **sink-wide**, not per-dispatcher limits.
Charge shared source chunks conservatively in full for each admitted event,
including bounded metadata overhead. Dispatcher batch hints are not a global
memory reservation.

`AddDMLEvent` waits on shared quota or lane-queue pressure instead of marking the
changefeed unretryable. It publishes only after reserving bytes/counts under the
admission mutex. A lane dequeue or source release wakes waiters. Waiting does
not hold the mutex, allocate another event queue or start per-event goroutines.
Cancellation, fatal failure and Close wake/terminate waiting calls without a
success callback. An individual event larger than the entire configured source
byte pool is an unretryable configuration/data-size error, not ordinary pressure.

Waiting occurs inside the existing synchronous sink call. A shared handler can
therefore temporarily wait with other paths on that handler; there is no promise
of per-table latency isolation. Independent lane workers, timer flushes and
quota-pressure draining do not require new upstream DML to make progress.
Waiting callers still own already-allocated chunks under upstream accounting;
the sink source limit bounds **admitted** retention, not total process RSS or
all framework-delivered memory. No collector/dynstream API is changed.

### Encoding and spool

Tables hash to a fixed number of serial lanes. Each lane incrementally encodes
bounded immutable fragments rather than copy an entire large transaction into a
second in-memory buffer. The source event is released after encoding. Low
traffic flushes on a timer; row/byte/entry thresholds and spool pressure trigger
earlier flushes. Quota pressure drains pending tables and reclaims applied
fragments, allowing an event larger than the available spool quota to progress.

The spool has a bounded memory hot set plus a physical disk quota. Released but
not physically reclaimed segments remain charged by the shared spool. Entries
are checked with `spool.AccountingBytes` before enqueue; a single oversized row
or entry is rejected rather than use the shared spool's oversized-memory bypass.
Source retention, encoded fragments, readback, HTTP results and schema metadata
have separate limits; their sum is not an absolute RSS/physical-volume cap.

### Completion and cleanup

A source event owns an `OPEN -> SEALED -> COMPLETED` tracker. Register a fragment
before publishing it. Seal only after enumerating the complete event. Complete
once only when sealed, all registered fragments are applied, and no failure
occurred. Applied fragments release storage independently of whole-event ACK.

`PostEnqueue` is a flow-control signal, not target application. `PostFlush` is
allowed only after confirmed target application or a proven applied-DDL fence.
Spool entries never own source-success callbacks. Fatal/Close cleanup uses
resource release, not `Discard`, whose callback semantics could falsely ACK.
Transport session reset does not discard data already transferred to the sink.
Fatal errors end the old lifecycle; do not clear progress by fabricating flushes.

## 5. Raw writes, MERGE and retention

Each table has a target projection and a `<table>_cdc_raw` table containing:

```text
id, table_name, raw_data, add_time, event_pos, action
```

For a serial table batch:

1. Validate the complete batch and filter rows fenced by an applied DDL epoch.
2. Execute all parameter-bound raw INSERT sub-statements in source order.
3. MERGE the target from retained raw rows newer than that epoch. Select each
   key's maximum `(event_pos, add_time)` action and suppress dominated versions.
4. Obtain matching server terminal success before completing covered fragments.
5. Compact strictly dominated historical raw versions.

The source is deliberately **raw**, not a stream cursor. There is no stream
consume hint or planned-coverage witness to interpret. Full-raw MERGE can scan
and rewrite more than an incremental stream path; benchmark it before making a
cost or throughput claim.

`event_pos` is the native commit TSO. `add_time` is a compatibility tie-breaker,
not a new source business version: quantize to stored microseconds, then advance
by one microsecond when needed. Seed the clock from retained `MAX(add_time)` on
bootstrap so a clock rollback/restart cannot reorder a later same-TSO action.
This relies on the exclusive serial writer and existing source ordering; it is
not a solution for conflicting concurrent owners.

Compaction keeps each key's latest watermark/tombstone. Do not TTL-delete the
latest tombstone: an older replayed UPSERT could otherwise resurrect a deletion.
Distinct historical keys can grow raw even if the target stays small and all
maintenance succeeds. `remote-bytes-limit` is a measured raw-storage stop
threshold with batch/statistics lag, not a strict cross-capture budget or a
physical billing cap. Reaching it requires diagnosis and capacity planning or
controlled reinitialization, not unsafe tombstone deletion.

Raw INSERTs, target MERGE and compaction are separate mutations, not one atomic
transaction. A successful MERGE followed by a maintenance failure remains
applied; do not replay business SQL merely to repair maintenance statistics.

## 6. Query outcomes and write fencing

The client sends bound SQL parameters and a client query ID. It follows only
same-origin query continuations, carries required routing hints, disallows
redirects and checks capture write admission again immediately before mutation
POST. Errors must not expose credentials, SQL payloads or row values.

| Result | Required handling |
|---|---|
| COMMITTED | Matching query ID and terminal `Succeeded`; complete application even if optional statistics are unavailable |
| NOT_COMMITTED | Definitive server execution failure; only explicitly proven retryable failures may enter bounded retry |
| UNKNOWN | Missing/contradictory terminal evidence, transport loss or timeout; stop immediately, no hidden retry, next batch, DDL or source ACK |

A result page containing rows is not a commit certificate. Poll through terminal
execution; distinguish execution/transport failures from observation failures.
The HTTP adapter is conservative and does not label arbitrary network errors as
safe retries. Query timeout or a successful cancellation function is not evidence
that the server has stopped. An arbitrary UNKNOWN needs independent old-query
terminal/quiescence evidence and target exclusivity before manual recovery.
The sink does not implement automatic discovery or distributed fencing of old
MERGE/TRUNCATE requests across captures.

## 7. DDL journal, barriers and recovery

Use native DDL `FinishedTs` as the application epoch, not `TableInfoVersion`.
Existing TiCDC block-event handling drains old DML. Each table has a sink-owned
`<table>_cdc_schema_epoch` journal with `epoch`, `applied` and a serialized schema
plan. Plans contain the intended projection and explicit reconciliation steps.

```text
drain old DML -> load journal -> skip an already-applied DDL
             -> persist pending plan -> reconcile target/raw
             -> verify projection -> persist applied epoch -> DDL callback
```

On restart, pending work is reconciled before accepting new-schema DML. Use the
durable applied schema to handle old events; rows at/before the applied epoch
are skipped. Bootstrap alone does not invent an applied epoch. Schema state is
bounded by `max-entries`; exceeding a schema-cache limit is a metadata-capacity
error rather than unbounded table retention.

TRUNCATE recovery order is pending journal, drop raw, truncate target, recreate
raw, applied journal. Reenter pending steps before opening writes. Applied DDL
replay skips the reset so newly written rows survive. DROP removes target/raw
while preserving the journal needed for replay handling.

Column reconciliation checks actual target metadata. Supported cases include
literal-default backfill, nullability changes, TINYINT -> INT -> BIGINT widening,
VARCHAR -> TEXT, and signed INT32 -> VARCHAR(n >= 11). Reject unknown narrowing,
unsafe casts, expression-default changes and conflict-key removal/rename or
unsupported composite-key modification. Secondary index DDL does not create a
Lake index but can advance the compatible schema fence. These rules do not
promise arbitrary multi-operation SQL or source/target transaction equivalence.

## 8. Configuration and operational boundaries

All sink-specific settings live in the URI. Credentials must be URL-encoded and
kept out of command history, logs and shared reports.

```text
lake://DB_USER:TOKEN@HOST/DB_NAME?exclusive=true&workers=4&batch-rows=2048&batch-bytes=1048576&flush-interval=1s&spool-bytes=10737418240&memory-ratio=0.02&source-bytes=268435456&source-events=128&max-entries=4096&query-timeout=2m
```

`lake://` uses HTTPS. `lake+http://` is restricted to literal loopback IPs or
`localhost` for isolated local testing; other hostnames/IPs are rejected before
sending Basic authentication. There are no alternative branded public scheme
aliases in this initial unreleased feature. An explicit database path overrides
the source schema name. Do not collapse different source tables onto one target;
arbitrary schema/table routing is not supported.

| Option | Default | Contract |
|---|---|---|
| `exclusive` | required `true` | Operator-owned static target exclusivity, not a lock |
| `conflict-key` | unset | Feed-wide stable unique non-null single-column override; truly keyless tables also need `force-replicate = true` |
| `workers` | 4 | Fixed lanes, maximum 64; one table hashes to one lane |
| `batch-rows` / `batch-bytes` | 2048 / 1 MiB | Fragment and write thresholds; maximum 100000 rows / 64 MiB, minimum 1 KiB |
| `flush-interval` | 1s | Low-traffic flush bound under a healthy backend |
| `spool-bytes` | 10 GiB | Shared local physical spool budget, at least twice batch bytes |
| `memory-ratio` | 0.02 | Spool hot-memory fraction of spool budget, strictly between 0 and 1 |
| `source-bytes` / `source-events` | 256 MiB / 128 | Shared admitted native-event pool; bytes must cover at least one batch |
| `max-entries` | 4096 | Per-lane fragment pressure and schema-cache limit |
| `spool-dir` | data-directory based | Capture/feed-separated temporary spool, not a replay log |
| `query-timeout` | 2m | Total single-query/pagination deadline, not proof of server termination |
| `warehouse` | unset | Backend warehouse routing header |
| `remote-bytes-limit` | disabled | Measured raw-storage stop threshold; not a global hard quota |

Unknown/duplicate options and unsupported TiCDC modes are rejected. Source-event
and metadata counts are positive and at most 65536. Disk quota is at most 1 PiB.
The target must support parameter binding, matching query IDs, terminal query
states and the required SQL. `Verify` checks a bound SELECT; there is no string
interpolation fallback for incompatible endpoints.

### Migration and recovery procedure

1. Independently prepare consistent target contents and a compatible start-ts.
2. Stop the old writer. Establish that its in-flight SQL cannot mutate the target
   later. An S3 file read cursor alone is not proof of applied target progress.
3. Do not transparently reuse the old two-column bend-cdc journal: the new sink
   has its own pending/applied schema-plan format. Plan target/journal ownership
   and reinitialization explicitly before cutover.
4. Start one capture with the target-exclusive assertion and verify actual data,
   checkpoints, spool pressure and raw growth.
5. If an outcome becomes UNKNOWN, keep writes closed until old-query terminal
   evidence and exclusivity are independently established, then replay from a
   safe checkpoint with a fresh lifecycle.

Removing the intermediate consumer/storage hop can reduce infrastructure and
serialization work. Type conversion still runs in TiCDC; local spool, raw/target
storage and warehouse computation remain. Full-raw scans and compaction can add
cost. No cost-saving percentage or production HA/SLO has been measured here.

## 9. Reproducible tests

### Fully managed local E2E

From the checkout root:

```sh
make integration_test_lake
# Or select/repeat suites:
python3 tests/integration_tests/lake_native/local.py --suite acceptance
# Reuse an explicitly built current-tree binary:
E2E_CDC_BINARY="$PWD/bin/cdc" make integration_test_lake
```

Prerequisites: a running Docker engine, TiUP, mysql CLI, Python **3.10+** and the
repository's Go/build dependencies. Allow enough disk for Go's build cache and
the source cluster, and approximately 8 GiB Docker memory. The runner checks the
Python minimum before starting a build and hashes binaries incrementally rather
than depend on Python 3.11-only APIs. It does not install tools or start Docker.

The runner builds current-tree TiCDC and starts a unique backend meta/query
container using the pinned multi-platform image:

```text
datafuselabs/databend:v1.2.925-patch-13@sha256:7f93f005aa0d626ebc7554d142b0a6b6ebb90e085b0d5bd0d97470084c42d41b
```

The actual image identifier is required to reproduce the compatible server; it
is not the public sink name. The container uses filesystem storage, publishes
HTTP only on a random loopback port, and uses random temporary credentials. No
cloud bucket, MinIO or external Lake credentials are needed. Readiness includes
real SQL parameter binding. An older tested image at v1.2.852-nightly failed
binding and is not listed as supported; no unsafe interpolation workaround was
introduced.

Start an owned TiUP v8.5.1 source with one PD/TiKV/TiDB, no TiFlash/monitor, and an
owned `--newarch --tz UTC` capture. Probe source ports by connection and bind;
refuse reachable loopback/wildcard listeners rather than run SQL against an
existing cluster. Permit TIME_WAIT for immediate reruns. Owner election is not
sufficient readiness: wait for the changefeed read API after coordinator bootstrap.
Only on this owned source, disable fast/distributed index construction to avoid
low-disk laptop failures unrelated to CDC.

Run acceptance, DDL, explicit-key, skipped-DDL, four expected-rejection probes
and real-backend Go tests. On success, failure or interruption, stop only owned
processes/container and clean the unique TiUP tag. Restart cleanup verifies the
exact canonical binary and data-directory paths. Do not use global `pkill` or
`tiup clean --all`; the shared Docker daemon is left running. Target container
data is ephemeral even on failure; private logs/results remain and the temporary
URI file is removed.

The runner prints its artifacts directory. `summary.json` records build/server
versions, source commit/dirty status, image identity, suite exits and cleanup.
Each Python suite has a `result.json`; real-backend assertions are in `fault.log`.
Artifacts contain no credentials intended for publication. No personal machine
paths, generated database IDs or PIDs are required in this document.

Optional environment: `LAKE_E2E_IMAGE`, `TIDB_E2E_VERSION`, `E2E_PORT_OFFSET`
(default 10000), `E2E_CDC_BINARY`, and an existing `OUT_DIR` parent. The managed
runner does not adopt inherited `LAKE_URI_FILE`, `LAKE_NATIVE_TEST_URI`, capture
address or source-port settings. A source/capture already running outside this
runner is not its test environment.

### External authorized warehouse and baseline probes

The [acceptance runner](../../tests/integration_tests/lake_native/run.py) can also
use an explicitly authorized test warehouse and already-owned local TiUP/capture.
Set `LAKE_URI_FILE` to a private mode-0600 file, `CDC_SERVER` (default local port
18300), `MYSQL_PORT` (default 14000) and a fresh `E2E_RUN_DIR` per invocation.
Never pass real credentials on argv or point these tests at production.

```sh
python3 tests/integration_tests/lake_native/run.py
python3 tests/integration_tests/lake_native/extended.py
python3 tests/integration_tests/lake_native/options.py --manual-key
python3 tests/integration_tests/lake_native/options.py
```

External-mode failures retain the isolated database/feed for inspection. After
confirming old requests have terminated, use the same environment/run directory
with `run.py --cleanup`. This mode does not stop borrowed TiUP/capture services;
that remains the operator's responsibility. Its optional drained restart needs
`E2E_CDC_BINARY`, `E2E_CDC_DATA_DIR` and `E2E_PD` identifying owned processes.

The optional [baseline overlay](../../tests/integration_tests/lake_native/baseline.py)
requires `BEND_CDC_DIR` to identify a clean, fixed `b648162` checkout and the same
private URI/run-directory setup. It modifies no baseline files. It exercises the
old core on an authorized HTTPS endpoint, not the entire S3 listing/decoding
pipeline. Its legacy driver/API identifiers must remain unchanged.

### Focused local checks

```sh
python3 -m unittest discover -s tests/integration_tests/lake_native -p 'test_*.py'
go test -p 2 -race ./downstreamadapter/sink/lake ./downstreamadapter/sink ./pkg/sink/spool ./pkg/errors ./pkg/config ./downstreamadapter/sink/statistics -count=1 -timeout=180s
go test -p 2 -race ./pkg/common/event -run 'TestTableSchemaStore|TestNeedTableNameStore' -count=1
go test -p 2 ./pkg/metrics -run TestDownstreamTypeFromSinkURI -count=1
tools/bin/golangci-lint run --timeout 5m ./downstreamadapter/sink/lake/... ./downstreamadapter/sink/statistics/...
```

The local managed suite supplies `LAKE_NATIVE_TEST_URI` to the Go live tests via
the environment. Without explicit opt-in these tests skip; an offline HTTP
fixture is not equivalent to real SQL execution.

## 10. E2E results and evidence boundaries

### Completed acceptance record

On 2026-10-26 the Lake-named implementation with the review fixes passed all nine
suites on macOS arm64, Go 1.26.9, TiDB v8.5.1 and the pinned compatible backend
above. Validation used the review working tree on top of `a3fb6259a`, based on
upstream `b9179182f`. These are local results, not a claim about current CI.
The acceptance configuration deliberately limits the shared source pool to one
event while exercising multiple source tables; normal contention does not stop
the task.

| Suite | Assertions | Result |
|---|---|---|
| Core acceptance | 9: CRUD; PK/UK changes; composite/quoted keys; type fidelity; column DDL; TRUNCATE; large transaction; drained restart; DROP cleanup | PASS |
| Extended DDL | 11: non-null unique handle; INT-to-wide-VARCHAR; defaults/nullability; widening; literal backfill; CHANGE; multi-operation projection | PASS |
| Explicit conflict key | 1: forced keyless CRUD/ADD COLUMN and single-native-key override | PASS |
| Skipped DDL | 2: RENAME and DROP DATABASE preserve target objects while later DML/checkpoints progress | PASS |
| Expected rejections | 4 separate probes: partition dispatcher, genuinely keyless/no override, narrowing, expression default | PASS as rejection, not feature support |
| Real-backend Go tests | 3 parent tests: replay/fences, partial raw/MERGE response loss, DDL interruption recovery | PASS |

There are 27 Python scenarios plus three live Go parent tests. The acceptance
run observed **41 actual disk-spool readbacks**, with a 1200-row transaction and
64 KiB spool quota. Comparisons are key-based and check actual values, not merely
row counts. They include 32-column scalar/binary/decimal/JSON/temporal fidelity;
FLOAT uses tolerance while exact values, null distinctions and schema defaults
are explicitly checked. The drained restart creates a source DML/DDL backlog
and resumes from the original safe checkpoint, without overriding its timestamp.

Real response-loss injection delegates SQL to the actual server, independently
observes a matching terminal success, then drops that response before the sink
sees it. Partial raw testing commits the first INSERT and blocks the second
before send. Unknown paths issue no early flush or blind retry. Recovery is
allowed because the test knows the old terminal outcome and has ended the old
sink lifecycle. This is not a general automatic UNKNOWN recovery service.

| DDL | Lost-response boundaries | Count |
|---|---|---|
| CREATE | pending journal, target create, raw create, applied journal | 4 |
| ALTER | pending, ADD, RENAME, MODIFY, raw ensure, applied | 6 |
| TRUNCATE | pending, raw drop, target truncate, raw recreate, applied | 5 |
| DROP | pending, target drop, raw drop, applied | 4 |

All **19 DDL boundaries** passed. Non-DROP recovery verifies new rows and rejects
old DML/applied-DDL replay without clearing new data. DROP verifies target/raw
absence. These are commit-boundary response-loss tests, not every OS instruction
point, server failure during execution or cross-capture failover.

All suite-owned database/feed cleanup passed. Independent checks found no old
or restarted capture PIDs, no owned backend containers and no URI file. The
current binary was built with `make cdc GOBUILD='CGO_ENABLED=1 GO111MODULE=on go build -p 2'` to reuse the local Go cache, then supplied through `E2E_CDC_BINARY`.
Production build defaults were not changed.

A separate 2026-10-25 authorized staging run also passed the core/DDL matrix and
response-loss probes. Fixed-version baseline core probes identified the original
scope boundaries: non-null unique keys are valid handles, table rename/database
drop are skipped rather than projected, and INT32-to-wide-VARCHAR must remain
supported. They do not prove complete old-S3-chain equivalence or seamless live
journal takeover.

### Review regressions and remaining limits

Review regression validation passed:

- Event-count, byte-budget and control-filled lane-queue pressure wait without
  stopping the sink or bypassing the shared quota; independent workers drain
  them. Close/fatal failure terminates waiters with zero false flush callbacks.
- Admission/Close race scenarios passed 30 repeated race-enabled runs.
- Plaintext configuration accepts only loopback IPs/localhost; remote, wildcard
  and misleading hostname cases are rejected before authentication.
- Real loopback/wildcard listeners are rejected before source startup/SQL; a
  closed listener permits a new run.
- Nine service-free Python lifecycle tests passed, including streaming hashing
  and early minimum-version rejection. All runners parse with Python 3.10
  grammar; an older installed interpreter exits before building services.
- Six focused Go race-test packages, schema-store/metrics tests, scoped vet and
  fresh-cache lint passed (zero lint issues). Go/import/Python/shell formatting
  and repository-relative document-link checks passed.

No Linux/amd64 CI execution, production deployment, general HA/fencing proof,
long-running churn/capacity bound, cloud gateway/S3 qualification, cost benchmark
or performance/SLO acceptance is implied by these tests. Pinning a multi-platform
image is not evidence that a second platform was tested. Latest tombstones and
old-query exclusivity remain operational responsibilities, not closed risks.
