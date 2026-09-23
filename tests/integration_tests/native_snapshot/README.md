# Native CSE snapshot demo

This demo runs a normal TiCDC changefeed from a frozen CSE packed backup and then
continues consuming TiKV incremental events strictly after the backup TSO `B`.
It uses one TiCDC capture, Kafka open protocol, and a shared local artifact
directory. Snapshot rows are mounted with the schema at `B` and enter the same
capture-owned Kafka sink as incremental rows.

## Modules

| Module | Responsibility |
| --- | --- |
| CSE `cse-ctl snapshot` | Plan physical ranges and scan the immutable backup into bounded SnapshotKV chunks; no row decoding or sink |
| `pkg/snapshot/protocol` | Versioned artifacts, exact coverage checks, chunk decoder, stable row identity |
| `pkg/snapshot/provider`, `cmd/snapshot-provider` | Independent BR controller/Runner; launch CSE, publish one immutable RangeExport winner, reconcile publication gaps, serve cursor discovery |
| `pkg/snapshot/store` | Single-host durable file CAS, immutable objects and paged range tasks |
| `pkg/snapshot/bootstrap` | Generation, schema freeze, discovery, assignment, receipt and finalization controller |
| `coordinator/changefeed` | Persist the optional snapshot child **inside the existing ChangeFeedStatus key**, with status/info revision and owner-epoch checks |
| `maintainer/snapshot.go` | Gate normal dispatcher bootstrap until final marker is durable |
| `downstreamadapter/{dispatcherorchestrator,dispatchermanager}/snapshot.go` | Local capture assignment interface, mounting, broker acknowledgements, writer drain/fencing |

No separate CDC global job key is introduced. Task pages contain only range
state. BR owns its source control state separately. Schema/spec/plan/export/chunk
objects are immutable; attempts never overwrite a winner. The final
`SNAPSHOT_COMMITTED` phase and global marker are one status CAS transaction.
Normal checkpoint, pause and epoch updates preserve the snapshot child.

## Build

Use the `codex/snapshot-e2e-demo` worktree in each repository. The CSE worktree
is based on packed-reader PR #5709 at `ce471b0d203d3e5d3800355e1324a7ed12aa25de`;
the TiCDC worktree is based on `6a1cb553c`. CSE planning uses the current
`sample_range` coverage validation and materialization consumes its lazy
`scan_range` batch stream, retaining the reader's resource ownership. Build on Linux for the development host:

```sh
# TiCDC repository; VERSION is a demo build label, not a released artifact.
mkdir -p "$RUN/bin"
go build -tags=nextgen -ldflags "-X github.com/pingcap/ticdc/pkg/version.ReleaseVersion=v9.0.0 -X github.com/pingcap/ticdc/pkg/version.GitHash=$(git rev-parse HEAD) -X github.com/pingcap/ticdc/pkg/version.GitBranch=codex/snapshot-e2e-demo" -o "$RUN/bin/cdc" ./cmd/cdc
go build -o "$RUN/bin/snapshot-provider" ./cmd/snapshot-provider
go build -o "$RUN/bin/snapshot-verify" ./tests/integration_tests/native_snapshot/verify

# CSE repository, pinned nightly toolchain from rust-toolchain.toml.
cargo build -p cse-ctl
cp "${CARGO_TARGET_DIR:-target}/debug/cse-ctl" "$RUN/bin/cse-ctl"
```

## Run on 10.2.15.7

The installed run is `/data/nvme0n1/cse-snapshot-demo-20260921`:

| Component | Address |
| --- | --- |
| Dedicated SQL / keyspace | `127.0.0.1:4400`, `snap_demo_0921` (102), root without password |
| Upstream PD / CSE TiKV | `127.0.0.1:2379`, `10.2.15.7:20160` |
| Native capture API | `http://10.2.15.7:18300` |
| BR Provider | `http://127.0.0.1:18400` |
| CSE lightweight backup worker | `http://127.0.0.1:39000` |
| Kafka | `10.2.15.7:9092` |
| Lab MinIO | `127.0.0.1:19000` |

The upstream TiKV needs `[cdc] enable=true`. PD 8.5.5 requires the capture's
`enable-legacy-safepoint=true`; API v2 requires a `nextgen` capture build.
Keep TiDB GC retention long enough to cover preparation and incremental catchup.
The helper sets the dedicated SQL keyspace retention to 24 hours; standard CDC
GC protection applies after changefeed creation. Existing SQL port 4000 was
stopped because the old TiDB build's DDL-owner etcd namespace is shared between
keyspaces. Other downstream/Kafka services are unchanged.

To inspect the existing result (run in the TiCDC checkout on the host):

```sh
export RUN=/data/nvme0n1/cse-snapshot-demo-20260921
python3 tests/integration_tests/native_snapshot/demo.py wait --root "$RUN"
python3 tests/integration_tests/native_snapshot/demo.py verify --root "$RUN"
curl -s 'http://127.0.0.1:18300/api/v2/changefeeds/native-snapshot-demo?keyspace=snap_demo_0921'
```

To prepare another fixture, use a **new run directory**, copy/build its binaries,
and stop the old demo processes before reusing the same service ports. Removing
old demo changefeeds or choosing a fresh keyspace avoids feeding fixture resets
into prior validation topics. `--reset-fixture` explicitly drops only the
`snapshot_demo` database. It does not rebuild the cluster.

```sh
python3 tests/integration_tests/native_snapshot/demo.py stop --root "$OLD_RUN"
# A Python environment with boto3 is needed only for prepare; credentials are
# read from the existing CSE config and never put in the manifest or sink URI.
python3 tests/integration_tests/native_snapshot/demo.py prepare --root "$RUN" --reset-fixture
python3 tests/integration_tests/native_snapshot/demo.py start --root "$RUN" --cluster-id unique-demo-cluster --provider-workers 1
python3 tests/integration_tests/native_snapshot/demo.py create --root "$RUN" --name unique-demo-feed --topic unique-demo-topic
python3 tests/integration_tests/native_snapshot/demo.py wait --root "$RUN" --name unique-demo-feed
python3 tests/integration_tests/native_snapshot/demo.py verify --root "$RUN" --name unique-demo-feed --topic unique-demo-topic
```

Preparation writes 3,000 accounts and an empty table, takes a lightweight backup,
packs it, hashes the exact manifest, and records `B`. It then executes one update,
one delete, two inserts and `ADD COLUMN revision DEFAULT 0`. `wait` requires both
the durable snapshot marker and a checkpoint beyond those commits. `verify`
checks all original snapshot values and recomputes the stable record ID, rejects
snapshot output after incremental activation, replays DML/DDL, and compares every
final row and all four columns against SQL. It captures topic end offsets at
startup, so run it only after `wait` and without concurrent fixture writes.

## Continuous incremental validation

After `demo.py wait`, run the live workload against a fresh fixture:

```sh
python3 tests/integration_tests/native_snapshot/live.py \
  --root "$RUN" --name unique-demo-feed --topic unique-demo-topic --duration 60
```

The workload repeatedly commits an update, an insert and a delete in one SQL
optimistic transaction (the older lab CSE does not support TiDB's pessimistic
lock-conflict option). While writes are active, it samples Kafka end offsets and replays
the topic, requiring multiple advancing values of the live marker. After writes
stop, it waits for the final checkpoint and compares every row and column with
SQL, including the exact expected incremental event count. Reports and the write
journal stay under `$RUN`. The observed row age includes polling and replay
cost; it is a freshness observation, not a per-event latency benchmark.

The latest-reader run uses `/data/nvme0n1/cse-snapshot-latest5709-20260921` and
installs its own `bin/cse-ctl`. Pass `--cse-binary "$RUN/bin/cse-ctl"` to
`demo.py prepare` and `start` so a later build cannot replace its exporter.

The latest-reader validation seeded 3,000 rows plus an empty table at
`B=469229866966581249`. Initial catchup applied four DML events and one DDL,
matching all 3,001 SQL rows. The 60-second live phase committed 526 optimistic
transactions and 1,577 additional DML events; nine samples observed Kafka values
advancing while SQL writes were active. Final replay contained exactly 1,581
incremental row events and matched all 3,002 SQL rows and columns.

Killing the capture during `APPLYING` preserved generation
`6d38fc3d-ce47-4b81-ade1-2376cf9966c0`. Restart replayed 128 snapshot rows with
stable IDs; deduplication recovered the same 3,000 snapshot rows, all 1,581
incremental events, and the matching 3,002 final rows. The already committed
feed retained zero snapshot duplicates after that restart. The wait helper
retries transient API failures while keyspace controllers initialize.

## Recovery and observed result

The second changefeed `native-snapshot-recovery` uses topic
`native-snapshot-recovery-20260921`. Its capture was SIGKILLed during APPLYING,
after Kafka had accepted part of a range but before its receipt was durable.
Restart reused generation `faa21988-3856-4376-a900-317b9c5ce7da`, replayed that
range, and then committed the marker and started incremental dispatchers.

| Scenario | Unique snapshot rows | Replay duplicates | Incremental DML | DDL | Final SQL rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| Initial run | 3,000 | 0 | 4 | 1 | 3,001 |
| Capture killed during APPLYING | 3,000 | 2,816 | 4 | 1 | 3,001 |

Both runs compare equal for every final row and column. This demonstrates
**at-least-once** snapshot delivery, with stable IDs for downstream deduplication.
An already committed marker prevents snapshot replay on subsequent capture
restarts. Source-controller recovery is also tested at the export-published /
journal-not-updated boundary; status/list polls restart reconciliation.

## Contract and demo limits

Snapshot metadata keys in Kafka open protocol are `snapshot_record_id`,
`snapshot_id`, `snapshot_ts`, `snapshot_schema` and `snapshot_schema_encoding`.
Schema encoding is `ticdc-table-info-base64-v1`: base64 of the existing TiCDC
`TableInfo.Marshal()` bytes, decoded with `UnmarshalJSONToTableInfo`.
Normal incremental event keys remain unchanged.

Record ID is lowercase SHA-256 of `snapshot-key-v1\0`, u32-BE snapshot-ID byte
length, snapshot-ID UTF-8 bytes, u64-BE physical-table-ID, u32-BE inner-key length,
and the raw inner row key. It deliberately excludes attempt, lease and owner.

SnapshotKV v1: `SNAPKV01`; u32-LE JSON-header length and header; repeated
u32-LE block length / u32-LE row count / framed records; zero block length,
u32-LE JSON-footer length, footer, `SNAPEND1`. Each record has u32-LE key/value
lengths, key, value. JSON byte fields are base64, counts are JSON u64. SHA-256
covers the entire stored chunk. Footer and exact length/checksum are mandatory.

This profile uses uncompressed 1 MiB blocks, 4 MiB target chunks (configurable
1–64 MiB in CSE), 16 MiB record ceiling plus the selected chunk budget, 64 KiB
header/footer, 4 MiB metadata and 512 MiB source file-memory quota. Metadata is
bounded before full allocation. Empty ranges publish a zero-row export/receipt.

This is not a production distributed exporter: one local Runner processes ranges
sequentially; the plan is limited to 256 ranges and 2,048 chunks per range. Local
flock/fsync/rename CAS requires one host and shared paths. Capture fencing uses
the local manager epoch and process writer lock. Multi-capture transport, remote
object CAS, Kubernetes Runner pools, compressed/encrypted chunk output, source
repair/retry policy, lake/relational sinks and scale qualification remain outside
this demo. Source backup decryption uses the existing packed reader support.
Per-row schema envelopes favor a simple verifiable demo over Kafka efficiency.
The test replay utility supports this fixture's integer primary key and DDL;
it is not a general open-protocol consumer.

## Tests

```sh
go test -tags=intest ./pkg/snapshot/... ./coordinator/changefeed ./pkg/sink/codec/open
go test -tags=intest ./maintainer ./coordinator ./api/v2 ./downstreamadapter/dispatchermanager ./downstreamadapter/dispatcherorchestrator -run '^$'
# CSE repository
cargo test -p cse-ctl snapshot
cargo test -p native_br packed_reader
```

### Local parallel export workers

`demo.py start --provider-workers 4` runs up to four `cse-ctl snapshot
materialize-range` processes concurrently per job. The standalone Provider flag
is `-workers 4` (default 1; accepted range 1–32). Planning still runs once, and
TiCDC retains its existing snapshot consumption path. This is local process
concurrency, not Kubernetes scheduling or multiple TiCDC captures.

Each worker has a separate attempt directory. Completed exports are validated
and published through the existing immutable winner/CAS journal. A worker error
cancels and joins the other workers before the job becomes FAILED; previously
published valid results remain durable. Recovery reconciles those winners
without exporting them again.
