# Sync-point and Redo-wait Backpressure and Memory Reclamation

## Scope

This design addresses the memory starvation described in
[#4605](https://github.com/pingcap/ticdc/issues/4605). The implementation baseline
is master `7c8599a91b5e88aa778faae7425f97039637430d`.

Two changes work together:

1. Check for stalled EventCollector memory periodically, independently of new
   events, and retain a recovery event when reclaiming queued data.
2. Advertise zero scan quota for an ordinary dispatcher actually waiting for,
   or writing, a sync-point, or caching a batch above the redo watermark.
   Resume advertising its available memory when that wait is satisfied.

The design does not add another broker prepare/commit protocol, change
maintainer barrier coverage, skip non-replicating members, or alter transaction
cursors and scan-window escape paths. Adaptive window tuning and general
cross-broker quota fairness remain separate work. This does not claim to resolve
all possible causes of prolonged dispatcher migration.

## Existing behavior

A fast dispatcher A can already be waiting at sync-point S while its pending
queue holds substantial data after S. A slow dispatcher B needs shared memory
to scan up to S. If A consumes the shared quota, a cycle exists:

```text
A waits for B at S -> B needs shared quota -> quota needs A to drain -> A waits
```

Redo introduces another dependency: an ordinary dispatcher caches its active
batch until `redoGlobalTs` reaches the batch's final commit TS. Its pending queue
can fill with later events. Collector changefeed availability is the minimum
of ordinary and redo stream availability, so a full ordinary stream can stop a
lagging redo dispatcher from scanning. That dispatcher then pins the global
redo watermark needed to drain the ordinary stream. Periodic reclamation frees
pending payload; stopping prefetch on the waiting ordinary path prevents it
from immediately consuming that space again.

Current reclamation is invoked from `areaMemStat.appendEvent`. It requires
recent input, more than five seconds without ordinary event consumption or
reclamation, and memory usage above 60%. Adjacent periodic signals return from
the merge branch before that check. Moreover, a broker with an unfinished
transaction cursor cannot send a signal resolved event. Consequently, neither
silence nor repeated resolved signals guarantees another reclamation check.

Current manual `Release` clears pending events without retaining recovery
notifications. Recovery normally relies on a later event revealing a sequence
gap. Silence can also prevent that recovery step.

The reverted [two-stage implementation](https://github.com/pingcap/ticdc/pull/4429)
coordinated a broker's members before emitting S. Once S was emitted it allowed
scanning toward the next sync-point. The proposed backpressure observes the
actual consumer barrier state, including waits caused by another broker. It
reacts later than a prepare fence, so reclamation is still necessary.

## Invariants

- Only an ordinary dispatcher in sync-point WAITING/WRITING or with an actual
  cached batch above the redo watermark is paused by this rule. Redo dispatchers
  produce that watermark and must remain eligible to scan.
- WAITING retries, Write/Pass, heartbeats, registration, handshake and reset must
  remain usable while scan quota is zero.
- Reclamation cannot discard the active barrier or events already owned by the sink.
- Periodic checks, completion callbacks and queued recovery notifications provide
  progress without a new upstream event.
- Data loss from reclamation is recovered through the existing checkpoint,
  sequence and epoch rules; never advance a checkpoint to skip missing data.
- Only the owning dynamic-stream handle loop mutates pending queues.
- Memory accounting reflects actual retained and released bytes. A reclamation
  request is not itself progress.
- Stale recovery notifications cannot reset a new source/epoch. Delayed quota
  snapshots must not create a sticky pause requiring a matching resume message.
- A main dispatcher's synthetic zero quota must not become a changefeed-wide or
  redo-wide synthetic zero quota.
- Checks, feedback congestion, Wake/Release/Remove and shutdown must not create
  lock/channel dependency cycles.

## Periodic reclamation

### Scheduling and policy

EventCollector invokes an explicit `DynamicStream.CheckMemoryPressure()` on its
main and redo streams from the existing one-second congestion-control loop,
before publishing memory snapshots. `GetMetrics()` remains observational. No
additional timer goroutine or user setting is needed.

The memory controller snapshots area references under its mutex and releases
that mutex before checking areas. Only `MemoryControlForEventCollector` is
eligible. The periodic check requires usage above 60%, more than five seconds
since real ordinary-event consumption or reclamation, and reclaimable blocked
pending data. It does not require recent append activity.

Retain the existing candidate ordering (descending last handled event TS),
256-byte path threshold and 60% pending-byte reclamation target. Use a CAS to
claim the per-area one-second request interval across append-driven and periodic
checks. Do not hold controller, path-map or barrier locks while waiting for
feedback. Feedback delivery must be nonblocking and retried on a later check
when congested. Do not update the actual-progress timestamp for a request alone.

Other ordinary traffic in an area can defer the stall detector. This first
change covers the exhausted-quota, no-consumption cycle; it is not a proof of
fair scheduling under arbitrary competing traffic. A per-path stall policy
would require a separately reviewed extension if experiments demonstrate that
need.

### Owner-thread reclamation

Validate that a feedback destination still matches the registered dispatcher.
Bind release to the particular path object, so feedback for a removed object
cannot release a replacement. In the owning handle loop, recheck that the path
is still present, blocked and has reclaimable data. A release arriving after
Wake may be skipped. If the path is blocked at another event, only its current
pending data can be reclaimed; the active event remains outside that queue.
Admission to the owning stream queue is nonblocking too. If it is full, the
next pressure check retries; cancellation does not wait for reclamation queue
space. Once a path has been visited, skip it until a new droppable data event
arrives, so retained controls alone do not cause repeated ineffective requests.

The owning stream reconciles both reclamation and removal accounting.
RemovePath only unregisters the path; it leaves pending bytes accounted until
the owner clears the queue. If removal precedes or occurs during reclamation,
clear the removed path completely and deduct its pending bytes once. Otherwise,
subtract the reclaimed payload bytes from both the path and area. The existing
admission-time payload-to-DropEvent replacement also
charges the actual retained marker size, preserving wrapper overhead.

The EventCollector memory-release path retains recovery/control events:

- Replace discarded DML, BatchDML, DDL and sync-point data with the lightweight
  event returned by the existing handler `OnDrop` callback.
- Preserve source and epoch. Do not coalesce recovery markers across events or
  epochs in this first implementation. The first current-epoch marker resets
  the stream; later old-epoch markers are ignored.
- Preserve non-droppable events, including Ready, Handshake, NotReusable and
  existing DropEvent notifications, in order.
- Preserve resolved watermarks using the existing adjacent-signal coalescing
  behavior. Dropping the last resolved event can otherwise strand an idle
  table's checkpoint without further upstream input.
- Only apply a replacement that reduces retained bytes. Count markers and
  control events in pending memory; do not set the accounting blindly to zero.
- Keep signal-queue accounting correct so Wake schedules retained markers even
  with no new input.

Puller reclamation policy and removal cleanup are unchanged. Avoid clearing the
barrier or issuing Wake as a side effect of memory reclamation.

```text
A handles S and blocks
 -> reclaim A's queued post-S data, retaining DropEvent
 -> B obtains memory and reaches S
 -> maintainer coordinates Write/Pass
 -> A successfully completes S and wakes
 -> DropEvent triggers reset from the existing safe checkpoint
```

This retains the existing sequence checks as another recovery defense without
requiring a future message to reveal a gap.

## Dispatcher scan backpressure

### Derive state from the existing barrier

Expose a lightweight `DispatcherService.ShouldPauseScan() bool`. The basic
dispatcher checks the existing barrier under its mutex and returns true only
for a non-redo dispatcher handling a sync-point in WAITING or WRITING. Do not
copy large protobuf status objects or perform I/O or stream operations under
that mutex.

The ordinary event dispatcher also checks its redo cache. An atomic required TS
is stored before publishing a cached batch and cleared before retrying it. A
retry that still needs redo progress republishes the required TS. Clearing
before handling or Wake prevents completion of an old batch from clearing a
new batch's wait. Removal clears the required TS under the existing cache
mutex; a concurrent retry cannot cache another batch after removal.

Pause for redo only when redo is enabled and the cached required TS exceeds
the current `redoGlobalTs`. A low watermark without a cached batch is not a
pause condition. Reaching the required TS removes the redo pause even before
the cache retry runs, so no paired resume notification or new scanned event
is required. Pre-WAITING asynchronous sync-point flush remains outside the
pause condition.

Each one-second congestion snapshot computes:

```text
path quota = 0                      if ShouldPauseScan()
path quota = actual available bytes otherwise
```

Apply this override to the individual target ID after obtaining actual memory
snapshots. Keep actual main/redo area accounting; do not propagate a synthetic
zero by table ID or changefeed. Reuse the existing quota protocol without an
additional pause/resume RPC or immediate-notification callback.

### Broker and resume behavior

In `doScan`, after Ready/Handshake and scan-request calculation, reject zero
per-dispatcher quota before consuming scan-rate tokens or reserving shared
quota. Retain no-data resolved fast paths, safe signal-resolved emission,
pending-barrier local advance and same-commit/large-transaction cursors. Do not
cancel active scans merely because feedback changed. Already queued or in-flight
data may still arrive and remains eligible for reclamation.

Table-trigger dispatchers use their separate tick instead of the DML quota gate.
Do not add a new DDL/resolved pause there; doing so can prevent schema/scheduling
progress. Their blocked pending queues still participate in memory reclamation.

Existing successful PostFlush/Pass clears barrier state before waking the data
stream. The next periodic snapshot advertises actual availability without
waiting for checkpoint advancement or new input. If the next sync-point is
already pending, continuing to advertise zero is correct. A failed write must
not clear the barrier.

Redo watermark forwarding already calls `HandleCacheEvents()` after advancing
`redoGlobalTs`. This retries the active batch and wakes the stream when it can
continue. If processing enters a sync-point barrier, that barrier independently
keeps the scan pause active. Do not gate the actual redo dispatcher on
`redoGlobalTs`, since its progress is needed to advance that value.

Current quota messages do not carry a per-path epoch. A delayed zero snapshot
can temporarily overwrite a newer value, but periodic current snapshots
converge after a finite backlog and network recovery. There is no persistent
pause bit requiring a paired Resume. Reset/move/recreated dispatchers derive
state from their own live barrier; removed sessions must not be resurrected.

## Liveness and concurrency review

Under readable upstream/schema progress, eventually writable sink, eventual
control delivery and scheduling, and sufficient reclaimable pending data:

1. Periodic checks request reclamation without needing input.
2. Owner-thread reclamation frees actual bytes and the next quota report exposes them.
3. The slow participant can scan to S and satisfy existing barrier coverage.
4. Write/Pass runs independently of DML scan quota and clears the pause reason.
5. Wake processes the retained DropEvent and recovery proceeds through the existing epoch protocol.

This breaks the stated resource cycle. Permanent sink failures, disconnected
members and infinite starvation by other producers are outside these premises.

Required interleavings include both Wake/Release orders, Remove/recreation,
feedback saturation, old/new sources and epochs, back-to-back barriers, reset
while a barrier is pending, redo dependencies, and shutdown. Memory requests
must not masquerade as progress; recovery markers must survive later reclamation.

## Observability, cost and compatibility

No configuration, protobuf or wire version changes are required. Existing
brokers understand zero quota. Full behavior requires an updated collector;
mixed-version deployments do not provide all guarantees immediately.

The status getter is constant-time once per dispatcher per second. Area checks
are linear in area count; only pressured stalled areas enumerate/sort paths.
Reclamation remains linear in the affected queue, and retained recovery markers
consume real memory. Measure control latency with long queues.

Reuse existing quota-skip, release, DropEvent/reset and barrier diagnostics.
Any additions should be bounded area summaries with representative IDs rather
than per-event logging or high-cardinality Prometheus labels. Correlate barrier
TS, epoch, pending bytes, quota, cursor, reclaimed bytes and checkpoint. DS
pending memory is not equivalent to total process RSS.

## Implementation and validation

Changes are owned by dynamic-stream memory/queue management, dispatcher barrier
state, collector congestion reporting and the broker's scan admission gate.
Only necessary interface test doubles are adapted. Generated mocks, if affected,
are regenerated through the repository workflow.

First construct failing regressions, then implement reclamation/recovery before
adding sync-point-derived zero quota. Cover:

- Silence and merged resolved signals with blocked high-memory queues.
- Concurrent periodic/append checks, bounded feedback, retry and actual accounting.
- Retained data-loss markers and idle resolved watermarks; Wake without new input.
- Non-droppable events and multiple sources/epochs; removal/shutdown races.
- Puller exclusion, redo separation and WAITING/WRITING eligibility.
- Redo cache waits, partial/sufficient watermark advances, consecutive waits,
  concurrent removal and handoff to sync-point backpressure.
- Successful/failed completion, delayed/lost quota reports and consecutive barriers.
- Zero-quota rate/shared-budget preservation, cursor safety and local advance.
- Shared A/B starvation followed by barrier completion and checksum-correct replay.

Run targeted package tests using the repository race/failpoint workflow:

```sh
make unit_test_pkg PKG=./utils/dynstream/...
make unit_test_pkg PKG=./downstreamadapter/dispatcher/...
make unit_test_pkg PKG=./downstreamadapter/eventcollector/...
make unit_test_pkg PKG=./pkg/eventservice/...
make cdc
```

### Development-machine comparison

Use isolated TiUP-managed upstream/downstream clusters with at least two CDC
nodes and Prometheus/Grafana on the development host. Inventory existing tasks,
ports and resources first; do not overwrite unrelated deployments. Pin binary
hashes, topology, configuration, data, resource limits and workload for master
and patched variants. Record upstream TiDB-to-TiKV write throughput separately
from internal scanned-DML bytes.

Use many fast paths and a slower path sharing collector quota, with real large
transaction cursors and 32–64 MiB quota as an initial calibration range. Cover
same-broker and cross-broker senders. Match the incident's ten-minute sync-point
interval for the formal comparison; identify shorter debug intervals explicitly.
Create skew through bounded scheduling or downstream delays, then remove all
injected faults and stop extra input before assessing the resource cycle.

A baseline stall must persist for at least fifteen minutes and at least three
configured sync-point intervals without meaningful target barrier, slow cursor
or checkpoint progress, while required upstream/schema progress, sink and
control delivery remain healthy. Combine observations with dependency evidence;
a finite timeout alone is not proof of an irreversible deadlock.

The patched run must demonstrate the entire check -> reclaim -> quota -> scan ->
barrier -> replay chain without restart, manual resume, larger quota or disabling
sync-points. The initial small-reproducer target is completion within 120 seconds
after faults are removed; calibrate any revised bound before comparison. Run at
least three comparisons, then complete three further sync-points and checksums
of all test tables. Include insert/update/delete, normal load, redo, DDL,
move/scale, reset and maintainer failover as subsequent regressions.

For the redo-wait case, first disable sync-points and enable redo. Use a bounded
delay on one redo participant while fast ordinary paths cache batches above
the global redo watermark. After removing the delay and stopping input, verify
pending-memory reclamation, the slow redo participant's scan/checkpoint,
durable redo meta progress, ordinary cache retry and checkpoint-correct replay.
Repeat with sync-points enabled and same-broker/cross-broker placements. This
validates the watermark dependency cycle, not fairness among continuously
runnable redo paths competing for scan-rate tokens.

Identical bounded test hooks may establish otherwise rare legal interleavings,
but must not disable baseline reclamation or alter quota/barrier outcomes. Label
such builds as master plus test instrumentation and separately attempt an
unmodified-master workload reproduction. If master only shows latency, the stall
acceptance criterion remains unmet and investigation continues.

Preserve per-run manifests, SHA/diff/binary hashes, topology/config, fault and
observation windows, logs, metrics, checksums and a factual summary in the
external development notebook. Changes requiring a new broker coordination
phase, barrier completion semantics, wire protocol or general quota scheduler
need an updated design review before implementation.
