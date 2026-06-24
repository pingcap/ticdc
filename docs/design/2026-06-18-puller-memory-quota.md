# Puller Memory Quota and Region Scan Throttling

## Status

Proposed.

## Background

The log puller receives Region events from TiKV and pushes them into a dynamic
stream for processing. The dynamic stream currently uses a fixed 1 GiB memory
quota and applies area-level flow control with the following thresholds:

- Pause input when pending memory reaches 80% of the quota.
- Resume input when pending memory falls below 50% of the quota.

Once input is paused, the puller stops consuming from the TiKV gRPC stream. This
protects TiCDC from unbounded pending data, but transfers the pressure to TiKV.
In an extreme case, TiKV may continue running newly requested incremental Region
scans while it is unable to send their output to TiCDC, increasing TiKV memory
usage.

The fixed 80% pause threshold also leaves a significant portion of the puller
quota unused. The puller should be able to use all available quota before
applying hard backpressure.

## Goals

- Give the log puller a dedicated, configurable memory quota.
- Allow the puller to use available quota up to the hard limit.
- Stop sending new Region incremental scan requests before the quota is full.
- Retain hard backpressure as the final memory protection mechanism.
- Always allow subscription cleanup requests to make progress.
- Keep the change local to the log puller where possible.

## Non-goals

- Limiting the total RSS of the TiCDC process.
- Accounting for all puller-related allocations, such as gRPC internals, Region
  state, transaction matcher state, or Go runtime overhead.
- Cancelling Region scans that have already been sent to TiKV.
- Dynamically tuning the scan throttling thresholds.
- Providing separate quotas for individual changefeeds or subscriptions.

The quota in this design is a logical budget for Region events admitted into the
puller processing pipeline. It is not a strict process memory limit.

## Configuration

Add one server-level debug configuration:

```toml
[debug.puller]
memory-quota = 1073741824 # 1 GiB
```

`memory-quota` is expressed in bytes and must be greater than zero. Its default
value is 1 GiB, preserving the current effective quota.

The following thresholds are implementation constants and are not configurable:

```go
const (
	regionScanPauseRatio  = 0.5
	regionScanResumeRatio = 0.2
)
```

The hard event admission limit is always 100% of `memory-quota`.

## Design Overview

The subscription client owns one `PullerMemoryQuota`. All subscriptions handled
by that client share the quota.

The quota controls two independent actions:

1. Region scan admission: stop new Region register requests at 50% usage and
   resume them below 20% usage.
2. Event admission: block new Region events when admitting them would exceed the
   hard quota, and admit them as soon as sufficient space becomes available.

```text
                       usage >= 50%
    NORMAL ---------------------------------> SCAN_THROTTLED
      ^                                             |
      | usage < 20%                                 | next event does not fit
      |                                             v
      +------------------------------------------- FULL
                                                    |
                                                    | sufficient space released
                                                    v
                                             SCAN_THROTTLED
```

`FULL` does not have a separate low-watermark. A waiting event proceeds as soon
as sufficient quota is released. This removes the current behavior where input
remains paused until usage falls from 80% to 50%.

The 50%/20% hysteresis applies only to Region scan admission. It prevents Region
requests from repeatedly pausing and resuming when usage fluctuates around 50%.
The slow-consumer integration test must verify that normal steady-state usage can
fall below 20%; otherwise the resume threshold would starve new subscriptions
and Region retries and must be adjusted before rollout.

## Puller Memory Quota

The quota controller belongs to `logservice/logpuller`, rather than a shared
package, because its Region scan gate and lifecycle semantics are puller-specific.

### Event admission

After receiving a Region event and calculating its size, the subscription client
acquires quota before admitting the event into the processing pipeline.

- The event is admitted immediately when it fits in the remaining quota.
- Otherwise its receive goroutine waits for memory release, subscription
  stop, or context cancellation.
- Concurrent waiters use atomic compare-and-swap and cannot over-admit memory.
- A single event larger than the configured quota is admitted only when it can
  run alone, avoiding permanent deadlock.

The event has already been allocated by gRPC when its size becomes known. Each
receive goroutine may therefore hold one unadmitted event while waiting. These
events are outside the logical quota, but no additional events are received by
the blocked goroutines.

### Fast path

Memory quota is on the event receive critical path. The normal path must not use
a global mutex or allocate an object per event.

- Global admitted memory is maintained by an atomic counter.
- Acquire reserves memory with a compare-and-swap loop.
- Subscription accounting state is created at subscription setup and referenced
  directly by events; Acquire does not look up a global map.
- The event stores its accounted byte count and subscription owner directly.
- A handler batch sums the accounted bytes and performs one Release operation.
- Waiter locks and notification channels are used only when quota is exhausted.
- Scan gate locks are used only when usage crosses a threshold.

### Event release

Accounted memory is released:

- When the consumer starts handling the event and it leaves the buffered
  pipeline.
- When the pipeline rejects the event.
- In aggregate when its subscription is removed.
- In aggregate when the subscription client closes.

Each subscription accounting object has an active lifecycle protected by a local
lock. Aggregate subscription release marks it inactive and releases its total
usage. A later release from an in-flight stale event becomes a no-op instead of
decrementing unrelated memory.

## Region Request Limiting

Region request queueing, incremental scan concurrency, and sent-request tracking
are separate responsibilities. They must not share one counter or repair each
other's state periodically.

### Store-level scan limiter

Each TiKV store owns one `regionScanLimiter`. A scan slot represents exactly one
Register request that has been sent but has not completed its incremental scan.
The limit is enforced across all request workers connected to the store instead
of being divided approximately between workers.

A slot is acquired immediately before sending Register and released on exactly
one of these terminal paths:

- The Region reports Initialized.
- TiKV returns a Region error.
- Sending Register fails.
- The subscription is deregistered.
- The gRPC stream reconnects and its in-flight requests are rescheduled.

Every acquired slot is represented by an idempotent token. Releasing the token
more than once is a no-op. Counter underflow is an invariant violation and must
not be repaired at runtime.

### Request queues

Pending requests are split by semantics:

- A store-level `registerQueue` contains initial subscriptions and Region error
  retries. All workers for the store consume this shared priority queue. It is
  subject to both the memory scan gate and the store-level scan limiter.
- Each worker has a `controlQueue` containing Deregister requests for its gRPC
  stream. Deregister is broadcast to the relevant workers, bypasses the memory
  scan gate and scan limiter, and is always selected first.

Queue capacity protects local TiCDC memory only. It is not used as the number of
active incremental scans. Retry and initial-subscription priorities remain
explicit request attributes; priority never implies bypassing the limiter.

### In-flight tracker

Each request worker tracks only Register requests sent on its own gRPC stream.
The tracker owns the corresponding scan slot until a terminal event occurs. It
does not contain pending queues and does not maintain a second flow-control
counter.

The following state is removed:

- `pendingCount` and `spaceAvailable`.
- The `force` bypass derived from dynamic request priority.
- Periodic reconciliation between pending count and actual requests.
- The `region worker pending request count is not equal to actual region request
  count, correct it` log and its correction path.

```text
Region discovery
      |
      v
store register queue
      |
      v
memory scan gate
      |
      v
store scan limiter
      |
      v
worker Send(Register) ---> in-flight tracker
                               |
                               +-- Initialized -> release slot
                               +-- Error       -> release and reschedule
                               +-- Reconnect   -> release and reschedule

Deregister -> control queue -> worker Send(Deregister)
```

## Region Scan Gate

Every Region register request can initiate an incremental scan in TiKV. This
includes both the first request for a subscription and a registration retried
after a Region error. Therefore all register requests are subject to the scan
gate.

Requests already sent to TiKV remain active. Pausing them would discard work and
could cause repeated scans after resubscription.

The gate never blocks:

- Deregister requests.
- Subscription cleanup.
- Local error handling.
- Resolve-lock processing.

The gate contains an atomic paused flag and a notification mechanism used only
by waiters. Checking an open gate is one atomic load.

### Sender gate

The effective check is performed when a worker selects a Register request for
sending. Request selection has these rules:

- When the gate is open, the worker may select control or Register requests.
- When the gate is closed, the worker may select only control requests, gate
  changes, or context cancellation.
- A Register selected immediately before the gate closes is allowed to finish
  sending. Holding a lock across gRPC Send is explicitly forbidden.

Queue selection and gate waiting are implemented in one helper. Workers do not
inspect memory quota internals or maintain a nested loop for a retained Register
request.

### Producer gate

An additional gate is checked in `divideSpanAndScheduleRegionRequests`:

- Before each `BatchLoadRegionsWithKeyRange` call.

This gate is an optimization. It avoids unnecessary PD requests, Region range
splitting, and growth of local task queues while scans are paused. It is not
checked for every Region returned by a batch. At most one loaded batch, currently
1024 Regions, may be queued after the gate closes; the sender gate prevents those
requests from reaching TiKV.

Waiting at this gate must exit when the context is cancelled or the subscribed
span is stopped.

## Concurrency and Ordering

Quota state transitions and Region scan gate transitions must be atomic with
respect to memory accounting:

- An Acquire that moves usage to or above 50% closes the scan gate.
- A Release that moves usage below 20% opens the scan gate and wakes waiting
  Region workers.
- A `Release` wakes event admission waiters whenever their request may now fit.
- State-change logs are emitted only on threshold crossings.

Thresholds are precomputed in bytes. Acquire and Release compare integer byte
counts rather than performing floating-point division on the critical path.

## Implementation Plan

The refactoring is split into independently testable steps. Region request
limiting is completed before the new quota fast path so memory throttling is not
built on the current request-cache state machine.

### Step 0: Establish performance and behavior baselines

Add benchmarks and assertions around the current critical paths before changing
their implementation:

- Parallel event admission and release with 1, 8, 32, and 128 goroutines.
- Single-subscription and many-subscription workloads.
- Region request enqueue, Send, Initialized, error, and reconnect lifecycles.
- Mutex and allocation profiles for the puller receive path.
- The maximum number of active incremental scans per store.

The baseline records throughput, nanoseconds per operation, allocations per
operation, mutex wait time, and the number of scan requests sent during memory
pressure.

### Step 1: Replace Region request flow control

Introduce the store-level limiter, semantic request queues, and worker in-flight
tracker without changing memory quota behavior.

1. Add `regionScanLimiter` to `requestedStore` so all workers for a store share
   one exact limit.
2. Add an idempotent scan slot token. Attach it only after a Register request is
   selected for sending.
3. Split pending Register and Deregister requests into `registerQueue` and
   `controlQueue`.
4. Move sent-request state into `regionRequestTracker` owned by each worker.
5. Release scan slots from Initialized, Region error, Send failure, Deregister,
   reconnect, and shutdown paths.
6. Remove `pendingCount`, `spaceAvailable`, `force`, stale count correction, and
   the count-correction log.

During migration, old and new counters must not run in parallel. The new limiter
becomes the only source of active-scan count in the same change that removes
`pendingCount`.

Step 1 is complete when tests prove that every acquired slot reaches exactly one
terminal release and the configured per-store limit is never exceeded.

### Step 2: Introduce the standalone Region scan gate

Add `regionScanGate` independently of memory accounting, initially leaving it
open in production tests.

1. Implement atomic open/paused state and transition notifications.
2. Change worker request selection so a closed gate disables Register selection
   but leaves Deregister selection enabled.
3. Move gate-aware selection into one queue helper and remove worker-level nested
   wait loops.
4. Check the producer gate once before each PD Region batch load and remove the
   per-Region gate check.
5. Add deterministic tests for pause during queueing, pause immediately before
   Send, Deregister bypass, cancellation, and resume.

Step 2 is complete when the open-gate fast path is one atomic load and no worker
depends on quota-internal channels or locks.

### Step 3: Implement the zero-allocation memory quota fast path

Replace the mutex-based quota and per-event reservation object.

1. Create subscription memory state during Subscribe and store its pointer on
   `subscribedSpan`.
2. Store accounted bytes and the subscription owner directly in `regionEvent`.
3. Implement global quota reservation with atomic compare-and-swap.
4. Protect only subscription lifecycle changes with a subscription-local lock.
5. Aggregate Release bytes for every handler batch.
6. Allocate waiter notification state only after quota exhaustion; Release does
   not touch the waiter lock when there are no waiters.
7. Precompute pause and resume thresholds in bytes and drive `regionScanGate`
   only when usage crosses them.
8. Allow an oversized event only when it can run alone.

Acquire that races with unsubscribe must either complete under the active
subscription lifecycle or return cancellation. Aggregate unsubscribe release
must not race past an in-progress Acquire.

Step 3 is complete when quota-available Acquire and batched Release have zero
allocations, do not acquire a global mutex, and preserve exact admitted-byte
accounting under the race detector.

### Step 4: Integrate, observe, and remove transitional code

Connect memory usage transitions to the standalone scan gate and remove all
remaining transitional paths:

1. Pause Region Register selection at 50% admitted memory.
2. Resume selection below 20% admitted memory.
3. Keep event admission open until the next event cannot fit.
4. Remove obsolete request-cache metrics and replace them with queue, active
   scan, gate, waiter, and quota metrics.
5. Remove dead methods, compatibility branches, and comments describing deleted
   algorithms.
6. Update the design document with any threshold changes justified by benchmark
   or stress-test results.

Step 4 is complete after focused unit tests, race tests, the main binary build,
and a slow-consumer integration test all pass. A mutex profile must show no quota
global lock on the normal receive path.

## Observability

Add metrics for:

- Configured puller quota in bytes.
- Admitted memory in bytes.
- Available quota in bytes.
- Number and duration of quota waiters.
- Whether the Region scan gate is open or closed.
- Number of Region register requests waiting on the gate.
- Scan gate pause and resume counts.
- Register and control queue lengths.
- Active incremental scan slots per store and their configured limit.
- Scan slot lifetime from Register Send to Initialized or another terminal path.

Log quota and scan gate state only on transitions. Transition logs should include
the configured quota, current usage, threshold, and number of waiting requests.

## Failure and Shutdown Behavior

- Context cancellation wakes all quota and Region gate waiters.
- Unsubscribe and deregister continue even while the quota is full.
- Closing the subscription client wakes blocked event pushes and Region gate
  waiters through context cancellation.
- A Region worker reconnect does not bypass the scan gate for register requests.
- Quota and scan-slot accounting errors must not be corrected silently.
  Underflow, double release, or leaked ownership should be surfaced during tests
  and with an operational error log.

## Compatibility

The default 1 GiB value preserves the current configuration-free behavior with
the following intentional changes:

- New Region scans stop at 50% puller memory usage.
- Event input continues beyond 80% while space remains.
- Hard backpressure starts only when the next event cannot fit.
- Input resumes as soon as the waiting event can fit instead of waiting for usage
  to fall to 50%.

No TiKV protocol change is required.

## Test Plan

### Quota unit tests

- An event is admitted whenever it fits in the remaining quota.
- A non-fitting event waits and resumes as soon as sufficient quota is released.
- An oversized event runs alone without deadlocking the puller.
- Subscription removal releases all of its accounted memory.
- Releases from stale subscriptions do not corrupt accounting.
- Available-quota Acquire and batch Release perform zero allocations.
- Concurrent Acquire and unsubscribe preserve exact global and subscription
  accounting.

### Region gate unit tests

- The gate closes when usage reaches 50%.
- The gate remains closed between 20% and 50%.
- The gate opens when usage falls below 20%.
- Register requests are not sent while the gate is closed.
- Both initial and retry register requests are gated.
- Deregister requests are sent while register requests are waiting.
- Cancellation and unsubscribe wake producer-side waiters.

### Integration tests

Use a deliberately slow event consumer and verify that:

- Puller admitted memory remains bounded by the expected quota and receive
  headroom.
- The number of active TiKV incremental scans stops increasing after the 50%
  threshold is crossed.
- Existing Region streams continue making progress until hard backpressure is
  required.
- Region request scheduling resumes after usage falls below 20%.
- Active incremental scans for a store never exceed its configured limit.

### Performance tests

- Benchmark quota Acquire and Release with 1, 8, 32, and 128 goroutines.
- Benchmark Region request selection with the gate open and closed.
- Compare event receive throughput and allocations against the Step 0 baseline.
- Capture mutex and allocation profiles under sustained high Region traffic.

The quota fast path must have zero allocations per operation and must not expose
a globally contended mutex in the profile.
