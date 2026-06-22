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
	regionScanResumeRatio = 0.4
)
```

The hard event admission limit is always 100% of `memory-quota`.

## Design Overview

The subscription client owns one `PullerMemoryQuota`. All subscriptions handled
by that client share the quota.

The quota controls two independent actions:

1. Region scan admission: stop new Region register requests at 50% usage and
   resume them below 40% usage.
2. Event admission: block new Region events when admitting them would exceed the
   hard quota, and admit them as soon as sufficient space becomes available.

```text
                       usage >= 50%
    NORMAL ---------------------------------> SCAN_THROTTLED
      ^                                             |
      | usage < 40%                                 | next event does not fit
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

The 50%/40% hysteresis applies only to Region scan admission. It prevents Region
requests from repeatedly pausing and resuming when usage fluctuates around 50%.

## Puller Memory Quota

The quota controller belongs to `logservice/logpuller`, rather than a shared
package, because its Region scan gate and lifecycle semantics are puller-specific.
It is independent from dynamic stream memory control. The puller does not enable
dynamic stream memory control, consume its feedback, or implement its memory
control algorithm interface.

### Event admission

After receiving a Region event and calculating its size, the subscription client
acquires a puller-owned reservation before calling `DynamicStream.Push`.

- The event is admitted immediately when it fits in the remaining quota.
- Otherwise its receive goroutine waits for a reservation release, subscription
  stop, or context cancellation.
- Concurrent waiters are serialized by the quota and cannot over-admit memory.
- A single event larger than the configured quota is admitted only when it can
  run alone, avoiding permanent deadlock.

The event has already been allocated by gRPC when its size becomes known. Each
receive goroutine may therefore hold one unadmitted event while waiting. These
events are outside the logical quota, but no additional events are received by
the blocked goroutines.

### Event release

Every admitted event carries a puller reservation. The reservation is released:

- At the start of normal dynamic stream handling, when the event leaves the
  buffered pipeline.
- From `OnDrop` when dynamic stream rejects the event.
- In aggregate when its subscription is removed.
- In aggregate when the subscription client closes.

Reservations refer to an internal subscription accounting object. Aggregate
subscription release removes that object, so a later release from an in-flight
stale event becomes a no-op instead of decrementing unrelated memory.

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

### Authoritative gate

The authoritative check is immediately before sending a Region register request
on the TiKV gRPC stream. This is the only point that can guarantee no additional
incremental scan is started after the gate closes.

If the gate closes after a request has been dequeued, the worker retains that
request and waits. It must still process deregister requests while waiting. The
request cache or send loop therefore needs a separate control path for deregister
requests so that cleanup cannot be blocked behind a waiting register request.

### Non-authoritative producer gate

An additional gate is checked in `divideSpanAndScheduleRegionRequests`:

- Before each `BatchLoadRegionsWithKeyRange` call.
- While scheduling the Regions returned by a batch.

This gate is an optimization. It avoids unnecessary PD requests, Region range
splitting, and growth of local task queues while scans are paused. It is called
non-authoritative because memory usage can cross 50% after this check, and
requests may already exist in the Region task queue or worker request cache.

Waiting at this gate must exit when the context is cancelled or the subscribed
span is stopped. The send-time gate remains mandatory even when the producer
gate is present.

## Concurrency and Ordering

Quota state transitions and Region scan gate transitions must be atomic with
respect to memory reservation:

- An `Acquire` that moves usage to or above 50% closes the scan gate before it
  returns.
- A `Release` that moves usage below 40% opens the scan gate and wakes waiting
  Region workers.
- A `Release` wakes event admission waiters whenever their request may now fit.
- State-change logs are emitted only on threshold crossings.

The existing Region request priority continues to apply after the scan gate
opens. Deregister requests have a separate bypass and do not depend on that
priority ordering.

## Observability

Add metrics for:

- Configured puller quota in bytes.
- Admitted memory in bytes.
- Available quota in bytes.
- Whether the Region scan gate is open or closed.
- Number of Region register requests waiting on the gate.
- Scan gate pause and resume counts.

Log quota and scan gate state only on transitions. Transition logs should include
the configured quota, current usage, threshold, and number of waiting requests.

## Failure and Shutdown Behavior

- Context cancellation wakes all quota and Region gate waiters.
- Unsubscribe and deregister continue even while the quota is full.
- Closing the subscription client wakes blocked event pushes and Region gate
  waiters through context cancellation.
- A Region worker reconnect does not bypass the scan gate for register requests.
- Quota accounting errors must not be corrected silently. Underflow, double
  release, or leaked reservations should be surfaced during tests and with an
  operational error log.

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
- Subscription removal releases all of its reservations.
- Releases from stale subscriptions do not corrupt accounting.

### Region gate unit tests

- The gate closes when usage reaches 50%.
- The gate remains closed between 40% and 50%.
- The gate opens when usage falls below 40%.
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
- Region request scheduling resumes after usage falls below 40%.
