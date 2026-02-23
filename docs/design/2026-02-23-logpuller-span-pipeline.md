<!--
Copyright 2025 PingCAP, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
See the License for the specific language governing permissions and
limitations under the License.
-->

# Pipeline in Log Puller 

## Background & Motivation

In the log puller scenario, the legacy implementation relied on `dynstream` to deliver per-subscription-span data batches and resolved-ts signals to EventStore. A key limitation is that for a given subscribed span, `dynstream` effectively serialized delivery: while one batch is being persisted, subsequent events for the same span cannot enter EventStore. This prevents forming a true pipeline (receive -> compress/group -> persist) for the same span.

This design introduces a new delivery layer, **Span Pipeline**, which removes log puller’s dependency on dynstream while keeping the EventStore `Subscribe` interface unchanged and enforcing a strict resolved-ts barrier semantics.

## Semantics (The Only Must-Have Contract)

For each subscription span (represented by `subID` in code):

- “Earlier data” is defined by the **puller receive order** (not sorted by `commitTs`).
- EventStore persist callbacks may be **out-of-order**.
- The only requirement: for a given subscription span, a `resolved-ts` signal must not call `advanceResolvedTs(ts)` until **all** data batches that arrived **before** that resolved-ts (in puller receive order) have been persisted.
- No cross-span ordering and no global ordering are required.

## Constraints & Non-Goals

- Do **not** change EventStore’s `Subscribe(subID, span, startTs, consumeKVEvents, advanceResolvedTs, ...)` interface.
- The pipeline does not write to Pebble directly. It only calls:
  - `consumeKVEvents(kvs, finishCallback) bool`
  - `advanceResolvedTs(ts)`
- Callback safety: `finishCallback` only enqueues `PersistedEvent` into the pipeline, and must not mutate pipeline state or call `advanceResolvedTs` directly.

## Overview

### Core Idea: Per-subID State Machine + Barriers

Each `subID` has a lightweight pipeline state machine with:

- `nextDataSeq`: the next data batch sequence number (monotonically increasing)
- `ackedSeq`: the largest sequence number **N** such that **all** data batches with sequence in `[1..N]` have been persisted (monotonically increasing)
- `doneSet`: completed sequences that are **greater than `ackedSeq`** but cannot advance `ackedSeq` yet because some earlier sequences are still missing (used to handle out-of-order callbacks)
- `pendingResolved`: pending resolved barriers (mergeable)
  - each barrier is `{waitSeq, ts}`: allowed to call `advanceResolvedTs(ts)` only when `ackedSeq >= waitSeq`
  - merge rule: for the same `waitSeq`, keep only the maximum `ts`
  - memory hygiene: even if the queue is never fully drained (because new resolved signals keep coming), the implementation periodically compacts the already-flushed prefix so it does not retain memory unnecessarily

### Event Types & Handling

The pipeline processes three event types. All events are routed by `subID` to a single worker, so a given `subID`’s state is mutated by only one goroutine (no locks required for state).

1) `DataEvent{kvs}` (KV batch from puller; participates in puller receive order)

- Allocate `seq := nextDataSeq; nextDataSeq++`
- Call `consumeKVEvents(kvs, onPersist(seq))` immediately, without waiting for persistence
- `onPersist(seq)` only enqueues `PersistedEvent{seq}`

2) `ResolvedEvent{ts}` (span resolved-ts from puller)

- Bind the barrier at arrival:
  - `waitSeq := nextDataSeq - 1` (the “last Data seq that has entered the pipeline before this resolved”)
- Insert/merge `{waitSeq, ts}` into `pendingResolved`
- Try `flushResolvedIfReady()`

3) `PersistedEvent{seq}`

- Mark `seq` completed: add to `doneSet`
- Advance `ackedSeq` using `doneSet`:
  - `while doneSet contains ackedSeq+1 { delete it; ackedSeq++ }`
- Call `flushResolvedIfReady()`

### `flushResolvedIfReady()`

While there exists a pending barrier whose `waitSeq` is satisfied (`ackedSeq >= waitSeq`), call `advanceResolvedTs(ts)` in order and remove the sent barrier.

Note: out-of-order persist callbacks do not affect correctness because `ackedSeq` only advances when **all earlier sequences are known to be persisted**.

### Why `ackedSeq` is defined this way

Persist callbacks may arrive out-of-order, e.g. sequences `1,2,3` are enqueued in order, but callbacks could arrive as `2,3,1`.

- After seeing callback `2`, we cannot safely conclude that “all data before resolved barrier is persisted”, because `1` might still be in-flight.
- Therefore we maintain:
  - `ackedSeq`: "the highest N such that `[1..N]` are all persisted"
  - `doneSet`: "persisted sequences that arrived but are waiting for gaps to be filled"

Example:

1. Callbacks arrive: `2` -> `doneSet={2}`, `ackedSeq=0` (cannot advance)
2. Callbacks arrive: `1` -> `doneSet={2}` then advance `ackedSeq` to `2` by consuming `1` and `2`

This is exactly what the `while doneSet contains ackedSeq+1` loop achieves.

## Concurrency Model: Shared Worker Pool (`pipelineManager`)

To avoid per-span goroutines, we use a pipeline manager with a fixed number of workers:

- `newSpanPipelineManager(ctx, N, queueSize, quota)` starts N goroutines
- Each worker holds `map[subID]*spanPipelineState`
- Route `Data/Resolved/Persisted` by `subID % N`
- State for a `subID` is mutated by only one worker goroutine, so no locks are needed for state

Code entry: `logservice/logpuller/span_pipeline.go`

## Ordering Notes (Why No Reorder Buffer Is Needed)

The log puller processes region events concurrently (sharded by regionID), but the Span Pipeline does **not** maintain an additional reorder buffer. This is intentional.

What we must preserve is the relative order between:

- Data batches, and
- the span-level resolved-ts signal that is derived from region resolved progress.

Key observations:

1) **Per-region event order is already serialized.**

All events for the same region are processed by the same `regionEventProcessor` worker goroutine, so within a region, “entries then resolved” order is preserved.

2) **Span-level resolved-ts is computed from all regions.**

`maybeAdvanceSpanResolvedTs` uses the minimum resolved-ts across all regions/ranges in the span. Therefore, a span-level resolved-ts can advance to `T` only if every region/range in the span has already advanced its own resolved-ts to at least `T`.

3) **A region cannot advance its resolved-ts past data it has not enqueued.**

In the region worker goroutine, data batches are assembled from `entries` and immediately enqueued to the pipeline. If data enqueue is blocked (e.g., by quota), the goroutine cannot continue to process later resolved-ts events for that region, so the region’s resolved progress cannot advance. This indirectly prevents the span-level resolved-ts from advancing as well (because it is the minimum across regions).

Given these properties, additional reordering at the pipeline boundary is unnecessary: the channel delivery order is sufficient, and correctness is ensured by the region-local serialization plus the “min across regions” rule for span resolved-ts.

## Puller-Side Dataflow: `regionRequestWorker` and `regionEventProcessor`

This section describes how the log puller receives change events from TiKV and turns them into (1) KV batches and (2) span-level resolved-ts signals that are then fed into Span Pipeline.

### `regionRequestWorker` (per TiKV store)

Source: `logservice/logpuller/region_request_worker.go`

High level responsibilities:

- Maintains a long-lived gRPC streaming connection to a specific TiKV store.
- Sends `ChangeDataRequest` (register / deregister) to the store for regions that should be subscribed.
- Receives `ChangeDataEvent` messages (region events + resolved-ts batches) from TiKV and dispatches them to `regionEventProcessor`.
- Tracks per-subscription per-region state (`regionFeedState`) so incoming messages can be mapped back to the right in-memory state machine.

Key loops:

1) **Send loop (`processRegionSendTask`)**

- Pulls pending region subscription requests from an internal `requestCache` (acts as the upstream flow-control for region requests).
- For a normal region, it:
  - creates a `regionFeedState` (`newRegionFeedState`), starts it, and stores it in `requestedRegions[subID][regionID]`
  - sends a `cdcpb.ChangeDataRequest` with `RequestId=subID`, `RegionId=regionID`, and `CheckpointTs=region.resolvedTs()`
- For a “stop span” task (a special regionInfo marker), it sends a `Deregister` request and marks all region states under that `subID` as stopped.

2) **Receive loop (`receiveAndDispatchChangeEvents`)**

- Reads from the gRPC stream (`Recv()`), and for each message:
  - dispatches `changeEvent.Events` (per-region events) via `dispatchRegionChangeEvents`
  - dispatches `changeEvent.ResolvedTs` (batch resolved message) via `dispatchResolvedTsEvent`

Dispatch rules:

- **Per-region events** (`dispatchRegionChangeEvents`)
  - Locates `regionFeedState` by `(RequestId=subID, RegionId)`
  - Converts the protobuf union into a `regionEvent`:
    - `entries` (`cdcpb.Event_Entries_`)
    - `resolvedTs` (`cdcpb.Event_ResolvedTs`)
    - `error` -> marks the state stopped
  - Sends to `regionEventProcessor.dispatch(regionEvent)`
- **ResolvedTs batch** (`dispatchResolvedTsEvent`)
  - TiKV can send one `cdcpb.ResolvedTs{Ts, Regions[]}` for many regions at once.
  - The worker collects corresponding `regionFeedState` pointers for those regions (with missing-region logging), and calls:
    - `regionEventProcessor.dispatchResolvedTsBatch(ts, states)`

### `regionEventProcessor` (sharded by regionID)

Source: `logservice/logpuller/region_event_processor.go`

High level responsibilities:

- Provides per-region serialization by sharding all events by `regionID % workerCount`.
  - This preserves the correctness of region-local ordering constraints while still processing different regions in parallel.
- Transforms region-level events into span-level outputs:
  - Data batches: `[]common.RawKVEntry`
  - Span resolved-ts: a single resolved-ts per `subID` produced by `maybeAdvanceSpanResolvedTs`
- Ensures region correctness checks remain intact (e.g., commitTs must be greater than the region’s last resolvedTs).

Handling details:

1) **Entries events (`cdcpb.Event_Entries_`)**

- Calls `appendKVEntriesFromRegionEntries(...)` which:
  - Handles initialization (`cdcpb.Event_INITIALIZED`) and flushes cached matches from the txn matcher.
  - Matches PREWRITE/COMMIT/ROLLBACK using the matcher, and builds `common.RawKVEntry` with commitTs (`CRTs`) and regionID.
  - Enforces region-local invariants like `CommitTs > lastResolvedTs`.
- The resulting `[]RawKVEntry` is a per-region batch; the processor emits it to the span pipeline:
  - `pipeline.EnqueueData(ctx, span, batch)`

2) **Per-region resolvedTs events (`cdcpb.Event_ResolvedTs`)**

- Updates region state:
  - `updateRegionResolvedTs(span, state, resolvedTs)`
  - updates `span.rangeLock` heap for computing span-level resolved
- Attempts to advance span-level resolved-ts:
  - `ts := maybeAdvanceSpanResolvedTs(span, triggerRegionID)`
  - This computes the span’s resolved-ts as the minimum resolved among all locked ranges (heap min), and applies `advanceInterval` throttling.
- If a new span-level resolved-ts is produced, it is emitted to the span pipeline:
  - `pipeline.EnqueueResolved(span, ts)`

3) **Batch resolvedTs (`cdcpb.ResolvedTs{Ts, Regions[]}`)**

- The processor shards the region state pointers by regionID and applies `updateRegionResolvedTs` for each state in the batch.
- After applying the batch, it tries `maybeAdvanceSpanResolvedTs` once and emits at most one span-level resolved-ts for that `subID`.

### Relationship to Span Pipeline semantics

- `regionEventProcessor` preserves **region-local** ordering by region sharding, and produces span-level Data/Resolved outputs concurrently across regions.
- The span pipeline relies on region-local serialization and the “min across regions” span-resolved computation to preserve the necessary ordering between Data batches and span-level resolved-ts signals.
- Span Pipeline then enforces the only required contract: a span-level resolved-ts is advanced only after all earlier Data batches have been persisted.

## Backpressure & Memory Control (Replacing dynstream Pause/Resume)

dynstream Pause/Resume is no longer used. The pipeline implements a simple quota at `EnqueueData`:

- Use a global `semaphore.Weighted` as in-flight quota (default 1GiB)
- Compute weight using an approximate byte size of RawKVEntry (`key/value/oldValue`)
- `EnqueueData(ctx, ...)` calls `Acquire(weight)` first:
  - blocks when exceeding the quota to prevent unbounded memory growth
  - releases the weight when the corresponding PersistedEvent is observed

Goals:

- resolved-ts is no longer blocked behind “wait for persist” delivery serialization
- data inflight is bounded to avoid memory blow-ups

## Metrics

Key Prometheus metrics exposed by this implementation:

- `ticdc_log_puller_span_pipeline_inflight_bytes`: approximate bytes currently held by the pipeline quota.
- `ticdc_log_puller_span_pipeline_inflight_batches`: number of data batches currently in-flight (quota held, waiting for persist callback).
- `ticdc_log_puller_span_pipeline_pending_resolved_barriers`: number of pending resolved barriers waiting for `ackedSeq` to pass.
- `ticdc_log_puller_span_pipeline_active_subscriptions`: number of active subscription spans tracked by pipeline workers.
- `ticdc_log_puller_span_pipeline_quota_acquire_duration_seconds`: histogram of time spent waiting for quota before enqueueing data.
- `ticdc_log_puller_span_pipeline_resolved_barrier_dropped_total`: redundant resolved barriers dropped (e.g., duplicate/obsolete ts).
- `ticdc_log_puller_span_pipeline_resolved_barrier_compaction_total`: number of times the pending resolved queue is compacted.

## Lifecycle & Cleanup

- `Subscribe`: after creating `subscribedSpan`, call `pipeline.Register(span)` so the pipeline can access `consumeKVEvents/advanceResolvedTs`.
- `Unsubscribe/onTableDrained`: call `pipeline.Unregister(subID)` and clear state. If there are still in-flight callbacks, quota will be released on callback completion and the state will be removed eventually.

## EventStore Interface Remains Unchanged

EventStore still provides:

- `consumeKVEvents(kvs, finishCallback) bool`: push into EventStore write queue, and call `finishCallback` after persistence.
- `advanceResolvedTs(ts)`: notify resolved-ts (eventually notifying downstream dispatchers).

The pipeline only changes **when** `advanceResolvedTs` is called (from dynstream serialization to seq barrier semantics).

## Unit Tests

Unit tests validate the core semantics (`logservice/logpuller/span_pipeline_test.go`):

- out-of-order persist callbacks: resolved-ts still waits for the persisted prefix before advancing
- resolved merge: for the same `waitSeq`, keep the maximum `ts`
- multiple subIDs: independent and non-blocking

## Change Summary

- Removed dynstream dependency and its path/feedback handling in log puller
- `regionEventProcessor` now enqueues Data/Resolved directly into the pipeline
- Added Span Pipeline as the log puller -> EventStore delivery layer