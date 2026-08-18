# MySQL Sink DML Barrier Design

## Problem

Issue #5353 requires MySQL sink block events, especially syncpoints, to wait until all DMLs enqueued before the block event are actually flushed to downstream. The current direct fix tracks every DML with a `PostFlush` callback in `downstreamadapter/sink/mysql/sink.go` and makes `FlushDMLBeforeBlock` wait for those callbacks.

That is correct on ordering, but it adds per-DML overhead even when syncpoint is disabled or block events are rare. The desired optimization is to pay the synchronization cost only when a block event needs a DML flush barrier.

The new barrier must be careful about the existing MySQL sink pipeline. `Sink.AddDMLEvent` does not write directly to a worker queue. It first admits the DML into the conflict detector, and the conflict detector may delay assigning the DML to a writer queue until conflicting transactions are flushed or a blocked cache becomes available. A correct barrier therefore cannot only append tokens to the current writer queues; it must also order itself after DMLs that were admitted into the conflict detector but have not yet reached any writer queue.

## Goals

- Preserve the ordering guarantee: a block event must not be written or reported as flushed before earlier MySQL DMLs reach downstream.
- Avoid adding per-DML `PostFlush` callbacks only for block-event barriers.
- Keep the barrier local to MySQL sink internals.
- Return an error instead of hanging if a DML writer fails or the sink closes while a barrier is waiting.

## Non-Goals

- Do not change dispatcher block-event semantics.
- Do not change MySQL DML conflict detection rules.
- Do not change DML SQL generation or batching behavior.
- Do not make every sink implement the same barrier mechanism.

## Current Flow

The relevant dispatcher path is implemented in `downstreamadapter/dispatcher/basic_dispatcher.go`:

```text
BasicDispatcher.DealWithBlockEvent
  -> sink.FlushDMLBeforeBlock(blockEvent)
  -> sink.WriteBlockEvent(blockEvent) or report WAITING status
```

`DealWithBlockEvent` calls `FlushDMLBeforeBlock` before both non-blocking block-event writes and blocking `WAITING` reports (`basic_dispatcher.go:1007` and `basic_dispatcher.go:1201`). This is the point where MySQL DMLs already accepted by the sink must be known to have reached downstream.

For MySQL DMLs, the current flow is:

```text
Sink.AddDMLEvent
  -> create pendingDML and attach event.AddPostFlushFunc
  -> conflictDetector.Add(event)
  -> slots.Add(node)
  -> node.TrySendToTxnCache(cacheID)
  -> resolvedTxnCaches[cacheID].add(event)
  -> runDMLWriter(ctx, cacheID)
  -> inputCh.GetMultipleNoGroup(buffer)
  -> dmlWriter[cacheID].Flush(events)
  -> event.PostFlush()
  -> pendingDML.complete(nil)
```

Important code locations:

- `downstreamadapter/sink/mysql/sink.go:320-347`: current per-DML pending tracking and `FlushDMLBeforeBlock` wait.
- `downstreamadapter/sink/mysql/sink.go:216-302`: DML writer loop and batch flushing.
- `downstreamadapter/sink/mysql/causality/conflict_detector.go:101-129`: conflict-detector admission and slot-node callbacks.
- `downstreamadapter/sink/mysql/causality/txn_cache.go:45-119`: bounded writer queues and `BlockStrategyWaitEmpty` behavior.
- `pkg/sink/mysql/mysql_writer.go:208-255`: downstream DML flush and `PostFlush` invocation after successful execution.
- `pkg/common/event/dml_event.go:654-676`: `PostFlush` runs flush callbacks first and then calls idempotent `PostEnqueue` as a fallback wake-up path.

`cfg.WorkerCount` creates both the number of MySQL DML writers and the number of conflict-detector output caches, so `cacheID == dmlWriter index`.

## Existing Correctness Constraints

The implementation must preserve these properties from the current pipeline:

1. A DML slot node is removed only through the `PostFlush` callback registered in `ConflictDetector.Add` (`conflict_detector.go:106-108`). Conflict dependencies therefore represent downstream flush completion, not only queue assignment.
2. A DML can be admitted before a block event but remain outside writer queues if it conflicts with an earlier DML or if `BlockStrategyWaitEmpty` rejects insertion while the cache is blocked.
3. `runDMLWriter` receives batches from `UnlimitedChannel.GetMultipleNoGroup`. If a batch contains both DML items and barrier tokens, the writer must process the batch in exact queue order.
4. `Writer.Flush` calls `event.PostFlush()` only after successful downstream execution. A barrier acknowledgement must happen after the same success boundary for all preceding DMLs.
5. `FlushDMLBeforeBlock` has no context parameter in the sink interface, so close and writer-error paths must actively fail waiters.

## Proposed Design: Broadcast Barrier Token

Add a MySQL-sink-only barrier protocol that broadcasts a barrier token to all DML writer queues through the conflict detector. `FlushDMLBeforeBlock` waits for all barrier acknowledgements.

High-level flow:

```text
FlushDMLBeforeBlock(blockEvent)
  -> barrier := newDMLBarrier(workerCount)
  -> conflictDetector.BroadcastBarrier(barrier)
  -> wait barrier until all writer queues ack or one returns error

runDMLWriter(ctx, idx)
  -> receive DML events and barrier tokens from cache idx
  -> flush all DML events before the barrier token
  -> ack barrier from writer idx
```

This makes the barrier an in-band queue item. Because each DML writer consumes its queue sequentially, receiving the barrier means all earlier events in that writer queue have already been flushed or are being flushed immediately before acknowledgement.

The key implementation detail is that `BroadcastBarrier` must not bypass the conflict detector's unresolved DML nodes. It should create a conflict-detector fence that is ordered after all DML nodes already admitted to the detector, and only then broadcast one in-band token to every writer queue.

The resulting shape is:

```text
                  existing DML path
AddDMLEvent --------------------------------------+
                                                   |
                                                   v
                                           conflict slots
                                                   |
FlushDMLBeforeBlock                                |
  -> BroadcastBarrier --------------------> barrier fence
                                                   |
                                         all prior DML flushed
                                                   |
                                                   v
                                  cache 0  cache 1  ... cache N
                                     |        |            |
                                     v        v            v
                                  writer 0 writer 1 ... writer N
                                     |        |            |
                                     +---- barrier acks ---+
```

## Interface Shape

### Writer Queue Item

The existing `txnCache` only carries `*commonEvent.DMLEvent`. Generalize the item type to a small value struct instead of using `interface{}` so normal DMLs do not allocate an extra wrapper object:

```go
type writerItem struct {
    dml     *commonEvent.DMLEvent
    barrier *dmlBarrier
}

func newDMLItem(event *commonEvent.DMLEvent) writerItem {
    return writerItem{dml: event}
}

func newBarrierItem(barrier *dmlBarrier) writerItem {
    return writerItem{barrier: barrier}
}
```

Then change the cache interface and output channel types:

```go
type txnCache interface {
    add(item writerItem) bool
    forceAdd(item writerItem) bool
    out() *chann.UnlimitedChannel[writerItem, any]
}
```

`add` preserves the current cache-full semantics for DMLs. `forceAdd` is only for barrier tokens after the conflict-detector fence is resolved; it appends the token even when `BlockStrategyWaitEmpty` is currently blocked. This avoids a deadlock where the barrier is waiting for writer progress, while the blocked-cache policy prevents the token needed to observe that progress from entering the queue.

`utils/chann.UnlimitedChannel.Push` currently logs and returns nothing when the channel is closed. `forceAdd` cannot reliably report broadcast failure with that API. The implementation should add a narrow cache-level or channel-level push method that returns whether the item was accepted, and `forceAdd` should return false when the output channel has already been closed. Do not infer success from absence of a log.

The normal hot path remains simple:

- `ConflictDetector.Add` creates `writerItem{dml: event}` as a stack value.
- No barrier object or `PostFlush` callback is attached for block-event waiting.
- The DML writer reuses preallocated buffers when converting queued `writerItem` values into `[]*commonEvent.DMLEvent` batches for `Writer.Flush`.

### Conflict Detector

Add a broadcast method:

```go
func (d *ConflictDetector) BroadcastBarrier(barrier *dmlBarrier) error
```

`BroadcastBarrier` must define a precise snapshot point. All DMLs whose `ConflictDetector.Add` call completes before the snapshot must be ordered before the barrier. DMLs admitted after the snapshot are not required by that barrier and must not be allowed to overtake the barrier tokens.

Implement this with a lightweight barrier gate inside the conflict detector:

1. Serialize `Add` and `BroadcastBarrier` admission with a small mutex in `ConflictDetector`. This mutex protects only admission and barrier snapshot state; it must not be held during downstream IO or barrier waiting.
2. When broadcasting, create a barrier fence node that depends on the current in-flight DML frontier. The frontier is the set of slot tail nodes currently stored in `Slots`; those nodes are removed only after downstream `PostFlush`, so depending on them is sufficient to wait for all earlier conflicting chains.
3. Publish the barrier fence as the current global fence before releasing the admission mutex. DMLs admitted after this point should depend on the current global fence before normal conflict-key resolution, so they cannot be assigned to writer queues before the barrier tokens.
4. When the barrier fence resolves, append one `writerItem{barrier: barrier}` to every resolved transaction cache with `forceAdd`.
5. When every writer acknowledges the barrier, remove the global fence and notify DML nodes that depended on it.

This makes the barrier a flush-order fence, not a DML conflict rule change. DML conflict-key detection remains unchanged for DML-to-DML ordering; the barrier only creates a temporary global ordering point around a block event.

The `Slots` helper needed by the barrier should be explicit and small:

```go
func (s *Slots) SnapshotTailNodes() []*Node
```

`SnapshotTailNodes` must lock slot mutexes in stable slot order, collect unique tail nodes from `slot.nodes`, and return them. The number of slots is fixed (`defaultConflictDetectorSlots`, currently `16 * 1024`), and barriers are rare, so this O(slot-count) scan is acceptable outside the DML hot path. The helper must not run on every DML.

### Barrier

The barrier should collect one acknowledgement per DML writer:

```go
type dmlBarrier struct {
    done chan struct{}
    // mutex-protected first error, remaining ack count, and ack bitmap
}
```

Required operations:

- `Ack(writerID int)` for successful writer flush up to the barrier.
- `Fail(err error)` for writer error, broadcast failure, sink close, or run-context cancellation.
- `Wait() error` for `FlushDMLBeforeBlock`.

`Ack` must be idempotent per writer. A small bitmap or `[]bool` sized by `workerCount` is sufficient because the worker count is fixed for the sink. Duplicate acknowledgements should be ignored after the first successful acknowledgement from the same writer.

`Fail` must unblock all waiters and preserve the first error. Completion must be idempotent to tolerate races between normal acknowledgements, writer failures, and sink close. If `workerCount == 0` is ever observed, sink construction should fail earlier because the existing MySQL sink requires at least one DML writer.

`FlushDMLBeforeBlock` should register the barrier in the sink's outstanding-barrier set before calling `BroadcastBarrier`, unregister it after `Wait` returns, and fail it immediately if broadcasting returns an error.

## MySQL Sink Writer Loop

`runDMLWriter` currently batches only `*commonEvent.DMLEvent` values. After introducing `writerItem`, it must scan each returned batch in queue order and flush only contiguous DML ranges before acknowledging a barrier.

Conceptual loop:

```text
itemBuffer := make([]writerItem, 0, maxTxnRows)
dmlBuffer  := make([]*commonEvent.DMLEvent, 0, maxTxnRows)

for {
  items, ok := inputCh.GetMultipleNoGroup(itemBuffer)
  if !ok:
    fail outstanding barriers and return

  for each item in items in order:
    if item is DML:
      append item.dml to dmlBuffer
      if dmlBuffer row count reaches maxTxnRows:
        flush dmlBuffer
        clear dmlBuffer

    if item is barrier:
      flush dmlBuffer
      clear dmlBuffer
      item.barrier.Ack(writerID)

  flush dmlBuffer
  clear dmlBuffer
  clear itemBuffer
}
```

The writer must acknowledge a barrier only after `Writer.Flush` returns nil for all DMLs before that token. If `Writer.Flush` returns an error while flushing DMLs before a barrier, the writer must call `barrier.Fail(err)` before returning the writer error. In practice, the sink should also call `failOutstandingBarriers(err)` on any writer error, because the writer might fail before it has dequeued a waiting barrier token.

Metrics should keep the same meaning as today:

- `WorkerEventRowCount` observes only DML events.
- `WorkerHandledRows` counts only flushed DML rows.
- `WorkerFlushDuration`, `WorkerBatchFlushDuration`, and `WorkerTotalDuration` measure DML flush work; barrier-only batches should not inflate handled-row metrics.

The normal DML path should avoid new per-DML heap allocations. Reuse `itemBuffer` and `dmlBuffer` for the lifetime of the writer goroutine. Do not log per barrier on the normal success path; use tests and metrics rather than high-cardinality logs.

## Ordering Guarantees

The design relies on these invariants:

1. Dispatcher calls `FlushDMLBeforeBlock` after enqueueing earlier DMLs and before writing or reporting the block event.
2. `BroadcastBarrier` defines a snapshot after all completed `ConflictDetector.Add` calls that happened before the barrier.
3. The conflict-detector barrier fence depends on every in-flight DML frontier node visible at the snapshot. Because DML slot nodes are removed only after `PostFlush`, the fence cannot resolve before those DMLs reach downstream.
4. DMLs admitted after the snapshot depend on the active barrier fence, so they cannot be assigned to writer queues ahead of the barrier token.
5. Once the fence resolves, `BroadcastBarrier` appends one barrier token to every writer queue.
6. Each DML writer processes its queue in order.
7. A writer acknowledges the barrier only after all DMLs before that barrier token have reached downstream successfully.
8. `FlushDMLBeforeBlock` returns only after every writer has acknowledged the barrier, so the subsequent block event cannot overtake earlier DMLs.

This is stronger than waiting for `PostEnqueue` and equivalent to waiting for `PostFlush` for all DMLs before the block event, but without registering a block-event pending callback on every DML.

## Error and Close Semantics

The barrier must never wait forever.

- If `BroadcastBarrier` cannot install the fence or cannot append all tokens because the detector is closed, `FlushDMLBeforeBlock` must fail the barrier and return an error immediately.
- If any DML writer fails, the sink must fail all outstanding barriers with the writer error before returning from `runDMLWriter`.
- If a writer fails while flushing DMLs immediately before a barrier token, it should call `barrier.Fail(err)` directly and then return the same error.
- If the sink closes while a barrier is waiting, `Close` must fail all outstanding barriers with `context.Canceled`, close the conflict detector notification path, close writers, and close the DB as it does today.
- If the `Run` context is canceled, `runDMLWriter` should fail outstanding barriers with `ctx.Err()` before returning.
- Barrier completion must be idempotent to tolerate races between writer failure, sink close, broadcast failure, and normal acknowledgements.

Use repository error conventions for newly generated errors. Existing writer errors are already wrapped by the MySQL writer path and can be propagated. For new barrier-specific internal errors, use an existing suitable predefined error such as `errors.ErrMySQLTxnError.GenWithStackByArgs(...)` or add a narrowly named predefined error if the implementation needs a distinct RFC code. Do not introduce bare `fmt.Errorf` or unwrapped third-party errors in new code.

## Cache Full Behavior

The current MySQL sink creates causality caches with `BlockStrategyWaitEmpty` (`downstreamadapter/sink/mysql/sink.go:175-180`). With this strategy, a normal DML insertion can return false when a cache is blocked until empty.

Barrier tokens should use a separate `forceAdd` path after the barrier fence resolves:

- Normal DMLs keep using `add`, so existing cache pressure behavior is unchanged.
- Barrier tokens use `forceAdd`, because they are control markers and must be able to enter every queue to let waiting block events finish.
- `forceAdd` must still fail if the underlying channel is closed, so `FlushDMLBeforeBlock` can return an error instead of hanging.
- The channel or cache API must expose accepted-or-closed status for this path; the current `UnlimitedChannel.Push` return type is insufficient for this requirement.

Allowing a small number of barrier tokens to bypass the cache-size policy is safe because the number of tokens is bounded by `workerCount * activeBarrierCount`, and block events are rare compared with DML events. This is simpler and safer than retrying through the existing notification loop, which can deadlock if no new DML notification is produced after the cache becomes empty.

## Multiple and Concurrent Barriers

Multiple block events can call `FlushDMLBeforeBlock` concurrently through the block-event executor paths. The implementation should support independent barriers rather than using a single global waiter.

Rules:

- Each call creates a distinct `dmlBarrier` with its own acknowledgement state and error.
- `BroadcastBarrier` serializes barrier fence installation so barriers preserve call order.
- If barrier B is installed while barrier A is active, B should depend on A's fence. This keeps token order stable in every writer queue.
- Failing the sink or detector fails all outstanding barriers.
- Successfully completing one barrier must not close or mutate another barrier's state.

This keeps the semantics equivalent to separate snapshots while avoiding a shared condition variable that can miss notifications.

## Performance Considerations

The optimization target is the steady-state DML hot path when no block-event barrier is active.

Expected hot-path properties:

- No per-DML block-barrier `PostFlush` callback is registered in `Sink.AddDMLEvent`.
- No per-DML barrier object is allocated.
- `writerItem` is a small value type stored directly in the existing unlimited channel.
- DML writer buffers are allocated once per writer goroutine and reused.
- The O(slot-count) `SnapshotTailNodes` scan runs only when `FlushDMLBeforeBlock` is called.

The implementation should include a microbenchmark or reuse existing causality benchmarks to compare DML-only throughput before and after the queue item type change. The acceptable result is no material regression in normal DML throughput and allocation count. If the value-struct queue introduces measurable allocation growth, the implementation should first optimize buffer reuse and escape behavior rather than adding broader abstractions.

## Advantages

- No per-DML `PostFlush` callback is needed for block-event ordering.
- Runtime overhead is paid only when `FlushDMLBeforeBlock` is called, except for the small queue item shape change.
- The barrier is aligned with existing writer queues, so it naturally waits for each writer's prior DMLs.
- The conflict-detector fence closes the correctness gap for DMLs that are admitted but not yet assigned to a writer queue.
- The design handles DMLs distributed across all workers, including conflict-free round-robin DMLs.

## Risks

- Generalizing the queue item type changes the causality cache and writer loop internals. The change should stay inside `downstreamadapter/sink/mysql` and `downstreamadapter/sink/mysql/causality`.
- Barrier latency is bounded by the slowest DML writer and by earlier unresolved conflict chains.
- Incorrect handling of mixed DML/barrier batches can break ordering.
- Queue-close, writer-error, and sink-close races must be tested to avoid deadlocks.
- If the barrier fence does not cover DMLs already admitted to the conflict detector, block events can still overtake unresolved DMLs.
- If post-barrier DMLs are not ordered behind an active barrier fence, they can enter writer queues before barrier tokens and create unnecessary latency or incorrect ordering.

## Test Plan

Add focused tests for:

1. A DML below syncpoint primary ts is delayed in a MySQL DML writer; syncpoint waits until the barrier ack.
2. DMLs routed to multiple DML writers all flush before the barrier completes.
3. A DML writer error before barrier ack makes `FlushDMLBeforeBlock` return an error instead of hanging.
4. Sink close while a barrier is waiting unblocks the wait with an error.
5. Barrier ordering with a mixed queue: DML, barrier, DML. The first DML must flush before ack; the second DML must not be required for that barrier.
6. Cache-full or blocked-cache behavior does not deadlock `BroadcastBarrier`.
7. A DML admitted to `ConflictDetector.Add` before `BroadcastBarrier`, but not yet assigned to any writer queue because it conflicts with an earlier DML, is flushed before the barrier completes.
8. A DML admitted after `BroadcastBarrier` does not overtake the active barrier token in any writer queue.
9. Two concurrent `FlushDMLBeforeBlock` calls complete independently and preserve barrier token order in every writer.
10. Closing the conflict detector or writer queue while broadcasting returns an error instead of leaving `Wait` blocked.

The existing tests `TestMysqlSinkFlushDMLBeforeBlockReturnsOnDMLError` and `TestMysqlSinkFlushDMLBeforeBlockWaitsForDMLPostFlush` in `downstreamadapter/sink/mysql/sink_test.go` should be adapted to the barrier implementation rather than removed. The failpoint regression around delayed DML `PostFlush` remains valuable because it verifies that the barrier waits for downstream flush completion, not only SQL execution or queue dequeue.

## Migration Plan

1. Introduce `writerItem`, update `txnCache` channel types, and keep normal DML insertion behavior unchanged.
2. Add `dmlBarrier` and sink-level outstanding-barrier tracking with idempotent ack/fail semantics.
3. Add `Slots.SnapshotTailNodes` and the conflict-detector barrier fence used by `BroadcastBarrier`.
4. Add `ConflictDetector.BroadcastBarrier` without changing DML-to-DML conflict-key rules.
5. Update `runDMLWriter` to process DML and barrier items in queue order while reusing buffers.
6. Replace per-DML pending callback tracking in `Sink.AddDMLEvent` and `FlushDMLBeforeBlock` with broadcast barrier waiting.
7. Keep the failpoint regression from issue #5353 and add error, close, cache-full, unresolved-conflict, and concurrent-barrier tests.
8. Run focused MySQL sink and causality tests, then run a DML-only benchmark or allocation check to confirm the normal hot path does not regress materially.
