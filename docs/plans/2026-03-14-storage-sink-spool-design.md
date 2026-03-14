# Storage Sink Spool and Quota Mechanism

Status: Proposed (Approved)
Date: 2026-03-14
Owner Team: TiCDC

## Background

Cloud storage sink writes encoded row-change messages to object storage in file batches.
Without a local spill layer, encoded messages stay in memory until the writer decides to
flush. When object storage becomes slow, the writer cannot drain those batches quickly,
and memory pressure accumulates inside the sink.

The prototype branch `enhance-storage-sink` introduces a local spool between encoding
and the final object-storage write. The prototype also introduces a quota-like mechanism
that decides when to keep data in memory, when to spill it to local files, and when to
delay wake callbacks.

This document explains:

- why spool exists
- what problem spool solves
- how spool works end to end
- what the quota mechanism currently does
- why the current quota mechanism is not yet a reusable generic component
- how to evolve it if reuse becomes a real requirement

## Problem Statement

The storage sink needs a local buffering layer with the following properties:

- Reduce long-lived Go heap usage when object storage is slower than encoding.
- Keep the existing cloud-storage file layout and downstream behavior unchanged.
- Preserve the writer's current flush contract: data is only acknowledged after the
  final data file and index file are written.
- Provide basic backpressure smoothing without turning spool into a durable store.

The immediate problem is not "build a universal quota framework". The immediate problem
is "add a local spill layer to storage sink and keep its behavior predictable".

## Goals

- Explain the storage sink spool design in a standalone way.
- Describe the current quota mechanism precisely, including its limitations.
- Make the module boundaries explicit so future refactoring can stay disciplined.
- Record the main operating-system-level effects of introducing local spill files.

## Non-Goals

- Do not redesign storage consumer.
- Do not redesign cloud-storage file paths or external file layout.
- Do not claim that spool is durable storage.
- Do not commit to extracting a generic quota component immediately.

## Current State

`master` does not contain spool yet. The analysis below refers to the prototype branch
`enhance-storage-sink`, mainly:

- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/dml_writers.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/writer.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/spool/manager.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/spool/quota.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/spool/codec.go`
- `enhance-storage-sink:pkg/sink/cloudstorage/config.go`

In that prototype:

- `dmlWriters` creates one `spool.Manager` per changefeed and shares it across all
  writer shards.
- `writer` no longer keeps raw encoded messages in its batch. It keeps `spool.Entry`
  handles instead.
- `spool-disk-quota` is added to cloud-storage sink config and defaults to `10 GiB`.
- New metrics expose current spool memory bytes, disk bytes, total bytes, and the
  number of suppressed wake callbacks.

## Why Spool Exists

Spool exists to decouple two parts of the pipeline that naturally run at different
speeds:

- encoding row changes into sink messages
- uploading final files to object storage

Without spool, the only buffer is the writer's in-memory batch. That means every extra
second of object-storage latency translates directly into more Go heap retention inside
the sink. Large bursts or long object-storage tail latency can then create:

- higher heap usage
- more GC pressure
- more memory retained per table batch
- less predictable wake and drain behavior

Spool changes that shape. It lets the sink keep a small hot layer in memory and move
older buffered payloads into local append-only segment files. The sink still flushes to
object storage in the same place, but it no longer needs to keep every pending payload
on the Go heap until that flush happens.

## What Problem Spool Solves

Spool primarily solves a resource-shaping problem, not a correctness problem.

It helps with:

- lowering long-lived heap retention in the writer path
- smoothing bursts when encoding is temporarily faster than object storage
- preserving the existing file-based flush boundary
- keeping callback execution aligned with successful final flush

It does not solve:

- durable local persistence across process crash
- end-to-end backpressure enforcement
- global quota enforcement across all sink resources

That boundary matters. Spool is a temporary staging layer. Entering spool is not an
ack point.

## Spool Data Flow

```text
WriteEvents
  -> dmlWriters.msgCh
  -> encoderGroup
  -> writer.enqueueTask
  -> spool.Enqueue
       -> keep in memory
       -> or spill to local segment file
  -> writer batch keeps Entry handles
  -> flush trigger
  -> spool.Load
  -> build final data file buffer
  -> write data file to object storage
  -> write index file to object storage
  -> run callbacks
  -> spool.Release
```

The key point is that spool sits between "encoded messages exist" and "final object
storage files are written". It does not change the final write boundary.

## Components and Boundaries

### `dmlWriters`

Responsibilities:

- create one shared spool manager per changefeed
- inject that manager into all writer shards
- own spool lifetime at the changefeed level

It should not own spill policy details.

### `writer`

Responsibilities:

- enqueue encoded messages into spool
- keep `spool.Entry` handles in per-table batches
- load entries during flush
- write the final data and index files
- run callbacks only after successful final write
- release or discard spool entries afterward

It should not own local segment-file management.

### `spool.Manager`

Responsibilities:

- account for entry bytes
- decide memory vs spill
- manage append-only segment files
- load spilled payloads back
- release segment references
- manage wake suppression state

It should not redefine downstream flush semantics.

## Detailed Spool Mechanics

### Entry creation

`Manager.Enqueue` first computes one `accountingBytes` value for the entire batch. The
prototype counts:

- 4 bytes for serialized message count
- 12 bytes of per-message header
- key payload bytes
- value payload bytes

It also computes `fileBytes`, which represents only the payload bytes that eventually
contribute to the final data file size.

Callbacks are detached from the original messages and stored on the resulting
`spool.Entry`. That lets the writer control exactly when callbacks run later.

### Memory path

If the batch still fits under the in-memory threshold, the entry keeps a direct
reference to the encoded messages.

This path avoids local file I/O and is the hot path for small or short-lived bursts.

### Spill path

If the batch would exceed the memory threshold, `spool.Manager` serializes the batch and
appends it to a local segment file. The entry then stores only a small location record:

- segment id
- file offset
- length

The actual encoded payload leaves the writer batch and lives in local spool storage.

### Flush path

During flush, the writer iterates over `spool.Entry` handles:

- `Load` returns either the in-memory messages or deserialized messages from a segment
  file
- the writer rebuilds the final output buffer
- the writer writes the final data file and index file to object storage
- callbacks run after the final write succeeds
- `Release` frees the entry's memory or segment reference

If the writer decides a task should be ignored, it calls `Discard`, which runs the
callbacks and then releases local resources.

## Quota Mechanism: Current Working Principle

The current prototype uses `spool-disk-quota` as a single total-capacity baseline.

From that single number it derives three thresholds:

- `memoryQuotaBytes = QuotaBytes * MemoryRatio`
- `highWatermarkBytes = QuotaBytes * HighWatermarkRatio`
- `lowWatermarkBytes = QuotaBytes * LowWatermarkRatio`

In the prototype defaults:

- `MemoryRatio = 0.2`
- `HighWatermarkRatio = 0.8`
- `LowWatermarkRatio = 0.6`

The controller then drives three behaviors:

1. Memory vs spill decision
   If `memoryBytes + entryBytes > memoryQuotaBytes`, the new entry spills to disk.

2. Usage accounting
   `reserve` increments either `memoryBytes` or `diskBytes`. `release` decrements the
   matching counter.

3. Wake suppression with hysteresis
   If `memoryBytes + diskBytes > highWatermarkBytes`, `onEnqueued` callbacks stop
   running immediately and are queued in `pendingWake`. They are resumed only when total
   usage drops to `lowWatermarkBytes` or below.

This is a soft-control mechanism. It does not reject writes. It does not block writers.
It does not guarantee a hard upper bound on local disk growth.

## Why the Current Quota Mechanism Is Not Yet a Generic Component

It is tempting to call the current code a generic quota controller, but that would be
misleading. The current implementation is still spool-specific for several reasons.

### It mixes policy, accounting, actuation, and metrics

The same object currently owns:

- threshold derivation
- byte accounting
- wake suppression state
- queued callbacks
- Prometheus metric handles

A generic component should not know about storage-sink callback queues or cloud-storage
metric names.

### It depends on spool's two-tier storage model

The API assumes exactly two resource classes:

- memory
- spilled local files

That fits spool well, but it is not yet a general resource-budget abstraction.

### It has no admission contract

The current controller cannot answer the most important generic quota question:

- when usage exceeds the budget, should the caller block, reject, wait, degrade, or
  shed work?

Today the answer is "none of the above". The controller just changes tiering behavior
and callback timing.

### It relies on outer locking

The controller does not manage its own concurrency. It assumes `spool.Manager` holds the
relevant mutex when calling `shouldSpill`, `reserve`, and `release`. That is another
sign it is not yet a standalone primitive.

## Recommended Direction If Reuse Becomes Necessary

The right move is not to extract the current `quotaController` as-is. The right move is
to split it into smaller layers and only extract the reusable part.

### Phase 1: Make the storage-sink contract honest

Before any extraction, the storage sink needs a clear answer to these questions:

- Is `spool-disk-quota` a hard quota or a soft watermark baseline?
- When the budget is exceeded, should storage sink spill, block, or return an error?
- Is the current config name still accurate?

Without that contract, any genericization will bake in the wrong behavior.

### Phase 2: Separate reusable core from spool-specific behavior

If reuse is still needed after Phase 1, split the mechanism into:

- `BudgetPolicy`
  - pure threshold math and validation
- `UsageTracker`
  - thread-safe accounting and watermark transitions
- `SignalGate`
  - delayed callback or wake handling
- `MetricsAdapter`
  - sink-specific metric export

In that shape, `BudgetPolicy` and `UsageTracker` are candidates for reuse. `SignalGate`
and most metrics wiring should probably stay local to the consumer.

### Phase 3: Extract only after a second real user exists

Do not extract a generic quota package based on one prototype alone. First wait until a
second subsystem has the same semantics. Otherwise the code will become abstract before
the contract is stable.

## Operating-System-Level Effects

Introducing spool changes where pressure lands in the system.

### Less Go heap pressure, more local I/O

Once entries spill, the writer no longer keeps all pending encoded payloads as long-lived
Go heap objects. That is good for GC pressure.

The cost is local filesystem activity:

- append-only writes into segment files
- `ReadAt` reads during flush
- file create / close / remove during segment rotation and cleanup

### More use of page cache

Spilled data leaves the Go heap, but it does not disappear from memory immediately.
Recently written file pages can still live in the kernel page cache. In practice this
means spool often trades "Go heap memory" for "OS-managed file cache + local disk I/O".

That is usually still a win because page cache is cheaper than retaining large message
graphs on the managed heap, but it is not free.

### Sequential write pattern is favorable

The segment-file design is append-only. That is friendly to local filesystems because the
write pattern is mostly sequential. This reduces fragmentation pressure and is much
better than many small random rewrites.

### File descriptor lifetime matters

Each live segment keeps an open file descriptor until it becomes unreferenced and is no
longer the active segment. Segment count therefore affects:

- open file descriptor usage
- cleanup latency
- how long old spilled data remains mapped to live local files

### No `fsync`, so spool is not durable

The prototype uses ordinary file writes and closes. It does not call `fsync`.
Operationally this means:

- data may still only exist in page cache for some time
- process crash or host crash can lose spilled-but-not-flushed data

That is acceptable only because spool is explicitly a temporary buffering layer, not a
durable commit point.

### Unix file-deletion semantics affect failure timing

If an external process deletes the spool root directory while the process is still
running, an already-open segment file can keep working until a new segment must be
opened. That is a normal Unix filesystem behavior: deleting a directory entry is not the
same thing as invalidating an already-open file descriptor.

This is why the current prototype only returns an error on the next segment rotation,
not immediately at the moment of external deletion.

## Performance Considerations

Main benefits:

- lower long-lived heap retention in the writer path
- lower GC pressure under slow object storage
- more predictable batching behavior under bursty load

Main costs:

- extra serialization when spilling
- extra local copy into segment files
- extra `ReadAt` and deserialization during flush
- possible double buffering through page cache and final object-storage write buffers

The expected tradeoff is good when object storage is the bottleneck and the alternative
is retaining large in-memory batches for too long.

## Testing Strategy

The prototype branch already covers the main pieces with unit tests:

- wake suppression and resume
- spill and read-back
- invalid config handling
- cleanup of spool-owned files only
- spill triggered by serialized size
- delayed error after external root-directory deletion

Next-step tests should focus on:

- the final contract of `spool-disk-quota`
- the desired failure contract when spool local paths are externally damaged
- end-to-end writer integration under mixed memory and spill paths

## Observability

The prototype exports:

- `cloud_storage_spool_memory_bytes`
- `cloud_storage_spool_disk_bytes`
- `cloud_storage_spool_total_bytes`
- `cloud_storage_wake_suppressed_total`

These are enough to see where buffered bytes live and whether the sink has entered wake
suppression. They are not yet enough to claim a complete runtime quota story.

## Rollout Plan

Recommended next iteration order:

1. Keep spool local to storage sink and finish its contract first.
2. Clarify whether `spool-disk-quota` is soft or hard.
3. Clarify failure semantics for external local-path damage.
4. Only then consider extracting `BudgetPolicy` and `UsageTracker` if another subsystem
   needs the same semantics.

## Alternatives Considered

### No spool, keep everything in memory

Rejected because it keeps the writer path too sensitive to object-storage latency and
heap growth.

### Build a generic quota framework first

Rejected because the contract is not stable yet. Extracting now would freeze the wrong
abstractions.

### Make quota hard from day one

This is a valid future direction, but not the current prototype behavior. It should be
decided explicitly rather than smuggled in through the existing soft-control code.

## Open Questions / Future Work

- Should `spool-disk-quota` become a real hard limit?
- Should external spool-path damage fail immediately or only when the path is next
  needed?
- Which parts of the current controller, if any, will actually have a second consumer?

## References

- Prototype branch: `enhance-storage-sink`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/dml_writers.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/writer.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/spool/manager.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/spool/quota.go`
- `enhance-storage-sink:downstreamadapter/sink/cloudstorage/spool/codec.go`
- `enhance-storage-sink:pkg/sink/cloudstorage/config.go`
