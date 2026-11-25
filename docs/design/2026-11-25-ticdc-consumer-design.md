# TiCDC Consumer Design

- Author(s): TiCDC Team
- Tracking Issue(s): [6996](https://github.com/pingcap/ticdc/issues/6996)

## Background

TiCDC Consumer replaces separate MQ and Cloud Storage consumer tools with a single unified pipeline for integration tests and large-scale data verification. It reads change events produced by TiCDC (from Kafka, Pulsar, or Cloud Storage) and replays them into downstream databases such as MySQL and TiDB.

Across sources, formats, and concurrency levels, the Consumer preserves two consistency invariants:

1. **In-order execution of row changes**: Changes affecting the same row execute in upstream order so the final downstream state matches the source.
2. **DDL and DML ordering constraint (DDL Barrier)**: Every DDL statement executes only after all preceding DML events for affected tables have persisted downstream, and subsequent DML events for those tables wait until the DDL completes.

---

## Design Details

### Architecture and Unified Source Model

The Consumer connects four stages in sequence: `source` -> `decoder group` -> `conductor` -> `writer`.

```text
  [Kafka / Pulsar / Cloud Storage]
                 |
                 v
           +-----------+
           |  source   |  Encapsulates source access; commits offsets; wakes Storage scans
           +-----------+
                 |  Normalized payloads
                 v
         +---------------+
         | decoder group |  Concurrent decoding; shared schema cache; early DDL dedup
         +---------------+
                 |  DML / DDL / Watermark
                 v
           +-----------+
           | conductor |  Aggregates W; enforces DDL Barrier & commit-ts <= W scheduling
           +-----------+
                 |  Ready DML / DDL
                 v
           +-----------+
           |  writer   |  Filters replays; drives DB sink persistence; advances C
           +-----------+
                 |
                 v
        [Downstream DB Sink]
```

- **`source`**: Hides data source details; validates sharding consistency when routing rules apply; triggers prompt Cloud Storage file refreshes during `DDL Barrier` waits; and commits consumption progress asynchronously.
- **`decoder group`**: Decodes normalized payloads concurrently without coupling to source partition layout while preserving per-sequence submission order.
- **`conductor`**: Aggregates the global Source Watermark $W$, enforces `DDL Barrier` and `commit-ts <= W` release gating, preserves per-row order under concurrent dispatch, coordinates schema cache GC, and drives `source` commits as $C$ advances.
- **`writer`**: Filters restart replays, delegates DML batching and asynchronous execution to the downstream DB sink, and advances the Persisted Checkpoint $C$.

All supported sources normalize into the same abstraction model, and every replayed event carries a valid `commit-ts` so downstream gating and deduplication operate identically across sources:

| Source        | Input Unit                  | Source Watermark ($W$)                     | Progress Commit             |
| ------------- | --------------------------- | ------------------------------------------ | --------------------------- |
| Kafka         | Message batch per partition | Minimum `ResolvedTs` across all partitions | Async consumer group offset |
| Pulsar        | Message batch per partition | Minimum `ResolvedTs` across all partitions | Async subscription ack      |
| Cloud Storage | Data or schema file payload | Persisted `metadata.checkpoint-ts`         | In-memory / scan progress   |

---

### Watermarks, Checkpointing, and Offset Confirmation

The pipeline tracks two globally monotonic non-decreasing watermarks:

- **Source Watermark ($W$)**: The timestamp boundary up to which upstream output is complete and no events with a smaller `commit-ts` will arrive.
- **Persisted Checkpoint ($C$)**: The timestamp boundary up to which every DML and DDL event with `commit-ts <= C` has persisted downstream and succeeded.

$C$ never outruns in-flight work. It advances only after asynchronous sink persistence callbacks complete, bounded above by:

$$C \le \min\bigl(W,\ \text{earliest pending event commit-ts in } \text{conductor}/\text{writer} - 1\bigr)$$

To confirm external progress safely under concurrent or out-of-order downstream completion, `conductor` records confirmable `source` positions per partition with a monotonically non-decreasing `through` timestamp (the running maximum observed on that partition). Whenever $C$ advances, `conductor` finds the prefix on each partition with `through <= C` and triggers asynchronous offset acknowledgement via `source`.

---

### Scheduling, Concurrency, and DDL Barrier

`conductor` governs release order across partitions and files using $W$:

- **Unified `commit-ts <= W` gate**: DML events become eligible for downstream dispatch only when `commit-ts <= W`, ensuring all partitions or files up to that timestamp have been observed before row changes execute.
- **Concurrent execution with row-level order**: Once `commit-ts <= W` holds, `conductor` does not serialize an entire table strictly by `commit-ts`. Instead, it dispatches independent rows and tables concurrently to persist data early, while preserving single-row execution order when related changes arrive across partitions or files.
- **DDL Barrier readiness**: A DDL statement waits until both conditions hold before execution:
  1. **Watermark readiness**: $W \ge \text{ddl.commit-ts}$, guaranteeing no slower partition or file still holds an earlier DML targeting the affected tables (standalone `CREATE DATABASE` and `CREATE TABLE` statements without source-table dependencies may bypass this wait).
  2. **Preceding DML persistence**: All previously dispatched DML events for affected tables finish persisting downstream.
  While a DDL executes, subsequent DML events on affected tables are held back; unrelated tables continue progressing concurrently.

---

### Schema Lifecycle and Fault-Tolerant Replay

- **Shared multi-version schema cache**: Decoders share a `commit-ts`-indexed `TableInfoProvider` so schema updates from DDL events are visible across all concurrent decoders. As $W$ advances, `conductor` notifies the cache to reclaim expired schema versions below $W$. Before querying historical table schemas, decoders skip stale replay events already covered by $W$.
- **Early broadcast DDL deduplication**: Because sources may broadcast identical DDLs across partitions, `decoder group` updates schema state and deduplicates redundant broadcast DDLs early at the decoder boundary before forwarding to `conductor`.
- **Fail-fast error handling and at-least-once recovery**: Unrecoverable decoding errors or downstream write failures abort the pipeline immediately without advancing $C$ or committing unpersisted offsets. On restart, `source` replays from the last committed offset.
- **Three-layer replay deduplication and sink batching**: `writer` passes ready DML events directly to the downstream DB sink for native batching and filters restart replays via three checks:
  1. Drops events with `commit-ts <= C` as already durable;
  2. Tracks written Handle keys in the `commit-ts > C` window to drop duplicates, reclaiming keys with `commit-ts <= C` once $C$ advances;
  3. Retains an in-memory `(commit-ts, schema, table, query)` set for executed DDLs so non-idempotent DDLs never re-execute across replay windows.

---

### Benefits

- **Source-agnostic correctness**: Unifying Kafka, Pulsar, and Cloud Storage behind one `commit-ts <= W` gate, `DDL Barrier`, and $C$-driven acknowledgement model eliminates per-source consistency gaps.
- **High throughput without sacrificing safety**: Concurrent decoding and cross-table/cross-row dispatch avoid single-partition or whole-table bottlenecks while keeping per-row order and `DDL Barrier` invariants intact.
- **Safe restartability**: Bounding $C$ by downstream persistence callbacks and pairing offset commits with `writer` deduplication provides clean at-least-once replay idempotency.
