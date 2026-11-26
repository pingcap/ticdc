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
- **`decoder group`**: Decodes normalized payloads concurrently without coupling to source partition layout while preserving per-sequence submission order. Schema-only metadata messages without a `commit-ts` (such as Simple protocol `Bootstrap`) are digested inside `decoder group` to populate the shared schema cache and wake pending DMLs, without forwarding downstream events or entering `DDL Barrier`, while their source offsets still advance normally.
- **`conductor`**: Aggregates the global Source Watermark $W$ and effective watermark $W_{\text{eff}}$, enforces `DDL Barrier` and `commit-ts` <= $W_{\text{eff}}$ release gating, preserves per-row order under concurrent dispatch, coordinates schema cache GC, and drives `source` commits as $C$ advances.
- **`writer`**: Filters restart replays, delegates DML batching and asynchronous execution to the downstream DB sink, and advances the Persisted Checkpoint $C$.

All supported sources normalize into the same abstraction model, and every downstream event forwarded to `conductor` (`DML`, `DDL`, and `Watermark`) carries a valid `commit-ts` so downstream gating and deduplication operate identically across sources:

| Source        | Input Unit                  | Source Watermark ($W$)                     | Progress Commit             |
| ------------- | --------------------------- | ------------------------------------------ | --------------------------- |
| Kafka         | Message batch per partition | Minimum `ResolvedTs` across all partitions | Async consumer group offset |
| Pulsar        | Message batch per partition | Minimum `ResolvedTs` across all partitions | Async subscription ack      |
| Cloud Storage | Data or schema file payload | Persisted `metadata.checkpoint-ts`         | In-memory / scan progress   |

---

### Watermarks, Checkpointing, and Offset Confirmation

The pipeline tracks the global watermarks below (each monotonically non-decreasing):

- **Source Watermark ($W$)**: The timestamp boundary up to which upstream output is complete and no events with a smaller `commit-ts` will arrive.
- **Effective Watermark ($W_{\text{eff}}$)**: The scheduling, barrier, and schema-retention boundary adjusted for DMLs whose schema has not arrived yet (for example, Simple protocol DMLs arriving ahead of their `Bootstrap` or cross-partition `DDL`). When a DML arrives without a matching schema, `decoder group` parks the raw DML in a shared waiting table and completes the submission slot immediately with a lightweight pending placeholder carrying the event's `commit-ts`, preventing head-of-line blocking so subsequent `DDL` and `Bootstrap` messages keep flowing. `conductor` computes:

$$W_{\text{eff}} = \min\bigl(W,\ \text{earliest pending decode commit-ts} - 1\bigr)$$

  When no DML is awaiting schema resolution, $W_{\text{eff}} = W$. Once the matching schema arrives on any partition, `decoder group` synchronously wakes and decodes the waiting DMLs using that schema and clears their placeholders, allowing $W_{\text{eff}}$ and $C$ to advance.
- **Persisted Checkpoint ($C$)**: The timestamp boundary up to which every DML and DDL event with `commit-ts <= C` has persisted downstream and succeeded.

$C$ never outruns in-flight work or unresolved decode placeholders. It advances only after asynchronous sink persistence callbacks complete, bounded above by:

$$C \le \min\bigl(W_{\text{eff}},\ \text{earliest pending event commit-ts in } \text{conductor}/\text{writer} - 1\bigr)$$

To confirm external progress safely under concurrent or out-of-order downstream completion, `conductor` records confirmable `source` positions per partition with a monotonically non-decreasing `through` timestamp (the running maximum observed on that partition). Whenever $C$ advances, `conductor` finds the prefix on each partition with `through <= C` and triggers asynchronous offset acknowledgement via `source`.

---

### Scheduling, Concurrency, and DDL Barrier

`conductor` governs release order across partitions and files using $W_{\text{eff}}$:

- **Unified `commit-ts` <= $W_{\text{eff}}$ gate**: DML events become eligible for downstream dispatch only when `commit-ts` <= $W_{\text{eff}}$, ensuring all partitions or files up to that timestamp have been observed and decoded before row changes execute.
- **Concurrent execution with row-level order**: Once `commit-ts` <= $W_{\text{eff}}$ holds, `conductor` does not serialize an entire table strictly by `commit-ts`. Instead, it dispatches independent rows and tables concurrently to persist data early, while preserving single-row execution order when related changes arrive across partitions or files.
- **DDL Barrier readiness**: A DDL statement waits until both conditions hold before execution:
  1. **Watermark readiness**: $W_{\text{eff}} \ge \text{ddl.commit-ts}$, guaranteeing no slower partition or file still holds an earlier or pending-decode DML targeting the affected tables (standalone `CREATE DATABASE` and `CREATE TABLE` statements without source-table dependencies may bypass this wait).
  2. **Preceding DML persistence**: All previously dispatched DML events for affected tables finish persisting downstream.
  While a DDL executes, subsequent DML events on affected tables are held back; unrelated tables continue progressing concurrently.

---

### Schema Lifecycle and Fault-Tolerant Replay

- **Shared multi-version schema cache**: Decoders share a versioned `TableInfoProvider` so schema updates from `DDL` and `Bootstrap` messages are visible across all concurrent decoders. As $W_{\text{eff}}$ advances, `conductor` notifies the cache to reclaim expired schema versions below $W_{\text{eff}}$. Before querying historical schemas or parking a DML in the waiting table, decoders skip stale replay events whose `commit-ts` is already covered by the reclaimed watermark, filtering directly on `commit-ts` without requiring table schemas.
- **Early broadcast DDL deduplication and ordering**: Because sources may broadcast identical DDLs across partitions, every DDL or `Bootstrap` message processed at the `decoder group` boundary follows a strict three-step order:
  1. Decode and update the shared schema cache;
  2. Synchronously wake and decode all pending DMLs waiting on that schema across any partition;
  3. Drop schema-only metadata (`Bootstrap`) and deduplicate broadcast DDLs by DDL identity so the first-arriving partition forwards the DDL to `conductor` while duplicate copies on other partitions are dropped (retaining the DDL's `commit-ts` as each partition's marker so offsets are not acknowledged before the DDL persists).
- **Fail-fast error handling and at-least-once recovery**: Unrecoverable decoding errors or downstream write failures abort the pipeline immediately without advancing $C$ or committing unpersisted offsets. On restart, `source` replays from the last committed offset.
- **Three-layer replay deduplication and sink batching**: `writer` passes ready DML events directly to the downstream DB sink for native batching and filters restart replays via three checks:
  1. Drops events with `commit-ts <= C` as already durable;
  2. Tracks written Handle keys in the `commit-ts > C` window to drop duplicates, reclaiming keys with `commit-ts <= C` once $C$ advances;
  3. Retains an in-memory `(commit-ts, schema, table, query)` set for executed DDLs so non-idempotent DDLs never re-execute across replay windows.

---

### Benefits

- **Source-agnostic correctness**: Unifying Kafka, Pulsar, and Cloud Storage behind one `commit-ts` <= $W_{\text{eff}}$ gate, `DDL Barrier`, and $C$-driven acknowledgement model eliminates per-source consistency gaps.
- **High throughput without sacrificing safety**: Concurrent decoding and cross-table/cross-row dispatch avoid single-partition or whole-table bottlenecks while keeping per-row order and `DDL Barrier` invariants intact.
- **Safe restartability**: Bounding $C$ by downstream persistence callbacks and pairing offset commits with `writer` deduplication provides clean at-least-once replay idempotency.
