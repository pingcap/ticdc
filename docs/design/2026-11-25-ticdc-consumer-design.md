# TiCDC Consumer Design

- Author(s): TiCDC Team
- Tracking Issue(s): [6996](https://github.com/pingcap/ticdc/issues/6996)

## Background

TiCDC Consumer is used in integration tests and large-scale data verification to read change events from TiCDC upstream outputs (Kafka, Pulsar, and Cloud Storage) and replay them to downstream databases (such as MySQL and TiDB).

Upstream outputs vary across partitions, file layouts, encoding protocols, and `ResolvedTs` / `Watermark` progress tracking. The Consumer encapsulates these source and protocol differences at local boundaries and exposes a unified, concise pipeline to write events accurately and efficiently downstream while preserving two core consistency invariants:

1. **In-order execution of row changes**: Multiple changes to the same row execute strictly in order so the final downstream state matches the upstream source.
2. **DDL and DML ordering constraint (DDL Barrier)**: Before a DDL statement executes, all preceding DML events for affected tables must be physically persisted; subsequent DML events for affected tables must wait until the DDL completes.

---

## Design Details

### Architecture and Data Flow

The Consumer pipeline consists of four sequential components: `source` -> `decoder group` -> `conductor` -> `writer`.

```text
  [Kafka / Pulsar / Cloud Storage]
                 |
                 v
           +-----------+
           |  source   |  Hides data source details; async acks & Storage wakeup
           +-----------+
                 |  Normalized payloads
                 v
         +---------------+
         | decoder group |  Concurrent decoding; shared schema cache; early DDL dedup
         +---------------+
                 |  DML / DDL / Watermark
                 v
           +-----------+
           | conductor |  Global watermark W; DDL Barrier; DML gating & scheduling
           +-----------+
                 |  Ready DML / DDL
                 v
           +-----------+
           |  writer   |  Replay dedup (<= C / > C / DDL); drives Sink & advances C
           +-----------+
                 |
                 v
        [Downstream DB Sink]
```

#### Dual Watermarks

The pipeline maintains two globally monotonic non-decreasing watermarks:

- **Source Watermark ($W$)**: The timestamp boundary up to which upstream output is complete and no events with a smaller commit timestamp will arrive. For MQ sources (Kafka and Pulsar), $W$ is the minimum `ResolvedTs` across all partitions; for Cloud Storage, $W$ is the persisted `metadata.checkpoint-ts`.
- **Persisted Checkpoint ($C$)**: The timestamp boundary up to which all DML and DDL events with `commit-ts <= C` have been persisted to the downstream database and acknowledged. `source` uses $C$ to asynchronously commit consumption progress to external systems (such as Kafka Consumer Group offsets) so restarts resume without data loss.

---

### Component Responsibilities

#### 1. Source

- **Hiding data source details**: Encapsulates connection, polling, and directory scanning logic across Kafka, Pulsar, and Cloud Storage behind normalized output payloads.
- **Routing and partition validation**: Verifies that message routing targets match actual partitions when sharding rules are enabled.
- **File scan wakeup during DDL blocking**: For Cloud Storage, refreshes newly arrived snapshot or change files promptly while waiting on a `DDL Barrier` to prevent stalls.
- **Asynchronous checkpoint acknowledgement**: Receives $C$ advancement notifications from `conductor` and commits offsets asynchronously according to source semantics.

#### 2. Decoder Group

- **Concurrent decoding**: Decodes normalized payloads concurrently without coupling to underlying source partitions while preserving per-sequence event ordering, preventing decoding from becoming a throughput bottleneck.
- **Unified schema cache**: Shares a multi-version schema cache (`TableInfoProvider`) across decoders, indexed by `commit-ts`. Events carrying only key fields (such as Avro key-only `DELETE` messages) query `TableInfo` directly from the shared cache, eliminating decoder-private schema caches and divergence on DDL invalidation.
- **Early deduplication and stale replay skipping**: Deduplicates broadcast DDL events early at the decoder boundary by `(commit-ts, schema, table, query)`, and skips stale replay events already covered by the global watermark before querying historical schemas.

#### 3. Conductor

`conductor` acts as the central coordinator for watermark aggregation, concurrent scheduling, and execution ordering:

- **Unified gating (`CommitTs <= W`)**: Applies a uniform `commit-ts <= W` release gate across Kafka, Pulsar, and Cloud Storage before dispatching DML events downstream. In Cloud Storage mode, updates to `metadata.checkpoint-ts` also advance $W$ and notify `conductor` promptly even when no new data files are produced, preventing gated DML events from stalling.
- **DDL Barrier scheduling**:
  - Waits for all previously dispatched DML events on affected tables to persist before executing a DDL;
  - Blocks subsequent DML events on affected tables until the DDL finishes;
  - Allows unrelated tables to progress independently and executes standalone database/table creation DDLs without waiting on unrelated tables.
- **Concurrent dispatch and row ordering**: Once `commit-ts <= W` is satisfied, does not serialize an entire table strictly by `commit-ts`; instead dispatches independent rows and tables concurrently to persist data as early as possible while preserving per-row execution order across partitions or files.
- **Cache lifecycle coordination**: Notifies upstream decoder caches to release expired schema versions as $W$ advances, and drives `source` offset commits as $C$ advances.

#### 4. Writer

- **Focused responsibilities and sink batching**: Remains decoupled from encoding protocols and redundant `DDL Barrier` state machines, removing format-specific batching buffers and delegating DML batching directly to the downstream DB sink (such as `mysql.Sink`).
- **Replay deduplication**: Filters duplicate events produced during upstream restart replays using three precise checks:
  1. Drops events with `commit-ts <= C`, as they are already persisted downstream (replacing per-table maximum written `commit-ts` tracking, which risks dropping out-of-order rows);
  2. Tracks written Handle keys for events in the `commit-ts > C` window to filter duplicates, and reclaims records with `commit-ts <= C` as $C$ advances;
  3. Maintains a persistent `(commit-ts, schema, table, query)` set for DDLs to prevent non-idempotent DDL replays.
- **Persisted checkpoint advancement**: Advances $C$ based on real asynchronous persistence callbacks from the downstream sink and notifies `conductor`.

---

### Benefits

- **Unified behavior**: Kafka, Pulsar, and Cloud Storage share identical `CommitTs <= W` gating and `DDL Barrier` semantics in the core scheduler, eliminating source-specific behavioral drift and edge-case gaps.
- **Throughput with correctness**: Concurrent decoding scales independently of source partition layout while `conductor` centralizes per-row ordering and cross-table concurrency for high write throughput and data consistency.
- **Clear separation of concerns**: Decoding, ordering/scheduling, replay deduplication, and sink batching are cleanly isolated, simplifying independent optimization for large-scale replay workloads.
