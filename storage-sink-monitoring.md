# Storage Sink Monitoring & Performance Notes (cloudstorage / file://)

Status: Draft  
Date: 2026-01-20  
Scope: `downstreamadapter/sink/cloudstorage`（本文中的 “storage sink” 指 cloudstorage sink，包含 `file://` 本地文件系统与对象存储）

本文聚焦两件事：

1. 基于当前实现，梳理 storage sink 可能的性能瓶颈与优化方向（不写代码，只给建议）。
2. 给出“该加什么监控/该删什么监控”的具体建议，帮助排障与回归验证。

> 背景与根因分析见：`storage-sink-slow.md`。吞吐/稳定性修复方案以 `docs/plans/2026-02-04-cloudstorage-sink-ga-design.md` 为准。

---

## 1. 快速上下文：链路与关键语义

核心链路（简化）：

```
TiDB -> EventService -> EventCollector(dynstream) -> Dispatcher -> CloudStorageSink
     -> dmlWriters.msgCh -> encodingGroup -> defragmenter -> writers -> ExternalStorage
```

几个对性能/监控很关键的语义点：

- dynstream 的 `await/wake` 决定了每个 path（dispatcher）是否继续被投喂：`utils/dynstream/interfaces.go`。
- 现状下 dispatcher 遇到 DML 会 `await=true`，并用 `tableProgress.Empty()` + `PostFlush` 回调作为 wake 条件：`downstreamadapter/dispatcher/basic_dispatcher.go`、`downstreamadapter/dispatcher/table_progress.go`。
- cloudstorage writer 的 flush 触发点是 interval tick 与 per-table fileSize：`downstreamadapter/sink/cloudstorage/writer.go`。

---

## 2. 潜在性能瓶颈点（按优先级）

### 2.1 “flush 作为放行边界”的节拍化（吞吐被 `flush-interval` 锁死）

严格来说这是 downstream adapter 与 storage sink 的组合瓶颈，但它决定了 storage sink 的端到端吞吐上限：

- dispatcher 对 DML 默认 `await=true`：`downstreamadapter/dispatcher/basic_dispatcher.go:480`
- wake 依赖 flush 完成：`tableProgress.Empty()` + `PostFlush`（由 sink 在写出后触发）
- cloudstorage 默认 `flush-interval=5s`、`file-size=64MiB`：`pkg/sink/cloudstorage/config.go`

典型现象：

- 写出 bytes/s 很低，但 cloudstorage “flush duration” 看上去很小；
- dynstream 的 batch duration（或 popEvents 扫描成本）显著升高；
- dynstream memory quota 长期高水位（backlog 留在 dynstream）。

### 2.2 defragmenter 的编码后重排缓存（未来 map 的最坏内存与 GC）

当前 defragmenter 的关键风险点：

- `defragmenter.future map[uint64]eventFragment` 无上界：`downstreamadapter/sink/cloudstorage/defragmenter.go`
- `eventFragment` 持有 `encodedMsgs []*common.Message`（包含大 byte slice）
- 一旦 “next seq 对应的编码较慢”，后续已编码的 fragments 会被持续塞进 `future`，造成：
  - 内存瞬时膨胀（与编码输出速度相关）
  - GC 压力显著上升
  - 极端情况下触发 OOM

这类问题通常会表现为：写出并不慢，但内存与 GC 先崩。

### 2.3 无界队列的热点与 goroutine 搬运成本（msgCh / writerInputChs）

当前 cloudstorage sink 里至少有两处“默认无界”的队列结构：

- encoding 输入：`dmlWriters.msgCh` 使用 `UnlimitedChannel`（mutex/cond + deque）：`utils/chann/unlimited_chann.go`
- writer 输入：`chann.NewAutoDrainChann` 默认 unbounded（额外 goroutine + 内部队列）：`utils/chann/chann.go`

这类结构的典型风险/开销：

- 在高 QPS 下 mutex/cond 会成为热点（msgCh 竞争）；
- unboundedProcessing goroutine 的调度与搬运带来额外 CPU；
- 一旦上游允许持续投喂（例如 early-wake 后 wake 很频繁），无界队列会把压力“搬进进程内存”，从 dynstream 的 quota 体系中逃逸（需要额外约束，见 `docs/plans/2026-02-04-cloudstorage-sink-ga-design.md` 的 watermarks/spool 设计）。

### 2.4 writer 的 flush 路径：每次写 data + 重写 index（对象存储 API 放大）

writer 每次 flush 的基本行为：

- 写 data file（可能走 `WriteFile`，也可能走 multipart）：`downstreamadapter/sink/cloudstorage/writer.go: writeDataFile`
- 写 index file（每次覆盖写最后一个 data file 的 basename）：`downstreamadapter/sink/cloudstorage/writer.go: writeIndexFile`

潜在瓶颈：

- 大量小文件场景下，index 的写放大与对象存储请求数会快速成为瓶颈；
- `EnableTableAcrossNodes=false` 时 index 文件可能被多并发写（需严格路由/串行保证，见 `storage-sink-encoder-group.md` 的正确性约束）。

### 2.5 schema 检查/写入：`FileExists` + `WalkDir` 的对象存储列举成本

`CheckOrWriteSchema` 在首次看到一个 table/version 时会：

- `FileExists` 点查 schema 文件；
- miss 后会 `WalkDir` table meta 子目录找最新 schema：`pkg/sink/cloudstorage/path.go: CheckOrWriteSchema`

这在对象存储上可能非常昂贵（尤其是大量表第一次被触达时）。虽然有 `versionMap` 缓存，但“表多且 churn 大”的场景依然会把成本摊平到长期。

### 2.6 “活跃表 fanout”导致一次 flush 需要遍历巨大 map（规模风险）

writer 的 `batchedTask.batch` 是按 `VersionedTableName` 聚合的 map：`downstreamadapter/sink/cloudstorage/writer.go: batchedTask`

- active tables 越多，batchedTask map 越大；
- interval tick flush 会遍历整个 map 并对每个表写文件。

当 dispatcher 数量极大且活跃集合大时（例如 10^6 active），这本质上是架构不可行：即使内存扛住，对象存储 API 与文件数也会先把系统压垮。

### 2.7 写 data file 的内存拷贝与组包（bytes.Buffer -> []byte）

`writeDataFile` 通过 bytes.Buffer 拼接 `msg.Key` 与 `msg.Value`，再一次性写出：

- 对大事务/大批量数据，会产生明显的内存峰值；
- 对 multipart 路径仍需要先把 data 组织成连续 buffer（目前实现如此）。

这类瓶颈通常表现为：CPU/内存高，I/O 并未跑满。

---

## 3. 优化建议（不写代码的设计建议）

### 3.1 必做（高收益，优先解决“最坏内存/吞吐节拍化”）

1. **修复吞吐节拍化：two-stage ack（early-wake）+ watermarks 背压**
   - 以 `docs/plans/2026-02-04-cloudstorage-sink-ga-design.md` 的方案为准：wake 与 `PostFlush()`（checkpoint）解耦，sink 通过 spool 水位决定是否触发 wake。
   - 目标：让 storage sink 在一个 flush 窗口内持续有数据可写，并且在远端慢时通过水位把 backlog 留在 dynstream（避免 sink 内无界增长）。
2. **移除 defragmenter（或至少限制其 future map）**
   - 优先落地 `storage-sink-encoder-group.md` 的方案 A（全局有序 future 模型），移除“编码后缓存大对象”的 future map。
   - 这是 storage sink 里最明确的“最坏内存不可控点”之一。

### 3.2 应做（中高收益，更多是 CPU/对象存储成本）

1. **拆分 flush 指标维度**
   - 区分 data write 与 index write（当前共用 `CloudStorageFlushDurationHistogram`，容易误导排障）。
2. **减少 schema 检查的列举成本**
   - 确保缓存命中路径尽可能快（尽量避免重复 `WalkDir`）。
   - 评估是否能把 schema 写入前移到 DDL 路径或更稳定的触发点（避免在热 flush 路径做昂贵检查）。
3. **路由策略评估（仅在安全前提下）**
   - 当 `EnableTableAcrossNodes=true`（默认）时，index file path 包含 dispatcherID，理论上更适合按 dispatcherID 分摊到不同 writer，避免“同表不同 dispatcher 全落到一个 writer”的热点。
   - 这会改变并行度分布，需压测与正确性确认后再做。

### 3.3 可做（收益依赖 workload，偏工程细节）

1. **批量化从队列取事件**
   - `UnlimitedChannel` 支持 `GetMultiple*`；编码侧可以批量取出降低锁竞争与调度开销（需仔细评估编码器状态机是否允许）。
2. **减少组包内存峰值**
   - 评估在 multipart 路径上直接流式写入（避免把整批数据拼成一个大 []byte）。
3. **长期状态回收**
   - `FilePathGenerator.fileIndex/versionMap` 随“历史触达集合”增长的问题，在 total 极大且 churn 大时可能需要 TTL 回收（见 `docs/plans/2026-02-04-cloudstorage-sink-ga-design.md` 的“per-span 状态回收”设计）。

---

## 4. 监控建议：新增 / 调整 / 删除

### 4.1 建议新增（低基数，面向排障与回归验证）

以下建议均应避免按 table/dispatcher 打 label（高基数风险）。

1. **flush reason counters**
   - `cloud_storage_flush_tasks_total{reason="interval|size"}`：writer 侧统计 interval tick 与 size flush 触发比例（定位 tick-limited 的直观证据）。
2. **sink 内部 backlog gauges**
   - `cloud_storage_msg_queue_len`：`dmlWriters.msgCh.Len()`（观察是否持续堆积）。
   - `cloud_storage_defragmenter_future_len`：`len(defragmenter.future)`（若 defragmenter 仍存在）。
   - `cloud_storage_writer_input_len{worker_id="0..N-1"}`：`writerInputCh.Len()`（writerId label 低基数可接受）。
   - `cloud_storage_to_be_flushed_len{worker_id="0..N-1"}`：`len(writer.toBeFlushedCh)`（观察 flush 调度是否跟不上）。
3. **active tables / fanout**
   - `cloud_storage_active_tables{worker_id="0..N-1"}`：每次 tick flush 时 `len(batchedTask.batch)`（反映 fanout）。
4. **外部存储写入耗时拆分**
   - `cloud_storage_data_write_duration_seconds`（data file）
   - `cloud_storage_index_write_duration_seconds`（index file）
   - `cloud_storage_schema_check_duration_seconds`（`CheckOrWriteSchema`，含 `FileExists/WalkDir`）

> 其中 2/3 是定位 OOM/抖动与 backpressure 的关键；1/4 是定位“为什么吞吐低/为什么小文件多”的关键。

### 4.2 建议调整（避免误读或空指标）

1. **`CloudStorageWriteDurationHistogram` 当前可能未被使用**
   - 定位：`downstreamadapter/sink/metrics/cloudstorage.go`
   - 建议：要么补齐打点（覆盖 data write），要么从 dashboard/告警里移除，避免长期为 0 造成误判。
2. **`CloudStorageWorkerBusyRatio` 命名与类型不匹配**
   - 当前实现是 Counter 累加 busy seconds（`flushTimeSlice.Seconds()`），但名字叫 ratio。
   - 建议：改名为 `cloud_storage_worker_busy_seconds_total`，或改为 Gauge 真正输出 ratio（需定义窗口与采样）。
3. **`CloudStorageFlushDurationHistogram` 语义过宽**
   - 当前 data 与 index 写都打到同一个 histogram，会掩盖“data write 慢 vs index write 慢”的差异。

### 4.3 建议删除（或至少不做告警依据）

如果短期无法调整实现，建议在 dashboard 层面“降级处理”：

- 不以 `CloudStorageFlushDurationHistogram` 的单一曲线作为“写出是否健康”的判断依据；
- 不以 `CloudStorageWorkerBusyRatio`（当前语义）直接做比例告警。

---

## 5. 典型排障路径（用监控快速定位根因）

### 5.1 写出 bytes/s 低，但 flush duration 很小

优先检查：

- `flush_tasks_total{reason=interval}` 是否远大于 `{reason=size}`（tick-limited）
- dynstream 的 batch duration / memory quota 是否高水位（大量 path blocking）
- `cloud_storage_active_tables` 是否很小（单表或少量表吞吐被 tick 锁死）

### 5.2 内存持续上涨（怀疑 OOM）

优先检查：

- `cloud_storage_defragmenter_future_len` 是否持续增长（编码后重排缓存）
- `cloud_storage_msg_queue_len`、`cloud_storage_writer_input_len` 是否持续增长（sink 内部 backlog）
- 结合 spool 水位/背压相关指标（例如 `spool_bytes`、`wake_suppressed_total`、flush backlog）判断是“内存水位触顶导致背压”还是“外部存储变慢导致 flush ack 变慢”。

### 5.3 文件数激增 / 对象存储请求数异常高

优先检查：

- `flush_tasks_total{reason=interval}` 是否占比过高（小文件多）
- `file-size` 与实际 batch_bytes 的数量级是否匹配（配置不匹配导致 size flush 触发不了）
- `cloud_storage_active_tables` 是否很大（fanout 导致每次 tick 写很多小文件）

---

## 6. 关键代码位置（方便对齐）

- cloudstorage sink：`downstreamadapter/sink/cloudstorage/sink.go`
- DML pipeline：
  - 输入队列：`downstreamadapter/sink/cloudstorage/dml_writers.go`
  - 编码：`downstreamadapter/sink/cloudstorage/encoding_group.go`
  - 重排：`downstreamadapter/sink/cloudstorage/defragmenter.go`
  - 写入：`downstreamadapter/sink/cloudstorage/writer.go`
- schema/path：
  - `pkg/sink/cloudstorage/path.go: CheckOrWriteSchema`
- 监控定义：
  - cloudstorage sink metrics：`downstreamadapter/sink/metrics/cloudstorage.go`
  - sink statistics：`pkg/metrics/statistics.go`
