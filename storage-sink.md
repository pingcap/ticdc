# Cloud Storage Sink（`downstreamadapter/sink/cloudstorage`）实现原理与性能分析

> 结论先行：当前 cloud storage sink 的总体结构是“先编码、再按全局序号纠偏、再按表分发到 writer、最后批量刷盘写外部存储”。它天然擅长“多表并行”，但对“单表极热（例如一次性删除 2 万行都落在同一张表/同一分区）”并不友好：吞吐很容易被**单 writer 串行刷盘 + 编码 CPU/GC + 对象存储小文件/小写入开销**限制。
>
> 另外需要特别关注：**编码协议不同，delete 的 CPU/内存成本差异很大**；批量 delete 是否被上游拆成“很多小事务”也会显著影响该模块的表现。

---

## 1. 模块职责与边界

### 1.1 这个 sink 做什么

`downstreamadapter/sink/cloudstorage` 是 downstream adapter 层的 cloud storage sink 实现，负责将 TiCDC 的事件（DML/DDL/checkpoint）写入外部存储（本地文件、S3、GCS、Azure Blob 等，统一抽象为 `storage.ExternalStorage`）。

它不做“下游表数据”的 apply，而是输出“变更日志文件”（data file + index file + schema file），供下游消费端读取并自行应用。

### 1.2 依赖模块

- 配置：`pkg/sink/cloudstorage/config.go`（flush-interval / file-size / worker-count / flush-concurrency 等）
- 路径与元数据：`pkg/sink/cloudstorage/path.go`（schema 文件、data/index 文件路径生成与恢复）
- 编码：通过 `pkg/sink/codec` 的 `TxnEventEncoder`
  - 当前这条链路使用的是 `codec.NewTxnEventEncoder`（见 `pkg/sink/codec/builder.go`），只支持：
    - `csv`：`pkg/sink/codec/csv`
    - `canal-json`：`pkg/sink/codec/canal`
  - 这点很重要：如果在 storage sink 上选择了其他协议，`Verify` 可能能过（它只校验 codec config），但运行时编码 goroutine 会在 `codec.NewTxnEventEncoder` 处报错退出。

---

## 2. 代码结构速览（按文件）

- `downstreamadapter/sink/cloudstorage/sink.go`
  - 入口：`New` / `Run` / `AddDMLEvent` / `WriteBlockEvent` / `AddCheckpointTs` / `Close`
  - DDL：写 schema 文件（`schema_{tableVersion}_{checksum}.json`）
  - checkpoint：周期性写 `metadata`（JSON：`{"checkpoint-ts":...}`）
  - cleanup：按 cron 清理过期文件、（本地 scheme）清理空目录
- `downstreamadapter/sink/cloudstorage/dml_writers.go`
  - DML 总控：`msgCh`（无限队列）→ `encodingGroup` → `defragmenter` → 多个 `writer`
  - `AddDMLEvent`：给每个 DML 事件分配全局递增 `seqNumber`，包装为 `eventFragment`
- `downstreamadapter/sink/cloudstorage/encoding_group.go`
  - 固定并发 `defaultEncodingConcurrency=8`
  - 每个 encoder goroutine 创建一个 `TxnEventEncoder`，对 `DMLEvent` 进行编码得到 `[]*common.Message`
- `downstreamadapter/sink/cloudstorage/defragmenter.go`
  - 单 goroutine 按 `seqNumber` 纠偏，保证输出给 writer 的 fragment 序号连续
  - 再按 hash(schema, table) 选择 writer（**只按 schema/table；不包含 physical table id / partition id / dispatcher id**）
- `downstreamadapter/sink/cloudstorage/writer.go`
  - 每个 writer 两个 goroutine：
    - `genAndDispatchTask`：将 fragment 聚合成 `batchedTask`（按表聚合），满足 flush 条件后丢给 `toBeFlushedCh`
    - `flushMessages`：真正写外部存储（schema check → data file → index file → callback）

---

## 3. 数据流与并发模型（核心）

### 3.1 数据流（DML）

用一句话描述：

`DMLEvent` →（附加 seq + table 信息）→ **并行编码** →（按 seq 纠偏）→ **按表哈希分发** → **按表聚合成文件** → 写外部存储（data + index）→ 执行 PostFlush callback

ASCII 图：

```
AddDMLEvent
   |
   v
Unlimited msgCh (eventFragment{seq, table, DMLEvent})
   |
   v
encodingGroup (N=8)  -- parallel -->  encodedOutCh (eventFragment{..., encodedMsgs})
   |
   v
defragmenter (single goroutine, enforce seq order)
   |
   +--> writerInputCh[hash(schema,table)%WorkerCount] (unbounded)
             |
             v
         writer:
           - genAndDispatchTask: aggregate per-table, flush by interval/size
           - flushMessages: write schema(if needed) + data + index, then callbacks
```

### 3.2 关键“顺序性”语义

- **编码阶段并行但不保证完成顺序**：不同 encoder goroutine 编码耗时不同，输出会乱序。
- `defragmenter` 通过 `seqNumber` 强制“严格按输入顺序”把 fragment 交给下游 writer。
  - 优点：callback（事务 flush 回调）天然按输入顺序触发，语义简单。
  - 代价：引入全局 head-of-line blocking（详见第 5 节）。

### 3.3 关键并发度来源

- 编码并发：固定 8（`defaultEncodingConcurrency`）
- 写入并发：
  - 多 writer 并行（默认 `worker-count=16`），但**同一张表**总是落到同一个 writer
  - 单 writer 内按“表”顺序写（同一批 flush task 中多表串行）
  - 单文件上传可通过 `flush-concurrency` 提高（`storage.Create(..., WriterOption{Concurrency:...})`）

---

## 4. 外部存储输出布局（schema / data / index）

这一块主要由 `pkg/sink/cloudstorage/path.go` 决定。

### 4.1 schema 文件

- 路径：`<schema>/<table>/meta/schema_{tableVersion}_{checksum}.json`
- 生成逻辑：
  - `writer.flushMessages` 在写第一份 data file 前调用 `FilePathGenerator.CheckOrWriteSchema`
  - 先 `FileExists` 点查；不存在则 `WalkDir` 限定在 `<schema>/<table>/meta/` 下找历史 schema
  - 如果存储里存在更高版本 schema（`version > table.TableInfoVersion`），返回 `hasNewerSchemaVersion=true`，writer 会丢弃该 task（并执行 callback），避免写“旧 schema 版本的 DML”

### 4.2 data/index 文件

目录结构（启用 date-separator / partition-separator 时会多几级）：

- data dir：`<schema>/<table>/<tableVersion>/[<partitionID>/][<date>/]`
- data file：
  - enable-table-across-nodes=true 时：`CDC_<dispatcherID>_<index>.<ext>`
  - 否则：`CDC<index>.<ext>`
- index file：
  - enable-table-across-nodes=true：`meta/CDC_<dispatcherID>.index`
  - 否则：`meta/CDC.index`
  - 内容就是“最后写入的数据文件名 + \n”

文件序号恢复：

- `FilePathGenerator` 第一次为某表生成 data file 时，会读 index file 拿到最后一个文件名，再根据文件名解析出 index，并检查“最后一个文件是否存在/是否为空”，以决定是否复用 index。
- 这在对象存储上会产生额外的 `FileExists` / `ReadFile` / `Open+Read(1 byte)` 开销（但只在冷启动首次触发）。

---

## 5. 性能瓶颈画像（可优化点）

下面按“CPU/内存/并发/外部存储 I/O”四类拆解。

### 5.1 编码 CPU/内存成本（协议相关）

cloud storage sink 的 DML 编码使用 `TxnEventEncoder`，所以“每个事务（DMLEvent）”会被编码成一条 message（内部包含多行）。

编码成本主要来自：

- 遍历 `DMLEvent` 的每个 row change（`GetNextRow`）
- 按列做类型转换与字符串化
- 组装（CSV 用 `strings.Builder`；canal-json 用 map + jwriter）

协议差异（与 delete 直接相关）：

- `csv`：`pkg/sink/codec/csv`
  - 对每行 delete，会走 `rowChangeColumns2CSVColumns(..., e.GetPreRows(), ...)`，按列调用 `fromColValToCsvVal`
  - 大字段（blob/varbinary）会 base64/hex 编码；字符串可能触发 quote/escape 扫描与替换
- `canal-json`：`pkg/sink/codec/canal`
  - `newJSONMessageForDML` 每行都会分配多个 map：`mysqlTypeMap/valueMap/javaTypeMap`，并进行多轮遍历
  - delete 若开启 `delete-only-output-handle-key-columns`，可以显著减少 per-row 的编码量（只输出 handle key 列）

可优化方向（代码级）：

- `canal-json`：把“列类型信息（mysqlType/sqlType/javaType）”缓存到 table 级别，避免每行构建 map；value 从 map 改为 slice（按列序）避免大量 map 分配。
- `csv`：减少每行中间对象分配（`[]any`、`csvMessage`、`strings.Builder`），可考虑复用 buffer/对象池；对 `fmt.Sprintf("%v")` 这类反射路径做更明确的类型分支。

### 5.2 全局 defragmenter 的 head-of-line blocking

`defragmenter` 是单 goroutine，并且按全局 `seqNumber` 强制 fragment 连续输出。

这会带来两个典型问题：

1. **只要有一个“慢编码”的 fragment 卡住**，后续所有 fragment（哪怕属于别的表）也会积压在 `future` map 里，直到缺口补齐才能继续下发。
2. 积压会导致内存涨、map 操作与 GC 压力变大，进一步拖慢系统。

可优化方向（架构级）：

- 把“顺序性”从全局降级到“按 dispatcher/按表”：
  - 例如：按 `(schema, table, physicalTableID, dispatcherID)` 分流到固定 encoder worker，天然保持该分片内顺序，减少/取消 defragmenter。
  - 或：每个 writer 自己维护序号纠偏，仅保证本 writer 内的顺序，不做全局 barrier。

### 5.3 writer 粒度：单表热点无法扩展

当前 writer 分片规则是 `hash(schema, table) % workerCount`（见 `defragmenter.dispatchFragToDMLWorker`），因此：

- 同一张表的所有事件总在一个 writer 上串行聚合、串行刷盘。
- 分区表（不同 physical table id）也仍然因为只 hash(schema,table) 而落到同一 writer（即便路径层面 partition 目录不同，也没利用起来）。

这决定了：

- “单表极热”场景吞吐上限 ≈ 单 writer 的编码+刷盘能力。
- 批量 delete 通常正是“单表极热”，所以很容易遇到瓶颈。

可优化方向：

- 对分区表把 `physicalTableID` 纳入 hash，使不同分区并行（前提是目录/文件名不冲突，当前已经按 partition dir 区分，具备可行性）。
- 更激进：单表内部再分片（例如按 row key hash），但这会改变下游消费端的读取/合并语义，需要设计。

### 5.4 外部存储 I/O：小文件/高频写 index 的放大效应

每次 flush：

1. 写 data file（可能是多 MB/多十 MB）
2. 再写一次 index file（很小，但也是一次完整外部存储写）

典型瓶颈：

- 对象存储对“小对象高频写”通常吞吐很差，延迟也高；index file 的更新会成为额外开销。
- `file-size` 过小 / `flush-interval` 过短会导致文件数爆炸，I/O overhead 被放大。
- 单表热点时只有一个 writer 在写，I/O 延迟就直接变成全链路瓶颈。

可优化方向：

- 配置层面优先：增大 `file-size`、适当增大 `flush-interval`（吞吐优先），并评估 `flush-concurrency`（单文件上传并发）。
- 设计层面：减少 index file 的写频率（例如批量更新/延迟更新/单独 meta 机制），或允许消费端通过 list+regex 推导最新文件（取决于存储一致性与目录规模）。

---

## 6. “批量删除 20000 行，delete 事件处理很慢”的可能原因

下面按“最可能→次可能”给出定位思路。这里的“慢”既可能是吞吐慢（持续跟不上），也可能是延迟大（delete commit 后很久才落盘）。

### 6.1 上游把 delete 拆成很多小事务（事务数 ≫ 行数）

这常见于应用层“循环按主键 delete”，会产生 20000 个事务，每个事务 1 行。

对本模块的影响：

- 编码/组装的固定开销按“事务数”放大（每个事务要走一遍 encoder Build，生成一条 message）。
- writer 侧 callback 数量也按事务数放大（每个 message 一个 callback），flush 后要执行大量函数回调。
- 如果启用了 CSV header（`csv.output-field-header`），CSV encoder 可能会为每个事务重复构建 header（虽然 writer 只会写一次到文件头，其他 header 都是白算的）。

建议先确认：

- TiCDC 看到的是 1 个 `DMLEvent(len=20000)`，还是 20000 个 `DMLEvent(len=1)`。
- 如果是后者，优先从上游 SQL 行为入手（批量 delete、分批 commit、减少事务数）。

### 6.2 单表热点导致并行度不足（天然只能 1 个 writer）

无论 delete 是一个大事务还是很多小事务，只要都落在同一张表：

- `defragmenter` 会把它们都发到同一个 writer；
- writer 的 `flushMessages` 对该表的写入是串行的；
- 外部存储延迟/带宽会直接决定整体速度。

这属于设计上的“单表上限”，可以通过：

- 提升单 writer 的写能力（`flush-concurrency`、更大的批次文件）
- 或改造分片策略（例如分区表按 physicalTableID hash）来改善。

### 6.3 delete 的编码/序列化开销特别高（行宽/大字段/协议实现）

即便 insert/update 也会序列化，但 delete 的特点是“输出 before image”：

- 如果表有大字段（blob/text/json/vec 等），delete 时会把这些字段一并输出（CSV/canal-json 默认行为），导致：
  - 编码 CPU 激增（base64/escape/json 序列化）
  - 输出字节数显著增大（写外部存储更慢）
  - 内存分配与 GC 压力上升（尤其 canal-json 每行多个 map 分配）

可行的缓解：

- 若使用 `canal-json`，评估开启 `delete-only-output-handle-key-columns`（只输出 handle key 列，极大降低 delete payload）。
  - 注意：这会改变下游消费语义（下游必须能靠 handle key 完成 delete apply）。
- 若使用 `csv`：当前配置校验明确禁止 `delete-only-output-handle-key-columns=true`（见 `pkg/config/sink.go`），因此只能通过：
  - 降低行宽/大字段输出（如果业务允许：列选择、协议切换等）
  - 或提升批量与 I/O（更大的 file-size、更高 flush-concurrency）

### 6.4 文件粒度太碎导致外部存储 API overhead 主导

如果 `file-size` 设置很小，或 delete 的 message 很小但 `flush-interval` 很短，会产生大量小文件：

- 每个 data file 一次写 + 每个 index file 再一次写（两次 API）
- 对象存储的吞吐会显著下降，延迟波动也更明显

表现为：

- `CloudStorageFileCount` 增长很快
- `CloudStorageFlushDurationHistogram`/`CloudStorageWriteDurationHistogram` 显著变大（尤其是小对象写）

### 6.5 defragmenter 的全局顺序 barrier 放大“偶发慢编码”

如果 delete 里夹杂少量“特别慢”的行（超长字符串/大 blob），编码阶段的耗时差异会导致：

- 乱序完成的 fragment 大量堆在 `defragmenter.future` map 中等待缺口补齐
- 造成内存上升与后续处理延迟扩大

在“单表热点 + 大事务”场景下，这种现象更明显。

---

## 7. 建议的排查指标与定位路径

### 7.1 先看：瓶颈在编码还是写存储

建议观察（Prometheus）：

- 写入侧：
  - `cloud_storage_write_bytes` / `cloud_storage_file_count`
  - `cloud_storage_flush_duration`（写 index 的耗时也会计入）
  - `cloud_storage_worker_busy_ratio`（writer 忙碌时间）
- 统计侧（全局 sink 统计）：
  - `ExecBatchWriteBytesHistogram`、`ExecBatchHistogram`（由 `Statistics.RecordBatchExecution` 记录）

判读方式（经验）：

- 如果 writer busy 很高、flush duration 很高：I/O 是主要瓶颈（考虑 file-size / flush-concurrency / 存储端吞吐）
- 如果 writer busy 不高但整体 lag 大：可能卡在编码/defragmenter（CPU/GC、全局 barrier）

### 7.2 再确认：事务形态（1 大事务 vs 多小事务）

这是解释 delete 慢的关键变量之一。

### 7.3 最后回到代码：看协议实现的 per-row 分配

- CSV：`pkg/sink/codec/csv/csv_message.go`
- Canal-JSON：`pkg/sink/codec/canal/canal_json_encoder.go` 的 `newJSONMessageForDML`

---

## 8. 可落地的优化清单（从“无需改代码”到“需要改代码”）

### 8.1 配置侧（优先尝试）

- 增大 `file-size`（减少文件数、减少 index 写次数）
- 适当增大 `flush-interval`（吞吐优先时更有效）
- 对对象存储设置更高 `flush-concurrency`（提升大文件上传吞吐；注意并发过高可能导致限流/失败率升高）
- 选择更轻的输出协议/配置：
  - 若可切 `canal-json`：评估 `delete-only-output-handle-key-columns`

### 8.2 架构/代码侧（需要研发改动）

- 分片改造：
  - 分区表把 `physicalTableID` 纳入 writer hash，提高并行度
  - 更进一步：同表按 row key hash 分片（需要重新定义消费端语义）
- 顺序性改造：
  - 去掉全局 seq 的 defragmenter barrier，改为按表/dispatcher 保序
- 编码实现优化（高收益但需小心兼容）：
  - canal-json 的 map 分配与 mysqlType/sqlType 计算缓存
  - csv 的对象复用与减少反射路径
- I/O 侧优化：
  - 降低 index file 写频率或改 meta 机制
  - 支持 streaming 写（避免一次性构造整块 buffer，减少内存峰值与 copy）

