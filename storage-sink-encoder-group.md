# Storage sink encoder group 设计：移除 defragmenter 的两种选择（全局有序 vs 按表有序）

Status: Proposed
Date: 2026-01-20
Scope: `downstreamadapter/sink/cloudstorage`（本文中的 “storage sink” 指 cloudstorage/local filesystem sink）

---

## 1. 背景与动机

目前 cloudstorage sink 的 DML 数据链路是：

```
DMLEvent -> encodingGroup(并发) -> encodedOutCh(乱序) -> defragmenter(按 seq 重排) -> writers(按 table 并行写) -> ExternalStorage
```

其中：

- `encodingGroup` 是一个并发 worker pool，多个 goroutine 从同一个 `UnlimitedChannel` 取活，谁先编码完谁先把结果写入 `outputCh`，因此输出天然是乱序的（`downstreamadapter/sink/cloudstorage/encoding_group.go`）。
- `defragmenter` 通过 `seqNumber` 将乱序的 `eventFragment` 重排为输入顺序，再按 table 哈希分发给 writer（`downstreamadapter/sink/cloudstorage/defragmenter.go`）。
- Kafka sink 的 `codec.EncoderGroup` 把 `future` 按 `AddEvents` 的调用顺序写入 `outputCh`，消费者按序 `Ready()` 等待并消费，编码并发但输出有序（`pkg/sink/codec/encoder_group.go`、`downstreamadapter/sink/kafka/sink.go: sendMessages`）。

目标：

- 让 storage sink 的“编码后输出”也具备可控顺序，从而移除 `defragmenter`。
- 同时评估：顺序约束要做到多强（全局 vs 按表）才能满足正确性，并在不引入新风险的前提下简化链路。

---

## 2. 现状（As-is）与关键代码位置

### 2.1 As-is 数据流

```
dmlWriters.AddDMLEvent
  -> msgCh(UnlimitedChannel[eventFragment])
    -> encodingGroup(concurrency=8, 并发编码)
      -> encodedOutCh(chan eventFragment, 1024)  // 完成即输出，乱序
        -> defragmenter(按 seqNumber 重排)
          -> writerInputChs([]DrainableChann[eventFragment], 按 table 哈希分发)
            -> writer.genAndDispatchTask -> flushMessages -> ExternalStorage.WriteFile/Create
```

### 2.2 关键点

- `seqNumber` 的来源：`dmlWriters.AddDMLEvent` 用 `atomic.AddUint64(&lastSeqNum, 1)` 生成，全局单调递增（`downstreamadapter/sink/cloudstorage/dml_writers.go`）。
- 重排的对象：当前 defragmenter 缓存的是“已编码完”的 `eventFragment`，其中 `encodedMsgs` 持有大 byte slice（`downstreamadapter/sink/cloudstorage/defragmenter.go`）。
- Callback 绑定：`TxnEventEncoder` 把 `common.Message.Callback` 设为 `event.PostFlush`（例如 CSV：`pkg/sink/codec/csv/csv_encoder.go: AppendTxnEvent/Build`），writer 在落盘后调用 callback（`downstreamadapter/sink/cloudstorage/writer.go: writeDataFile`）。

---

## 3. 正确性约束（需要保持）

### 3.1 必须保持的约束

1. **同一 writer 负责同一 index file path（避免并发写 index）**  
   `pkg/sink/cloudstorage/path.go: GenerateIndexFilePath` 在 `EnableTableAcrossNodes=false` 时生成 `meta/CDC.index`（不带 dispatcherID）。如果同一张表被分配到多个 writer，会并发更新同一个 index file，存在严重风险。  
   现状通过 `hash(schema, table)` 固定路由到一个 writer（`downstreamadapter/sink/cloudstorage/defragmenter.go: dispatchFragToDMLWorker`）。
2. **同一 `VersionedTableName` 的消息顺序不乱**  
   writer 会按输入顺序把 `encodedMsgs` append 到 per-table task（`downstreamadapter/sink/cloudstorage/writer.go: handleSingleTableEvent`）；若同一表的 fragments 乱序到达，文件内容/文件序号语义将难以推理。
3. **Callback 最终会被调用（用于推进 tableProgress）**  
   callback 的严格全局顺序在现有实现里并不容易保证（多 writer 并发 + map 遍历无序），因此这里的核心是“不会漏回调、不会永久阻塞导致进度不动”。

### 3.2 “全局顺序”是否真的是硬约束？

现状 defragmenter 把 **编码完成后的输出** 强制恢复为 `seqNumber=1,2,3...` 的全局顺序再分发到 writer。  
但 writer 写出与 callback 触发的时机仍可能跨表交错（多个 writer 并行、flush tick、map iteration），因此“跨表的 callback 全局顺序”并非强保证。

这为两种设计提供了空间：

- A：继续维持“全局 seq 推进”的约束（最接近现状 defragmenter）。
- B：只保证 per-`VersionedTableName` 有序，跨表不再强制排队（更并行）。

---

## 4. 方案选择概览

| 选项 | 顺序约束 | 主要优点 | 主要风险 |
|---|---|---|---|
| A. 全局有序 | 以全局序列推进（跨表也排队） | 最接近现状行为；实现路径贴近 Kafka future 模型；背压点清晰 | Head-of-line（HO-L）不可避免；需要定义/维护全局序列 |
| B. 按表有序 | 仅保证同 `VersionedTableName` 有序 | 避免跨表 HO-L，更适合多表 skew；实现可更短（无需 future） | 背压设计更敏感；需要确认上层确实不依赖跨表顺序 |

---

## 5. 选项 A：全局有序（future + sequencer）

### 5.1 “全局顺序”的定义（A1 / A2）

由于 `sink.AddDMLEvent` 会被多个 dispatcher 并发调用（`downstreamadapter/dispatcher/basic_dispatcher.go: AddDMLEventsToSink`），需要先明确“全局顺序”到底按什么来排：

- **A1（更兼容现状）**：沿用现有 `globalSeq = atomic.AddUint64(...)` 的序列，要求下游按 `globalSeq=1,2,3...` 推进。
- **A2（更简单）**：以 sink 的“入队顺序”作为全局序列（并发情况下与 A1 的 interleaving 可能不同）。

如果你强调“行为尽量不变”，选 A1；如果你只需要一个稳定的总顺序而不关心它的来源，选 A2。

### 5.2 To-be 数据流

```
AddDMLEvent(concurrent)
  -> input queue (tag globalSeq if A1)
    -> sequencer(保证未来 pipeline 的全局顺序)
      -> orderedTxnEncoderGroup(并发编码，output 是 future 且全局有序)
        -> router(按序 Ready + routeToWriter)
          -> writers
```

### 5.3 组件：sequencer（把“重排”前移到编码之前）

动机：要移除 defragmenter，仍可能需要某种“顺序恢复”。与其在 **编码后** 缓存 `encodedMsgs`（大对象），不如在 **编码前** 只缓存 `eventFragment`（基本是指针 + 元数据）。

sequencer 的逻辑（A1）：

- 输入：乱序到达的 `task{seq, frag}`
- 状态：`nextSeq uint64` + `pending map[uint64]task`
- 行为：
  - 若 `seq == nextSeq`：放行，并持续放行 `pending[nextSeq+1]` 的连续段
  - 若 `seq > nextSeq`：缓存
  - 若 `seq < nextSeq`：属于重复/回退，直接报错或忽略（理论上不应发生）

> 这看起来像 defragmenter，但关键区别是：缓存对象从 “已编码 msgs” 变成 “未编码 frag”，大幅降低最坏内存。

对 A2：sequencer 可以直接退化为一个简单的单 goroutine dequeue（不做 map 重排）。

### 5.4 组件：`orderedTxnEncoderGroup`（Txn 版 Kafka future 模型）

参考原型：`pkg/sink/codec/encoder_group.go`，把 RowEventEncoder 换成 TxnEventEncoder。

接口建议：

```go
type txnEncoderGroup interface {
  Run(ctx context.Context) error
  Add(ctx context.Context, frag eventFragment) (*txnFuture, error)
  Output() <-chan *txnFuture
}
```

`txnFuture`：

- `frag eventFragment`（含 `encodedMsgs`）
- `done chan struct{}`
- `err error`
- `Ready(ctx) error`

并发模型（推荐对齐 Kafka：per-worker input + RR 分发）：

- `inputCh []chan *txnFuture`（每 worker 一个，容量如 128）
- `outputCh chan *txnFuture`（容量如 `128 * concurrency`）
- `Add` 的行为：
  1) 创建 future
  2) RR 选择 worker：`inputCh[idx] <- future`
  3) **立即** `outputCh <- future`（保证 output 有序，顺序由 Add 调用决定）
- worker loop：
  - `encoder.AppendTxnEvent(future.frag.event)`
  - `future.frag.encodedMsgs = encoder.Build()`
  - `future.err = err`
  - `close(future.done)`

错误传播建议：

- `Run` 内部用 `context.WithCancelCause`，一旦某 worker 出错就 `cancel(err)`，让 router 的 `Ready(ctx)` 能尽快退出。
- 即使出错，也优先 `close(done)`，避免 future 永久悬挂。

### 5.5 组件：router（替代 defragmenter 的 dispatchFragToDMLWorker）

router 是一个单 goroutine：

1. 按序读取 `encoderGroup.Output()`
2. 对每个 future：`Ready(ctx)`（按序等待）
3. 计算 writer：
   - 默认保持现状：`writerID = hash(schema, table) % writerCount`
4. `writerInputChs[writerID].In() <- future.frag`

这段逻辑等价于：

- Kafka sink 的 `sendMessages`（future 模型）
- + cloudstorage 的 `dispatchFragToDMLWorker`（路由到 writer）

### 5.6 背压与性能特征

- HO-L：如果 “全局序列的 next future” 编码很慢，会挡住后续所有 future 的分发（这与现有 defragmenter 的 next-seq 语义一致）。
- 资源：在途量由 `outputCh`/`inputCh` 容量控制；最大的“重排缓存”发生在 sequencer，但缓存对象是未编码 frag。

---

## 6. 选项 B：按表有序（keyed shards，放宽跨表顺序）

### 6.1 顺序定义

只保证同一 `VersionedTableName` 的 fragments 以 FIFO 顺序进入 writer；不同 `VersionedTableName` 之间允许乱序推进。

对 writer 来说，这通常是最核心的正确性约束：同表不乱序、index file 不并发写。

### 6.2 To-be 数据流

```
AddDMLEvent(concurrent)
  -> route to encoding shard by key(versionedTable)
    -> shard worker(串行编码，保证该 shard 内 key 的 FIFO)
      -> routeToWriter(schema/table)
        -> writers
```

### 6.3 组件：encoding shards（固定数量、每个 shard 串行）

核心思想：把“顺序”限制在 key 上，并让 key 的并行度由 shard 数量决定。

- shard 数 `N`：建议可配置（默认 8 或随 CPU 调整）
- shard 输入：建议每 shard 一个队列（可以先沿用 `UnlimitedChannel` 以避免中心路由阻塞）
- shard worker：单 goroutine + 一个 `TxnEventEncoder`

路由规则：

- `shardID = hash(VersionedTableName) % N`

只要同一 key 落在同一 shard，且 shard 串行处理，则同 key 顺序天然保持，不需要 future，也不需要任何 defragment map。

### 6.4 writer 路由约束（避免 index file 并发写）

选项 B 也必须遵守 “同一 index file path 不能被多个 writer 并发写”。

因此 writer 路由建议默认保持现状：

- `writerID = hash(schema, table) % writerCount`（与 `defragmenter.dispatchFragToDMLWorker` 一致）

可选优化（需要单独确认与灰度）：

- `EnableTableAcrossNodes=true` 时 index file 名包含 dispatcherID：`meta/CDC_<dispatcher>.index`，理论上可将不同 dispatcher 分散到不同 writer。
- `EnablePartitionSeparator=true` 且目录包含 physical table ID 时，可考虑用 physical table id 作为 hash 维度分摊写入热点。

这些优化会改变写入并行度分布与产物形态，不建议和“移除 defragmenter”强绑定。

### 6.5 背压设计（选项 B 的关键）

要避免一种反效果：如果存在一个“中心 router goroutine”，它在向某个满的 shard 发送时阻塞，会导致所有表都被拖慢，B 的并行优势消失。

因此推荐：

- **B1（推荐）**：`AddDMLEvent` 直接根据 key 选择 shard 并入队（背压粒度按 shard）
- **B2**：如果必须通过中心 router，则 shard 队列必须是非阻塞入队（例如无界队列），否则会退化成全局阻塞

### 6.6 性能特征

- 优势：跨表不再 HO-L，一个慢表不会挡住其他表推进。
- 代价：单个 key 的编码无法并发（因为要 FIFO）。如果你非常依赖“对同表 txn 做 look-ahead 并发编码”，B 可能降低编码侧的峰值并行；但 cloudstorage sink 往往瓶颈在 writer flush/IO，实际影响需用压测确认。

---

## 7. 性能对比与预期

共同点（A/B 都有）：

- 移除 defragmenter 的 “编码后重排 + 缓存大对象”。
- 代码路径更短，减少 map 操作与 goroutine 间搬运。

差异点：

- A：更像现状（全局 next-seq 放行），背压点清晰，但 HO-L 依旧存在。
- B：更可能改善多表 skew 场景下的尾延迟与并行度，但需要小心背压与路由策略。

结论建议：

- 如果你的首要目标是“简化并尽量不改语义”，优先 A（尤其 A1）。
- 如果你明确要优化多表并发/尾延迟，并且确认上层不依赖跨表顺序，考虑 B。

---

## 8. 测试策略（针对 A/B）

单测建议：

- A：并发编码（随机 sleep）下，`Output()` 的 future 顺序严格等于 sequencer 放行顺序；并验证出错时 future 不会永远卡住。
- B：构造多个 key，验证每个 key 的输出保持 FIFO，且不同 key 可以交错推进。

集成/行为验证：

- 复用 `downstreamadapter/sink/cloudstorage/*_test.go`，重点断言：
  - callback 最终会被调用（计数正确）
  - 文件序号与 index file 内容符合预期（尤其是重复启动/继续写的场景）

---

## 9. 迁移与回滚

建议提供短期开关，便于灰度与回滚：

- `encoder-ordering=legacy|global|per-table`
  - `legacy`：现有 encodingGroup + defragmenter
  - `global`：选项 A（建议再细分 A1/A2 时写入日志与指标）
  - `per-table`：选项 B

稳定后删除 legacy 分支并移除 defragmenter 代码。

---

## 10. 需要你确认的点（用于最终定方案）

1. 你要的“顺序”是哪一种？
   - A：全局有序（最接近现状 defragmenter）
   - B：按表有序（跨表不排队）
2. 如果选 A，你希望全局序列来源是哪一种？
   - A1：沿用现有 `lastSeqNum`（更兼容）
   - A2：以 sink 入队顺序为准（更简单）

---

## 11. 参考代码

- Kafka 的有序 encoder group：`pkg/sink/codec/encoder_group.go`
- Kafka 消费 future 的方式：`downstreamadapter/sink/kafka/sink.go: sendMessages`
- CloudStorage 现有 encoding/defragment：`downstreamadapter/sink/cloudstorage/encoding_group.go`、`downstreamadapter/sink/cloudstorage/defragmenter.go`
- CloudStorage writer：`downstreamadapter/sink/cloudstorage/writer.go`
