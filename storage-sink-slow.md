# Storage sink 写出流量低下问题分析（cloudstorage / local filesystem）

本文记录一次压测场景下 storage sink（实现为 cloudstorage sink，写入本地文件系统）写出流量明显偏低的问题：先说明场景，再给出根本原因与解决方案。`flush-interval` 与 `flush-size(file-size)` 的调参仅作为 workaround（临时缓解/验证），不作为专门解决方案。最后附录给出数据链路与关键代码位置，便于快速对齐实现。

## 1. 场景

### 1.1 Workload

- 上游：TiDB
- 数据：10 张表；先执行 `prepare`，每张表写入 100 万条数据；再执行 `run`，`oltp_update_non_index` 持续约 10 小时
- 结果：上游产生大量变更；CDC 能收到每表约 100 万条 insert events + 大量 update events
- 备注：TiDB `information_schema.tables` 中的 “16 bytes/row” 是存储层面的估算，不等价于 CDC event 的大小或 storage sink 的输出字节数

### 1.2 CDC 配置

同时开启 4 个 changefeeds，分别使用：

- blackhole sink
- kafka sink
- mysql sink
- storage sink（本地文件系统，磁盘性能充裕）

### 1.3 主要现象与监控

你观测到：

- 只有 storage sink 写出流量低：`write bytes/s` 约 3.1MiB/s
- 监控显示 `flush duration` 约 1ms（看起来很快）
- event collector 内 dynstream 的 memory quota 几乎满，长期高水位
- dynstream 相关监控：
  - `batch count`：p99 ≈ 4050，avg ≈ 2400（BatchCount=4096）
  - `batch bytes`：p99 ≈ 4.16MiB，avg ≈ 2.2MiB
  - `batch duration`：p99 ≈ 9–10ms，avg ≈ 1–2ms
  - 其他 sink 类型在相同 workload 下，该 `batch duration` 长期为 us 级

## 2. 根本原因

### 2.1 关键点：dispatcher 把 “flush 完成” 作为每表放行节拍

dynstream 的 handler 有如下语义：

- `Handle(dest, events...) (await bool)`
  - 返回 `false`：本次处理完成，后续事件可继续处理
  - 返回 `true`：异步处理，**该 path 会被标记为 blocking**，直到外部显式调用 `DynamicStream.Wake(path)` 才会继续处理后续事件

现状（与代码对齐）：

- dispatcher 只要处理到 DML，就倾向于让 dynstream `await=true`（即 blocking 当前 path）
- `Wake(path)` 的触发点依赖该表“真正 flush 完成”（通过 `PostFlush` 回调 + `tableProgress.Empty()`）

因此对任意单表 path：**每次放行一个批次后，必须等这批数据 flush 完成，下一批才能进入 sink**。这会把单表吞吐节拍化为“批次大小 / flush 周期”。

### 2.2 storage sink 的 flush 触发方式让节拍化更致命

以 cloudstorage writer 为例：

- `flush-size`（配置名为 `file-size`）是按“单表（更准确说 `VersionedTableName`）维度”累计判断（默认 64MiB）
- 若 `flush-size` 达不到，就只能等 `flush-interval` tick 触发 flush（默认 5s）

结合上一节“每表任意时刻最多只有 1 个批次 in-flight”，单表吞吐上限近似变成：

```
单表吞吐上限 ≈ batch_bytes / flush_interval
```

按观测值估算：

- avg：`2.2MiB / 5s ≈ 0.44MiB/s`
- p99：`4.16MiB / 5s ≈ 0.83MiB/s`

而要在 `flush-interval=5s` 内触发默认 `flush-size=64MiB`，单表需要：

```
单表平均写入速率 >= 64MiB / 5s ≈ 12.8MiB/s
```

结论：在当前实现与 workload 下，**size flush 几乎不会发生**，吞吐被 `flush-interval` 锁死（每表每个 tick 只能推进一个批次），从而整体写出流量显著偏低。

### 2.3 为什么 dynstream 的 batch duration 会变高、memory quota 会长期高水位

当大量表 path 因 `await=true` 长时间处于 blocking 状态时：

- 上游事件仍在不断进入 dynstream 的 pending 队列（否则不会看到 memory quota 接近满）
- dynstream 的 `popEvents` 需要不断在 signalQueue 中跳过 blocking path，扫描/跳过成本上升，`batch_duration` p99 上升到 ms 级

因此：**batch duration 变高是 storage sink 反压在 dynstream 侧的二次症状**，不是 dynstream batch 策略本身的根因。

### 2.4 为什么 flush duration 很小但写出仍低

现有 `flush duration` 打点并不能完整覆盖 data file 的默认直写路径；在本地文件系统 + 默认并发配置下，“flush duration≈1ms”更可能主要反映 index file 路径，而不是端到端 flush 周期本身。真正限制吞吐的是 **flush 作为 ack 边界 + dispatcher 以 flush 完成作为 wake 条件** 形成的节拍化。

## 3. 解决方案（专门针对 storage sink，不影响其他 sink）

目标：在不把 `flush-interval` / `flush-size(file-size)` 当作“专门解决方案”的前提下，解除 storage sink 的“每表每 flush 才放行一次”的节拍化限制，让同一张表能在一个 flush 窗口内持续进入 sink，进而更容易触发 size flush，同时保持可控背压与正确性（DDL barrier / resolved ts 等）。

### 3.1 关键前提：`Sink.AddDMLEvent` 必须是 non-blocking

需要先强调一个实现约束：**downstream adapter 调用 `sink.AddDMLEvent` 必须是 non-blocking**。

原因：

- `AddDMLEvent` 发生在 dynstream 的 handler goroutine 内（dispatcher 处理事件的 hot path）。一旦这里阻塞，除了会卡住当前 dispatcher，还会占用 handler worker，进而拖慢同一个 dynstream 上其他 dispatchers 的处理，造成系统性 head of line。
- 因此现有所有 sink 在 `AddDMLEvent` 路径上都采用“写入一个无界队列然后立刻返回”的模式（例如 `utils/chann/unlimited_chann.go`）。

推论：

- 不能依赖“把 sink 内部队列从无界改为有界，然后满了就阻塞 `AddDMLEvent`”来做主背压；否则会违背上述 non-blocking 要求。
- 由于 `Sink.AddDMLEvent` 本身不返回 `(bool/error)`，sink 也无法在队列满时用“拒绝/返回需要背压”的方式让上游停住（除非引入新的接口面）。
- 因此要同时满足 “持续投喂（提高吞吐）” 与 “不 OOM”，背压边界必须仍然落在 dynstream（通过 dispatcher 返回 `await=true` 把 backlog 留在 dynstream 的 pendingQueue，让 dynstream memory control 生效）。

### 3.2 推荐方案：dispatcher 侧 in-flight budget（背压留在 dynstream，不改 sink interface）

在不引入新的 sink-side flow control 接口、且保持 `AddDMLEvent` non-blocking 的前提下，推荐将“是否继续放行下一批 DML”的决策上移到 dispatcher：

- dispatcher 维护 **in-flight budget**（建议以 bytes 为主，per-dispatcher + 可选 global）：
  - “in-flight”指 **已经进入 sink（调用过 `AddDMLEvent`），但还没触发 `PostFlush` ack** 的那部分数据量。
- 当 in-flight 未超过水位：dispatcher 对包含 DML 的 batch 返回 `await=false`，让 dynstream 持续投喂同一表，从而在 writer 内更容易累积到 `file-size` 并触发 size flush（摆脱 tick-limited）。
- 当 in-flight 超过水位：dispatcher 返回 `await=true`，把 backlog 留在 dynstream pendingQueue（复用 dynstream memory quota），避免把压力“搬进” sink 内部的无界队列导致 OOM。
- 当 flush 回调让 in-flight 回落：触发 `Wake(path)` 恢复投喂（需要处理 global cap 下的活性问题，避免永久 blocking）。

这套方案的核心是：**不改 `AddDMLEvent` 的 non-blocking 语义**，同时把“放行节拍”从“每次必须等 flush 完成”改为“在预算范围内可连续 in-flight”。

（完整设计与边界情况见：`design.md`）

### 3.3 可选方向（需要新增接口面）：storage sink 的 `give_me_more` 能力

如果未来允许扩展 sink 的可选接口面，也可以让 sink 提供一个“非阻塞的背压反馈”方法（例如 `GiveMeMore(...) bool`，概念名 `give_me_more`），由 dispatcher 在写入 DML 后调用，用于决定是否继续放行后续批次：

- `give_me_more == true`：sink 仍希望继续接收该表数据（例如 buffered bytes 还不足以达到期望文件大小、flush task 队列可控）
  - dispatcher 返回 `await=false`：**不阻塞当前 path**，让 dynstream 继续投喂下一批
- `give_me_more == false`：sink 需要背压（例如 buffer 过大、flush 队列已满、内存预算接近上限）
  - dispatcher 返回 `await=true` 并注册 wake：当 sink 释放 buffer / 完成 flush 后主动触发 `Wake(path)` 再继续

该方向的特点是：仍然保持 `AddDMLEvent` non-blocking，但引入新的 sink-side flow control 接口与演进面；本轮优先选择 3.2 的 dispatcher budget 方案来避免接口膨胀与回归风险。

### 3.4 正确性与边界

- DML：允许多批次 in-flight，但必须保持 per-table 顺序（dispatcher 仍串行调用 sink；sink 内部队列需按序写出）。
- DDL/SyncPoint/ResolvedTs：仍以 “tableProgress 为空 / flush 完成” 作为 barrier；遇到这些事件时，dispatcher 可以继续采用 `await=true`，确保不会越过未落盘的 DML。
- 背压：主背压边界应留在 dynstream（通过 `await=true`），避免 sink 内部无界队列失控；budget/`give_me_more` 的决策应基于可观测的水位（bytes/队列长度/flush in-flight 等），避免把问题从“吞吐低”转为 “OOM/抖动”。

### 3.5 可观测性（建议随修复一起补齐）

- data file 写入耗时打点（覆盖 `storage.WriteFile` 直写路径）
- flush 触发原因计数（interval vs size）
- sink 内部队列长度/待写 bytes（用于观察 budget / `give_me_more` 是否把压力正确留在 dynstream，而不是搬进 sink）

## 4. Workaround（仅用于临时缓解/验证根因）

调参可以快速提升吞吐，但会改变产物形态（更多小文件）并可能显著影响对象存储成本/列举性能；因此不作为专门解决方案，仅建议用于：

- 临时把压测跑起来
- 快速验证“节拍化 + size flush 触发不了”这一根因

### 4.1 调小 `flush-size`（`file-size`）

把 `file-size` 调到接近单次 `batch_bytes` 的数量级（例如 1–4MiB），让 size flush 不再强依赖 5s tick。

### 4.2 调小 `flush-interval`

将 5s 缩短到 100ms–1s，减少等待 tick 的时间；通常需要与 `file-size` 联动，避免过度碎片化。

## 5. 附录：数据链路与关键代码位置

### 5.1 数据链路（简化）

```
TiDB -> EventService -> EventCollector(dynstream) -> Dispatcher -> Sink(cloudstorage)
     -> encoding -> defragment -> writer(batch by table) -> ExternalStorage(file://...)
```

### 5.2 关键语义与代码位置（便于快速定位）

- dynstream `await/wake` 语义：`utils/dynstream/interfaces.go`
- dynstream 设置 blocking：`utils/dynstream/stream.go:301`
- dynstream `popEvents` 跳过 blocked path：`utils/dynstream/event_queue.go:125`
- dynstream 配置（BatchCount=4096）：`downstreamadapter/eventcollector/helper.go:25`
- dispatcher DML 触发 block 与 wake：`downstreamadapter/dispatcher/basic_dispatcher.go:480`
- TableProgress flush 跟踪：`downstreamadapter/dispatcher/table_progress.go:162`
- cloudstorage flush 条件：`downstreamadapter/sink/cloudstorage/writer.go:310`、`downstreamadapter/sink/cloudstorage/writer.go:331`
- cloudstorage 默认配置：`pkg/sink/cloudstorage/config.go:39`、`pkg/sink/cloudstorage/config.go:51`
