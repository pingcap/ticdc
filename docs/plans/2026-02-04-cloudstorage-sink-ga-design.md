# Cloud Storage Sink GA - Design（设计文档）

- Date: 2026-02-04
- Scope: Cloud Storage Sink only (+ minimal dispatcher glue)
- Status: Draft

## Table of Contents
- [1. 现状与关键观察](#1-现状与关键观察)
- [2. 总体架构](#2-总体架构)
- [3. wake 与 checkpoint 解耦](#3-wake-与-checkpoint-解耦)
- [4. Spool 设计](#4-spool-设计)
- [5. 文件写入与 file-size 贴近策略](#5-文件写入与-file-size-贴近策略)
- [6. DDL：DrainMarker 栅栏设计（内部独立 item）](#6-ddldrainmarker-栅栏设计内部独立-item)
- [7. 1000 dispatchers 稳定性设计](#7-1000-dispatchers-稳定性设计)
- [8. 可观测性](#8-可观测性)
- [9. 失败处理与回滚](#9-失败处理与回滚)

---

## 1. 现状与关键观察

### 1.1 DDL 当前实现（master 事实）
目前 cloudstorage sink 的 `WriteBlockEvent(DDLEvent)` 逻辑是：
- 执行 `writeDDLEvent(e)`，成功后调用 `event.PostFlush()`。
- **不包含** sink 内部显式 “flush `<T` DML” 的逻辑。

因此 master 上 DDL 顺序正确性很大概率依赖 dispatcher 的阻塞语义（DML flush 完才 wake），使得 DDL 到达 sink 时 `<T` DML 已基本 flush 完成。

### 1.2 GA 改造对 DDL 的影响
GA 要解决慢点，会把 DML 的 wake 提前到“入 spool 成功”。因此 DDL 可能在 `<T` DML 仍在 encoding queue/spool/未落远端时到达 sink。

=> 必须在 sink 内补齐 DDL 前 drain 的确定性机制（DrainMarker），以保持正确顺序。

另外需要特别强调 “拆表跨节点 / 多 dispatcher” 场景：maintainer barrier 最终只会选择一个 dispatcher 执行 `WriteBlockEvent` 真实写入 DDL schema 文件，其余 dispatchers 走 pass。若 drain 仅在 `WriteBlockEvent` 内部执行，则无法保证其他 involved dispatchers 的 `<T` DML 已远端落盘，仍可能触发 stale 丢弃风险。因此需要一个额外的 glue：由每个 involved dispatcher 在进入 maintainer barrier **之前**通知 sink 并完成本地 drain（`PassBlockEvent`）。

---

## 2. 总体架构

### 2.1 数据流（逻辑）

```
dispatcher (await=true)
   |
   | AddDMLEvent(event)  [sink interface unchanged]
   | (event carries enqueue-wake hook)
   v
cloudstorage sink
   |
   | (async) encode -> enqueue encoded payload into spool
   | (if watermarks allow) trigger enqueue-wake hook (wake dynstream path)
   v
writer(s)
   |
   | aggregate to near file-size -> remote storage write (data+index)
   | on success: run PostFlush() callbacks
   v
checkpoint advances (unchanged semantics)
```

### 2.2 最小 dispatcher glue
本 GA 需要两类 glue：DML early-wake、以及 DDL drain 的 per-dispatcher 协议。

- **DML glue（early-wake）**：不新增新的 DML sink interface 方法，仍使用现有 `sink.AddDMLEvent(event)`，但把“wake”与“checkpoint/table progress 推进”拆成两类回调，形成 two-stage ack：
  - enqueue ack（wake）：由 sink 在“编码后 payload 成功入 spool 且水位允许”时触发；用于解除 dynstream path 阻塞；
  - flush ack（checkpoint）：由 writer 在“远端写成功”后触发 `event.PostFlush()`；用于推进 table progress / checkpoint（语义不变）。

  为了让 sink 能在合适的时机触发 wake（并在水位过高时延迟 wake），需要把 `wakeCallback` 以**事件携带的方式**传递到 sink，而不是由 dispatcher 在本地直接调用。实现上建议在 `DMLEvent` 上新增一个仅用于 enqueue ack 的 hook（名称示意）：
  - `event.AddPostEnqueueFunc(func())`
  - `event.PostEnqueue()`

  dispatcher 在构造 DML batch 时：
  - 继续通过 `TableProgress.Add(event)` 绑定 flush ack（远端成功后移除 element，推进 checkpoint）；
  - 不再把 wake 绑定在 `PostFlush()` 阶段；
  - 改为把 wakeCallback（带 `sync.Once` 去重）注册到 `PostEnqueue()` hook 中。

  cloudstorage sink 在 payload encode 完成并 append 到 spool 后：
  - 若水位允许：触发 `PostEnqueue()`（从而 wake dynstream）；
  - 若水位过高：暂不触发 wake，并在水位回落到 low watermark 时补触发一次（避免 path 永久阻塞）。
- **DDL glue（per-dispatcher drain）**：sink interface 增加 `PassBlockEvent(event BlockEvent) error`：
  - dispatcher 收到 `DDLEvent(commitTs=T)` 后，在进入 DDL 处理流程之前调用 `sink.PassBlockEvent(ddl)` 并等待返回：
    - 若该 DDL 需要 maintainer barrier 协调：则在上报 maintainer barrier **之前**调用；
    - 若该 DDL 不需要 barrier（单 dispatcher 直接写）：则在调用 `WriteBlockEvent` **之前**调用；
  - 返回成功表示该 dispatcher 的 `<T` DML 已完成确定性 drain（覆盖 encoding queue + spool + remote），随后才允许继续 DDL 处理（进入 maintainer barrier 或执行 `WriteBlockEvent`）；
  - 失败则直接返回 error（不进入 barrier / 不执行 DDL）；

---

## 3. wake 与 checkpoint 解耦

本 GA 采用 **two-stage ack**（两阶段确认）模型：对 DML 将“解除 path 阻塞的 wake”与“推进 checkpoint 的远端 flush 成功”严格解耦。
背压仅通过 sink 内 spool 的 internal watermarks 控制是否触发 wakeCallback（不暴露用户配置）。

### 3.1 wake（提前）
- wake（enqueue ack）触发点：**编码后 payload 成功入 spool 且当前水位允许**。
- wake 的承载方式：由 sink 触发 `DMLEvent.PostEnqueue()`（名称示意，仅用于 enqueue ack），而不是复用 `DMLEvent.PostFlush()`。
- 若超过 highWatermark：不触发 wake（维持 path block），降到 lowWatermark 后恢复触发。

### 3.2 checkpoint（不变）
- `event.PostFlush()` 的触发点：**远端写成功（data+index 等完成）**后触发，推进 table progress / checkpoint。
- 任何情况下不得在“仅入 spool 成功”时触发 PostFlush。

---

## 4. Spool 设计

> spool 的详细用途/语义边界/数据结构/水位策略见：`docs/plans/2026-02-04-cloudstorage-sink-ga-spool.md`。

### 4.1 spool item 类型（内部独立）
- `dataItem`
  - `dispatcherID`
  - `commitTsRange`（或最小/最大 commitTs）
  - `encodedBytes`
  - `bytesLen`
  - `postFlushFuncs`（远端成功后执行）
- `drainMarkerItem`（见 DDL 设计）
  - `dispatcherID`
  - `ddlCommitTs`
  - `markerID`
  - `ackCh`（writer 完成远端 flush 后关闭/通知）

### 4.2 水位与背压
- 记账：全局 `spoolBytes`（以及可选的分 shard/dispatcher 统计）。
- 水位策略（内置，无需用户配置）：
  - 内部维护 high/low watermarks，用于控制 wake 抑制与恢复（hysteresis），避免远端变慢时 OOM/抖动；
  - 当 memory tier 超过水位时，将 `dataItem` spill 到 disk tier（spool），降低 GC 压力并提供额外缓冲。
- 行为：
  - `spoolBytes > high`：停止触发 wakeCallback（背压）
  - `spoolBytes < low`：恢复触发 wakeCallback

### 4.3 non-blocking 入口
- `AddDMLEvent` 必须 non-blocking：不能等待 encode/remote，也不能在入口处做慢 I/O（包括同步写盘）。
- spill 到 disk tier 必须由 sink 内部后台 worker 异步完成；入口只负责 enqueue。

---

## 5. 文件写入与 file-size 贴近策略

### 5.1 flush 原则
- 尽可能通过 size 触发 flush，使文件贴近 `file-size`。
- `flush-interval` 做兜底（低流量/长尾 dispatcher）。
- DDL drain 触发强制 flush（即使未满）。

### 5.2 active set（关键）
- writer 不应每 tick 扫描所有 dispatchers（会造成不必要的 CPU 开销，并让复杂度随 dispatcher 数线性增长）。
- 需要维护 active set（活跃 dispatcher 集合）：
  - dispatcher 有新 `dataItem` 入 spool -> 标记 active
  - dispatcher flush 完且队列为空 -> 变为 inactive
- 任何周期性逻辑只遍历 active set（复杂度与活跃 dispatcher 成正比）。

### 5.3 同步单元（dispatcher）：如何确保及时、正确、高效写入远端

> 本节回答一个常见疑问：从 storage sink 的视角看，每个 dispatcher（同步单元）都是独立的（无需关心它是否与其他 dispatcher 来自同一张逻辑表）。那么如何保证每个同步单元的数据能及时、正确、且高效地被写到远端？

**同步单元（dispatcher）的定义：**
- dispatcher 是 TiCDC 的调度/同步单元：一个 dispatcher 负责一条独立的事件流。
- 对 storage sink 而言，dispatcher 就是“保序、聚合与 flush 的最小单位”，flush 的触发与 ack 都按 dispatcher 独立发生；sink 不需要识别“多个 dispatchers 是否来自同一张逻辑表”。

**及时（Timely）：满足 size/interval/DDL 三类 flush 触发条件**
- **size flush（优先）**：dispatcher 的聚合 buffer 一旦达到 `file-size`，立刻 emit 一个 flush task，让 writer 尽快开始远端写入（不在入队/编码路径做慢 I/O）。
- **interval flush（兜底）**：每个 dispatcher 维护 `lastFlushTime`（或等价时间点）。当 “距离上一次 flush ≥ `flush-interval` 且仍有未 flush 数据” 时，必须 emit flush task，避免低流量长尾永不落盘。
  - 实现形态可以是“全局 tick + active set 扫描”，也可以是“每 dispatcher 独立 timer”，但要求相同：对 active dispatcher 不应出现无界等待。
- **DDL drain flush（强制）**：`DrainMarker` 到达该 dispatcher 的队列时必须强制 flush（即使未满、未到 interval），并在远端成功后 ack marker。

**正确（Correct）：顺序与 ack 语义不被破坏**
- **单 dispatcher 内保序**：同一个 dispatcher 的 item（DML dataItem + drainMarkerItem）必须走同一条保序通道（encoding queue → spool → writer），writer 按 FIFO 处理，避免“marker 先于部分 DML”的错序。
- **flush ack 只在远端成功后发生**：`PostFlush()`（推进 table progress / checkpoint）必须且只在远端写成功后触发；enqueue ack（wake）不得推进 checkpoint。
- **schema/data 顺序**：若某 dispatcher 首次输出 data file 前需要 schema file，必须先确保 schema file 已写入远端（或已存在），避免消费者读取到无 schema 的 data。

**高效（Efficient）：并行、批处理、避免全局 HO-L**
- **并行**：不同 dispatchers 之间可以并行写远端（多个 writer / 多 shard），不要求全局顺序。
- **批处理**：flush task 可以包含多个 dispatchers，以减少对象存储 API 调用的固定开销；但单个 dispatcher 的 size flush 触发后不应被“等待其它 dispatcher 凑批”无限延迟。
- **只处理活跃集合**：用 active set 避免每 tick 扫全量 dispatchers，保证 1000 dispatchers 下 CPU 成本与活跃 dispatcher 成正比。

---

## 6. DDL：DrainMarker 栅栏设计（内部独立 item）

### 6.1 目标
对于 DDL(commitTs=T)，必须保证 DDL schema 文件写入远端之前，所有 `<T` DML 已远端落盘成功。

并且 “flush `<T`” 必须覆盖：
- encoding queue 中尚未入 spool 的 `<T`
- 已入 spool 但未落远端的 `<T`

### 6.2 核心机制：DrainMarker
在 early-wake 下，DDL 顺序正确性不能再依赖 “flush 完才 wake”。同时在多 dispatcher 场景，只有一个 dispatcher 会 `WriteBlockEvent` 写 DDL，因此我们采用 **per-dispatcher drain**：

为什么不需要 barrier 的 DDL 也要 `PassBlockEvent`：
- barrier 解决的是“跨 dispatcher 的协同与 write/pass 选择”，但不解决 sink 内部 encoding queue/spool 的在途数据覆盖问题；
- early-wake 之后，DDL 到达 sink 时 `<T` DML 仍可能停留在 encoding queue 或 spool 中；
- 因此即使单 dispatcher 直接 `WriteBlockEvent`，也必须先通过 `PassBlockEvent` 注入 `DrainMarker`，保证 drain 覆盖 encoding→spool→remote，再写 schema file。

实现 `sink.PassBlockEvent(ddl)`（对 `DDLEvent(commitTs=T)`）：
1) dispatcher 收到 DDL 后，进入 block 状态（不再继续处理后续事件），并调用 `PassBlockEvent(ddl)`：
   - 若该 DDL 需要 maintainer barrier 协调：则在上报 maintainer barrier 之前调用；
   - 若该 DDL 不需要 barrier：则在调用 `WriteBlockEvent` 之前调用（此时只有当前 dispatcher involved）；
2) `PassBlockEvent` 在 sink 内对“该 dispatcher”注入 `DrainMarker(T)`，进入与该 dispatcher DML 相同的顺序通道；
3) encoder 处理到 marker：
   - 代表 marker 前面的 DML 已全部完成编码并 append 到 spool；
   - 将 marker 自己 append 为 `drainMarkerItem` 到 spool；
4) writer 处理 `drainMarkerItem`：
   - 强制 flush 当前 dispatcher 的未完成文件到远端（data+index）；
   - 等待远端成功后 ack marker；
5) `PassBlockEvent` 等待 marker ack 完成后返回：
   - 成功：dispatcher 才允许继续 DDL 流程（进入 maintainer barrier 或直接写入）；
   - 失败：直接返回 error，dispatcher 不进入 barrier / 不执行 DDL 写入。

若该 DDL 不需要 barrier，则当前 dispatcher 在 `PassBlockEvent` 返回成功后直接执行 `WriteBlockEvent` 写 DDL schema 文件，并在成功后触发 `ddl.PostFlush()`（保持 checkpoint 语义不变）。

若该 DDL 需要 maintainer barrier，则 maintainer barrier 选择一个 dispatcher 执行 `WriteBlockEvent` 写 DDL schema 文件，并在成功后触发 `ddl.PostFlush()`（保持 checkpoint 语义不变）。`WriteBlockEvent` 本身不承担跨节点 drain 协议。

> 备注：该方案不需要修改 maintainer/barrier 协议。barrier 仍按原流程协调 write/pass；“DDL 前必须 drain `<T` DML” 的前置保证通过 dispatcher `PassBlockEvent` 成功后再上报实现。

### 6.3 affected dispatchers 的范围（由 barrier 决定）
- sink 不主动枚举 affected dispatchers。本 GA 将 DDL 的影响范围交给现有 dispatcher/maintainer barrier 机制决定。
- 对每个收到该 DDL 的 dispatcher：`PassBlockEvent` 仅 drain “本 dispatcher 范围内的 DML”。对于多表/跨 dispatcher 的 DDL，barrier 机制应确保所有 involved dispatchers 都会执行各自的 `PassBlockEvent`。

> 重要假设：DDL 仍走 dispatcher 的 BlockEvent 通道并保持强阻塞，从而 `>T` 的 DML 不会在 DDL 完成前被放行到 sink（至少对受影响范围成立）。这样我们无需额外处理 `>T` 交错复杂性。

---

## 7. 1000 dispatchers 稳定性设计

### 7.1 per-dispatcher 状态回收
- 路径生成器、版本号、fileIndex 等 per-dispatcher map 必须支持回收：
  - TTL/LRU（按最近访问时间）
  - 或绑定 writer 生命周期：writer 关闭则释放 dispatcher state
- 否则 dispatchers 长时间运行会出现不可控的常驻内存增长。

### 7.2 去全局 HO-L
- 避免全局 defragment/reorder 结构导致 head-of-line blocking。
- **结论（GA 的最终状态）：移除 `defragmenter`。**
  - 现有实现的 `defragmenter` 以 **全局 seqNumber** 推进 next-seq，并用 `future map` 暂存“已编码完的 fragment”（其中包含大块 `encodedMsgs` bytes）。这会带来两个问题：
    1) 全局 HO-L：任意一个慢编码/慢表会挡住所有后续表的分发；
    2) 重排缓存持有大对象，和 GA 引入的 spool/watermarks 的“可控内存”目标冲突。
- 替代方案：**按 shard 保序（keyed shards）**：
  - 在编码之前按 key（例如 dispatcherID / versionedTable 等，具体以实现为准）把事件路由到固定数量的 shard；
  - 每个 shard 内按 FIFO 串行（或以 future 模型保证有序）完成编码并 append 到 spool，从而天然“shard 内有序”；
  - shard 之间并行推进，不再要求全局重排。

说明：
- 该策略只要求“同 key 有序”，不要求跨 key 的全局顺序；这与 cloud storage writer 的实际行为一致（多 writer 并发、flush tick、map 遍历无序，本就无法保证跨表严格顺序）。
- `encodingGroup` 在 GA 中会被重构为“按 shard 的 encoder workers”（仍然是编码并发能力的一部分），但不再以“全局乱序输出 + defragmenter 重排”的形式存在。

### 7.3 writer fanout 控制
- 避免单次 tick 构造包含全量 dispatchers key 的 map 批处理。
- flush/聚合应按 active set 分批、分 shard 处理。

---

## 8. 可观测性

必须提供（至少）：
- spool：`spool_bytes`, `spool_items`, `wake_calls_total`, `wake_suppressed_total`
- flush：
  - `flush_count{reason=size|interval|ddl|close|error}`
  - `flush_duration_seconds{reason=...}`
  - `flush_file_size_bytes`（直方图，观测贴近 file-size）
- DDL：
  - `ddl_drain_duration_seconds`
  - `ddl_affected_dispatchers`
  - （可选）`ddl_drain_wait_encode_seconds`, `ddl_drain_wait_remote_seconds`

---

## 9. 失败处理与回滚

- 远端写失败：
  - `PostFlush()` 不触发（checkpoint 不推进），sink 标记异常并按现有错误处理策略退出/重试。
- `PassBlockEvent` 失败：
  - 直接返回 error（不做本地吞错/强行进入 barrier），dispatcher 走现有错误处理流程；
  - 失败时不得触发 `ddl.PostFlush()`。
- 回滚：
  - early wake 是 GA 必须项，不提供独立开关回滚到旧行为。

---
