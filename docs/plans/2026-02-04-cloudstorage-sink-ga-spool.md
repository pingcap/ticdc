# Cloud Storage Sink GA - Spool 详解（memory + disk spillover）

- Date: 2026-02-04
- Scope: Cloud Storage Sink（spool 默认包含 memory tier + disk spillover；无需用户配置）
- Status: Draft
- References:
  - `docs/plans/2026-02-04-cloudstorage-sink-ga-requirements.md`
  - `docs/plans/2026-02-04-cloudstorage-sink-ga-design.md`
  - `docs/plans/2026-02-04-cloudstorage-sink-ga-implementation.md`

## 1. 背景：为什么要引入 spool

本 GA 的核心目标是：在不改变 checkpoint 语义的前提下，解除 storage sink 在“超多 dispatchers + 单表低流量”场景下被 `flush-interval` 节拍化限速的问题。

要实现这一点，必须把 DML 的确认拆成 two-stage ack：
- **enqueue ack（wake）**：仅用于解除 dynstream path 的阻塞，让同一个 dispatcher 在一个 flush window 内持续进入 sink，以便更容易聚合到接近 `file-size`；
- **flush ack（PostFlush）**：远端写成功后才触发，用于推进 table progress / checkpoint，语义不变。

在 two-stage ack 下，sink 需要一个“可控的本地承载层”来实现以下能力：
1) 作为 enqueue ack 的“可信边界”：事件一旦进入 spool，就意味着 sink 已接管并能在内部保证顺序与最终写出；
2) **背压（watermarks）**：当 spool 内存水位过高时抑制 wake，让 backlog 留在 dynstream（复用 dynstream 的 memory quota），避免把压力无界搬进 sink；
3) **DDL 顺序正确性**：early-wake 之后 DDL 不能再依赖旧的隐含顺序，需要 `DrainMarker` 穿透 encoding queue → spool → remote；spool 需要能承载 marker item 并驱动 writer 做强制 flush；
4) **1000 dispatchers 的可扩展性**：writer/调度必须只遍历活跃 dispatcher，避免每 tick 扫全量 dispatchers；spool 需要维护 active set。

因此，本 GA 引入 **spool**：一个 sink 内部的数据结构，用于暂存“编码后的、按 dispatcher 保序的写入 item（payload + marker）”。当 memory tier 水位偏高时，允许把部分 payload 溢写到 disk tier 来降低 GC 压力与内存峰值；同时通过 internal watermarks 控制 wake，保证整体背压可控且用户无需配置。

---

## 2. spool 的定义与语义边界

### 2.1 spool 是什么

spool 是 cloud storage sink 内部的一层缓冲层，由两部分组成：
- **memory tier**：用于承载热点 dispatchers 的近期数据与控制类 item，提供低延迟的 enqueue/dequeue；
- **disk tier（spillover）**：当 memory tier 水位偏高时，把部分已编码 payload 溢写到磁盘，降低内存峰值与 GC 压力。

spool 用于存放 **编码后的 payload** 以及少量控制类 item（例如 DDL drain marker）。其中 disk tier 仅用于 payload spillover，marker 等控制类 item 应保持极小且优先留在内存结构中。

> 术语说明：本文统一使用 “dispatcher” 表示一个同步单元。
> - 上游是否拆表、是否跨节点，只会让 sink 同时看到更多 dispatchers；sink 不需要识别它们是否来自同一张逻辑表。

spool 的关键属性：
- **按 dispatcher 保序**：对同一个 dispatcher，spool 中的 item 顺序与上游投递顺序一致；
- **可分片并行**：不同 dispatchers 之间可以并行写出，不需要全局串行；
- **可记账（bytes accounting）**：spool 需要统计其占用的 memory bytes（以及可选的 spilled disk bytes），用于 internal watermarks 与可观测性；
- **不是持久化语义边界**：即使 payload 被 spill 到 disk tier，也不意味着 checkpoint 可以推进。spool 只是内部缓冲；进程重启时 disk tier 文件可以被清理并从 checkpoint 重放，不影响语义正确性。

### 2.2 spool 不是什么

- spool **不是** checkpoint 的边界：进入 spool 不能触发 `PostFlush()`，也不能推进 checkpoint。
- spool **不是** dynstream 的主背压边界：主背压仍应通过“抑制 wake → path 继续 blocking → backlog 留在 dynstream pending queue”实现；spool 只负责提供水位信息与抑制 wake 的机制。

### 2.3 two-stage ack 在 spool 上的落点

本 GA 规定：
- **enqueue ack（wake）** 的触发点：编码完成并成功入 spool（且水位允许）。
- **flush ack（PostFlush）** 的触发点：writer 将对应的数据（data + index 等）成功写入远端之后。

换句话说，spool 是 “wake 可以提前发生” 的承载层，但 spool 本身不改变 flush ack 的语义。

---

## 3. 数据模型：spool item 类型

建议的最小 item 类型如下（名称仅示意，以实现为准）：

### 3.1 `dataItem`（DML 写入条目）

字段建议：
- `dispatcherID`：同步单元标识；用于路由到 shard，并保持 per-dispatcher 顺序；
- `commitTsRange`：该 item 覆盖的 commitTs 范围（至少包含 maxCommitTs，用于调试/指标/边界检查）；
- `encodedBytes`：编码后的 payload（writer 最终写入 data file 的字节序列或其引用）；
- `bytesLen`：用于 bytes accounting；
- `postFlushFuncs`：远端写成功后要执行的回调（最终会触发 DML 的 `PostFlush()`，推进 table progress / checkpoint）。

约束：
- `postFlushFuncs` 必须且只在远端写成功后执行一次。
- `bytesLen` 应尽量与 `encodedBytes` 占用一致（允许近似，但必须可观测且能稳定反映水位趋势）。

### 3.2 `drainMarkerItem`（DDL drain 控制单元）

用途：用于 `PassBlockEvent` 的确定性 drain。

字段建议：
- `dispatcherID`
- `ddlCommitTs`：对应 DDL 的 commitTs（记为 `T`）
- `markerID`：用于排障定位（可选）
- `ackCh`：writer 在完成远端 flush 后通过该 channel 通知 marker 完成

writer 处理逻辑（语义）：
- marker 必须触发 **强制 flush**：确保该 dispatcher 上 `<T` 的所有在途 `dataItem` 已写入远端；
- flush 成功后 ack marker；
- marker 不产生 `PostFlush()`（它只是 drain 的控制点）。

---

## 4. 结构设计：分片、保序与 active set

### 4.1 为什么要分片（sharding）

在 1000 dispatchers 目标下，spool 必须同时满足：
- 多 producer（编码 worker）并发写入；
- 多 consumer（writer worker）并行写出；
- per-dispatcher 保序；
- 避免全局锁与全局队列导致 head-of-line。

因此推荐按 `dispatcherID` 做一致性 hash，将 item 路由到固定 shard：
- shard 内负责维护 per-dispatcher 队列；
- shard 间并行，锁竞争与扫描成本可控。

### 4.1.1 shard 是什么（语义与边界）

这里的 shard 是 **spool 的内部实现细节**：把“1000 个 dispatcher 的队列 + spill/读取 + 回收”拆分成多个独立分区，以降低锁竞争并提升并行度。

shard 的核心语义：
- **路由（routing）**：对每个 item，根据其 `dispatcherID` 计算 `shardID = hash(dispatcherID) % numShards`，并把该 item 交给固定 shard 处理。
- **保序（ordering）**：同一个 `dispatcherID` 的所有 item 必须落在同一个 shard 内，从而只需要在 shard 内维护 per-dispatcher FIFO 队列即可实现“单 dispatcher 内保序”。
- **并发单元（concurrency unit）**：每个 shard 有自己的锁/队列；多个 shard 可以并行 enqueue/dequeue/spill，避免全局 HO-L。

> `numShards` 是实现侧内置常量/策略，不随 dispatcher 数线性增长，也不作为用户配置暴露。

### 4.2 推荐的数据结构（概念级）

一个可实现、且对大量 dispatchers 友好的结构：

```
spoolManager
  - shards[]: spoolShard
  - spoolBytes: atomic/int64 (global accounting)

spoolShard
  - mu: mutex
  - perDispatcherQueues: map[dispatcherID]queue[item]
  - activeDispatcherQueue: queue[dispatcherID]  (只包含当前非空的 dispatcher)
  - activeDispatcherSet: map[dispatcherID]bool  (去重，避免重复入队)
```

设计要点：
- **activeDispatcherQueue + activeDispatcherSet**：保证 writer 只遍历 active dispatchers，不扫描全量 dispatchers；
- per-dispatcher 队列可用 slice/ring buffer 实现，避免频繁分配；
- `spoolBytes` 全局记账，便于统一水位控制；必要时也可增加 per-shard bytes 以增强可观测性。

### 4.3 基本操作语义

spool 至少需要支持以下操作（概念语义）：

1) `Enqueue(dispatcherID, item)`
- 把 item 追加到该 dispatcher 的队列尾部；
- 若该 dispatcher 之前为空，则把 dispatcherID 加入 activeDispatcherQueue（并更新 activeDispatcherSet）；
- 增加 bytes 记账（通常先计入 memory bytes；若后续发生 spill，则从 memory bytes 迁移到 disk bytes）。

2) `TryDequeue(dispatcherID) -> item, ok`
- writer 从该 dispatcher 的队列头部取一个 item；
- 若取完后队列为空，清理 activeDispatcherSet（dispatcher 不再活跃）。

3) `PickActiveDispatcher() -> dispatcherID, ok`
- writer 从 activeDispatcherQueue 取下一个 dispatcherID（round-robin），以减少单一热点 dispatcher 独占；
- 若该 dispatcher 已不再活跃（可能被其他 worker 消耗完），则跳过并继续取下一个。

> 备注：上述只是“最小语义集合”。实际实现可以按 writer 模型做批量 pop（例如每次取 N 个 item）以降低锁开销。

### 4.4 disk tier（spillover）设计要点（默认实现）

spool 的默认实现相比纯内存缓冲的关键差异在于：允许把部分已编码 payload 溢写到磁盘，以换取更低的内存峰值与更可控的 GC 压力。

为了满足 1000 dispatchers 的规模约束，disk tier 的设计需要同时满足：
- **文件数低基数**：不能按 dispatcher 建文件（文件数会爆炸）；推荐按 shard 建少量 segment 文件（O(numShards)）。
- **顺序一致**：spill 不能打乱 per-dispatcher 的 item 顺序；disk 只是存储介质，item 的逻辑顺序仍由 per-dispatcher 队列决定。
- **不阻塞入口**：`AddDMLEvent` 必须 non-blocking，因此 spill 的写盘必须由后台 worker 异步完成。

一个推荐的实现形态（概念级）：

- disk 存储为 **append-only segment**：
  - 每个 shard 一个 active segment，按顺序 append payload bytes；
  - segment 达到阈值后 rotate（生成新文件），旧 segment 等待被完全消费后删除；
  - `dataItem` 中保存一个 `diskPtr`（例如 `{segmentID, offset, length}`），当 payload 已 spill 后 `encodedBytes` 可置空以释放内存。

### 4.4.1 一个 segment 里是否会包含多个 dispatcher 的数据？

会的：**同一个 shard 的 segment 通常会包含多个 dispatcher 的 payload，且它们在文件中会交错出现**。

原因很简单：我们必须控制文件数。
- 1000 个 dispatcher 如果按 dispatcher 建 spill 文件，文件/句柄/清理成本都会爆炸；
- 因此 disk tier 必须共享少量文件（按 shard 组织是一个自然且易实现的方式）。

注意：segment 是否“混写多个 dispatcher”不影响逻辑顺序，因为：
- per-dispatcher 的逻辑顺序由 shard 内的队列保证；
- segment 只是字节存储介质，不承担“按 dispatcher 分组”或“按 dispatcher 查找”的职责。

### 4.4.2 segment 如何组织：record + 指针（diskPtr）

为了让 segment 同时支持“快速随机读取 + 可诊断”，建议把 spill 写入组织为 record：
- `recordHeader`（固定长度，示意字段）：
  - `magic` / `version`：快速校验与升级；
  - `payloadLen`：payload 长度；
  - `checksum`（可选）：用于发现磁盘损坏或读写异常；
  - （可选）`dispatcherID`：便于离线排障（不是查找必需字段）。
- `payloadBytes`：编码后的 payload 原始字节（即 `dataItem.encodedBytes`）。

spiller 在 append 一个 record 时生成 `diskPtr` 并写回 `dataItem`：
- `{segmentID, offset, length}`：直接指向该 record 在 segment 中的位置（offset/length 的语义实现侧保持一致即可）。
- 只有当 record 写入完成后，才把 `diskPtr` 设置到 `dataItem` 上；这样 writer 不会读到半写入的数据。

### 4.4.3 快速查找：不扫描文件，直接按 diskPtr 读取

segment 里混写多个 dispatcher 时，“快速查找”的关键不是建索引，而是 **不丢位置**：
- writer flush 某个 dispatcher 时，数据来源仍然是 **per-dispatcher 队列**；
- 对每个 `dataItem`：
  - 若在内存：直接拿 `encodedBytes`；
  - 若已 spill：直接对 `segmentID` 对应文件做 `ReadAt(offset, length)`（或等价 pread）读取 payload。

因此读取复杂度是 O(1) 定位（每个 item 一次指针读取 + 一次随机读），不需要按 dispatcher 在 segment 内做任何搜索。

- spill 策略（建议）：
  - 当 memory tier 水位偏高（或触发 GC 压力信号）时，后台 spiller 从队列中选择“可 spill 的 dataItem”（例如最旧的一批，或按 shard round-robin）写入 disk；
  - spill 完成后更新该 item 的 `diskPtr`，并把 bytes 记账从 memory bytes 转移到 disk bytes。
- 读策略（writer 侧）：
  - writer dequeue `dataItem` 时：若 payload 在内存，直接使用；若 payload 已 spill，按 `diskPtr` 从 segment 读取；
  - 读出的 bytes 仅作为临时 buffer 使用，远端写成功后立即释放，不再回写到 memory tier。
- 清理策略：
  - segment 的生命周期应可追踪（例如引用计数或“最大已消费 offset”）；当一个 segment 中所有 payload 都已被 writer 消费并远端确认后删除；
  - disk tier 的落盘目录应复用 TiCDC 既有的 `data-dir`（按 changefeed 维度隔离），从而满足“无需用户配置”；进程启动时可清理上一轮残留的 spool 目录（spool 不是持久化语义边界，清理不影响 correctness，只会导致更多重放）。
- 磁盘耗尽处理：
  - 当 disk tier 达到内置上限或磁盘空间不足时，应优先 **抑制 wake**（把背压留在 dynstream），并记录清晰的错误/告警；
  - 若磁盘耗尽导致无法继续 spill 且内存也无法承载，应返回可诊断的错误而不是静默卡死。

---

## 5. 水位（watermarks）与 wake 抑制（背压）

### 5.1 设计目标

early-wake 允许上游在 flush window 内持续投喂，因此必须有明确的上限策略，避免远端变慢时 sink 内部无界增长。

本 GA 选择的背压方式是：
- spool 以 `spoolBytes`（可拆分为 memory bytes + spilled disk bytes）为核心水位信号；
- **超过 high watermark 时抑制 wake**，让 dispatcher path 保持阻塞，backlog 留在 dynstream；
- 低于 low watermark 后恢复 wake（hysteresis），避免频繁抖动。

### 5.2 watermarks 规则（内置软上限，无需用户配置）

watermarks 采用内置软上限（实现侧维护 high/low 两条水位线）：
- high：超过后抑制 wake（将背压留在 dynstream）
- low：低于后恢复 wake（hysteresis）

行为：
- `spoolBytes > high`：不触发 wakeCallback（wake_suppressed）
- `spoolBytes < low`：恢复触发 wakeCallback

为什么是“软上限”：
- 编码/入队是并发的，严格不超过 high 需要在入队点阻塞，这会把背压挪到 sink 内部 worker，增加死锁与级联阻塞的风险；
- 软上限配合 dynstream 阻塞可以实现稳定的“控制进入 sink 的速度”，允许短暂超出但可回落。

> 备注：disk tier 主要用于降低内存峰值与 GC 压力，并不意味着可以无限制地把 backlog 从 dynstream 转移到磁盘。实现必须仍然在高水位时抑制 wake，避免 disk 被写满造成更严重的故障面。

### 5.3 wakeCallback 的触发点与去重

建议语义：
- 对某个 DML batch 的 wakeCallback，只要该 batch 对应的 payload **成功入 spool**，且当前水位允许，即可触发；
- 同一轮 dynstream await 的 wake 最好用 `sync.Once` 去重（防止重复 wake 造成无意义调度）。

注意：
- wake 不等价于 flush，不得触发 `PostFlush()`；
- wake 仅表示“允许 dynstream 继续下发下一批事件”，因此 watermarks 必须是 wake 的唯一背压开关。

---

## 6. DDL drain：DrainMarker 如何穿透 encoding queue → spool → remote

### 6.1 问题背景

early-wake 之后，DDL 到达 sink 时，`<T` DML 可能仍处于：
- encoding queue（尚未编码/尚未入 spool）
- spool（已编码但尚未写远端）
- writer 的聚合缓冲（尚未 flush）

若 DDL 直接写 schema file，会破坏远端顺序约束并可能触发 stale 丢弃风险。

### 6.2 DrainMarker 的关键要求

要保证 “DDL 写 schema file 前 `<T` DML 已落远端”，`PassBlockEvent(ddl)` 必须满足：
- 对当前 dispatcher 注入一个 marker；
- marker 必须排在该 dispatcher 上 `<T` DML 的后面；
- marker 被 writer 处理并 ack 之前，`PassBlockEvent` 必须阻塞等待；
- marker ack 表示该 dispatcher 上 `<T` 的在途数据已经远端落盘（encoding queue + spool + writer 缓冲都被覆盖）。

### 6.3 spool 在其中扮演的角色

spool 是 marker “可被 writer 观察并驱动 flush” 的载体：
- encoder 处理到 marker 时，先保证 marker 前的 DML 都已 encode 并 append 到 spool；
- 然后把 marker 作为 `drainMarkerItem` append 到同一个 dispatcher 队列；
- writer 按队列顺序消费到 marker 时，执行强制 flush 并在成功后 ack。

> 重要：marker 必须和该 dispatcher 的 `dataItem` 走同一条保序通道，否则会出现“marker 先于部分 dataItem 被消费”的错序。

---

## 7. writer 与 spool 的协作：file-size、interval 与强制 flush

### 7.1 常规 flush（size/interval）

writer 从 spool 获取 `dataItem` 后按 dispatcher 聚合：
- 累计到接近 `file-size`：触发 flush（reason=size）
- `flush-interval` 到期：触发 flush（reason=interval）

flush 成功后：
- 写 data/index 到远端；
- 执行 `postFlushFuncs`（触发 `PostFlush()` 推进 checkpoint 相关逻辑）；
- 释放 payload（可能是内存 `encodedBytes`，也可能是 disk tier 的 segment 引用）并减少 `spoolBytes` 记账（memory/disk）。

### 7.2 DDL drain 的强制 flush（marker）

当 writer 消费到 `drainMarkerItem`：
- 必须先把该 dispatcher 当前未 flush 的数据写到远端；
- 成功后 ack marker；
- marker 本身不携带 DML 的 `postFlushFuncs`，但强制 flush 会顺带执行之前累积的 DML 回调，从而保证 `<T` DML 的 flush ack 已发生。

---

## 8. 可观测性（建议）

spool 相关建议指标（低基数）：
- `spool_memory_bytes`（gauge）：memory tier 当前占用
- `spool_disk_bytes`（gauge）：disk tier 当前占用（已 spill 但未消费的 payload）
- （可选）`spool_bytes`（gauge）：memory + disk 的合计
- `spool_items`（gauge）：当前 item 总数（或按 shard 统计）
- `wake_calls_total`（counter）：触发 wake 的次数
- `wake_suppressed_total`（counter）：因高水位抑制 wake 的次数
- `ddl_drain_duration_seconds`（histogram）：从 `PassBlockEvent` 开始等待到 marker ack 的耗时

建议日志（注意避免高基数）：
- 水位状态切换（进入抑制、恢复）打印一次；
- markerID + ddlCommitTs 的 drain 开始/结束（debug 级别）。

---

## 9. 故障处理与边界

- 远端写失败：
  - 不执行 `PostFlush()`；spool 不应吞错；
  - sink 进入错误处理路径（按现有策略退出/重试）。
- 进程崩溃：
  - spool 数据丢失是预期行为；
  - 因为 checkpoint 仅在远端成功后推进，重启后从 checkpoint 重放不会破坏语义正确性。
- 内置水位策略不合理（过小/过大）：
  - 过小：wake 长期被抑制会导致吞吐下降（但仍正确）；
  - 过大：可能导致 spool backlog 过深（磁盘占用升高、恢复时间变长）；
  - 建议通过指标观测，在实现侧调整内置阈值/策略（不对用户暴露配置），必要时通过增加 writer 并发与提升远端写入能力缓解。

---

## 10. 测试建议（单测优先）

spool 相关建议单测覆盖：
1) bytes accounting 正确性（入队/出队/flush 后回落）
2) watermarks 行为（高水位抑制 wake，低水位恢复）
3) per-dispatcher 保序（同 dispatcher 的 item 不乱序）
4) marker 穿透（marker ack 前 `<T` DML 不得写在 DDL schema file 之后；marker ack 后满足顺序）
5) active set 行为（total dispatchers 很大但 active dispatchers 小时，调度不扫描全量）

---

## Appendix A：为什么叫 spool（术语来源与本项目语义）

“spool / spooling”是一个相对常见的系统工程术语，通常表示 **I/O 管线中的中间暂存区（staging area）**，用于把“生产者（更快）”与“消费者（更慢）”解耦，使两侧能够并行推进。

术语来源（简述）：
- 早期在打印系统/操作系统中非常常见：CPU 生成打印内容很快，但打印机很慢，因此把数据先写入一个 spool 区域（常见实现是磁盘上的 spool 目录/文件），打印机按自己的速度取走执行。
- 该词也常被解释为 “Simultaneous Peripheral Operations On-Line” 的缩写（业内常见说法之一），体现“外设 I/O 与上游计算并行”的初衷。

在本 GA 的语义中，“spool”强调的是以下边界：
- **它是 sink 内部承载层**：用于暂存“已编码 payload + 少量控制 item（例如 drain marker）”，并让 writer 在远端写慢时仍能平滑吸收一定量的短期 backlog。
- **它是 enqueue ack（wake）的可信边界**：payload 成功进入 spool（且水位允许）后可以触发 wake；但这并不意味着远端已写成功。
- **它不是 checkpoint 的边界**：无论 payload 在 memory tier 还是 disk spillover，都不能触发 `PostFlush()`，也不能推进 checkpoint；checkpoint 仍必须以远端写成功为准。
- **它不是持久化保证**：进程崩溃后 spool 数据丢失是允许的；依赖 checkpoint 重放恢复正确性。

---

## Appendix B：如何快速判断“某个同步单元（dispatcher）”触发了 size flush 还是 interval flush

先明确一个口径问题：cloud storage sink 的 flush 决策按一个“**同步单元（dispatcher）**”发生。
- 从 storage sink 的视角，每个 dispatcher 都是独立的：维护自己的 buffer bytes、lastFlushTime，并独立触发 size flush / interval flush。
- “拆表”发生在上游调度侧，它只会让 sink 同时看到更多 dispatchers；sink 不需要知道它们是否来自同一张逻辑表。

### B.1 最快的“外部观察”（无需改动实现）

对于某个同步单元（dispatcher），最直接的办法是观察对象存储上该同步单元的输出文件：
- 若大部分 data 文件大小接近 `file-size`，且 flush 间隔不固定：基本可以认为以 **size flush** 为主；
- 若大量文件明显偏小，且生成时间间隔接近 `flush-interval`：基本可以认为以 **interval flush** 为主。

该方法的优点是：不依赖进程内状态；缺点是：只能“事后推断”，且需要按 dispatcher 过滤路径/前缀（必要时依赖 dispatcherID 等信息定位）。

### B.2 最快的“进程内信号”（建议在 GA 落地时补齐）

为了让排障更直接（尤其是 1000 dispatchers），建议在 GA 实施时补齐两类信号：**低基数指标** + **按需日志/调试信息**。

#### 1) 低基数指标：先判断全局是否被 interval 主导

目的：快速回答“现在是不是大多数 flush 都是 interval 触发，导致小文件多”的问题（按 changefeed/namespace 聚合，避免高基数）。

建议补充（名称仅示意）：
- `cloud_storage_flush_count{reason=size|interval|ddl|close|error}`
- `cloud_storage_flush_file_size_bytes`（histogram，可选加 `reason` label）

典型判断：
- 若 `reason=interval` 占比明显升高，且 `flush_file_size_bytes` 的分布整体偏小：说明系统仍在被 interval flush 主导（可能是低流量、或 wake 仍然被抑制、或 writer 聚合不足）。
- 若 `reason=size` 为主，且 file size 分布贴近 `file-size`：说明目标行为成立。

#### 2) 针对“某一个同步单元（dispatcher）”：不要用 Prometheus labels，改用可控日志/按需查询

对单元定位通常需要“可见 tableID/dispatcherID”，但把这些标识做成 metrics label 会带来极高基数（1000 dispatchers 甚至更高），不适合作为常态指标。

更推荐两种方式（实现时二选一或同时提供）：

- **采样/限频日志（推荐）**：在每次 flush 决策发生时输出一条结构化日志，但做严格限频（例如“每个 dispatcher 每 N 秒最多一条”），并带上关键字段：
  - `tableID` / `dispatcherID`
  - `reason`（size / interval / ddl / close / error）
  - `fileBytes`（本次 flush 的文件大小）
  - `fileSizeTarget`、`flushInterval`
  - `spoolBytes`（可选，用于关联水位背压）
  - `timeSinceLastFlush`

  这样要定位“某个同步单元为啥总是 interval flush”，只需要按 tableID/dispatcherID grep 日志即可。

- **按需调试接口/状态输出（可选）**：提供一个只在调试时使用的查询入口，按 tableID/dispatcherID 返回该同步单元的 writer 状态（当前 buffer bytes、上次 flush reason/时间、近窗口 flush 计数等）。该信息不进入 Prometheus，不会造成 label 爆炸，但可以“点查”定位单元问题。

### B.3 建议的落地优先级

若目标是“快速发现”并方便排障，建议实施侧优先级为：
1) `cloud_storage_flush_count{reason=...}` + `flush_file_size_bytes`（低基数指标，立刻判断全局模式）
2) flush 决策日志（可控、可按 dispatcher 定位）
3)（可选）按需调试查询（用于复杂 case / 多 dispatchers）
