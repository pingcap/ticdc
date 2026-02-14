# Cloud Storage Sink GA - Implementation（实施文档）

- Date: 2026-02-04
- Scope: Cloud Storage Sink only (+ minimal dispatcher glue)
- Status: Draft

## Table of Contents
- [0. 实施原则](#0-实施原则)
- [1. 交付物](#1-交付物)
- [2. 里程碑与任务拆分](#2-里程碑与任务拆分)
- [3. 代码改动点（按模块）](#3-代码改动点按模块)
- [4. 测试计划](#4-测试计划)
- [5. 风险与缓解](#5-风险与缓解)
- [6. 发布与回滚](#6-发布与回滚)

---

## 0. 实施原则

- P0：checkpoint 语义不变（远端成功后才 `PostFlush()`）。
- P1：DDL 仍是强阻塞事件（BlockEvent）。每个 involved dispatcher 在进入 maintainer barrier **之前**必须调用 `sink.PassBlockEvent(ddl)` 并等待返回，确保 `<T` DML 已完成确定性 drain（DrainMarker 覆盖 encoding queue + spool + remote）；对于不需要 barrier 的 DDL，同样在调用 `WriteBlockEvent` 之前调用 `PassBlockEvent`。
- P2：DML 仍 await=true（dispatcher 语义不变），采用 **two-stage ack**：wake 从 “flush 完成” 提前到 “入 spool 成功（且水位允许）”，但 checkpoint 仍严格由“远端写成功”触发 `PostFlush()`；背压仅由 spool 的 internal watermarks 控制 wake（不需要用户配置）。
- P3：所有 1000 dispatchers 相关逻辑必须只扫描活跃 dispatcher；per-dispatcher 状态必须可回收。
- P4：spool 为内置实现（包含 memory tier + disk spillover），无需用户配置；并通过 internal watermarks 在远端变慢时提供可控背压与缓冲。

---

## 1. 交付物

- 文档：
  - `2026-02-04-cloudstorage-sink-ga-requirements.md`
  - `2026-02-04-cloudstorage-sink-ga-design.md`
  - `2026-02-04-cloudstorage-sink-ga-implementation.md`
  - `2026-02-04-cloudstorage-sink-ga-ddl-behavior.md`
  - `2026-02-04-cloudstorage-sink-ga-spool.md`
  - `2026-02-04-cloudstorage-sink-ga-task-breakdown.md`
- 代码：
  - spool（承载 encoded payload；包含 memory tier + disk spillover）
  - early wake（入 spool 成功 + 水位允许）
  - DDL drainMarker（覆盖 encoding queue + spool + remote）
  - active set + 状态回收（支持 1000 dispatchers）
  - 指标
- 测试：
  - 单测覆盖 checkpoint/DDL drain/watermark/active set
  - （可选）小型集成/模拟测试

---

## 2. 里程碑与任务拆分

> 更细粒度的工程任务拆分（建议按 PR 推进）见：`docs/plans/2026-02-04-cloudstorage-sink-ga-task-breakdown.md`。

### M1. 现状核对与 correctness bug 清理（必须先做）
- 目标：在改语义之前，先清掉 cloudstorage pipeline 中明显的并发/资源隐患（例如 goroutine 循环变量捕获、共享 channel close 等）。
- 产出：修复 + 单测（如能覆盖）。
- 备注：实现侧需要把 cloudstorage sink 的 `defaultEnableTableAcrossNodes` 默认值修正为 false（与 scheduler 默认一致）。文档先按默认 false 对齐。

### M2. two-stage ack 直接落地（拆分 wake 与 PostFlush，并保证 DDL 正确）
该里程碑对应本 GA 的核心语义改造，要求一次性把以下内容打通并在 cloud storage sink 上生效（具体拆分见 task breakdown 文档）：
- DML two-stage ack：
  - enqueue ack（wake）：在 payload encode 完成并成功入 spool 且水位允许时触发 `DMLEvent.PostEnqueue()`（名称以实现为准）；
  - flush ack（checkpoint）：远端写成功后才触发 `DMLEvent.PostFlush()`（语义不变）。
- 事件模型与 dispatcher glue：
  - `DMLEvent` 增加 enqueue ack hook（例如 `AddPostEnqueueFunc`/`PostEnqueue`）；
  - dispatcher 把 dynstream wakeCallback 注册到 enqueue ack hook（保持 await=true）。
- sink 内承载与背压：
  - 引入 spool（memory tier + disk spillover）并提供 bytes 记账；
  - internal watermarks/hysteresis：高水位抑制 wake，把背压留在 dynstream；水位回落后恢复 wake。
- 编码与路由对齐最终形态：
  - 移除 `defragmenter` 的“编码后全局重排 + 缓存已编码大对象”路径；
  - 编码侧改为按 shard 保序（keyed shards），直接 append 到 spool。
- DDL 顺序正确性（必须与 early-wake 同期落地）：
  - sink interface 增加 `PassBlockEvent`；
  - dispatcher 在进入 barrier / WriteBlockEvent 之前调用并等待 `PassBlockEvent(ddl)`；
  - cloudstorage sink 内部用 `DrainMarker` 覆盖 encoding queue → spool → remote 的 `<T` DML。

### M3. 1000 dispatchers 稳定性专项（active set + 状态回收）
- 保证周期性调度只遍历活跃 dispatcher，避免每 tick 扫全量 dispatchers。
- per-dispatcher 状态可回收（TTL/LRU 或随 writer 生命周期释放），避免常驻内存增长。

### M4. 可观测性与测试（GA 必须）
- 指标：spool/wake/flush/ddl drain 的关键指标（见 design/task breakdown）。
- 单测：覆盖 two-stage ack 不提前推进 checkpoint、watermarks 抑制 wake、DDL drain 覆盖 encoding/spool/remote、active set 行为。

---

## 3. 代码改动点（按模块）

> 注：以下是“预期改动点”，最终以实际代码为准。

### Cloud Storage Sink
- `downstreamadapter/sink/cloudstorage/sink.go`
  - `PassBlockEvent`：新增 DDL drain（DrainMarker）入口（同步等待 drain 完成）
  - `WriteBlockEvent`：保持写 DDL schema file，不承担跨节点 drain 协议
- `downstreamadapter/sink/cloudstorage/`（新增/修改若干文件）
  - `spool/` 或 `spool.go`：spool manager（bytes 记账、active set、队列）
  - `item.go`：定义 `dataItem`、`drainMarkerItem`
  - encoder 相关文件：按 shard 保序、输出 `dataItem`、处理 drain marker（并移除 `defragmenter` 路径）
  - writer 相关文件：按 dispatcher 聚合、flush reason、处理 drainMarkerItem 强制 flush
- `downstreamadapter/sink/sink.go`
  - Sink interface：增加 `PassBlockEvent`（其他 sink 默认 no-op）

### Dispatcher（最小 glue，不影响其他 sink）
- `downstreamadapter/dispatcher/basic_dispatcher.go`
  - 保持 DML await=true；把 wake 从 flush ack（`PostFlush()`）前移到 enqueue ack（`PostEnqueue()`）：
    - dispatcher 把 dynstream `wakeCallback`（`sync.Once` 去重）注册到 `DMLEvent` 的 enqueue ack hook（`AddPostEnqueueFunc`，名称以实现为准）；
    - sink 在“事件成功入 spool 且水位允许”时触发 `DMLEvent.PostEnqueue()` 来 wake dynstream。
  - 在 DDL block event 路径上调用 `sink.PassBlockEvent(ddl)`（进入 maintainer barrier 之前），失败则报错退出

### 可选：path/state 回收
- 若涉及 `pkg/sink/cloudstorage/path.go`（或等价路径状态模块）
  - 给 per-dispatcher state 增加回收策略（TTL/LRU/生命周期释放）

---

## 4. 测试计划

### 单元测试（建议）
- `TestEarlyWakeDoesNotAdvanceCheckpoint`
  - 模拟远端写很慢/失败：wake 发生不代表 PostFlush 发生
- `TestDDLPassBlockEventCoversEncodeQueue`
  - 构造：DDL 到来时有一部分 `<T` DML 在 encoder queue 未入 spool
  - 断言：`PassBlockEvent` 返回前不会进入 maintainer barrier（可用 mock/钩子验证）；`PassBlockEvent` 返回后 `<T` DML 已远端落盘
- `TestDDLDrainAcrossDispatchersBeforeWrite`（多 dispatcher / 跨节点语义）
  - 构造：多个 dispatchers 都收到同一 DDL；只有一个最终 `WriteBlockEvent` 写 DDL
  - 断言：DDL schema file 写入远端必须发生在所有 involved dispatchers 的 `<T` DML 远端落盘之后
- `TestWatermarkSuppressesWake`
  - spoolBytes > high 时 wake 不触发；降到 low 恢复
- `TestActiveSetOnlyProcessesActiveDispatchers`
  - 在 totalDispatchers 很大但 activeDispatchers 很小的情况下，writer 处理次数与 activeDispatchers 相关

### 小型集成（可选）
- mock storage：记录写入顺序（data/index/ddl），验证 `<T` DML -> DDL 顺序。

---

## 5. 风险与缓解

- R1：early wake 使更多数据堆在 sink 内，若 internal watermarks/spool 不生效，可能出现内存或磁盘耗尽
  - 缓解：internal watermarks 必须作为 GA 必须项（抑制 wake 把背压留在 dynstream）；spool 的 disk spillover 仅作为降低内存峰值的手段，必须有明确的内置磁盘上限与可观测性。
- R2：DDL drain 若只 flush spool，不覆盖 encoding queue，会产生“以为 flush 完但其实没 flush”的错序
  - 缓解：DrainMarker 必须穿透 encoding→spool→remote。
- R3：DDL drain 调用时机不正确（例如只在 barrier 场景调用、或只在 `WriteBlockEvent` 内部 drain）
  - 风险表现：拆表/多 dispatcher 场景下，只有一个 dispatcher 执行 `WriteBlockEvent`，其他 involved dispatchers 的 `<T` 在途数据不会被覆盖，仍可能导致 DDL schema file 早于 `<T` DML 落远端。
  - 缓解：实现必须按文档规则统一落地：
    - barrier DDL：每个 involved dispatcher 在 `reportBlockedEventToMaintainer()` **之前**调用并等待 `sink.PassBlockEvent(ddl)`；
    - 非 barrier DDL：在 `WriteBlockEvent()` **之前**调用并等待 `sink.PassBlockEvent(ddl)`；
    - 并且该逻辑必须跑在 block event executor/后台任务中，避免阻塞 dynstream handler goroutine。
- R4：`PassBlockEvent` 实现若在入口处发生阻塞（例如向 unbuffered channel 写 marker），可能反向阻塞 dispatcher 线程/后台任务，造成级联卡住
  - 缓解：`PassBlockEvent` 必须是 non-blocking enqueue + 同步等待 ack 的组合：入口 enqueue 必须走 buffered/unlimited channel 或内部队列；真正等待发生在 ackCh 上。
- R5：1000 dispatchers 下 per-dispatcher 状态不回收导致常驻内存增长
  - 缓解：必须落地 TTL/LRU/生命周期释放 + active set。
- R6：`TableProgress` 内部元素可能过多（early-wake 后 DML 会更快进入 tableProgress），导致内存/时间开销增加
  - 缓解：本 GA 暂不处理（见需求文档非目标 NG4）；先通过观测与压测评估影响，后续按需要优化（例如聚合/压缩 tableProgress 内部表示或引入更轻量的 ack 结构）。
- R7：部分 DDL 可能导致写入多个 schema 文件（或影响多个表），若 `BlockedTables/NeedAddedTables/NeedDroppedTables` 覆盖不全，可能遗漏 involved dispatchers
  - 缓解：
    - 实施阶段明确 cloud storage sink “会写 schema file 的 DDL types”清单，并与 `BlockedTables` 填充逻辑对齐；
    - 对 multi-table / DB/All DDL 坚持走 barrier；保证所有 involved dispatchers 都执行 `PassBlockEvent`；
    - 对于会写多个 schema 文件的 DDL，写入者 dispatcher 负责写完全部 schema file，再触发 `ddl.PostFlush()`。
- R8：`NotSync=true` / `BlockedTables=nil` 等 DDL 的语义分叉可能导致“该不该 drain”判断不一致
  - 缓解：统一按“是否会写 schema file”决策：
    - 若该 DDL 不会写 schema file（例如被过滤的 `NotSync`）：`PassBlockEvent` 可以是 no-op（避免无谓 drain）；
    - 若会写 schema file：无论是否走 barrier，必须先 `PassBlockEvent` 再写入。
- R9：disk spillover 带来新的故障面（磁盘写入抖动、磁盘空间不足、残留文件清理）
  - 缓解：
    - disk tier 采用低文件数的 segment 结构（按 shard），并在启动时清理历史残留；
    - 磁盘空间不足时优先抑制 wake（背压留在 dynstream），并输出清晰的错误/告警；避免静默卡死或无界占用。

---

## 6. 发布与回滚

- GA 行为：early wake 是必须项，不提供独立开关。
- 回滚：不提供“回到旧 wake 语义”的配置项；如需回滚需通过版本回退实现。
