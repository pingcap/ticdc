# Cloud Storage Sink GA - Requirements（需求文档）

- Date: 2026-02-04
- Scope: TiCDC Cloud Storage Sink（仅 cloud storage sink + 最小 dispatcher glue）
- Target: 支持 1000 个 dispatcher（同步单元）
- Status: Draft

## Table of Contents
- [1. 背景与问题](#1-背景与问题)
- [2. 目标与非目标](#2-目标与非目标)
- [3. 范围与约束](#3-范围与约束)
- [4. 术语](#4-术语)
- [5. 功能性需求（FR）](#5-功能性需求fr)
- [6. 非功能性需求（NFR）](#6-非功能性需求nfr)
- [7. 配置与默认值](#7-配置与默认值)
- [8. 验收标准](#8-验收标准)

---

## 1. 背景与问题

### 1.1 背景
Cloud storage sink（下称 storage sink）将下游数据写入对象存储，并产出 data/index/schema（DDL）等文件供下游消费。

### 1.2 当前问题（吞吐慢的根因）
已知现象（压测/线上观察总结）：
- 上游每发送一批 DML 就会 block，必须等这批数据真正 flush 到远端存储后才会 wake path 继续发送下一批。
- 导致单 path 的有效吞吐近似为 `batch_bytes / flush_interval`，尤其在“单表低流量、超多 dispatcher”场景下表现为 tick 限速，文件难以积累到接近 `file-size`。

### 1.3 GA 目标总体方向
- 解决“tick 限速”的低吞吐根因。
- 在不改变 checkpoint 语义的前提下，让文件大小尽量贴近 `file-size`，并保证 1000 个 dispatcher 长时间稳定运行。

---

## 2. 目标与非目标

### 2.1 目标（Goals）
G1. **规模稳定性（1000 dispatchers）**
- 在 1000 个 dispatcher 下长期运行，内存曲线可控，不出现状态无限增长导致的 OOM/退化。

G2. **吞吐改进（低流量/多 dispatchers）**
- 解除 “flush 完成才 wake path” 的耦合，使 storage sink 能持续接收数据、聚合编码后数据并尽量凑满 `file-size`。

G3. **文件大小策略（best-effort 贴近 file-size）**
- 活跃 dispatcher 的输出文件大小尽量贴近 `file-size`；低流量由 `flush-interval` 兜底。
- 保证稳定运行（远端慢时允许延迟/抖动，但不可 OOM）。

G4. **语义正确性（不可破坏）**
- checkpoint 语义不变：必须远端写成功后才能推进。

### 2.2 非目标（Non-goals）
NG1. 不引入需要用户配置/调参的 spool 模式与参数（spool 为内置实现，包含 memory tier + disk spillover，用户无需配置）。
NG2. 不修改 checkpoint 推进语义。
NG3. 不引入会改变 dispatcher DML 语义的机制：**DML 仍然保持 `await=true`（path 会 block）**。
NG4. 先不解决 `TableProgress` 内部元素可能过多的问题（作为后续优化项目；本 GA 重点保证 sink 侧“只扫描活跃 dispatcher”与可回收状态）。

---

## 3. 范围与约束

### 3.1 In-scope
- `downstreamadapter/sink/cloudstorage/` 内部架构调整、队列/写入策略、指标、单测。
- 允许对 `downstreamadapter/dispatcher/`（以及必要的事件模型）做**最小 glue**：
  - DML early-wake：不新增新的 DML sink interface 方法；wakeCallback 通过 DML 的 enqueue ack hook（例如 `PostEnqueue()`，名称以实现为准）从 dispatcher 传递到 sink；
  - DDL drain：sink interface 增加 `PassBlockEvent`（其他 sink 默认 no-op）；
  - 对其他 sink 行为无影响。

### 3.2 Out-of-scope
- 任何其他 sink（kafka/mysql/pulsar 等）行为不改。
- 远端对象存储侧特性（例如 server-side compose、远端事务/锁）不作为 GA 依赖。

### 3.3 关键语义约束
- C1. **checkpoint 只能在远端写成功后推进**（data/index 等 remote flush 成功之后才能触发 `event.PostFlush()`）。
- C2. DML dispatcher 语义不变：DML 处理仍是强阻塞（await=true），但 wake 的触发点从“flush 完成”提前到“入 spool 成功（且水位允许）”。该模型等价于 **two-stage ack**：enqueue ack（wake）与 flush ack（checkpoint）解耦。
- C3. DDL 是强阻塞事件：DDL 写远端完成之前不能 wake（仍然通过 block event 通道实现）。
- C4. DDL 的远端写入顺序必须成立：对于 DDL(commitTs=T)，任何 dispatcher 上 `<T` 的 DML 必须先远端落盘，再允许该 DDL 的 schema 文件写入远端（避免 stale 丢弃）。

---

## 4. 术语

- dispatcher（同步单元）：TiCDC 的同步/调度单元；本 GA 的规模口径与 flush/背压/顺序约束都按 dispatcher 独立生效。
- spool：storage sink 内部的缓冲层（memory tier + disk spillover），用于存放**编码后的** payload 以及少量控制类 item（例如 DDL 的 drain marker）。该机制为内置实现，用户无需配置。
- file-size：目标单文件大小（默认 64MiB，best-effort 贴近）。
- flush-interval：低流量兜底 flush 周期。
- two-stage ack：对 DML 拆分为两阶段确认：enqueue ack（仅用于 wake/解除 path 阻塞）与 flush ack（远端成功后触发 `PostFlush()` 推进 checkpoint），二者语义严格分离。
- BlockEvent：强阻塞事件（DDL 由 `WriteBlockEvent` 写入远端；为了 DDL drain，dispatcher 还会调用 `PassBlockEvent` 通知 sink）。
- DrainMarker：storage sink 内部独立 item 类型，用于保证 “DDL 前 flush `<T` DML” 覆盖编码队列与 spool。
- PassBlockEvent：sink interface 新增方法，仅对 DDL 有意义。dispatcher 在**写入/上报 DDL 之前**调用 `PassBlockEvent(ddl)` 并等待返回：
  - 若该 DDL 需要 maintainer barrier 协调：则在**上报 maintainer barrier 之前**调用；
  - 若该 DDL 不需要 barrier（单 dispatcher 直接写）：则在调用 `WriteBlockEvent` 之前调用。
  返回成功表示本 dispatcher 的 `<T` DML 已经完成确定性 drain（覆盖 encoding queue + spool + remote）。
  - 说明：即使该 DDL 不需要 barrier，也必须 `PassBlockEvent`。因为 early-wake 后 DDL 到达 sink 时，`<T` 的 DML 可能仍在 encoding queue/spool/未落远端；若直接 `WriteBlockEvent` 写 schema file，会破坏 C4 的远端顺序约束并触发 stale 丢弃风险。

---

## 5. 功能性需求（FR）

FR1. `AddDMLEvent`（或等价入口）必须 non-blocking。
- 注：Go 语义上 unbuffered channel send 会阻塞，因此实现上应使用 buffered/unlimited channel 或异步入队。

FR2. **wake 与 checkpoint 推进解耦**
- DML：在“编码后 payload 进入 spool 成功（且水位允许）”时触发 wakeCallback。
- checkpoint：仍严格由远端写成功触发 `PostFlush()`。

FR3. **文件大小贴近策略**
- 对活跃 dispatcher：尽量通过 `file-size` 触发 flush，减少小文件。
- 兜底：`flush-interval` 到期必须 flush，避免低流量无限不落盘。

FR4. **DDL 处理（简化语义 + 必须覆盖在途数据）**
收到 DDL(commitTs=T) 时，必须保证 DDL 写入远端之前，所有 `<T` 的 DML 已远端落盘。为兼容 “拆表跨节点 / 多 dispatcher” 场景，本 GA 将 DDL 顺序约束落在 **per-dispatcher drain** 上：

1) dispatcher 在进入 DDL 处理流程之前调用 `sink.PassBlockEvent(ddl)` 并等待其返回：
   - 若该 DDL 需要 maintainer barrier 协调：则在**上报 maintainer barrier 之前**调用；
   - 若该 DDL 不需要 barrier：则在调用 `WriteBlockEvent` 之前调用；
   - 解释：不需要 barrier 的 DDL 也必须先 `PassBlockEvent`，目的不是“跨节点协同”，而是保证 drain 覆盖 encoding queue→spool→remote，避免依赖旧的“flush 完才 wake”隐含顺序。
2) `PassBlockEvent` 返回成功表示：该 dispatcher 范围内 commitTs `<T` 的 DML 已经完成确定性 drain，并且该 drain 覆盖：
   - 已编码进入 spool 的 `<T` DML；
   - 仍在 encoding queue/worker 尚未进入 spool 的 `<T` DML；
   => 通过 DrainMarker 机制保证 drain 的确定性（覆盖 encoding→spool→remote）。
3) 对于需要 maintainer barrier 的 DDL：由于每个 involved dispatcher 仅在 `PassBlockEvent` 成功返回后才会上报 maintainer barrier，因此 maintainer 进入 barrier 协调时可视为所有 involved dispatchers 的 drain 已完成；随后 maintainer 选择一个 dispatcher 执行 `WriteBlockEvent` 真正写入 DDL schema 文件；
4) DDL 仍为强阻塞事件：DDL 写远端完成之前不能 wake。

FR5. **影响范围（由 barrier 决定）**
- DDL 的影响范围由 dispatcher/maintainer 的 barrier 机制决定；本 GA 不要求 sink 枚举所有 affected dispatchers。
- 对每个收到该 DDL 的 dispatcher：`PassBlockEvent` 只负责 drain “本 dispatcher 范围内的 DML”。对于多表/跨 dispatcher 的 DDL，barrier 机制应确保所有 involved dispatchers 都会执行各自的 `PassBlockEvent`。

FR6. **1000 dispatchers 稳定性**
- 周期性调度/flush 扫描必须与活跃 dispatcher 数成正比，避免每 tick 扫全量 dispatchers。
- per-dispatcher 状态必须可回收（TTL/LRU 或随 writer 生命周期释放），不得无限增长。

---

## 6. 非功能性需求（NFR）

NFR1. 正确性：不得出现 checkpoint 提前推进；不得出现 DDL 早于 `<T` DML 落远端。
NFR2. 稳定性：远端很慢时允许高延迟与抖动，但必须通过水位背压保证不 OOM。
NFR3. 可观测性：提供足够指标定位慢点（spool/flush/ddl drain）。
NFR4. 兼容性：默认行为可平滑升级。

---

## 7. 配置与默认值

> 说明：本 GA 的原则是“不新增需要用户理解/调参的配置项”。因此本节仅列出沿用的关键配置，并说明默认口径。

### 已有关键配置（沿用）
- `file-size`：默认 64MiB
- `flush-interval`：沿用现有默认
- `enable-table-across-nodes`（scheduler）：本 GA 默认按 **false** 理解（不拆表跨节点）。实现侧需确保 cloudstorage sink 的默认值与 scheduler 默认一致（当前 `pkg/sink/cloudstorage/config.go` 的 `defaultEnableTableAcrossNodes` 取值与此不一致，需要在实施阶段修正）。

### 本 GA 不新增用户侧配置
- spool（包含 memory tier + disk spillover）为内置实现，不暴露模式选择开关，也不要求用户配置水位参数。
- 背压仍然存在且必须有效：实现会在内部维护 watermarks，并在水位过高时抑制 wake，让 backlog 留在 dynstream（语义见设计/实施文档）。

---

## 8. 验收标准

### Correctness
- AC1. checkpoint 仅在远端写成功后推进（通过 mock storage 注入失败验证不触发 `PostFlush()`）。
- AC2. 单 dispatcher 场景：DDL 前 `<T` 的 DML 必须已落远端（通过 drain marker ack 语义与顺序校验验证）。
- AC3. 多 dispatcher / 跨节点场景：所有 involved dispatchers 在进入 maintainer barrier 前都必须完成 `PassBlockEvent`（drain 完成）；并且 DDL schema file 写入远端必须发生在所有 `<T` DML 远端落盘之后。

### Performance / Behavior
- AC4. 低流量多 dispatchers 时，不再被 flush tick 人为限速；活跃 dispatcher 能持续聚合到接近 `file-size`。
- AC5. flush reason 指标显示：活跃 dispatcher 的 flush 主要由 size 触发；低流量由 interval 触发。

### Scale
- AC6. 1000 个 dispatcher 场景下，内存与 per-dispatcher 状态不随时间无限增长；调度开销与活跃 dispatcher 数成正比。
