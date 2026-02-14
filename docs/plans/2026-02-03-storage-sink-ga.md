# Storage Sink (CloudStorage) GA Implementation Plan (Two-stage Ack + Optional Spool)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 系统性增强 storage sink（cloud storage sink）及其必要的上游调度语义，使其具备**稳定同步 100 万张表（1,000,000 tables/table-spans）**的能力：不 OOM、不全局卡死、可观测、可回滚，并尽可能让 data file 大小贴近 `file-size`（默认 64MiB）。

为避免丢失讨论结论，本计划明确两点核心语义：

1. **Checkpoint 语义不变（必须坚持）：** DML/DDL 的 `event.PostFlush()`（从而 `TableProgress.Remove()` 推进 checkpoint）必须在 **写入远端存储成功之后**才允许触发；
2. **新增 Enqueue/Wake 语义（吞吐关键）：** 对于 DML，允许在 **sink 内部完成 enqueue（memory/disk spool）** 后就唤醒 dynstream path 继续发送下一批（不必等待远端 flush）。

**Architecture:** 引入“two-stage ack”并保留原 checkpoint 语义：

- **Enqueue ACK（wake）**：DML 在 sink 内部 enqueue 完成后即可 wake path（解除 `await=true`），用于把上游从“远端写入耗时”中解耦出来，尽可能凑满 `file-size`；
- **Flush ACK（checkpoint）**：DML 数据文件 + index 文件写入远端存储成功后，才触发 `event.PostFlush()`（从而推进 `TableProgress` 与 checkpoint），保证语义不变；
- **DDL Barrier**：storage sink 不处理 SyncPoint，本 GA 只考虑 DDL。写入 DDL 到远端存储之前，必须确保该 DDL 之前的 DML 已经 flush 完成（避免“新 schema 先写入导致旧 DML 被误判为 stale 而丢弃”）。

在此基础上，对 cloudstorage sink 内部完成三类增强：

1. **去掉/替代 defragmenter 的全局顺序 barrier**，避免全局 HO-L（head-of-line）与 `future` map 无上界导致的内存/GC 风险；
2. **让 flush/ack 延迟在“单表低流量 & 海量表 fanout”场景下也可控**（避免被 `flush-interval` tick-limited 反复放大）；
3. **把 per-table 状态与 I/O 成本做“可控增长”**：缓存可回收、冷启动/大量新表触达时不爆炸，并补齐关键监控。

**Tech Stack:** Go；`github.com/pingcap/tidb/br/pkg/storage.ExternalStorage`；TiCDC codec（`TxnEventEncoder`）；Prometheus metrics；现有 cloudstorage sink 单测框架（`testify/require`）。

---

## 0. Scope & Hard Constraints

**允许修改（本 GA 的最小必要范围）：**

- `downstreamadapter/sink/cloudstorage/**`
- `pkg/sink/cloudstorage/**`
- `downstreamadapter/sink/metrics/cloudstorage.go`（仅新增/补齐 cloudstorage sink 指标，不改其他模块）
- `downstreamadapter/dispatcher/**`（仅限为 two-stage ack 拆分 wake vs checkpoint、以及 DDL barrier 所需的 drain 等待机制）
- （如需要）`downstreamadapter/sink/sink.go`（仅限新增可选扩展接口，保持其他 sink 不受影响）

**禁止修改（除非后续明确扩展范围）：**

- event-collector / dynstream 核心实现（例如 `downstreamadapter/eventcollector/**`、`utils/dynstream/**`）
- 全局配置结构与其他 sink 的行为（例如 Kafka/MySQL sink）

**Non-goals（本 GA 不做）：**

- 不改变产物语义：仍按“schema file + data files + index file”输出，不引入跨表合并文件或新的消费协议。
- 不把“调参（flush-interval/file-size）”当作专门解决方案（可以在 GA 文档里给推荐值，但不能依赖它修复根因）。

---

## 1. Inputs: storage-sink*.md 内容整合（共 4 份）

本实施文档整合了以下文件（位于 repo 根目录）：

1. `storage-sink-slow.md`
2. `storage-sink-monitoring.md`
3. `storage-sink-encoder-group.md`
4. `storage-sink.md`

### 1.1 `storage-sink-slow.md`（吞吐低根因）

核心结论：

- dynstream 的 `await/wake` 语义使得 dispatcher **很容易把“flush 完成”当作每表放行节拍**。
- 在现状代码里，dispatcher 只要处理到 DML 就倾向 `await=true`，wakeup 依赖 `PostFlush` + `tableProgress.Empty()`。
- cloudstorage writer 的 flush 条件是 **per-table `file-size`（默认 64MiB）** + **`flush-interval`（默认 5s）**；
  - 单表流量不够大时几乎触发不了 size flush，于是整体被 tick-limited。
- 监控上常表现为：写出 bytes/s 很低、dynstream pending/扫描开销上升，但“flush duration”看似很小（可能主要反映 index file 写）。

该文提出的 dispatcher 侧 in-flight budget 方案（3.2）是“让上游不卡住”的关键方向之一。本 GA 选择采用更明确的 **two-stage ack**：

- 通过 **enqueue ack** 提前 wake path，让上游继续发送下一批；
- 通过 **flush ack**（远端写入完成）推进 checkpoint；
- 通过 **spool 水位 / budget** 做稳定性闸门，避免把压力无上界搬到内存/磁盘。

### 1.2 `storage-sink-monitoring.md`（监控与瓶颈画像）

核心信息：

- defragmenter 的 `future map[uint64]eventFragment` 无上界，且 `eventFragment` 持有编码后的大块 byte slice，存在 **OOM/GC 风险**。
- sink 内部有多处“无界队列”（msgCh、writerInputCh 等），一旦上游持续投喂会把压力搬进进程内存；应补监控/约束。
- writer 的 flush 行为是“写 data file + 重写 index file”，小文件会放大对象存储 API 请求数。
- schema check 的 `FileExists` + `WalkDir` 在对象存储上可能很贵，表多时可能成为常态成本。
- 建议新增：flush reason、sink backlog、active tables fanout、拆分 data/index/schema 写入耗时等（低 label 基数）。

### 1.3 `storage-sink-encoder-group.md`（移除 defragmenter 的两种选择）

核心信息：

- 现状链路：并发编码输出乱序 → defragmenter 用全局 seq 重排 → writer。
- 移除 defragmenter 的两个方向：
  - **A：全局有序**（更接近现状语义，但 HO-L 不可避免）
  - **B：按表有序**（只保证同一 `VersionedTableName` FIFO，跨表不排队，更并行，适合多表 skew）
- 正确性约束强调：
  - 必须避免同一 index file 被多个 writer 并发写（尤其 `EnableTableAcrossNodes=false` 时 index 为 `meta/CDC.index`）。
  - 同一 `VersionedTableName` 的消息不能乱序，否则文件内容/文件序号语义不可推理。

### 1.4 `storage-sink.md`（模块总览 + delete 性能分析）

核心信息：

- 总体结构是“先编码 →（全局 seq 重排）→ 按表路由到 writer → 按表聚合 → 写外部存储”。
- 主要瓶颈类型：
  - 编码 CPU/GC（协议差异显著，delete 特别重）
  - defragmenter 的全局 HO-L（一个慢编码 fragment 会卡住其他表）
  - writer 分片规则导致单表热点上限（尤其 delete 常是单表热点）
  - 小文件与 index 重写导致对象存储请求放大
- 其中 “移除 defragmenter 的全局 barrier” 是对多表并发/尾延迟最关键的结构性优化之一。

---

## 2. GA Target: “同步 100 万张表”的定义与验收口径

为了让实施可落地，先把“100 万张表”拆成可验证的指标。

### 2.1 我们假设的 workload（可调整）

- 表总数：`N_tables = 1,000,000`
- 每表事件频率：极度稀疏（很多表可能只有少量 DML），但整体聚合吞吐可很高。
- 单表热点：仍可能存在（例如少量热表 delete/update）。
- 存储后端：
  - `file://`（本地文件系统）作为压测/回归基线
  - 对象存储（S3/GCS/Azure）作为真实 GA 场景

### 2.2 成功标准（GA 验收）

**稳定性（必须）**

- 不出现 sink 内 OOM（尤其 defragmenter `future`、per-table map、无界队列导致）。
- 不出现“全局卡死”：单个表/单个慢编码不能让整个 changefeed 停滞。
- 回调（`PostFlush`）最终能触发，dispatcher 不会永久 blocked。

**可观测性（必须）**

- 能从指标判断瓶颈是：编码/写存储/小文件/active tables fanout/backlog。
- 关键指标 label 基数受控（禁止按 table 打 label）。

**性能（目标）**

- 在 “海量表 + 低单表流量” 场景下，端到端 ack 延迟不被 `flush-interval` 固定锁死（不能出现“每表 5s 才推进一次”这种节拍化）。
- 在对象存储场景下，小文件/高频 index 写的放大效应有可控的策略与明确的观测。

---

## 3. Approach Options

### 3.1 选项 A：只靠调参（不推荐，仅作为对照组）

- 调小 `flush-interval`、调小 `file-size` 可以缓解 tick-limited，但会导致小文件爆炸和对象存储成本/列举性能问题。
- GA 不能依赖这种方式解决根因，只作为回归验证与临时缓解。

### 3.2 选项 B（推荐）：Two-stage ack + Spool（dispatcher + sink 联动）

核心点：

1. **拆分 wake 与 checkpoint**：DML enqueue 后即可 wake；远端 flush 后才允许 `PostFlush` 推进 checkpoint；
2. **可选 spool**：sink 支持 `memory`（默认）与 `disk`（可配置）两种 spool，用于内部 batch、降低内存峰值、支撑 1M 表 fanout；
3. **DDL barrier**：DDL 写入远端前必须等待本 dispatcher 的 DML flush 完成，并触发一次“强制 flush”把 DDL 之前的 DML 立刻推到远端，避免 schema 版本越界导致的 stale DML 丢弃，同时减少 DDL 等待时间。

在此之上，再叠加原有 sink 内结构性增强（移除 defragmenter、优化 flush、回收 per-table 状态、补齐监控）。

核心点：

1. **移除 defragmenter**（或至少默认不走它），把顺序约束从“全局”降级到“按表/按 key”；
2. **把“flush/ack”做成事件驱动/受控延迟**，避免在“单表无法凑满 file-size”的情况下被 tick-limited；
3. **把 per-table 状态做成可回收的 cache**，避免 100 万表规模下长期占用巨量内存；
4. **补齐监控**，让上述机制可验证、可回滚。

### 3.3 选项 C：只做 sink 内增强但不改 dispatcher（不推荐）

如果不做 dispatcher 拆分，wake 仍然绑定在 `PostFlush + tableProgress.Empty()`，则上游无法摆脱远端写入耗时，单表/海量表场景容易被 tick-limited，难以达成“尽量凑满 file-size”的目标。

---

## 4. Design: Two-stage Ack + Sink Enhancements

### 4.0 Dispatcher/Sink Contract: Two-stage Ack（核心语义拆分）

#### 4.0.1 DML：enqueue 后 wake（不推进 checkpoint）

目标：`HandleEvents()` 对 DML 返回 `await=true`，但 path 阻塞时间仅覆盖“enqueue/spool”而非“远端 flush”。

设计要点：

- dispatcher 在收到一批 DML 时：
  - 仍先 `tableProgress.Add(event)`，确保 checkpoint 语义仍以 flush ack 为准；
  - 不再把 `wakeCallback` 绑定在 `event.PostFlush()` 上；
  - 改为把 `wakeCallback`（或 batch-wake 聚合后的函数）传给 sink，让 sink 在 enqueue 完成后回调唤醒 dynstream path。
- sink 在内部完成 enqueue（memory spool 或 disk spool）后：
  - 调用 enqueue-wake 回调唤醒 path；
  - 但 **绝不调用** `event.PostFlush()`（否则会提前推进 checkpoint，违反语义）。

> 备注：`AddDMLEvent` / “enqueue API” 必须是 non-blocking 的（不能在 dispatcher/dynstream goroutine 里做磁盘 IO 或远端 IO）。unbuffered channel 的 send 会阻塞，因此不适合作为 non-blocking enqueue 实现。

#### 4.0.2 DML：远端 flush 后 PostFlush（推进 checkpoint）

writer 完成 “data file + index file” 写入远端存储后，才会触发消息的 `Callback`，最终调用到 `event.PostFlush()`，从而：

- `TableProgress.Remove()` 执行（由 `PushFrontFlushFunc` 注入），推进 checkpoint；
- 其他 flush 后逻辑继续生效（例如统计、监控等）。

#### 4.0.3 Backpressure：以 spool 水位作为稳定性闸门

two-stage ack 会把上游速度从远端写入耗时中解耦出来，因此必须引入稳定性闸门，避免无上界积压：

- **memory spool**：以 `spoolMaxBytes`（或 `spoolMaxEvents`）为上限；超过上限时 sink 暂停触发 enqueue-wake，使 path 停在 dynstream（等待消费/flush）；
- **disk spool**：以 `spoolMaxBytes` 及磁盘水位为上限；同样通过“延迟 wake”实现 backpressure。

#### 4.0.4 Spool：两种实现（memory 默认 + disk 可选）与 changefeed 级别配置

我们实现两种 spool，默认 `memory`，并提供 changefeed 级别配置项让用户按场景选择：

- **Memory spool（默认）**
  - 适合：表数较多但总体写入速率可控、机器内存充足、希望部署简单的场景；
  - 形态：sink 内部以有界内存队列保存“编码后的 payload”（或编码前 event 但需要注意内存占用），由后台 worker 聚合到 writer；
  - 保护：以 `spoolMaxBytes`（必选）作为硬上限，超过上限即停止 wake。
- **Disk spool（可选）**
  - 适合：极端海量表 fanout、远端存储抖动/慢写导致 backlog 很大、但希望进程仍稳定运行的场景；
  - 形态：编码后的数据写入本地磁盘（append-only segments / 文件队列），后台 uploader 读取并聚合后写远端；
  - 语义：disk spool **不是 checkpoint 的新边界**（checkpoint 仍然远端 flush 后推进）；崩溃/重启后 spool 丢失不会破坏一致性，因为 checkpoint 没前进，上游会重放缺失的数据。

**建议的配置项（changefeed 级别，建议通过 sink URI 或 changefeed config 下发）：**

- `dml-wake-mode = enqueue | flush`
  - `enqueue`：启用 two-stage ack（推荐默认）
  - `flush`：回退到 legacy 行为（wake 绑定远端 flush）
- `spool.type = memory | disk`（默认 `memory`）
- `spool.max-bytes`（memory/disk 共用；建议默认值与机器规格相关，例如 4–32GiB）
- `spool.dir`（仅 disk；默认可为 `${data-dir}/ticdc/cloudstorage-spool/<changefeed>`）

> 注：实现上需要确保 `AddDMLEvent` / enqueue API 仍然是 non-blocking：disk IO 必须在后台 goroutine 中完成，dispatcher/dynstream goroutine 只负责“提交 enqueue 请求 + 返回 await=true”。

#### 4.0.5 DDL：barrier（强制 flush + 严格远端写入顺序）

storage sink 不存在 SyncPoint，本 GA 只考虑 DDL。

在 two-stage ack（DML enqueue 后提前 wake）下，storage sink 可能已经接收并缓存了大量 DML（memory/disk spool、编码队列、writer buffer），但这些 DML 尚未写入远端存储。
此时如果直接写入 DDL 到远端，会触发 cloudstorage writer 的 “发现更新 schema version → 忽略旧 DML” 逻辑，存在数据丢失风险。

**目标语义（用户要求）：**

当 storage sink 收到某条 DDL（commit-ts = `T`）时：

1. 立刻触发把所有 **commit-ts < T** 的 DML 推送到远端存储（不再等待 `file-size` / `flush-interval`）；
2. 等上述 DML 全部远端落盘完成后，立刻发送该 DDL 到远端存储；
3. 在该 DDL 发送完成之前，不能把任何 **commit-ts > T** 的 DML 发送到远端存储（对该 DDL 影响范围内的表/dispatcher 成立）。

**落地策略：把 DDL 当成 “drain + freeze + execute” 的 barrier：**

- **Drain（排空）**：对 DDL 影响范围内的 dispatcher/table，进入 “drain mode”，强制把当前已接收的 DML 尽快 flush 到远端；
- **Freeze（冻结）**：在 DDL 完成之前，禁止远端侧写入更大 commit-ts 的 DML（通常通过阻塞 dispatcher 的事件流即可做到；如存在跨 dispatcher 的并发上传，还需 sink 内额外 gating）；
- **Execute（执行）**：在确认 drain 完成后再写 DDL 到远端。

**实现建议（dispatcher + sink 协作）：**

- dispatcher 收到 DDL（或从 maintainer 收到对该 DDL 的 Action_Write / Action_Pass）后，在真正调用 `sink.WriteBlockEvent(ddl)` 之前：
  - 调用 cloudstorage sink 的 “force flush” 接口（仅 storage sink 实现，其他 sink 不受影响），要求立刻 flush 该 dispatcher/影响范围内所有未远端落盘的 DML；
  - 等待 `tableProgress` 达到 “DDL 之前已 drain 完成” 的状态，再允许执行 DDL。
- “drain 完成” 的判断：
  - 对单 dispatcher：等待 `tableProgress.Empty()==true`；
  - 对拆表（同一逻辑表多个 dispatcher across nodes）：必须等待 **所有 involved dispatchers** 的 drain 都完成（由 maintainer barrier 协调，见后续 4.0.7）。

**重要备注：DDL barrier 可能导致小文件**

DDL 相对低频，允许在 barrier 时刻把未达到 `file-size` 的部分文件强制 flush（产生小文件）来换取正确性与 DDL 延迟。

#### 4.0.6 CheckpointTs message：不改变语义

changefeed 的 checkpoint-ts 仍应由 maintainer 基于各 dispatcher 的 flush ack（`TableProgress`）计算并下发。two-stage ack 只提前 wake，不会让 checkpoint 越过未远端落盘的数据。

---

#### 4.0.7 拆表跨节点：是否已支持（EnableTableAcrossNodes）

当开启 “拆表”（scheduler `enable-table-across-nodes=true`）时，一张逻辑表会被拆分成多个 table spans，并由多个 dispatchers 同步，这些 dispatchers 可能分布在不同 TiCDC 节点上。

**结论：cloud storage sink 的文件布局层面已具备基础支持，但 DDL barrier 需要在 two-stage ack 下加强 drain 约束。**

##### 4.0.7.1 远端文件布局：通过 dispatcherID 隔离 index/data，避免并发写冲突

cloudstorage sink 的 `Config.EnableTableAcrossNodes` 来自 changefeed 的 scheduler 配置（`cfg.EnableTableAcrossNodes`），开启后：

- data file 名包含 `dispatcherID`（`CDC_<dispatcherID>_<index>.<ext>`），不同 dispatcher 不会竞争同一个 data file；
- index file 名也包含 `dispatcherID`（`meta/CDC_<dispatcherID>.index`），不同 dispatcher 不会竞争同一个 index 文件；
- `VersionedTableName` 本身也携带 `DispatcherID`，writer 的聚合 key 能区分同一表的不同 dispatcher，从而并发安全。

因此，“同一表多个 dispatcher / 多节点并发写入同一表目录”在命名与文件粒度上是可行的。

##### 4.0.7.2 消费侧：已支持多 dispatcher index 文件与乱序 commit-ts

`cmd/storage-consumer` 在解析 `.index` 文件时会解析出 `dispatcherID`，并把同一表目录下的不同 dispatcher 视为不同的 file index key；
当 `EnableTableAcrossNodes=true` 时，consumer 允许 DML commit-ts 回退并按 commit-ts 插入（避免丢数据）。
这说明“跨节点多 dispatcher 的文件序列”是系统预期支持的场景。

##### 4.0.7.3 DDL：拆表场景必须走 maintainer barrier，并在 two-stage ack 下显式 drain

拆表场景下，单表 DDL 通常需要 **跨多个 dispatchers** 同步（同表不同 spans）。系统已有 maintainer `Barrier` 机制负责：

- 等待所有 involved dispatchers 报告该 DDL；
- 选择一个 dispatcher 执行写入（Action_Write），其他 dispatcher 走 pass；
- 等待所有 dispatcher 完成后再放行后续事件。

在 legacy（每批 DML flush 后才 wake）下，DDL 到达时通常“前序 DML 已经远端落盘”，因此不会出现 DDL 越过 DML 的风险。

但在 two-stage ack 下，DDL 到达时可能仍有大量 DML backlog 尚未远端落盘，因此必须额外保证：

- **每个 involved dispatcher** 在报告/执行 DDL 之前都已经完成 “DDL commit-ts 之前的 DML drain”；
- writer dispatcher 在真正 `WriteBlockEvent(ddl)` 之前，必须确认其他 dispatchers 的 drain 也已完成（通过 maintainer barrier 的汇聚信号/ACK 机制实现）。

这部分是“拆表跨节点场景在 two-stage ack 下仍然正确”的关键补强点。

---

### 4.1 编码链路：用“按表有序 encoder shards”替代 defragmenter（核心）

目标：解决 `defragmenter.future` 无上界 + 全局 HO-L 的稳定性问题，并提升多表并发下的尾延迟。

**选择：采用 `storage-sink-encoder-group.md` 的选项 B（按表有序）。**

#### 4.1.1 To-be 数据流

```
AddDMLEvent
  -> route by key(versionedTable) to encoderShard
    -> encoderShard worker (single goroutine, FIFO per shard)
      -> encode DMLEvent -> encodedMsgs
      -> route to writer by (schema, table) to avoid index concurrent write
        -> writer pipeline (aggregate + flush + callbacks)
```

#### 4.1.2 顺序与路由约束（必须满足）

- **同一 `VersionedTableName` 的事件顺序必须不乱。**
  - 依赖：同 key 落到同 shard 且 shard 串行处理。
- **同一 index file path 不得被多个 writer 并发写。**
  - 默认保持现状：`writerID = hash(schema, table) % workerCount`
  - 仅在明确安全的情况下再引入额外维度（例如开启 partition separator 时可考虑 `physicalTableID`，但要与 index path 一致）。

#### 4.1.3 迁移/回滚开关

建议引入一个 sink 内开关（优先放到 `pkg/sink/cloudstorage/config.go` 的 cloudstorage config 中）：

- `encoder-ordering = legacy | per-table`
  - `legacy`：encodingGroup + defragmenter（现状）
  - `per-table`：按表有序 encoder shards（新实现）

GA 默认建议：`per-table`，但必须保留 `legacy` 回滚路径。

---

### 4.2 writer flush 策略：避免 tick-limited 的“海量表节拍化”

在“不能改 dispatcher await 语义”的约束下，**每个表在任意时刻最多只有一个 batch in-flight** 这一事实很难改变。

因此，要避免 `flush-interval` 把吞吐锁死，唯一能在 sink 内做的，是让 **“拿到一个表的 batch 后尽快 flush 并触发 callback”**（否则 dispatcher 永远不会投喂下一批）。

#### 4.2.1 新增一个“受控 flush delay”策略（sink 内部，不依赖用户调参）

建议在 writer 内引入 `maxFlushDelay`（内部参数/可配置项均可，GA 先内部固定一个安全默认值，例如 100ms～500ms）：

- 对于每个 table 的聚合缓冲：
  - 仍保留 `file-size` 触发（达到就立刻 flush）
  - 仍保留 `flush-interval` 触发（作为上界）
  - **新增：table 的 oldest buffered event 等待超过 `maxFlushDelay` 时强制 flush**

这样在“1M 表低频”场景下：

- 每个表的 ack 延迟不会被 5s tick 固定锁死；
- 同时避免把 `flush-interval` 改得极小导致系统整体小文件/请求数爆炸（`maxFlushDelay` 可以做成更短，但由 sink 自己控制，后续可灰度）。

#### 4.2.2 fanout 保护：限制单次 batchedTask 持有的活跃表数

writer 目前的 `batchedTask.batch map[...]` 会随着“活跃表数”增长，存在规模风险。

建议加两个保护触发（任一触发就 flush 并 reset batchedTask）：

- `maxActiveTablesPerBatch`（例如 10k～50k，按内存评估）
- `maxBufferedBytesPerBatch`（例如 256MiB～1GiB，按后端与机器规格）

目的：避免一次 tick 把 100 万表都攒在内存 map 里。

---

### 4.3 Per-table 状态：FilePathGenerator 的内存可控增长

在 100 万表场景，per-table 状态缓存是必须的（否则每次都要读 index/schema，会把对象存储打爆），但缓存必须可回收。

#### 4.3.1 合并 `versionMap` 与 `fileIndex`（减少一半 map/key 开销）

现状 `FilePathGenerator` 同时维护：

- `versionMap map[VersionedTableName]uint64`
- `fileIndex map[VersionedTableName]*indexWithDate`

建议合并为单表状态：

```go
type tableState struct {
  schemaVersion uint64
  lastDate string
  fileIndex uint64
  lastAccess time.Time
}
map[VersionedTableName]*tableState
```

#### 4.3.2 TTL/LRU 回收（GA 必须）

建议实现一个低复杂度 TTL（比完整 LRU 更容易）：

- `stateTTL`：例如 30min～2h（可配置）
- 每次访问更新 `lastAccess`
- 定期（或在 map 超过上限时）回收过期状态

回收后再次触达该表：

- 通过 index file 读取恢复 fileIndex（现有逻辑）
- 通过 schema 文件检查恢复 schemaVersion（现有逻辑）

这保证：

- “历史触达集合”不再导致 map 无限增长；
- 1M 表 churn 大时仍能保持内存上界。

---

### 4.4 监控补齐（GA 必须，且低基数）

严格要求：**不按 table/dispatcher 打 label**。

按 `storage-sink-monitoring.md` 建议，优先补齐：

1. flush reason counters
   - `cloud_storage_flush_tasks_total{reason="interval|size|delay|fanout|bytes_cap"}`
2. sink backlog gauges
   - `cloud_storage_msg_queue_len`
   - `cloud_storage_writer_input_len{worker_id}`
   - `cloud_storage_to_be_flushed_len{worker_id}`
   - （legacy 路径）`cloud_storage_defragmenter_future_len`
3. active tables / fanout
   - `cloud_storage_active_tables{worker_id}`（每次 flush 时的 `len(batchedTask.batch)`）
4. 外部存储写入耗时拆分
   - `cloud_storage_data_write_duration_seconds`
   - `cloud_storage_index_write_duration_seconds`
   - `cloud_storage_schema_check_duration_seconds`

并明确当前“flush duration”可能偏向 index file 写的误读风险（见 `storage-sink-slow.md` 2.4）。

---

## 5. Testing Strategy（面向 GA 风险点）

### 5.1 单测（必须补）

- **按表有序 encoding shards**
  - 多表并发（随机 sleep）下：
    - 同一表输出顺序不乱
    - 不同表可以交错推进（证明无全局 HO-L）
- **flush delay/fanout cap**
  - 模拟“海量表每表只有 1 个 event”，确认不会等到 tick 才 flush（`maxFlushDelay` 生效）
  - 确认 `maxActiveTablesPerBatch`/`maxBufferedBytesPerBatch` 触发后能持续推进且不丢 callback
- **FilePathGenerator state TTL**
  - TTL 回收后再次写入，index 号不回退、不会覆盖旧文件名

### 5.2 基准/压测（GA 必须跑，但不要求在单测里跑 1M）

建议增加 benchmark（或独立压测工具）：

- 规模先从 10k/100k 表起，验证：
  - 内存曲线是否有上界（TTL 生效、无 future map）
  - ack 延迟曲线是否可控

> 1,000,000 表的全量压测建议走独立压测环境与对象存储真实后端，不建议塞进单测。

---

## 6. Implementation Plan（任务化，便于执行与回滚）

### Task 1: 增加 cloudstorage sink 关键指标

**Files:**
- Modify: `downstreamadapter/sink/metrics/cloudstorage.go`
- Modify: `downstreamadapter/sink/cloudstorage/writer.go`
- Modify: `downstreamadapter/sink/cloudstorage/dml_writers.go`
- (Optional) Modify: `downstreamadapter/sink/cloudstorage/defragmenter.go`（若保留 legacy 分支）

**Step 1: Write the failing test**
- 新增轻量单测，至少验证 metrics 能注册/更新（或以 `prometheus/testutil` 验证 counter/gauge 变化）。

**Step 2: Run test to verify it fails**
- Run: `go test ./downstreamadapter/sink/cloudstorage -run TestName -count=1`

**Step 3: Write minimal implementation**
- 增加 flush reason counter、backlog gauge、写入耗时 histogram（拆 data/index/schema）。

**Step 4: Run tests**
- Run: `go test ./downstreamadapter/sink/cloudstorage -count=1`

---

### Task 2: 修复 sink 内部已知正确性问题（必须先做）

**Files:**
- Modify: `downstreamadapter/sink/cloudstorage/dml_writers.go`

**Goal:**
- 修复 `Run()` 中对 `i` 的闭包捕获风险，避免 writer goroutine 运行错位/不确定行为（这是 GA 阻塞级别）。

**Step 1: Write the failing test**
- 构造一个很小的 dmlWriters，写入事件后确保所有 writer 都能被驱动、不会出现数组越界/错误 writer 执行。

**Step 2: Run test to verify it fails**
- Run: `go test ./downstreamadapter/sink/cloudstorage -run TestDMLWritersRun -count=1`

**Step 3: Fix implementation**
- 用局部变量 `writer := d.writers[i]` 或 `i := i` 规避闭包捕获。

**Step 4: Run tests**
- Run: `go test ./downstreamadapter/sink/cloudstorage -count=1`

---

### Task 3: 引入按表有序 encoder shards（默认开启，保留 legacy 回滚）

**Files:**
- Modify: `downstreamadapter/sink/cloudstorage/dml_writers.go`
- Modify: `downstreamadapter/sink/cloudstorage/encoding_group.go`（可能重构/替换）
- (Optional) Keep legacy: `downstreamadapter/sink/cloudstorage/defragmenter.go`
- Modify: `pkg/sink/cloudstorage/config.go`（新增 `encoder-ordering` 配置项）

**Step 1: Write failing tests**
- 覆盖：
  - 同表 FIFO
  - 跨表无 HO-L
  - callback 最终触发

**Step 2: Run tests to verify fail**
- Run: `go test ./downstreamadapter/sink/cloudstorage -run TestEncoderOrdering -count=1`

**Step 3: Implement minimal**
- 新增 `encoderShard`：
  - shard 数默认 8（或与现有 `defaultEncodingConcurrency` 对齐）
  - shard 内单 goroutine + `TxnEventEncoder`
  - shard 输入队列必须非阻塞入队（保持 `AddDMLEvent` non-blocking）
- 输出直接路由到 writer（保持 `hash(schema,table)` 规则）。

**Step 4: Run tests**
- Run: `go test ./downstreamadapter/sink/cloudstorage -count=1`

---

### Task 4: writer flush 政策增强（maxFlushDelay + fanout cap）

**Files:**
- Modify: `downstreamadapter/sink/cloudstorage/writer.go`
- Modify: `pkg/sink/cloudstorage/config.go`（若需要把参数暴露为可配置）

**Step 1: Write failing tests**
- 模拟大量表，每表 1 个 event，断言在小于 `flush-interval` 的时间内仍能触发 callback（不被 tick 锁死）。

**Step 2: Run tests**
- Run: `go test ./downstreamadapter/sink/cloudstorage -run TestFlushDelay -count=1`

**Step 3: Implement**
- 引入 `maxFlushDelay`（内部默认值先固定）。
- 引入 `maxActiveTablesPerBatch` / `maxBufferedBytesPerBatch`（内部默认值先固定）。
- 指标打点：flush reason、active tables。

**Step 4: Run tests**
- Run: `go test ./downstreamadapter/sink/cloudstorage -count=1`

---

### Task 5: FilePathGenerator state 合并与 TTL 回收

**Files:**
- Modify: `pkg/sink/cloudstorage/path.go`
- Test: `pkg/sink/cloudstorage/path_test.go`

**Step 1: Write failing tests**
- 覆盖 TTL 回收后恢复 index 与 schema 的正确性（不覆盖旧文件、不倒退 index）。

**Step 2: Run tests**
- Run: `go test ./pkg/sink/cloudstorage -run TestFilePathGeneratorTTL -count=1`

**Step 3: Implement**
- 合并 map，增加 `lastAccess` 与 TTL 回收逻辑。
- 避免“每次 tick 全表扫描”：优先在访问时做轻量回收，或设置上限触发回收。

**Step 4: Run tests**
- Run: `go test ./pkg/sink/cloudstorage -count=1`

---

## 7. Rollout & Operations

### 7.1 灰度开关

- `encoder-ordering=legacy|per-table`：默认 `per-table`，回滚到 `legacy` 不需要改 dispatcher。
- 关键指标与日志必须在 GA 前完成接入，否则不可灰度。

### 7.2 Dashboard 与告警（建议）

- 增加：
  - flush reason 比例图
  - backlog len 曲线（msgCh / writer input / toBeFlushed）
  - data/index/schema write duration 拆分曲线
  - active tables 曲线
- 调整：
  - 不再把单一 “flush duration” 视为端到端 flush 成本（避免误读）

---

## 8. One Question（只问一个，便于最终定默认策略）

“同步 100 万张表”里，最关键的目标更偏哪一个？

- **A（推荐默认）**：稳定性优先（不 OOM、不全局卡死、ack 延迟可控，允许更多小文件）
- **B**：产物形态优先（尽量大文件/少文件，但可能需要更复杂的机制与更大的回归面）
