# Cloud Storage Sink GA - Task Breakdown（任务拆分）

> 文档定位（已整合）
> - 类型：任务清单与执行进度（唯一打勾清单）
> - 总览文档：`storage-sink.md`
> - 说明：任务状态仅在本文维护；`storage-sink.md` 负责总体架构与语义说明。

> **范围与口径**
>
> - 本文用于把 `storage-sink.md` 的方案落成可执行工程任务（建议按 PR/里程碑推进）。
> - **只覆盖 cloud storage sink + 最小 dispatcher glue**。不改其他 sink 语义。
> - 本文不包含任何与本 GA 无关的方案关键词（例如其他历史方案的术语），避免造成阅读者误解。

## Source of truth（唯一基准）

- 总体方案与约束：`storage-sink.md`
- 任务清单与完成状态：本文
- 评审与风险清单：`docs/plans/2026-02-14-cloudstorage-sink-spool-review-recommendations.md`

## 核心不变量（写在任务拆分最前，避免实现偏航）

1) **DML 仍 await=true（dispatcher 语义不变）**
- dynstream 的 path 仍然会 blocking；区别仅在于 wake 的触发点从“flush 完成”前移到“入 spool 成功（且水位允许）”。

2) **two-stage ack（必须）**
- enqueue ack：只用于 wake（解除 dynstream path 阻塞）。
- flush ack：远端写成功后才 `PostFlush()`，用于推进 table progress / checkpoint。

3) **DDL 顺序正确性（必须）**
- 对 DDL(commitTs=T)：任何 dispatcher 上 `<T` 的 DML 必须先落到远端，再允许该 DDL 的 schema file 写入远端。
- 为满足 early-wake 下的顺序约束，必须实现：
  - `sink.PassBlockEvent(ddl)`（dispatcher 调用，等待返回），以及
  - sink 内部 `DrainMarker`（覆盖 encoding queue → spool → remote）。

4) **默认值对齐**
- `enable-table-across-nodes` 按 **false** 作为 GA 默认口径；实现侧需把 cloudstorage 默认值与 scheduler 默认一致（文档已注明现状不一致点，实施时修正）。

5) **明确不做**
- 不解决 `TableProgress` 内元素可能过多的问题（后续优化项）。
- 不新增需要用户配置/调参的 spool 参数（spool 默认包含 memory tier + disk spillover）。
- 不提供 early-wake 开关（必须项）。

---

# Implementation Plan (Task Breakdown)

**Goal:** 在不改变 checkpoint 语义的前提下，解除 tick 限速吞吐瓶颈，并保证 early-wake 下 DDL 顺序正确且可在 1000 dispatchers 规模稳定运行。

**Architecture:** 通过 spool（memory tier + disk spillover）+ internal watermarks 在 sink 内实现“可控缓冲 + 提前 wake”，并用 `PassBlockEvent + DrainMarker` 让 DDL 在 early-wake 下仍能覆盖 encoding/spool/remote 的在途数据顺序。

**Tech Stack:** Go；现有 TiCDC downstreamadapter dispatcher/sink 架构；metrics/log（pingcap/log + zap）。

## 执行进度（截至 2026-02-14）

- [x] PR/Task 0：cloudstorage 默认值修正（`enable-table-across-nodes=false`）与相关单测更新。
- [x] PR/Task 1.1：DML two-stage ack glue（`PostEnqueue` hook + `PostFlush` 兼容兜底 + dispatcher wake 切换）。
- [x] PR/Task 1.2：DDL glue（`sink.PassBlockEvent` 接口 + dispatcher DDL 调用点接入 + 非 cloudstorage sink no-op）。
- [x] PR/Task 1.3：cloudstorage spool 基线 + DDL drain（`DrainMarker` 路径打通，`PassBlockEvent` 在 cloudstorage 内生效）。
- [x] PR/Task 1.4：cloudstorage early-wake enablement（internal watermarks，wake 抑制/恢复，checkpoint 语义保持 flush ack）。
- [x] 结构化重构：spool 逻辑拆到独立 package `downstreamadapter/sink/cloudstorage/spool`，主流程仅依赖 `spool.Manager` 抽象。
- [x] PR/Task 2：规模与状态回收（writer 仅处理 active batched tables；`FilePathGenerator` 引入内部 TTL 清理与状态复用，避免 `versionMap/fileIndex` 长期膨胀）。
- [x] PR/Task 3：可观测性补全（新增 `flush_tasks_total{reason}`、`flush_duration_by_reason_seconds`、`flush_file_size_bytes{reason}`、`ddl_drain_duration_seconds`）。
- [x] PR/Task 4：测试矩阵补全（新增 task indexer 路由/缓存、writer drain + flush reason、DDL drain duration、path state cleanup 覆盖）。

---

## PR/Task 0：已知 bug 修复（必须先做）

**目标：** 在改语义之前先修复当前分支上已知的 correctness/并发/资源类问题，避免后续 two-stage ack + spool 改造放大隐患。

**Files（预期）：**
- Modify: `pkg/sink/cloudstorage/config.go`（修正 `defaultEnableTableAcrossNodes` 为 false）
- Test: `pkg/sink/cloudstorage/config_test.go`（新增/补齐默认值断言）
- Modify: `downstreamadapter/sink/cloudstorage/dml_writers.go`（修复 writer goroutine 循环变量捕获等并发问题）
- Modify: `downstreamadapter/sink/cloudstorage/*`（修复共享 channel close / 关闭顺序等资源问题，按实际发现为准）

**步骤：**
1. 为 cloudstorage config 默认值补一个单测（断言默认值为 false）。
2. 修正 `defaultEnableTableAcrossNodes` 的默认值。
3. 修复 cloudstorage DML pipeline 的已知并发/资源问题（示例）：
   - `dmlWriters.Run()` 中 writer goroutine 的循环变量捕获；
   - 多 goroutine `defer Close()` 同一 channel 的风险；
   - 关闭顺序导致的潜在 panic/泄露（以单测能覆盖的为准）。
4. 跑最小单测集确认无回归。

**验证：**
- Run: `make unit_test_pkg PKG=./pkg/sink/cloudstorage/...`
- Run: `make unit_test_pkg PKG=./downstreamadapter/sink/cloudstorage/...`

---

## PR/Task 1：two-stage ack 落地（拆分 wake 与 PostFlush，并直接应用）

**目标：** 在不影响其他 sink 的前提下，把 DML 的 wake 从 “flush ack（PostFlush）” 前移到 “enqueue ack（入 spool 成功且水位允许）”，并在 cloud storage sink 上**直接生效**：
- enqueue ack（wake）：让 dynstream path 不再被 `flush-interval` tick 限速；
- flush ack（checkpoint）：仍严格以远端写成功触发 `PostFlush()` 为准（语义不变）。

> 说明：一旦 DML wake 前移，DDL 顺序正确性不再能依赖旧的隐含顺序，因此本任务需要同时落地 `PassBlockEvent + DrainMarker`（否则 DDL 可能错序）。

**建议拆成 4 个 PR（按顺序合并；前 2 个 PR 不引入行为变化，后 2 个 PR 才让 early-wake 在 cloudstorage 上真正生效）：**

### PR/Task 1.1：DML two-stage ack glue（新增 enqueue ack hook，不改行为）

**目标：**
- 不新增新的 DML sink interface（仍使用 `sink.AddDMLEvent(event)`）。
- 把 wakeCallback 从“flush ack（PostFlush）”解耦出来，迁移到“enqueue ack（PostEnqueue）”上。
- 为了不影响其他 sink：提供兼容兜底，使得即使 sink 侧暂时不主动触发 `PostEnqueue()`，也不会造成 dynstream path 永久阻塞。

**Files（预期）：**
- Modify: `pkg/common/event/dml_event.go`（新增 enqueue ack hook：`PostEnqueue()` / `AddPostEnqueueFunc`，名称以实现为准）
- Modify: `downstreamadapter/dispatcher/basic_dispatcher.go`（保持 await=true 不变；wake 从 PostFlush 改为 PostEnqueue）
- Test: `pkg/common/event/dml_event_test.go`
- Test: `downstreamadapter/dispatcher/*_test.go`

**步骤：**
1) `DMLEvent` 新增 enqueue ack hook（只用于 wake，不用于 checkpoint）：
   - `AddPostEnqueueFunc(func())`
   - `PostEnqueue()`
2) 兼容兜底（关键点）：在 `DMLEvent.PostFlush()` 的实现中**保证会触发一次** `PostEnqueue()`（若尚未触发），从而保证：
   - 其他 sink 仍只需要在远端成功后调用 `PostFlush()`，wake 时机仍与历史一致（无行为变化）；
   - cloudstorage sink 后续可以在“入 spool 成功且水位允许”时提前调用 `PostEnqueue()` 来真正实现 early-wake。
3) dispatcher 改造（DML）：
   - DML 仍照常 `TableProgress.Add(event)`（flush ack / checkpoint 语义不变）；
   - wakeCallback（带 `sync.Once` 去重）注册到 `AddPostEnqueueFunc`，不再依赖 `PostFlush()`。

**验证：**
- Run: `make unit_test_pkg PKG=./pkg/common/event/...`
- Run: `make unit_test_pkg PKG=./downstreamadapter/dispatcher/...`

### PR/Task 1.2：DDL glue（新增 PassBlockEvent 接口 + dispatcher 调用点，不改行为）

**目标：**
- sink interface 增加 `PassBlockEvent(event BlockEvent) error`，让 sink 能拿到 DDL 信息并执行 per-dispatcher drain。
- dispatcher 增加调用点与调用时机（语义见设计文档），但先允许 sink 侧默认 no-op，因此本 PR 不引入行为变化。

**Files（预期）：**
- Modify: `downstreamadapter/sink/sink.go`（新增 `PassBlockEvent` 方法）
- Modify: `downstreamadapter/sink/*`（为所有 sink 增加默认实现：no-op return nil；cloudstorage 在 1.3 实现真实逻辑）
- Modify: `downstreamadapter/dispatcher/basic_dispatcher.go`（DDL 路径在 barrier 前/WriteBlockEvent 前调用 `PassBlockEvent` 并等待返回）
- Test: `downstreamadapter/dispatcher/*_test.go`

**dispatcher 调用时机（必须写进实现与测试里）：**
- barrier DDL：在 `reportBlockedEventToMaintainer()` **之前**调用并等待返回；
- 非 barrier DDL：在 `WriteBlockEvent()` **之前**调用并等待返回；
- 语义：返回成功表示该 dispatcher 的 `<T` DML 已完成确定性 drain（覆盖 encoding queue + spool + remote），随后才能进入 barrier 或执行 `WriteBlockEvent`。

**验证：**
- Run: `make unit_test_pkg PKG=./downstreamadapter/dispatcher/...`

### PR/Task 1.3：cloudstorage spool 基线 + DDL drain（实现 DrainMarker + PassBlockEvent，不提前 wake）

**目标：**
- 引入 spool（memory tier + disk spillover）作为 cloudstorage sink 的内部承载层，但本 PR **先不启用 early-wake**（仍保持 wake 发生在 flush ack）。
- 在 cloudstorage sink 内实现 `PassBlockEvent(ddl)`：对 DDL(commitTs=T) 注入 `DrainMarker(T)`，并等待 marker ack 后返回。
- DrainMarker 必须覆盖 encoding queue → spool → remote，确保 `<T` DML 已远端落盘后才允许写 schema file。
- 这一步先把“顺序正确性 + pipeline 重构”打通，再在 1.4 单独把 wake 时机前移，降低一次性改动风险。

**Files（预期）：**
- Modify/Create: `downstreamadapter/sink/cloudstorage/*`（spool 基线 + drain marker item + marker ack + PassBlockEvent 实现）
- Modify/Delete: `downstreamadapter/sink/cloudstorage/defragmenter.go`（或等价重构：移除 defragmenter 路径）
- Modify: `downstreamadapter/sink/cloudstorage/encoding_group.go`（重构为“按 shard 的 encoder workers”，不再全局乱序输出）
- Test: `downstreamadapter/sink/cloudstorage/*_test.go`

**验证：**
- Run: `make unit_test_pkg PKG=./downstreamadapter/sink/cloudstorage/...`
- 关键单测建议：
  - `TestDDLPassBlockEventCoversEncodeQueue`
  - `TestDDLDrainAcrossDispatchersBeforeWrite`

### PR/Task 1.4：cloudstorage early-wake enablement（水位背压 + PostEnqueue 时机前移）

**目标：**
- 基于 1.3 的 spool，引入 internal watermarks/hysteresis：
  - 水位允许：在“编码后 payload 成功入 spool”触发 `PostEnqueue()`（从而 wake dynstream）；
  - 水位过高：抑制 wake；水位回落到 low watermark 后补触发一次（避免 path 永久阻塞）。
- `PostFlush()` 仍只在远端写成功后触发（checkpoint 语义不变）。

**Files（预期）：**
- Modify: `downstreamadapter/sink/cloudstorage/*`（watermarks + wake 抑制/恢复 + PostEnqueue 触发点）
- Test: `downstreamadapter/sink/cloudstorage/*_test.go`

**验证：**
- Run: `make unit_test_pkg PKG=./downstreamadapter/sink/cloudstorage/...`
- 关键单测建议：
  - `TestEarlyWakeDoesNotAdvanceCheckpoint`
  - `TestWatermarkSuppressesWake`

---

## PR/Task 2：规模与状态回收（1000 dispatchers，active set + 回收）

**目标：** 保证周期性逻辑只遍历活跃 dispatcher，并让 per-dispatcher 状态可回收，避免长期运行常驻内存增长。

**Files（预期）：**
- Modify: `downstreamadapter/sink/cloudstorage/*`（active dispatchers set + 调度路径）
- Modify: `pkg/sink/cloudstorage/path.go`（或等价路径模块：加入 TTL/LRU/生命周期释放）
- Test: `downstreamadapter/sink/cloudstorage/*_test.go`

**步骤：**
1. active set：
   - writer/flush 调度只遍历 active dispatchers（不扫描全量）。
2. 状态回收：
   - 对 path/version/index 等 per-dispatcher map 增加回收策略（TTL/LRU 或绑定 writer 生命周期）。
3. 避免全局 HO-L：
   - 按 shard 保序，shard 间并行。

**验证：**
- Run: `make unit_test_pkg PKG=./downstreamadapter/sink/cloudstorage/...`
- 单测：构造 totalDispatchers 大、activeDispatchers 小的 case，断言调度/遍历行为与 activeDispatchers 成正比。

---

## PR/Task 3：可观测性与回归验证（指标 + 文档）

**目标：** 让“吞吐改善是否成立 / 是否 tick-limited / 是否水位背压 / DDL drain 是否异常慢”都能通过指标直观看到。

**Files（预期）：**
- Modify: `downstreamadapter/sink/metrics/cloudstorage.go`（新增/调整指标）
- Modify: `docs/*`（如需补充指标说明）

**指标建议（与设计文档一致）：**
- spool：`spool_bytes`, `spool_items`, `wake_calls_total`, `wake_suppressed_total`
- flush：`flush_count{reason=size|interval|ddl|close|error}`, `flush_duration_seconds{reason=...}`, `flush_file_size_bytes`
- DDL：`ddl_drain_duration_seconds`

---

## PR/Task 4：测试计划落地（单测为主）

**目标：** 把 GA 的“语义不变/顺序正确/背压有效/规模可控”用可重复的单测锁住。

**Files（预期）：**
- Test: `downstreamadapter/sink/cloudstorage/*_test.go`
- Test: `downstreamadapter/dispatcher/*_test.go`

**建议最小测试集合：**
1. `TestEarlyWakeDoesNotAdvanceCheckpoint`
2. `TestWatermarkSuppressesWake`
3. `TestDDLPassBlockEventCoversEncodeQueue`
4. `TestDDLDrainAcrossDispatchersBeforeWrite`（拆表/多 dispatcher）
5. `TestActiveSetOnlyProcessesActiveDispatchers`
