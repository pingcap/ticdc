# Dynamic Stream（`utils/dynstream`）学习笔记与建议

> 目标：把 `utils/dynstream` 作为一个“高并发、多 key（path）事件处理器”来理解，说明它目前**实际提供的语义**、与上层（log-puller / event-collector）的对接方式，以及我认为在**正确性 / 易用性 / 功能性**上的改进点。

## 1. 它解决什么问题

在 TiCDC 新架构里，很多场景是“同一个 key（例如 dispatcherID / subscriptionID）的事件必须严格串行处理，但不同 key 之间可以并行”。如果为每个 key 单独起 goroutine + channel，会在 key 数量巨大时带来：

- goroutine 数量爆炸、调度成本高；
- 每个 channel 的内存开销大；
- 在需要批处理/背压时，很难统一做控制。

Dynamic Stream 的基本思路是：

- 固定数量（`Option.StreamCount`）的 worker（内部叫 `stream`）；
- 每个 `path` 绑定到某一个 worker；
- worker 内部对每个 path 维护一个 `pendingQueue`，保证**同 path 串行**；
- worker 之间并行，保证**不同 path 并行**（上限是 `StreamCount`）。

代码入口：`utils/dynstream/interfaces.go` + `utils/dynstream/parallel_dynamic_stream.go`。

## 2. 核心抽象（Area / Path / Event / Dest / Handler）

### 2.1 `Path` 与 `Dest`

- `Path`：事件的路由 key（必须 `comparable`），例如：
  - log-puller：`SubscriptionID`
  - event-collector：`DispatcherID`
- `Dest`：处理事件所需的“目的对象”，由 `AddPath(path, dest)` 时注册，后续 `Handle(dest, events...)` 直接拿到，避免二次查表。

实现上：`parallelDynamicStream` 内有一个 `pathMap`（`map[P]*pathInfo`），`Push` 时通过 `path` 找到 `pathInfo{dest, stream}`。

### 2.2 `Area`

`Area` 用来把一组 path 归到同一个更大的分组（通常是 changefeed / GID），目前在实现里主要用于 **memory control 的统计与反馈**（见第 5 节）。

例如 event-collector 里 `GetArea` 返回 changefeed 的 `GID`：`downstreamadapter/eventcollector/helper.go`。

### 2.3 `Handler`

`Handler` 是“业务侧逻辑”的注入点：

- `Path(event)`：从事件里取出 path（实际实现里 `parallelDynamicStream.Push` **直接拿参数 path**，所以这个方法目前更多是为了 API 统一/未来扩展）。
- `Handle(dest, events...) (await bool)`：处理一批事件；若返回 `await=true`，表示异步处理中，后续事件需要等 `Wake(path)` 才能继续（见 4.4）。

另外 `Handler` 还包含 `GetType/GetSize/GetTimestamp/GetArea/IsPaused/OnDrop` 等方法（详见 `utils/dynstream/interfaces.go`）。

## 3. 运行时结构（并发模型）

整体结构可以理解为：

```
Push(path, event)
  -> pathMap[ path ] => *pathInfo{stream, dest, pendingQueue, ...}
  -> stream.addEvent(eventWrap{event, pathInfo, ...})

stream.handleLoop()  // 每个 stream 一个 goroutine
  -> 把 eventWrap 写入对应 path 的 pendingQueue
  -> 从全局 signalQueue 选一个 path
  -> popEvents() 得到 (events[], pathInfo)
  -> handler.Handle(dest, events...)
```

关键点：

- `parallelDynamicStream` 只有“路由与生命周期”职责；
- 实际执行发生在 `stream.handleLoop`；
- **每个 path 只属于一个 stream**，因此“同 path 串行”可以靠单 goroutine + 队列天然成立；
- 不同 stream 之间并行，上限由 `Option.StreamCount` 控制。

实现文件：

- `utils/dynstream/parallel_dynamic_stream.go`
- `utils/dynstream/stream.go`
- `utils/dynstream/event_queue.go`

## 4. 关键行为与语义（按 API 拆解）

### 4.1 `Start / Close`

- `Start()`：启动每个内部 `stream` 的 goroutine。
- `Close()`：设置关闭标志，清空 `pathMap`，并停止所有 `stream`。

注意：`Feedback()` 返回的 channel **不会在 `Close()` 时 close**（目前依赖调用方用 ctx/select 退出循环）。

### 4.2 `AddPath / RemovePath`

- `AddPath(path, dest)`：把 path 注册进 `pathMap`，并把它绑定到一个 stream。
  - 当前选择 stream 的策略是 **round-robin**（`addPathCount % StreamCount`），不是按 path hash。
- `RemovePath(path)`：从 `pathMap` 删除，并把 `pathInfo.removed=true`。
  - 后续 `Push` 到该 path 会触发 `OnDrop`。
  - 已进入 stream 的事件在 `handleLoop` 里会因为 `removed` 标志被忽略（不再进入 `pendingQueue`）。

### 4.3 `Push(path, event)` 与 `OnDrop`

`Push` 的关键路径：

1. `pathMap` 查找 `*pathInfo`；不存在则 `OnDrop(event)`；
2. 构造 `eventWrap`（包含 `eventType / eventSize / timestamp / queueTime` 等）；
3. 发送到 `pathInfo.stream` 的 channel。

`OnDrop` 被调用的典型原因：

- path 未注册；
- `DynamicStream` 已关闭；
- memory control 场景下，可能将 droppable event 转换成“DropEvent”再入队（见 5.3）。

### 4.4 `Handle(...)->await` 与 `Wake(path)`

这是 dynamic stream 的一个关键机制：把“异步处理导致的等待”显式从 worker goroutine 上剥离出来，避免阻塞整个 stream。

- 当 `Handle(dest, events...)` 返回 `await=true`：
  - 该 path 会被标记为 `blocking=true`；
  - `eventQueue.popEvents()` 会跳过该 path；
- 业务侧异步完成后必须调用 `Wake(path)`：
  - `Wake` 会把该 path 的 `blocking` 置回 false；
  - 并把该 path 当前 `pendingQueue` 的长度作为 signal，推到 `signalQueue` 的 front，尽快恢复处理。

典型用法：log-puller 的 `regionEventHandler.Handle()` 在把 KV events 异步交给下游消费后，通过回调调用 `wakeSubscription()`（`logservice/logpuller/region_event_handler.go`）。

### 4.5 批处理：`EventType{DataGroup, Property}`

批处理逻辑在 `eventQueue.popEvents()`：

- 从 `signalQueue` 取一个 path；
- 最多取 `Option.BatchCount` 个事件；
- 只会把**同 `DataGroup`** 且两者都不是 `NonBatchable` 的事件合并到同一个 batch。

`Property` 的语义（`utils/dynstream/interfaces.go`）：

- `BatchableData`：可批；
- `PeriodicSignal`：允许“尾部合并”去重（连续 periodic 只保留最后一个）；
- `NonBatchable`：强制单条处理。

注意：目前 batching 没有实现 `Option.BatchBytes`（字段存在但未使用）。

### 4.6 `Release(path)`

`Release` 的语义是“清空该 path 的 `pendingQueue`”（直接丢弃未处理事件），用于极端情况下释放内存。

在 event-collector 里：收到 `dynstream.ReleasePath` feedback 后会调用 `ds.Release(path)`（`downstreamadapter/eventcollector/event_collector.go`）。

## 5. Memory Control（EnableMemoryControl）与 Feedback

### 5.1 基本机制

当 `Option.EnableMemoryControl=true`：

- 每个 `Area` 会维护一个 `areaMemStat`：
  - 跟踪 `totalPendingSize`（字节）；
  - 维护 `pathMap`（area 内所有 pathInfo，用于统计/选择释放对象）；
  - 通过 `Feedback()` channel 向上层发出控制信号；
- `Handler.GetSize(event)` 用于估算事件内存；
- `EventType.Droppable` + `Handler.OnDrop(event)` 用于“可丢弃事件”的降级策略。

实现：`utils/dynstream/memory_control.go` + `utils/dynstream/memory_control_algorithm.go`。

### 5.2 Puller 场景：PauseArea / ResumeArea

`MemoryControlForPuller`（默认算法）会在 area 级别做 pause/resume：

- 使用阈值：>=80% pause，<50% resume（见 `memory_control_algorithm.go`）。
- Feedback：`PauseArea` / `ResumeArea`

log-puller 的 `subscriptionClient` 在 `handleDSFeedBack` 里收到 pause/resume 后，会暂停/恢复 push（`logservice/logpuller/subscription_client.go`）。

### 5.3 Event-Collector 场景：ReleasePath + Drop 转换

`MemoryControlForEventCollector` 当前的核心手段是：

- 当内存超限或检测到“疑似死锁”（事件持续进入但 pending size 长时间不下降）时：
  - 选择部分 **blocking 且 pendingSize 足够大** 的 path，发送 `ReleasePath`；
  - 若新进入的事件 `Droppable=true`，则尝试用 `OnDrop` 把它转换成“DropEvent”并入队，替代原事件。

event-collector 的 `processDSFeedback` 收到 `ReleasePath` 后调用 `Release(path)`，真正清空队列（`downstreamadapter/eventcollector/event_collector.go`）。

### 5.4 现状对照：设计意图 vs 当前实现

`docs/design/2024-12-20-ticdc-flow-control.md` 里描述了 path-level 与 area-level 控制。但目前 `dynstream` 实现里：

- 只实现了 **area-level pause/resume**（puller）；
- event-collector 侧更多是 **circuit-breaker 式 ReleasePath**；
- `ResumePath`、`MemoryControlAlgorithm.ShouldPausePath`、`AreaSettings.pathMaxPendingSize` 等目前基本未被使用。

## 6. Metrics/可观测性现状

Prometheus 指标定义在 `pkg/metrics/dynamic_stream.go`，使用方（例如 log-puller / event-collector）通过 `ds.GetMetrics()` 定期上报：

- `PendingQueueLen`：当前实现能返回（但它是 “signal 数” + channel len 的近似，不是严格事件数）。
- `EventChanSize`：`Metrics` 结构体有字段，但 `dynstream.GetMetrics()` 当前没有赋值（导致上层打到 metrics 的可能一直是 0）。
- Memory metrics：来自 `memControl.getMetrics()`，按 area 输出 max/used。

## 7. 我认为它“保证/不保证”的契约（以当前代码为准）

**保证：**

- 同一个 `path` 的事件不会并发调用 `Handle`（串行）；
- `Handle(await=true)` 可以阻塞该 path，直到 `Wake(path)`；
- `RemovePath` 之后，该 path 的后续 `Push` 会被 `OnDrop`。

**不保证 / 需调用方自行保证：**

- 不保证跨 path 的全局顺序（即使 `GetTimestamp` 被实现也不保证）；
- 不保证严格的公平性（signalQueue 是 FIFO，热点 path 可能导致其他 path 延迟增大）；
- `GetSize` 的准确性依赖 handler；PeriodicSignal 的“尾部合并”假设其 size 小且稳定；
- `Feedback()` 为 nil 时读会永久阻塞（调用方需判空或用 Option 决定是否启动反馈处理协程）。

## 8. 建议

### 8.1 正确性（Correctness）

1. **修复 memory control 的潜在 path 泄漏**
   - 现状：`memControl.removePathFromArea()` 只减少 `pathCount`，但没有从 `areaMemStat.pathMap` 删除对应 path。
   - 风险：area 内 path 数量长期变化时，`pathMap` 可能无限增长；`releaseMemory()`/metrics 也可能遍历到已删除 path。
   - 建议：在 remove 时 `area.pathMap.Delete(path.path)`，并考虑把 `path.areaMemStat=nil`（明确断链）。

2. **对齐注释/接口契约与真实行为**
   - `Handler` 注释写“可选方法”，但 Go 接口里它们是必需实现的；
   - `GetTimestamp/GetArea/IsPaused` 的注释里提到“用于调度优先级/反馈”，但当前实现并未用到。
   - 建议：要么补齐实现，要么改注释/接口形态，避免误导使用方。

3. **完善/修正 `GetMetrics()` 的字段含义**
   - 当前 `EventChanSize` 未赋值，上层依赖该值的监控会失真（见 `subscription_client.updateMetrics`、`EventCollector.updateMetrics`）。
   - 建议：明确 `EventChanSize` 的定义并实现（例如 sum(len(eventChan)) + buffer + …），或者删除该字段并改用更准确的指标。

4. **PeriodicSignal 合并下的内存统计一致性**
   - 当前“替换尾部 periodic 信号”不会调整 `pendingSize/totalPendingSize`，假设 periodic 的 size 稳定。
   - 建议：至少在 debug/测试环境做断言或采样校验；或在替换时按 delta 修正统计，避免 handler 实现不当导致 memory control 失真。

### 8.2 易用性（Ergonomics）

1. **提供一个可嵌入的默认 Handler 基类**
   - 现状：所有 handler 都必须写一堆“返回 0/false/nil”的样板方法（`GetSize/GetArea/GetTimestamp/...`）。
   - 建议：提供 `BaseHandler[...]`（实现默认方法），让业务侧只需实现 `Handle`（以及必要的 `GetType/GetSize`）。

2. **给出官方使用范式（尤其是 await/Wake 与 Feedback）**
   - 建议在 `utils/dynstream` 增加一个简短 README 或 Godoc 示例：
     - `await=true` 时如何确保一定会 `Wake`（避免永久阻塞）；
     - `EnableMemoryControl=true` 时如何启动 feedback loop（pause/resume 或 release）。

3. **明确 `AddPath/RemovePath` 的并发语义与成本**
   - 例如：并发调用是否安全（目前是安全的）；在高频变更 path 的场景下是否有额外开销（锁、map churn）。

### 8.3 功能性（Functionality）

1. **实现或删除“调度/均衡”相关的未完成字段**
   - `Option.InputChanSize/SchedulerInterval/ReportInterval/BatchBytes` 目前未被使用；
   - `parallel_dynamic_stream.go` 的注释提到 hasher，但实现是 round-robin。
   - 建议：若需要负载均衡，考虑：
     - path->stream 使用 hash（稳定映射）；
     - 或实现 scheduler 基于 stream backlog 动态迁移 path（但需要慎重处理迁移期间的串行语义）。

2. **补齐 path-level flow control（或收敛设计）**
   - 当前 `FeedbackType` 里有 `ResumePath`，算法接口也有 `ShouldPausePath`，但实现未用。
   - 建议：要么补齐 Pause/Resume path 的完整链路（包括上游执行与状态回流），要么删除这些未用接口/类型，减少维护负担。

3. **更强的可观测性：按 area/path 的 backlog/延迟**
   - 现有数据里 `eventWrap.queueTime` 已记录入队时间但未使用；
   - 建议增加：队列等待时间分位数、每个 area 的 pending bytes、释放次数、drop 转换次数等，便于定位拥塞与流控效果。

---

如果你希望我把其中某几条建议直接落到代码里（例如：修复 `areaMemStat.pathMap` 泄漏、补齐 `EventChanSize` 指标、或引入 `BaseHandler`），我可以基于当前实现再做一轮“小步可验证”的 patch。

