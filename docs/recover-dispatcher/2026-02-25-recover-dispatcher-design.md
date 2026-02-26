<!-- Copyright 2026 PingCAP, Inc. -->

# Recover Dispatcher 设计文档

- Date: 2026-02-25
- Status: Draft
- Related: `docs/recover-dispatcher/2026-02-25-recover-dispatcher-requirements.md`

## 1. 设计目标

在不改变总体调度框架的前提下，建立一条可验证、可终止的 dispatcher recover 闭环：

1. sink 上报 recoverable transient error；
2. DM 去重、跟踪、重发；
3. maintainer 执行 restart dispatcher；
4. DM 通过回执与生命周期事件清理状态；
5. 异常链路触发 changefeed 级兜底。

## 2. 总体架构

```text
Kafka Producer
  -> Reporter (dedupe: dispatcher + epoch)
    -> DispatcherManager.collectRecoverableEvents
      -> recoverTracker (pending state machine)
      -> HeartBeatCollector send RecoverDispatcherRequest
        -> Maintainer.recoverDispatcherHandler
          -> RestartDispatcherOperator (force move on same node)
        -> RecoverDispatcherResponse
      -> DispatcherManager.onRecoverDispatcherResponse
      -> remove/recreate callbacks clear pending
      -> timeout fallback (retryable changefeed restart)
```

## 3. 核心数据模型

### 3.1 Recover identity

`RecoverDispatcherIdentity = (dispatcherID, dispatcherEpoch, maintainerEpoch)`

约束：
1. `dispatcherEpoch` 对同一 `dispatcherID` 必须单调（跨重建不回退）。
2. `maintainerEpoch` 必须与当前 maintainer 一致。

### 3.2 DM pending 状态

每个 `dispatcherID` 最多一条 pending，包含：
1. identity
2. firstSeen
3. removed
4. responseHandled

## 4. 状态机与语义

### 4.1 上报与入队

1. Reporter 对输入 dispatcher 去重并保留最大 epoch。
2. DM 过滤 non-local / pending dispatcher。
3. 构造 identity 并 enqueue recover request。
4. enqueue 成功后写入 pending。

### 4.2 回执处理

1. `ACCEPTED/RUNNING/SUPERSEDED`：
   - 标记 `responseHandled=true`
   - 停止 resend
   - 等待 remove/recreate/non-local 清理
2. `FAILED`：
   - 移除 pending
   - 清 reporter 状态
   - 进入 changefeed 级兜底路径

### 4.3 生命周期清理

1. dispatcher remove -> `markRemoved`
2. dispatcher recreate（同 ID）-> `ack` 清 pending + clear reporter
3. non-local 且 removed -> 清 pending + clear reporter

### 4.4 timeout 兜底

pending 超时后触发 retryable changefeed 重启。  
该机制仅作为最后保险丝，不应承担主流程正确性。

### 4.5 Kafka transient 判定与分叉规则（阶段约束）

1. 仅对 allowlist 的 `sarama.KError` 走 recover 路径，其它错误保持原有 fatal 路径。
2. 阶段默认 allowlist：
   - `NOT_ENOUGH_REPLICAS_AFTER_APPEND`
   - `NOT_ENOUGH_REPLICAS`
   - `LEADER_NOT_AVAILABLE`
   - `NOT_LEADER_FOR_PARTITION`
   - `REQUEST_TIMED_OUT`
   - `NETWORK_EXCEPTION`
3. 恢复分叉规则：
   - 命中 allowlist：上报 recover event，不让 sink 主循环因该错误退出；
   - 未命中 allowlist：沿用原路径上报并触发 changefeed 级处理。

## 5. maintainer 切换设计

### 5.1 问题

如果只更新 maintainer ID，不更新 epoch，pending identity 会携带旧 epoch，导致新 maintainer 回 `SUPERSEDED`，DM 可能过早停重试。

### 5.2 方案

1. DM 维护并更新 `(maintainerID, maintainerEpoch)` 对。
2. maintainer 切换后，对所有 pending identity 执行 epoch 重绑定：
   - `identity.maintainerEpoch = currentMaintainerEpoch`
   - `responseHandled = false`
3. 下一轮重发使用新 epoch。

## 6. response 来源校验设计

在 DM 处理 recover response 前增加准入：

1. `msg.From == currentMaintainerID`；否则丢弃。
2. response identity 必须与 pending identity 完全匹配；否则忽略为 stale/out-of-order 回执。

## 7. 与 merge/split/move 的兼容

1. restart 本质是 same-node force move（remove -> add）。
2. 当 merge/split/move 与 recover 并发：
   - 若已有非 restart operator，maintainer 返回 `SUPERSEDED`；
   - DM 保留 pending 至生命周期清理；
   - 不依赖“时间猜测”清理。

## 8. 可观测性

指标建议：
1. recover event received count
2. recover pending gauge
3. recover timeout counter
4. invalid/stale response dropped counter（新增）

日志建议：
1. enqueue / resend / response / timeout 都记录 changefeedID + dispatcherID + epoch
2. 来源校验失败记录 `from` 与 `currentMaintainerID`

## 9. 风险与边界

1. 若 `dispatcherEpoch` 无法保持单调，identity 仍可能跨生命周期复用。
2. 若 maintainer 切换事件丢失，pending 重绑定可能延迟。
3. timeout 过短会误触发，过长会延迟兜底。
