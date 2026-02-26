<!-- Copyright 2026 PingCAP, Inc. -->

# Recover Dispatcher Review（统一版）

- Date: 2026-02-25
- Status: Active
- Related:
  - `docs/recover-dispatcher/2026-02-25-recover-dispatcher-requirements.md`
  - `docs/recover-dispatcher/2026-02-25-recover-dispatcher-design.md`
  - `docs/recover-dispatcher/2026-02-25-recover-dispatcher-implementation.md`

## 1. 阅读路径（建议）

1. reporter 与上报语义  
   - `pkg/sink/recoverable/reporter.go`
   - `pkg/sink/kafka/sarama_async_producer.go`
2. DM recover 主链路  
   - `downstreamadapter/dispatchermanager/dispatcher_manager.go`
   - `downstreamadapter/dispatchermanager/recover_tracker.go`
3. collector 收发链路  
   - `downstreamadapter/dispatchermanager/heartbeat_collector.go`
4. maintainer 处理与 operator  
   - `maintainer/recoverable.go`
   - `maintainer/operator/operator_move.go`

## 2. 当前问题清单（待关闭）

### [ ] R1（High）maintainer 切换后 recover pending 可能提前停重试

- 代码定位
  - `downstreamadapter/dispatcherorchestrator/dispatcher_orchestrator.go`
  - `downstreamadapter/dispatchermanager/dispatcher_manager_info.go`
  - `downstreamadapter/dispatchermanager/dispatcher_manager.go`
- 问题描述
  - 切换后 maintainer epoch 与 pending identity 未形成完整重绑定闭环。
- 修复标准
  - 切换后 pending 可被新 maintainer 正确继续处理，不因旧 epoch 提前终止。
- 回复
  - 结论：
  - 后续动作：

### [ ] R2（High）identity 复用风险（dispatcherEpoch 生命周期）

- 代码定位
  - `downstreamadapter/eventcollector/dispatcher_stat.go`
  - `heartbeatpb/heartbeat.proto`
  - `downstreamadapter/dispatchermanager/recover_tracker.go`
- 问题描述
  - dispatcher 重建后 epoch 语义未被严格保证单调，存在跨生命周期复用风险。
- 修复标准
  - 同 dispatcherID 的 dispatcherEpoch 在 recover 语义上不回退。
- 回复
  - 结论：
  - 后续动作：

### [ ] R3（Med）recover response 缺少来源 maintainer 校验

- 代码定位
  - `downstreamadapter/dispatchermanager/heartbeat_collector.go`
  - `downstreamadapter/dispatchermanager/dispatcher_manager.go`
- 问题描述
  - 当前按 changefeed 路由 response，但未校验 `msg.From`。
- 修复标准
  - stale/wrong source response 必须被丢弃，不影响 pending 状态。
- 回复
  - 结论：
  - 后续动作：

### [ ] R4（Med）merge/split/move 并发下 pending 清理边界

- 代码定位
  - `downstreamadapter/dispatchermanager/task.go`
  - `downstreamadapter/dispatchermanager/dispatcher_manager.go`
  - `maintainer/recoverable.go`
- 问题描述
  - 需要确保 remove/recreate/non-local 路径对 pending 清理一致。
- 修复标准
  - 不出现“已无恢复价值但 pending 长期残留”与“错误清理导致漏恢复”。
- 回复
  - 结论：
  - 后续动作：

### [ ] R5（Med）timeout fallback 角色收敛

- 代码定位
  - `downstreamadapter/dispatchermanager/recover_tracker.go`
  - `downstreamadapter/dispatchermanager/dispatcher_manager.go`
- 问题描述
  - timeout 应仅作为最后保险丝，而非主流程正确性依赖。
- 修复标准
  - 主流程依赖回执与生命周期事件；timeout 仅处理链路断裂。
- 回复
  - 结论：
  - 后续动作：

## 3. 回归测试基线（最小）

1. `go test ./downstreamadapter/dispatchermanager -count=1`
2. `go test ./maintainer -run TestRecoverDispatcherRequest -count=1`
3. `go test ./pkg/sink/recoverable -count=1`
4. `go test ./pkg/sink/kafka -run "TestReportTransientErrorByChannelState|TestSetRecoverReporter" -count=1`
5. `go test ./pkg/sink/codec/common -run "TestBuildMessageRecoverInfo|TestAttachMessageRecoverInfo|TestAttachMessageRecoverInfoSplitByRowsCount|TestAttachMessageRecoverInfoClampRowsToRemainingEvents|TestAttachMessageRecoverInfoClearRecoverInfoWhenRowsCountIsZero" -count=1`

## 4. 关闭规则

仅当满足以下条件可勾选关闭：
1. 代码完成并通过对应测试。
2. review 回复区填写“结论 + 后续动作（若有）”。
3. 与 `requirements/design/implementation` 三文档语义一致。
