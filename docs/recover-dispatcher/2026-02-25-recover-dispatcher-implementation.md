<!-- Copyright 2026 PingCAP, Inc. -->

# Recover Dispatcher 实现文档

- Date: 2026-02-25
- Status: Draft
- Related:
  - `docs/recover-dispatcher/2026-02-25-recover-dispatcher-requirements.md`
  - `docs/recover-dispatcher/2026-02-25-recover-dispatcher-design.md`

## 1. 当前实现概况

已具备的主链路：
1. Kafka producer transient error -> reporter 上报
2. DM 收集 recover event -> pending 跟踪 -> 请求 maintainer
3. maintainer 创建 restart dispatcher operator 并返回 response
4. DM 根据 response 与 remove/recreate 清理 pending
5. timeout fallback 触发 changefeed 级 retryable 重启

核心代码位置：
1. Reporter: `pkg/sink/recoverable/reporter.go`
2. DM 主链路: `downstreamadapter/dispatchermanager/dispatcher_manager.go`
3. Tracker: `downstreamadapter/dispatchermanager/recover_tracker.go`
4. Collector: `downstreamadapter/dispatchermanager/heartbeat_collector.go`
5. Maintainer handler: `maintainer/recoverable.go`

补充说明：
1. 历史归档中的“错误放大链路分析、阶段约束、任务拆分”内容已经并入本文件及配套需求/设计文档，不再单独维护归档入口文件。

## 2. 与目标设计的差距（Must Fix）

### M1 maintainer 切换语义不完整

现状：切换时主要更新 maintainer ID，epoch 未形成完整同步与 pending 重绑定闭环。  
影响：pending recover 可能被过早停重试。

### M2 response 来源未校验

现状：RecoverDispatcherResponse 按 changefeed 路由，但未校验 `msg.From`。  
影响：stale/错误来源回执可能污染状态。

### M3 identity 稳定性不足

现状：recover identity 使用 dispatcherEpoch，但 dispatcher 重建后的 epoch 生命周期语义未被严格保证。  
影响：理论上存在跨生命周期 identity 复用风险。

## 3. 实施计划

## 阶段一：止血（P0）

1. maintainer 元信息同步
   - 在 DM 中提供 maintainer meta 更新接口（ID + epoch）。
   - 在 orchestrator maintainer 切换路径调用该接口。
2. pending 重绑定
   - `recoverTracker` 增加 `rebindMaintainerEpoch(newEpoch)`。
   - 切换后重置 `responseHandled=false`，恢复可重发。
3. response 来源校验
   - collector 回调透传 `msg.From` 给 DM。
   - DM 处理前校验来源，不匹配直接丢弃。

交付标准：
1. maintainer 切换不导致 recover 提前终止。
2. stale source response 不再污染 pending。

## 阶段二：收口（P1）

1. dispatcherEpoch 单调语义加固
   - 在 EventCollector 侧为 dispatcher 维护稳定 epoch 基线。
   - 新建/重建 dispatcher 时 epoch 延续，不回退。
2. identity 匹配收紧
   - response 仅对“当前 pending identity 全匹配”生效。

交付标准：
1. identity 不出现跨生命周期复用误命中。
2. 乱序旧回执不会清理新 pending。

## 阶段三：完善（P2）

1. 增加可观测性指标
   - response source mismatch count
   - identity mismatch dropped count
2. 文档与注释收敛
   - 删除过时 TODO
   - 明确状态机注释

## 4. PR 拆分合并计划（便于评审）

### PR-1 协议与消息底座

目标：先独立合并协议与消息路由，降低后续 PR 冲突。  
范围：
1. `heartbeatpb/heartbeat.proto`
2. `heartbeatpb/heartbeat.pb.go`
3. `pkg/messaging/message.go`
4. `maintainer/maintainer_manager.go`
5. `pkg/messaging/mock_message_center.go`

### PR-2 sink 通用 recoverable 抽象

目标：沉淀通用模型，避免在 Kafka 代码中固化实现。  
范围：
1. `pkg/sink/recoverable/*`
2. `pkg/sink/codec/common/recover_info.go`
3. `pkg/sink/codec/common/recover_info_test.go`
4. `pkg/sink/codec/common/message.go`
5. `pkg/sink/codec/encoder_group.go`
6. `pkg/common/event/dml_event.go`
7. `pkg/common/event/row_change.go`
8. `pkg/common/event/dml_event_test.go`

### PR-3 Kafka sink 接入 reporter

目标：Kafka transient error 上报 recover event。  
范围：
1. `pkg/sink/kafka/sarama_async_producer.go`
2. `pkg/sink/kafka/sarama_async_producer_test.go`
3. `downstreamadapter/sink/kafka/sink.go`
4. `downstreamadapter/sink/kafka/sink_test.go`
5. `downstreamadapter/sink/pulsar/sink.go`
6. `pkg/sink/kafka/mock/factory_mock.go`
7. `scripts/generate-mock.sh`

### PR-4 maintainer recover handler

目标：maintainer 处理 recover request 并创建 restart operator。  
范围：
1. `maintainer/recoverable.go`
2. `maintainer/recoverable_test.go`
3. `maintainer/maintainer.go`
4. `maintainer/operator/operator_move.go`
5. `maintainer/operator/operator_move_test.go`
6. `maintainer/operator/operator_controller.go`
7. `pkg/errors/error.go`

### PR-5 DM recover 主链路 + tracker

目标：DM 收集、pending、重发、回执处理主链路。  
范围：
1. `downstreamadapter/dispatchermanager/recover_tracker.go`
2. `downstreamadapter/dispatchermanager/dispatcher_manager.go`
3. `downstreamadapter/dispatchermanager/dispatcher_manager_info.go`
4. `downstreamadapter/dispatchermanager/heartbeat_queue.go`
5. `pkg/metrics/dispatcher.go`

### PR-6 HeartBeatCollector recover 通路

目标：recover request 发送重试 + response 回流。  
范围：
1. `downstreamadapter/dispatchermanager/heartbeat_collector.go`

### PR-7 并发兼容收口（merge/split/move/redo）

目标：生命周期清理路径与 force close 兼容。  
范围：
1. `downstreamadapter/dispatchermanager/task.go`
2. `downstreamadapter/dispatchermanager/dispatcher_manager_redo.go`
3. `downstreamadapter/dispatchermanager/dispatcher_manager_helper.go`
4. `downstreamadapter/dispatcher/basic_dispatcher.go`
5. 对应测试文件

### PR-8 文档与规范

目标：避免功能 PR 混入文档噪声。  
范围：
1. `docs/recover-dispatcher/2026-02-25-recover-dispatcher-*.md`
2. `docs/recover-dispatcher/2026-02-25-recover-dispatcher-review.md`
3. 相关归档文档

## 5. 测试策略

优先补充以下单测：
1. maintainer 切换后 pending 被重绑定并继续重发。
2. stale maintainer response 被丢弃。
3. identity mismatch response 不影响 pending。
4. merge/split/move 并发下 pending 清理可达。

说明：
1. 不为监控指标编写单元测试（按项目约定）。
2. 指标通过集成/观测验证。

## 6. 回滚与兜底策略

1. 若 dispatcher recover 链路异常，timeout fallback 保证进入 changefeed 级 retryable 重启。
2. 任何 recover 路径不可导致 silent stall。

## 7. 完成定义（Definition of Done）

1. P0/P1 用例通过并覆盖 must-fix 场景。
2. review 文档中的 A3/A5/A6 关闭，A2/A4 收敛到已知边界。
3. 代码注释与文档语义一致，不存在冲突描述。
