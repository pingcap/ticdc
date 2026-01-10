# Implementation Plan: Drain Capture

## Overview

本实现计划将 Drain Capture 功能分解为可执行的编码任务。实现采用自底向上的方式：
1. 先完成 Liveness 扩展和 NodeManager 改造
2. 实现 Coordinator 层的 DrainState 和 DrainScheduler
3. 实现 API 层
4. 实现 Maintainer 层的 dispatcher 迁移逻辑
5. 集成测试和监控

核心设计原则：**Maintainer 先迁移，Dispatcher 自然跟随**。

## Tasks

- [-] 1. 扩展 Liveness 类型
  - [ ] 1.1 添加 LivenessCaptureDraining 常量
    - 在 `pkg/node/node.go` 中添加 `LivenessCaptureDraining = 2`
    - 添加 `IsSchedulable()` 方法，返回 `liveness == LivenessCaptureAlive`
    - 添加 `StoreDraining()` 方法，CAS 从 Alive 到 Draining
    - 添加 `DrainComplete()` 方法，CAS 从 Draining 到 Stopping
    - _Requirements: 9.1, 9.2_

  - [ ] 1.2 编写 Liveness 扩展的单元测试
    - 测试状态转换逻辑
    - 测试 `IsSchedulable()` 方法
    - _Requirements: 9.1, 9.2_

- [ ] 2. 扩展 NodeManager Liveness 管理
  - [ ] 2.1 添加 nodeLiveness 字段和管理方法
    - 在 NodeManager 中添加 `nodeLiveness sync.Map`
    - 实现 `GetNodeLiveness(id)`, `SetNodeLiveness(id, liveness)` 方法
    - 实现 `GetSchedulableNodes()` 方法，排除 Draining 和 Stopping 节点
    - 实现 `GetCoordinatorCandidates()` 方法，排除 Draining 和 Stopping 节点
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 9.6_

  - [ ]* 2.2 编写 NodeManager Liveness 管理的单元测试
    - 测试 `GetSchedulableNodes()` 正确排除 draining 节点
    - 测试 `GetCoordinatorCandidates()` 正确排除 draining 节点
    - _Requirements: 5.2, 5.3, 9.6_

- [ ] 3. Checkpoint - 确保 Liveness 扩展测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 4. 实现 Coordinator DrainState 管理
  - [ ] 4.1 创建 DrainState 结构体
    - 创建 `coordinator/drain_state.go` 文件
    - 实现 `DrainState` 结构体，包含 `drainingTarget`, `startTime`, `initialMaintainerCount`, `initialDispatcherCount` 字段
    - 实现 `SetDrainingTarget()` 方法，设置 draining 目标并更新 liveness
    - 实现 `GetDrainingTarget()`, `IsDraining()` 方法
    - 实现 `ClearDrainingTarget()` 方法，转换到 Stopping 状态
    - 实现 `ClearDrainingTargetWithoutTransition()` 方法，用于集群只剩 draining 节点时重置
    - _Requirements: 2.2, 2.3, 2.4, 2.9, 9.5, 9.8_

  - [ ]* 4.2 编写 DrainState 单元测试
    - 测试单一 draining target 不变量
    - 测试状态转换
    - 测试并发访问安全性
    - _Requirements: 2.3, 2.4_

- [ ] 5. Checkpoint - 确保 DrainState 测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 6. 实现 Drain Capture Scheduler
  - [ ] 6.1 创建 drainScheduler 结构体
    - 创建 `coordinator/scheduler/drain.go` 文件
    - 实现 `drainScheduler` 结构体，包含 `batchSize`, `operatorController`, `changefeedDB`, `drainState`, `nodeManager` 字段
    - 实现 `NewDrainScheduler()` 构造函数，默认 batchSize 为 1
    - _Requirements: 3.1, 3.3_

  - [ ] 6.2 实现 Execute() 方法
    - 检查是否有 draining target
    - 获取 draining 节点上的 maintainers
    - 如果没有 maintainers，检查 dispatchers 是否也迁移完成
    - 如果都迁移完成，调用 `drainState.ClearDrainingTarget()`
    - _Requirements: 3.2, 3.8_

  - [ ] 6.3 实现批量迁移逻辑
    - 统计当前进行中的 operators 数量
    - 计算可用的批次大小 `availableBatch = batchSize - inProgressCount`
    - 为每个需要迁移的 maintainer 生成 `MoveMaintainerOperator`
    - 选择负载最低的目标节点
    - _Requirements: 3.3, 3.4, 3.5, 3.6_

  - [ ] 6.4 实现 selectDestination() 方法
    - 获取可调度节点（排除 draining/stopping 节点）
    - 根据节点上的任务数量选择负载最低的节点
    - _Requirements: 3.5, 3.6_

  - [ ] 6.5 实现 allDispatchersMigrated() 方法
    - 检查所有 changefeed 在 draining 节点上是否还有 dispatchers
    - 通过 changefeedDB 获取各 changefeed 的 dispatcher 分布信息
    - _Requirements: 3.8_

  - [ ]* 6.6 编写 drainScheduler 单元测试
    - 测试批量迁移逻辑
    - 测试目标节点选择
    - 测试 drain 完成检测
    - _Requirements: 3.2, 3.3, 3.4, 3.5_

- [ ] 7. Checkpoint - 确保 DrainScheduler 测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 8. 集成 Scheduler 排除机制
  - [ ] 8.1 更新 basicScheduler
    - 修改 `coordinator/scheduler/basic.go`
    - 在 `doBasicSchedule()` 中使用 `GetSchedulableNodes()` 替代 `GetAliveNodes()`
    - _Requirements: 5.1, 5.2_

  - [ ] 8.2 更新 balanceScheduler
    - 修改 `coordinator/scheduler/balance.go`
    - 在 `Execute()` 中使用 `GetSchedulableNodes()` 进行负载均衡决策
    - _Requirements: 5.3_

  - [ ]* 8.3 编写 Scheduler 排除机制测试
    - 测试 basicScheduler 正确排除 draining 节点
    - 测试 balanceScheduler 正确排除 draining 节点
    - _Requirements: 5.2, 5.3_

- [ ] 9. 扩展 Heartbeat 消息
  - [ ] 9.1 修改 heartbeat.proto
    - 在 `heartbeatpb/heartbeat.proto` 中添加 `draining_capture` 字段到 `MaintainerHeartbeat` 消息
    - 运行 `make generate` 生成 Go 代码
    - _Requirements: 10.1, 10.2_

  - [ ] 9.2 更新 Coordinator heartbeat 发送逻辑
    - 在发送 heartbeat 时包含 `draining_capture` 字段
    - 从 DrainState 获取当前 draining target
    - _Requirements: 10.1, 10.2_

  - [ ]* 9.3 编写 Heartbeat 扩展测试
    - 测试 draining_capture 字段正确传递
    - _Requirements: 10.1, 10.2_

- [ ] 10. Checkpoint - 确保 Heartbeat 扩展测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 11. 实现 Maintainer Drain 状态处理
  - [ ] 11.1 添加 drainingCapture 字段
    - 在 `maintainer/maintainer.go` 中添加 `drainingCapture atomic.Value` 字段
    - 实现 `SetDrainingCapture()`, `GetDrainingCapture()` 方法
    - _Requirements: 4.2, 4.6_

  - [ ] 11.2 实现 OnDrainingCaptureNotified() 方法
    - 处理来自 heartbeat 的 draining 通知
    - 更新本地 draining 状态
    - 触发 dispatcher 迁移
    - _Requirements: 4.6_

  - [ ] 11.3 实现 checkAndMigrateDispatchersOnDrainingNode() 方法
    - 在 bootstrap 完成后调用
    - 检查 draining 节点上是否有 dispatchers
    - 调用 controller 生成 MoveDispatcher operators
    - _Requirements: 4.1, 4.3_

  - [ ] 11.4 更新 Maintainer Controller
    - 实现 `GenerateMoveDispatcherOperatorsForNode()` 方法
    - 为 draining 节点上的所有 dispatchers 生成迁移 operators
    - 排除 draining 节点作为目标节点
    - _Requirements: 4.3, 4.4, 4.5_

  - [ ]* 11.5 编写 Maintainer drain 处理测试
    - 测试 draining 通知处理
    - 测试 dispatcher 迁移触发
    - _Requirements: 4.1, 4.3, 4.6_

- [ ] 12. Checkpoint - 确保 Maintainer drain 处理测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 13. 实现 API 层
  - [ ] 13.1 定义 API 响应结构体
    - 在 `api/v2/model.go` 中添加 `DrainCaptureResponse` 结构体
    - 在 `api/v2/model.go` 中添加 `DrainStatusResponse` 结构体
    - _Requirements: 1.8, 1.9, 1.10, 7.3, 7.4, 7.5_

  - [ ] 13.2 实现 DrainCapture API handler
    - 在 `api/v2/capture.go` 中实现 `DrainCapture()` handler
    - 验证请求参数
    - 转发请求到 Coordinator（如果当前节点不是 coordinator）
    - 调用 Coordinator.DrainCapture()
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

  - [ ] 13.3 实现 GetDrainStatus API handler
    - 在 `api/v2/capture.go` 中实现 `GetDrainStatus()` handler
    - 查询当前 drain 状态
    - 返回进度信息
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ] 13.4 注册 API 路由
    - 在 `api/v2/api.go` 中注册 `PUT /api/v2/captures/{capture_id}/drain`
    - 在 `api/v2/api.go` 中注册 `GET /api/v2/captures/{capture_id}/drain`
    - _Requirements: 1.1, 7.1_

  - [ ]* 13.5 编写 API 层单元测试
    - 测试各种错误场景的响应
    - 测试正常 drain 请求的响应
    - 测试状态查询的响应
    - _Requirements: 1.4, 1.5, 1.6, 1.7_

- [ ] 14. Checkpoint - 确保 API 层测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 15. 实现 Coordinator Drain 接口
  - [ ] 15.1 扩展 Coordinator 接口
    - 在 `coordinator/coordinator.go` 中添加 `DrainCapture()` 方法
    - 在 `coordinator/coordinator.go` 中添加 `GetDrainStatus()` 方法
    - _Requirements: 2.1, 2.2, 7.1_

  - [ ] 15.2 实现 DrainCapture() 方法
    - 验证请求（目标节点存在、不是 coordinator、集群节点数 >= 2）
    - 调用 DrainState.SetDrainingTarget()
    - 返回当前 maintainer 和 dispatcher 数量
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [ ] 15.3 实现 GetDrainStatus() 方法
    - 查询 DrainState 获取当前状态
    - 统计剩余 maintainer 和 dispatcher 数量
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ]* 15.4 编写 Coordinator drain 接口测试
    - 测试 DrainCapture 验证逻辑
    - 测试 GetDrainStatus 返回正确信息
    - _Requirements: 2.1, 2.3, 2.4_

- [ ] 16. Checkpoint - 确保 Coordinator drain 接口测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 17. 集成 DrainScheduler 到 Controller
  - [ ] 17.1 添加 DrainScheduler 到 Controller
    - 在 `coordinator/controller.go` 中添加 `drainScheduler` 字段
    - 在 Controller 初始化时创建 DrainScheduler
    - _Requirements: 3.1_

  - [ ] 17.2 集成 DrainScheduler 到调度循环
    - 在调度循环中调用 DrainScheduler.Execute()
    - 确保 DrainScheduler 优先级高于 BalanceScheduler
    - _Requirements: 3.1_

  - [ ]* 17.3 编写 Controller 集成测试
    - 测试 DrainScheduler 正确集成
    - 测试调度优先级
    - _Requirements: 3.1_

- [ ] 18. 实现 Coordinator 选举约束
  - [ ] 18.1 更新选举逻辑
    - 在 coordinator 选举逻辑中使用 `GetCoordinatorCandidates()`
    - 排除 Draining 和 Stopping 节点
    - _Requirements: 9.6_

  - [ ] 18.2 实现特殊情况处理
    - 检测集群是否只剩 draining 节点
    - 如果是，重置 draining 状态为 Alive
    - 允许该节点成为 coordinator
    - _Requirements: 9.8_

  - [ ]* 18.3 编写选举约束测试
    - 测试 draining 节点不参与选举
    - 测试只剩 draining 节点时的特殊处理
    - _Requirements: 9.6, 9.8_

- [ ] 19. Checkpoint - 确保选举约束测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 20. 实现 Prometheus 监控指标
  - [ ] 20.1 定义 drain 相关指标
    - 在 `metrics/coordinator.go` 中添加 `DrainCaptureGauge`
    - 添加 `DrainCaptureMaintainerCount`
    - 添加 `DrainCaptureDispatcherCount`
    - 添加 `DrainCaptureDuration`
    - _Requirements: 7.6_

  - [ ] 20.2 集成指标更新
    - 在 DrainState 状态变化时更新指标
    - 在 DrainScheduler 执行时更新进度指标
    - _Requirements: 7.6_

  - [ ]* 20.3 编写指标测试
    - 测试指标正确更新
    - _Requirements: 7.6_

- [ ] 21. 实现错误处理与恢复
  - [ ] 21.1 实现 Coordinator failover 恢复
    - 新 coordinator 启动时检查是否有 draining 节点
    - 通过 NodeManager 的 liveness 状态恢复 DrainState
    - _Requirements: 8.1_

  - [ ] 21.2 实现 draining 节点崩溃处理
    - 在 NodeManager 检测到节点离线时
    - 如果是 draining 节点，清理 DrainState
    - 让 basic scheduler 处理 maintainer 重新调度
    - _Requirements: 9.7_

  - [ ] 21.3 实现目标节点故障处理
    - 复用现有 `MoveMaintainerOperator.OnNodeRemove` 逻辑
    - 确保 maintainer 被重新调度到其他节点
    - _Requirements: 8.2_

  - [ ]* 21.4 编写错误处理测试
    - 测试 coordinator failover 恢复
    - 测试 draining 节点崩溃处理
    - 测试目标节点故障处理
    - _Requirements: 8.1, 8.2, 9.7_

- [ ] 22. Checkpoint - 确保错误处理测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 23. 实现日志记录
  - [ ] 23.1 添加 drain 操作日志
    - 在 drain 开始时记录 INFO 日志
    - 在 maintainer 迁移时记录 INFO 日志
    - 在 dispatcher 迁移时记录 INFO 日志
    - 在 drain 完成时记录 INFO 日志
    - _Requirements: 7.7_

  - [ ] 23.2 添加错误和警告日志
    - 在 drain 失败时记录 ERROR 日志
    - 在无可用目标节点时记录 WARN 日志
    - 在迁移超时时记录 WARN 日志
    - _Requirements: 7.7, 8.3_

- [ ] 24. 添加配置项
  - [ ] 24.1 添加 DrainMaintainerBatchSize 配置
    - 在 `pkg/config/scheduler.go` 中添加 `DrainMaintainerBatchSize` 字段
    - 默认值为 1
    - 在 DrainScheduler 初始化时读取配置
    - _Requirements: 3.3_

- [ ] 25. Final Checkpoint - 运行完整测试套件
  - 运行所有单元测试
  - 确保所有测试通过
  - 如有问题请询问用户


- [ ] 5. 实现 Drain Capture Scheduler
  - [ ] 5.1 创建 drainScheduler 结构体
    - 创建 `coordinator/scheduler/drain.go` 文件
    - 实现 `drainScheduler` 结构体，包含 `batchSize` 配置（默认 1）
    - 实现 `Execute()` 方法，生成 MoveMaintainer operators
    - 实现 `selectDestination()` 方法，选择负载最低的目标节点
    - 实现 `countInProgressOperators()` 方法，统计进行中的 operators
    - 实现 `allDispatchersMigrated()` 方法，检查所有 dispatchers 是否迁移完成
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

  - [ ]* 5.2 编写 drainScheduler 单元测试
    - 测试节点排除逻辑
    - 测试批量生成逻辑
    - 测试目标节点选择
    - _Requirements: 3.3, 3.5, 3.6_

- [ ] 6. 集成 Scheduler 排除机制
  - [ ] 6.1 更新 basicScheduler 使用 GetSchedulableNodes
    - 修改 `coordinator/scheduler/basic.go`
    - 将调度逻辑中的节点获取改为使用 `GetSchedulableNodes()`
    - _Requirements: 5.1, 5.2_

  - [ ] 6.2 更新 balanceScheduler 使用 GetSchedulableNodes
    - 修改 `coordinator/scheduler/balance.go`
    - 将平衡逻辑中的节点获取改为使用 `GetSchedulableNodes()`
    - _Requirements: 5.3_

- [ ] 7. Checkpoint - 确保 Scheduler 测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 8. 扩展 Heartbeat 消息
  - [ ] 8.1 修改 heartbeat.proto 添加 draining_capture 字段
    - 在 `heartbeatpb/heartbeat.proto` 的 MaintainerHeartbeat 消息中添加 `draining_capture` 字段
    - _Requirements: 10.2_

  - [ ] 8.2 更新 Coordinator 心跳发送逻辑
    - 在心跳消息中包含 draining capture ID
    - _Requirements: 2.5, 10.1_

- [ ] 9. 实现 Maintainer Drain 状态处理
  - [ ] 9.1 添加 Maintainer draining 状态字段和方法
    - 在 `maintainer/maintainer.go` 中添加 `drainingCapture` 字段
    - 实现 `SetDrainingCapture()`, `GetDrainingCapture()` 方法
    - _Requirements: 4.1, 4.2_

  - [ ] 9.2 实现 Maintainer 接收 draining 通知处理
    - 实现 `OnDrainingCaptureNotified()` 方法
    - 处理心跳中的 draining 通知
    - 在 bootstrap 完成后检查并迁移 draining 节点上的 dispatchers
    - _Requirements: 4.1, 4.3, 10.3, 10.4_

  - [ ]* 9.3 编写 Maintainer drain 处理单元测试
    - 测试 draining 状态更新
    - 测试 dispatcher 迁移触发
    - _Requirements: 4.1, 4.3_

- [ ] 10. Checkpoint - 确保 Maintainer 层测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 11. 实现 API 层
  - [ ] 11.1 定义 API 请求和响应结构体
    - 在 `api/v2/model.go` 中添加 `DrainCaptureResponse`, `DrainStatusResponse` 结构体
    - _Requirements: 1.8, 1.9, 1.10, 7.2, 7.3, 7.4, 7.5_

  - [ ] 11.2 实现 DrainCapture API handler
    - 在 `api/v2/capture.go` 中实现 `DrainCapture()` 方法
    - 实现请求验证逻辑（目标存在、非 coordinator、非单节点、无进行中 drain）
    - 实现请求转发到 coordinator 逻辑
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

  - [ ] 11.3 实现 GetDrainStatus API handler
    - 实现 `GetDrainStatus()` 方法
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ] 11.4 注册 API 路由
    - 在 `api/v2/api.go` 中注册 drain 相关路由
    - _Requirements: 1.1, 7.1_

  - [ ]* 11.5 编写 API handler 单元测试
    - 测试所有错误情况的验证逻辑
    - 测试成功请求处理
    - _Requirements: 1.3, 1.4, 1.5, 1.6, 1.7_

- [ ] 12. 实现 Coordinator Drain 接口
  - [ ] 12.1 扩展 Coordinator 接口
    - 在 `pkg/server/coordinator.go` 中添加 `DrainCapture()`, `GetDrainStatus()` 方法声明
    - _Requirements: 2.1, 2.2_

  - [ ] 12.2 实现 Coordinator DrainCapture 方法
    - 在 `coordinator/coordinator.go` 中实现 `DrainCapture()` 方法
    - 实现请求验证
    - 设置 draining target
    - 返回当前工作负载计数
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [ ] 12.3 实现 Coordinator GetDrainStatus 方法
    - 返回当前 drain 状态和进度
    - _Requirements: 7.2, 7.3, 7.4, 7.5_

  - [ ]* 12.4 编写 Coordinator drain 方法单元测试
    - 测试验证逻辑
    - 测试状态管理
    - _Requirements: 2.1, 2.3, 2.4_

- [ ] 13. 集成 DrainScheduler 到 Controller
  - [ ] 13.1 在 Controller 中初始化 DrainScheduler
    - 修改 `coordinator/controller.go`
    - 创建并注册 drainScheduler
    - 设置优先级高于 balanceScheduler
    - _Requirements: 3.1_

  - [ ] 13.2 连接 DrainState 和 DrainScheduler
    - 确保 DrainScheduler 可以访问 DrainState
    - _Requirements: 3.2_

- [ ] 14. 实现 Coordinator 选举约束
  - [ ] 14.1 修改 Coordinator 选举逻辑
    - 使用 `GetCoordinatorCandidates()` 获取候选节点
    - 实现特殊情况：集群只剩 draining 节点时重置状态
    - _Requirements: 9.6, 9.8_

- [ ] 15. Checkpoint - 确保 API 和 Coordinator 集成测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

- [ ] 16. 实现 Prometheus 监控指标
  - [ ] 16.1 定义 drain 相关 metrics
    - 在 `metrics/` 中添加 `DrainCaptureGauge`, `DrainCaptureMaintainerCount`, `DrainCaptureDispatcherCount`, `DrainCaptureDuration`
    - _Requirements: 7.6_

  - [ ] 16.2 在 DrainState 中更新 metrics
    - 在状态变化时更新相应的 metrics
    - _Requirements: 7.6_

- [ ] 17. 添加日志记录
  - [ ] 17.1 在关键操作点添加日志
    - Drain 操作开始/完成
    - Maintainer 迁移事件
    - Dispatcher 迁移事件
    - 错误和警告情况
    - _Requirements: 7.7_

- [ ] 18. Final Checkpoint - 确保所有测试通过
  - 运行所有测试，确保通过，如有问题请询问用户

## Notes

- 标记 `*` 的任务为可选任务，可以跳过以加快 MVP 开发
- 每个任务都引用了具体的需求以便追溯
- Checkpoint 任务确保增量验证
- 核心设计：Maintainer 先迁移，新 Maintainer 启动后自动迁移 dispatchers
- 并发度配置：`batchSize` 默认为 1，可配置
