# Implementation Plan: Drain Capture

## Overview

本实现计划将 Drain Capture 功能拆解为可执行任务，并补齐当前实现与 spec 之间的关键缺口：

1. **状态一致性与恢复**：drain record + node liveness 需要持久化（否则 coordinator failover 会丢状态）。
2. **通知链路**：需要明确 coordinator→maintainer 的 drain 通知通道（`MaintainerHeartbeat` 是 maintainer→coordinator）。
3. **Dispatcher 维度闭环**：maintainer 侧迁移 + 进度上报 + coordinator 聚合，才能满足“完成条件=maintainer+dispatcher 都为 0”。
4. **调度优先级确定性**：drain scheduler 必须确定性地优先于 balance，并在 drain 期间避免无关 rebalance 干扰。

核心设计原则仍然是：**Maintainer 先迁移，Dispatcher 自然跟随**。

## Tasks

- [x] 1. 扩展 `pkg/node` Liveness
  - [x] 1.1 添加 `LivenessCaptureDraining` + `IsSchedulable/StoreDraining/DrainComplete`（`pkg/node/node.go`）
  - [x] 1.2 添加单元测试（`pkg/node/liveness_test.go`）
  - _Requirements: 9.1, 9.2_

- [x] 2. 扩展 NodeManager Liveness（内存态）
  - [x] 2.1 添加 `GetNodeLiveness/SetNodeLiveness/GetSchedulableNodes/GetCoordinatorCandidates`（`server/watcher/module_node_manager.go`）
  - [x] 2.2 添加单元测试（`server/watcher/module_node_manager_test.go`）
  - _Requirements: 5.2, 9.6_

- [ ] 3. 持久化 Liveness 与 DrainRecord（etcd）
  - [ ] 3.1 设计 etcd key 与 schema（单集群单 draining target；per-capture liveness）
  - [ ] 3.2 实现 liveness store（watch + cache），供 elector 与调度使用
  - [ ] 3.3 实现 drain record store（CAS 单 target、epoch 单调递增）
  - [ ] 3.4 coordinator 启动恢复：从 etcd 重建 drainState + 进度聚合结构
  - _Requirements: 8.1, 9.1_

- [-] 4. Coordinator DrainState（接入持久化）
  - [x] 4.1 内存态 DrainState（`coordinator/drain_state.go` + tests）
  - [ ] 4.2 DrainState 改为持久化后端（或包装为“内存 cache + etcd store”）
  - [ ] 4.3 draining 节点移除时清理 drain record（与 node remove 事件打通）
  - [ ] 4.4 单元测试：持久化/并发/幂等
  - _Requirements: 2.3, 2.4, 8.1, 9.7_

- [-] 5. Coordinator DrainScheduler（完善）
  - [x] 5.1 drain scheduler 框架（`coordinator/scheduler/drain.go`）
  - [ ] 5.2 调度顺序确定性：drain 优先于 balance；drain 期间可暂停 balance（`pkg/scheduler/scheduler.go`）
  - [ ] 5.3 目的节点选择：考虑 inflight batch，避免集中迁入单节点
  - [ ] 5.4 drain 完成条件：基于 maintainer 上报的 dispatcher counts（非 stub）
  - [ ] 5.5 drain 批大小配置化（`DrainMaintainerBatchSize`）
  - [ ] 5.6 单元测试：batch 上限/优先级/完成条件
  - _Requirements: 3.1, 3.3, 3.5, 3.8_

- [x] 6. Coordinator 侧调度排除（Maintainer）
  - [x] 6.1 basic scheduler 使用 `GetSchedulableNodeIDs()` 排除 Draining/Stopping（`coordinator/scheduler/basic.go`）
  - [x] 6.2 balance scheduler 使用 `GetSchedulableNodes()` 排除 Draining/Stopping（`coordinator/scheduler/balance.go`）
  - _Requirements: 5.2, 5.3_

- [ ] 7. Coordinator→Maintainer drain 通知通道（新增）
  - [ ] 7.1 定义 `DrainNotification` proto + messaging IOType
  - [ ] 7.2 coordinator 广播策略：状态变化立即发 + 周期性重发（<=1 heartbeat 周期）
  - [ ] 7.3 maintainer manager 接收并分发给各 maintainer
  - [ ] 7.4 单元测试：通知可达/去重（epoch）
  - _Requirements: 10.1, 10.2_

- [ ] 8. Maintainer 侧 dispatcher drain（排除 + 迁移 + 上报）
  - [ ] 8.1 maintainer 本地状态：`draining_capture/drain_epoch`
  - [ ] 8.2 dispatcher 调度排除 draining node（basic/balance/move/split 等路径）
  - [ ] 8.3 新增 drain-dispatcher scheduler：批量生成 `MoveDispatcher` operators
  - [ ] 8.4 触发点：收到通知 + bootstrap 完成后检查一次
  - [ ] 8.5 进度上报：扩展 `MaintainerStatus` 上报 `draining_dispatcher_count`
  - [ ] 8.6 单元测试：排除/迁移/上报
  - _Requirements: 4.1, 4.4, 4.7, 5.4_

- [-] 9. API/HTTP 语义对齐
  - [x] 9.1 路由与 handler 骨架（`api/v2/capture.go`, `api/v2/api.go`）
  - [x] 9.2 coordinator 侧 `DrainCapture/GetDrainStatus` 骨架（`coordinator/controller.go`）
  - [ ] 9.3 幂等：同目标重复 PUT 返回 202 + 当前计数；无工作负载返回 200 + 0 计数
  - [ ] 9.4 `GET /drain` 返回 remaining_dispatcher_count（由 coordinator 聚合）
  - [ ] 9.5 HTTP status mapping：支持 404/409/503（非 400/500 二分）
  - [ ] 9.6 API 测试覆盖主要错误场景
  - _Requirements: 1.6, 1.7, 7.5_

- [ ] 10. 选举约束（Elector）
  - [ ] 10.1 elector 基于持久化 liveness 决定是否 campaign（Draining/Stopping 不 campaign）
  - [ ] 10.2 “only draining node left”例外：重置 Alive + 清理 drain record
  - [ ] 10.3 单元/集成测试
  - _Requirements: 9.6, 9.8_

- [ ] 11. 可观测性
  - [ ] 11.1 Prometheus：drain 状态、剩余 maintainer/dispatcher、耗时
  - [ ] 11.2 日志：开始/完成/暂停/异常（含 epoch/target）
  - _Requirements: 7.6, 7.7_

- [ ] 12. 回归与集成验证
  - [ ] 12.1 e2e：maintainer 先迁移、dispatcher 后迁移、完成后 target 不再被调度
  - [ ] 12.2 failover：kill coordinator，恢复 drain（基于持久化）
  - [ ] 12.3 故障：destination node crash，迁移可继续

## Notes

- 本计划显式引入 `DrainRecord` 与 `drain_epoch`，用于解决通知/上报的“陈旧数据”问题。
- 若需要更快 MVP，可先落地：7 + 8 + 5.4（通知+迁移+进度聚合），再补 3/10（持久化与选举约束）。
