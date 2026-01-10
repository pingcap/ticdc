# Requirements Document

## Introduction

本文档定义了 TiCDC 新架构中 Drain Capture 功能的需求。Drain Capture 功能允许运维人员在不中断数据同步的情况下，将指定节点上的所有工作负载（包括 maintainers 和 dispatchers）迁移到其他节点，以便进行节点维护、升级或下线操作。

该功能参考了 tiflow 项目中的实现原理，但需要适配当前 ticdc 新架构的组件结构：
- **Coordinator**：负责全局调度，管理所有 changefeed 的 maintainer 分配
- **Maintainer**：负责单个 changefeed 的表调度和状态管理，需要将其上的 dispatchers 迁移走
- **Dispatcher Manager**：负责执行具体的表同步任务

Drain 操作需要同时处理两个层面的迁移：
1. **Maintainer 层面**：由 Coordinator 负责，将 maintainers 从目标节点迁移到其他节点
2. **Dispatcher 层面**：由各个 Maintainer 负责，将其管理的 dispatchers 从目标节点迁移到其他节点

## Glossary

- **Capture**: TiCDC 集群中的一个节点实例
- **Coordinator**: 协调器，负责管理所有 changefeed 的 maintainer 调度，运行在 owner 节点上
- **Maintainer**: 维护器，负责单个 changefeed 的表调度和 checkpoint 计算
- **Dispatcher**: 调度器，负责单个表的数据同步任务
- **Dispatcher_Manager**: 调度管理器，管理节点上所有 dispatchers 的生命周期
- **Drain**: 排空操作，将指定节点上的所有任务（maintainers 和 dispatchers）迁移到其他节点
- **Draining_Target**: 正在被排空的目标节点
- **Node_Manager**: 节点管理器，维护集群中所有存活节点的信息
- **Operator**: 操作符，表示一个调度操作（如添加、移动、停止 maintainer）
- **Operator_Controller**: 操作符控制器，管理和执行所有操作符
- **Span_Replication**: 表示一个表分片的复制任务
- **Drain_Record**: Drain 操作的持久化记录（包含 draining target、开始时间、计数与 epoch 等），用于 coordinator failover 恢复与一致性约束

## Requirements

### Requirement 1: HTTP API 接口

**User Story:** As a cluster administrator, I want to trigger drain capture via HTTP API, so that I can gracefully migrate workloads before node maintenance.

#### Acceptance Criteria

1. THE API_Server SHALL expose endpoint `PUT /api/v2/captures/{capture_id}/drain` for triggering drain operation
2. WHEN a drain request is received on a non-coordinator node, THE API_Server SHALL forward the request to the coordinator node
3. WHEN a drain request is received, THE API_Server SHALL validate that the target capture_id exists in the cluster
4. WHEN a drain request targets the coordinator node itself, THE API_Server SHALL return HTTP 400 with error message "cannot drain coordinator node"
5. WHEN the cluster has only one alive capture, THE API_Server SHALL return HTTP 400 with error message "at least 2 captures required for drain operation"
6. WHEN a drain operation is already in progress for another node, THE API_Server SHALL return HTTP 409 with error message "another drain operation is in progress"
7. WHEN the target capture has no workloads, THE API_Server SHALL return HTTP 200 with zero counts indicating drain is already complete
8. WHEN a valid drain request is processed, THE API_Server SHALL return HTTP 202 Accepted with the current workload count on the target node
9. THE API_Response SHALL include `current_maintainer_count` field indicating the number of maintainers on the target node
10. THE API_Response SHALL include `current_dispatcher_count` field indicating the total number of dispatchers on the target node across all changefeeds

### Requirement 2: Coordinator Drain 处理

**User Story:** As a coordinator, I want to handle drain requests and orchestrate maintainer migration, so that workloads are safely moved to other nodes.

#### Acceptance Criteria

1. WHEN the Coordinator receives a drain request, THE Coordinator SHALL validate the request parameters
2. WHEN validation passes, THE Coordinator SHALL set the target node as draining target in cluster state
3. THE Coordinator SHALL only allow one draining target at a time per cluster
4. IF a drain operation is already in progress, THEN THE Coordinator SHALL reject new drain requests with appropriate error
5. WHEN a node is set as draining target, THE Coordinator SHALL notify all maintainers about the draining node via a coordinator→maintainer notification channel (NOT via MaintainerHeartbeat)
6. WHEN a node is set as draining target, THE Coordinator SHALL generate MoveMaintainer operators for all maintainers on that node
7. THE Coordinator SHALL NOT schedule any new maintainers to the draining target node
8. THE Coordinator SHALL distribute maintainers to other nodes based on workload balancing
9. WHEN both maintainers AND dispatchers have been migrated from the draining node, THE Coordinator SHALL clear the draining target state

### Requirement 3: Drain Capture Scheduler

**User Story:** As a scheduler component, I want to generate migration tasks for draining nodes, so that maintainers are systematically moved to healthy nodes.

#### Acceptance Criteria

1. THE Drain_Capture_Scheduler SHALL be integrated into the existing scheduler framework and MUST run deterministically before balance scheduler while draining
2. WHEN a draining target is set, THE Drain_Capture_Scheduler SHALL generate MoveMaintainer operators in batches
3. THE Drain_Capture_Scheduler SHALL respect the configured batch size for concurrent migrations
4. THE Drain_Capture_Scheduler SHALL wait for current batch to complete before generating next batch
5. WHEN selecting destination nodes, THE Drain_Capture_Scheduler SHALL choose nodes with lowest workload, considering in-flight migrations in the current batch
6. THE Drain_Capture_Scheduler SHALL exclude the draining target from destination node selection
7. THE Drain_Capture_Scheduler SHALL skip maintainers that are not in stable state (replicating)
8. WHEN no more maintainers remain on draining node AND no dispatchers remain on draining node, THE Drain_Capture_Scheduler SHALL clear the draining target

### Requirement 4: Maintainer Dispatcher 迁移

**User Story:** As a maintainer, I want to migrate dispatchers away from draining nodes after being migrated to a new node, so that table replication continues without interruption.

#### Acceptance Criteria

1. WHEN a new Maintainer starts on a destination node after migration, THE Maintainer SHALL check for dispatchers on the draining node during bootstrap
2. THE Maintainer SHALL NOT schedule any new dispatchers to the draining node
3. WHEN a Maintainer detects dispatchers on the draining node, THE Maintainer SHALL generate MoveDispatcher operators for all those dispatchers
4. THE Maintainer SHALL migrate dispatchers in batches to avoid overwhelming the cluster
5. WHEN selecting destination nodes for dispatchers, THE Maintainer SHALL exclude the draining node
6. WHEN a Maintainer receives draining notification via the coordinator→maintainer notification channel, THE Maintainer SHALL update its local draining state and trigger dispatcher migration
7. THE Maintainer SHALL report the number of remaining dispatchers on the draining node back to the Coordinator for progress tracking

### Requirement 5: 调度排除机制

**User Story:** As a scheduler, I want to exclude draining nodes from scheduling decisions, so that no new workloads are assigned to nodes being drained.

#### Acceptance Criteria

1. THE Basic_Scheduler SHALL check draining target before scheduling new maintainers
2. WHEN a draining target is set, THE Basic_Scheduler SHALL exclude that node from available nodes list
3. THE Balance_Scheduler SHALL exclude draining target from rebalancing decisions
4. THE Maintainer_Scheduler SHALL exclude draining target when scheduling new dispatchers, based on the locally observed draining target provided by the coordinator

### Requirement 6: Maintainer 迁移执行

**User Story:** As a maintainer migration operator, I want to safely move maintainers between nodes, so that changefeed management continues without interruption.

#### Acceptance Criteria

1. WHEN a MoveMaintainer operator is created for drain, THE Operator SHALL stop the maintainer on source node
2. WHEN the source maintainer is stopped, THE Operator SHALL start the maintainer on destination node
3. THE new Maintainer SHALL create a new Table Trigger Event Dispatcher (ddlSpan) on the destination node
4. THE Operator SHALL track the migration state and report completion status
5. IF the destination node fails during migration, THEN THE Operator SHALL mark the maintainer as absent for rescheduling
6. IF the source node fails during migration, THEN THE Operator SHALL proceed with starting on destination node
7. THE Operator SHALL update the changefeed database with new node binding after successful migration
8. AFTER the new Maintainer bootstraps, THE Maintainer SHALL automatically migrate dispatchers from the draining node

### Requirement 7: 状态查询与监控

**User Story:** As a cluster administrator, I want to query drain progress and monitor the operation, so that I can track the migration status.

#### Acceptance Criteria

1. THE API_Server SHALL provide endpoint `GET /api/v2/captures/{capture_id}/drain` to query drain status
2. WHEN queried and no drain is in progress, THE System SHALL return HTTP 200 with `is_draining: false`
3. WHEN queried and drain is in progress, THE System SHALL return HTTP 200 with `is_draining: true` and progress details
4. THE Response SHALL include the count of remaining maintainers on draining node
5. THE Response SHALL include the count of remaining dispatchers on draining node per changefeed
6. THE System SHALL expose Prometheus metrics for drain operation progress
7. THE System SHALL log drain operation events with appropriate log levels for debugging and auditing

### Requirement 8: 错误处理与恢复

**User Story:** As a system operator, I want the drain operation to handle errors gracefully, so that the cluster remains stable during failures.

#### Acceptance Criteria

1. IF the coordinator fails during drain operation, THEN THE new coordinator SHALL detect and resume the drain operation based on persisted drain record and/or persisted node liveness state
2. IF a destination node fails during migration, THEN THE System SHALL reschedule affected maintainers and dispatchers to other available nodes
3. IF all non-draining nodes become unavailable, THEN THE System SHALL pause the drain operation and log warning
4. THE System SHALL implement timeout for individual maintainer and dispatcher migrations
5. WHEN a migration times out, THE System SHALL retry the migration with exponential backoff

### Requirement 9: 节点状态转换

**User Story:** As a system, I want to manage node liveness state transitions during drain, so that the node is properly shut down after drain completes.

#### Acceptance Criteria

1. WHEN a drain operation starts, THE System SHALL set the target node liveness to Draining AND persist this information for coordinator failover recovery
2. THE Draining state SHALL prevent new workloads from being scheduled to the node
3. WHEN all maintainers and dispatchers have been migrated, THE System SHALL transition the node liveness to Stopping
4. THE Stopping state is terminal; the node SHALL stop campaigning for coordinator election and MAY proceed to shutdown via operational control
5. THE System SHALL NOT allow transition from Draining back to Alive except when the draining node is the only node left in cluster
6. WHEN a node is in Draining state, THE node SHALL NOT participate in coordinator election
7. IF the draining node crashes during drain operation, THEN THE System SHALL clear the drain state and let basic scheduler handle rescheduling
8. IF the draining node is the only node left in cluster, THEN THE System SHALL reset its liveness to Alive and allow it to become coordinator

### Requirement 10: Drain 通知机制

**User Story:** As a coordinator, I want to notify nodes about draining status via a coordinator→maintainer notification channel, so that all components are aware of the drain operation.

#### Acceptance Criteria

1. WHEN a node is set as draining target, THE Coordinator SHALL broadcast draining status to all nodes via a coordinator→maintainer notification channel
2. THE notification payload SHALL include `draining_capture` field with the draining node ID (empty if no drain in progress) AND a monotonic `drain_epoch` for de-dup and stale protection
3. WHEN a Maintainer receives draining notification, THE Maintainer SHALL update its local draining state
4. WHEN a draining target is set, THE System SHALL propagate draining status to all relevant components within one heartbeat cycle
5. THE Coordinator SHALL be able to observe propagation and progress via maintainer heartbeats (e.g., remaining dispatcher counts)
