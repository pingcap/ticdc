# Design Document: Drain Capture

## Overview

本设计文档描述了 TiCDC 新架构中 Drain Capture 功能的详细设计。该功能允许运维人员将指定节点上的所有工作负载（maintainers 和 dispatchers）迁移到其他节点，然后下线该节点，以便进行节点维护、升级或下线操作。

### 设计目标

1. **无中断迁移**：在不中断数据同步的情况下完成工作负载迁移
2. **两层迁移**：同时处理 Maintainer 层面和 Dispatcher 层面的迁移
3. **调度排除**：确保不会有新任务调度到正在被排空的节点
4. **节点下线**：drain 完成后节点自动进入 Stopping 状态并下线
5. **可观测性**：提供状态查询和监控能力

### 核心设计原则

#### 1. Maintainer 与 Table Trigger Dispatcher 的 1:1 绑定

在 TiCDC 新架构中，每个 Maintainer 都有一个关联的 Table Trigger Event Dispatcher (ddlSpan)，它们必须在同一节点上运行：

- ddlSpan 在 Maintainer 创建时绑定到 `selfNode.ID`
- 负责处理 DDL 事件，与 Maintainer 紧密协作
- 如果 Maintainer 迁移到新节点，ddlSpan 会在新节点自动重建

#### 2. Maintainer 先迁移，Dispatcher 自然跟随

基于上述约束，我们采用 **Maintainer 先迁移** 的策略：

1. Coordinator 迁移 Maintainer 到新节点
2. 新 Maintainer 启动时自动创建新的 ddlSpan
3. 新 Maintainer bootstrap 后发现有 dispatchers 在 draining 节点上
4. 新 Maintainer 自动生成 MoveDispatcher operators 迁移这些 dispatchers

这种设计的优势：
- **复用现有机制**：利用现有的 Maintainer 迁移和 bootstrap 机制
- **自然满足约束**：ddlSpan 随 Maintainer 迁移自动重建
- **简化状态管理**：不需要复杂的两阶段协调


### Liveness 状态扩展

扩展现有的 `Liveness` 类型，添加 `Draining` 状态：

```go
type Liveness int32

const (
    LivenessCaptureAlive    Liveness = 0  // 节点正常运行
    LivenessCaptureStopping Liveness = 1  // 节点正在优雅关闭
    LivenessCaptureDraining Liveness = 2  // 节点正在被排空（新增）
)
```

**Liveness 状态说明**：

| 状态 | 说明 | 触发方式 | 行为 |
|------|------|----------|------|
| Alive | 节点正常运行 | 默认状态 | 可接收新任务 |
| Draining | 节点正在被排空 | drain API 触发 | 不接收新任务，主动迁移现有任务 |
| Stopping | 节点正在关闭 | drain 完成或节点主动关闭 | 不接收新任务，等待进程退出 |

**Liveness 状态转换规则**：
```
Alive → Draining：通过 drain API 触发
Draining → Stopping：drain 完成（所有任务迁移完成）
Alive → Stopping：节点优雅关闭（非 drain 场景）
```

**注意**：`Draining` 状态是单向的，不能回到 `Alive`，因为 drain 的目标是下线节点。

### 并发迁移配置

支持可配置的 Maintainer 迁移并发度：

```go
type DrainConfig struct {
    // MaintainerBatchSize is the number of maintainers to migrate concurrently
    // Default: 1
    MaintainerBatchSize int `toml:"maintainer-batch-size" json:"maintainer-batch-size"`
}
```

- 默认并发度为 1，保守策略，确保稳定性
- 可根据集群规模和网络状况调整
- 每批次等待当前迁移完成后再开始下一批


## Architecture

### Drain 流程时序图

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant API as API Server
    participant Coord as Coordinator
    participant OldMaint as Old Maintainer (Node A)
    participant NewMaint as New Maintainer (Node B)
    participant Disp as Dispatchers

    Client->>API: PUT /api/v2/captures/{id}/drain
    API->>Coord: DrainCapture(captureID)
    Coord->>Coord: Validate & Set DrainState
    Coord->>Coord: SetNodeLiveness(captureID, Draining)
    Coord-->>API: 202 Accepted + Counts
    API-->>Client: Response

    Note over Coord,Disp: Phase 1: Maintainer Migration
    Coord->>Coord: DrainScheduler generates MoveMaintainer ops (batch)
    Coord->>OldMaint: RemoveMaintainerRequest
    OldMaint->>OldMaint: Stop & Cleanup
    Coord->>NewMaint: AddMaintainerRequest
    NewMaint->>NewMaint: Create new ddlSpan
    NewMaint->>NewMaint: Bootstrap & discover dispatchers on draining node

    Note over Coord,Disp: Phase 2: Dispatcher Migration (driven by new Maintainer)
    NewMaint->>NewMaint: Generate MoveDispatcher ops for dispatchers on Node A
    NewMaint->>Disp: Move dispatchers to other nodes
    Disp-->>NewMaint: Migration complete

    Note over Coord,Disp: Phase 3: Completion
    Coord->>Coord: All maintainers migrated
    Coord->>Coord: Check all dispatchers migrated
    Coord->>Coord: SetNodeLiveness(captureID, Stopping)
    Coord->>Coord: Clear DrainState
```

### 组件交互图

```mermaid
flowchart TD
    Client["Client"] -->|PUT /api/v2/captures/{id}/drain| API["API Server"]

    API --> FWD{"Is Coordinator?"}
    FWD -- "no" --> Proxy["Forward to Coordinator"]
    Proxy --> CoordAPI["Coordinator API Handler"]
    FWD -- "yes" --> CoordAPI

    CoordAPI --> Validate["Validate Request"]
    Validate --> SetDrain["Set Draining Target"]
    SetDrain --> SetLiveness["Set Node Liveness = Draining"]
    SetLiveness --> Response["Return 202 + Counts"]

    SetDrain --> DrainSched["Drain Capture Scheduler"]
    DrainSched --> GenMaintOps["Generate MoveMaintainer Operators (batch)"]
    GenMaintOps --> OpCtrl["Operator Controller"]

    OpCtrl --> RemoveOld["RemoveMaintainerRequest to Old Node"]
    OpCtrl --> AddNew["AddMaintainerRequest to New Node"]

    AddNew --> NewMaint["New Maintainer"]
    NewMaint --> CreateDDL["Create new ddlSpan"]
    CreateDDL --> Bootstrap["Bootstrap"]
    Bootstrap --> CheckDrain["Check dispatchers on draining node"]
    CheckDrain --> GenDispOps["Generate MoveDispatcher Operators"]
    GenDispOps --> DispMigrate["Dispatcher Migration"]

    DispMigrate --> Complete["All Migrations Complete"]
    Complete --> SetStopping["Set Node Liveness = Stopping"]
    SetStopping --> ClearDrain["Clear Draining Target"]
```


### 状态流转图

```mermaid
stateDiagram-v2
    [*] --> Idle: Initial State

    Idle --> Draining: Drain Request Received

    state Draining {
        [*] --> MigratingMaintainers
        MigratingMaintainers --> MigratingDispatchers: Maintainer migrated, new Maintainer handles dispatchers
        MigratingDispatchers --> [*]: All Dispatchers Migrated
    }

    Draining --> Stopping: Drain Complete
    Stopping --> NodeShutdown: Process Exit
    NodeShutdown --> [*]: Node Removed
```

## Components and Interfaces

### 1. Liveness 扩展

```go
// pkg/node/node.go - 扩展 Liveness 类型

type Liveness int32

const (
    LivenessCaptureAlive    Liveness = 0
    LivenessCaptureStopping Liveness = 1
    LivenessCaptureDraining Liveness = 2  // 新增
)

// IsSchedulable returns true if the node can accept new workloads.
// Returns false if node is Draining or Stopping.
func (l *Liveness) IsSchedulable() bool {
    return l.Load() == LivenessCaptureAlive
}

// StoreDraining sets liveness to Draining. Returns true if successful.
// Can only transition from Alive to Draining.
func (l *Liveness) StoreDraining() bool {
    return atomic.CompareAndSwapInt32(
        (*int32)(l), int32(LivenessCaptureAlive), int32(LivenessCaptureDraining))
}

// DrainComplete transitions from Draining to Stopping.
// Returns true if successful.
func (l *Liveness) DrainComplete() bool {
    return atomic.CompareAndSwapInt32(
        (*int32)(l), int32(LivenessCaptureDraining), int32(LivenessCaptureStopping))
}
```

### 2. NodeManager Liveness 管理

```go
// 添加 Liveness 管理到 NodeManager

type NodeManager struct {
    // ... existing fields ...

    // nodeLiveness tracks the liveness state of each node
    nodeLiveness sync.Map // node.ID -> *Liveness
}

// GetNodeLiveness returns the liveness of a node
func (c *NodeManager) GetNodeLiveness(id node.ID) Liveness {
    if l, ok := c.nodeLiveness.Load(id); ok {
        return l.(*Liveness).Load()
    }
    return LivenessCaptureAlive
}

// SetNodeLiveness sets the liveness of a node
func (c *NodeManager) SetNodeLiveness(id node.ID, liveness Liveness) {
    l := &Liveness{}
    actual, _ := c.nodeLiveness.LoadOrStore(id, l)
    actual.(*Liveness).Store(liveness)
}

// GetSchedulableNodes returns nodes that can accept new workloads
// Excludes nodes with Draining or Stopping liveness
func (c *NodeManager) GetSchedulableNodes() map[node.ID]*node.Info {
    allNodes := c.GetAliveNodes()
    result := make(map[node.ID]*node.Info)
    for id, info := range allNodes {
        if c.GetNodeLiveness(id).IsSchedulable() {
            result[id] = info
        }
    }
    return result
}

// GetCoordinatorCandidates returns nodes that can become coordinator
// Excludes nodes with Draining or Stopping liveness
func (c *NodeManager) GetCoordinatorCandidates() map[node.ID]*node.Info {
    allNodes := c.GetAliveNodes()
    result := make(map[node.ID]*node.Info)
    for id, info := range allNodes {
        liveness := c.GetNodeLiveness(id)
        // Draining and Stopping nodes cannot become coordinator
        if liveness == LivenessCaptureAlive {
            result[id] = info
        }
    }
    return result
}
```

### 3. Coordinator 选举集成

```go
// 在 coordinator 选举逻辑中使用 GetCoordinatorCandidates

func (e *Election) GetCandidates() map[node.ID]*node.Info {
    // Only nodes with Alive liveness can become coordinator
    return e.nodeManager.GetCoordinatorCandidates()
}
```


### 3. API Layer

#### DrainCaptureResponse
```go
// DrainCaptureResponse represents the response of drain capture API
type DrainCaptureResponse struct {
    CurrentMaintainerCount int `json:"current_maintainer_count"`
    CurrentDispatcherCount int `json:"current_dispatcher_count"`
}
```

#### DrainStatusResponse
```go
// DrainStatusResponse represents the drain status query response
type DrainStatusResponse struct {
    IsDraining               bool           `json:"is_draining"`
    DrainingCaptureID        string         `json:"draining_capture_id,omitempty"`
    RemainingMaintainerCount int            `json:"remaining_maintainer_count"`
    RemainingDispatcherCount map[string]int `json:"remaining_dispatcher_count"` // changefeed_id -> count
}
```

#### API Endpoints
```go
// api/v2/api.go - 注册 drain 相关路由

// PUT /api/v2/captures/{capture_id}/drain - 触发 drain
// GET /api/v2/captures/{capture_id}/drain - 查询 drain 状态
```

### 4. Coordinator Layer

#### DrainState
```go
// coordinator/drain_state.go

// DrainState tracks the drain operation state in coordinator
type DrainState struct {
    mu sync.RWMutex

    // drainingTarget is the capture ID being drained, empty if no drain in progress
    drainingTarget node.ID

    // startTime is when the drain operation started
    startTime time.Time

    // initialMaintainerCount is the count when drain started
    initialMaintainerCount int

    // initialDispatcherCount is the count when drain started
    initialDispatcherCount int

    // nodeManager is used to update node liveness
    nodeManager NodeManager
}

func NewDrainState(nodeManager NodeManager) *DrainState {
    return &DrainState{
        nodeManager: nodeManager,
    }
}

func (s *DrainState) SetDrainingTarget(target node.ID, maintainerCount, dispatcherCount int) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.drainingTarget != "" {
        return errors.New("another drain operation is in progress")
    }

    s.drainingTarget = target
    s.startTime = time.Now()
    s.initialMaintainerCount = maintainerCount
    s.initialDispatcherCount = dispatcherCount

    // Update node liveness to Draining
    s.nodeManager.SetNodeLiveness(target, LivenessCaptureDraining)

    return nil
}

func (s *DrainState) GetDrainingTarget() node.ID {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.drainingTarget
}

func (s *DrainState) ClearDrainingTarget() {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.drainingTarget != "" {
        // Transition to Stopping state
        s.nodeManager.SetNodeLiveness(s.drainingTarget, LivenessCaptureStopping)
        s.drainingTarget = ""
    }
}

func (s *DrainState) IsDraining() bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.drainingTarget != ""
}
```


#### Coordinator Interface Extension
```go
// Add to Coordinator interface
type Coordinator interface {
    // ... existing methods ...

    // DrainCapture initiates drain operation for the specified capture
    DrainCapture(ctx context.Context, captureID node.ID) (*DrainCaptureResponse, error)

    // GetDrainStatus returns the current drain status
    GetDrainStatus(ctx context.Context, captureID node.ID) (*DrainStatusResponse, error)
}
```

### 5. Drain Capture Scheduler

```go
// coordinator/scheduler/drain.go

// drainScheduler generates MoveMaintainer operators for draining captures
type drainScheduler struct {
    id                 string
    batchSize          int  // configurable, default 1
    operatorController *operator.Controller
    changefeedDB       *changefeed.ChangefeedDB
    nodeManager        NodeManager
    drainState         *DrainState
}

func NewDrainScheduler(
    id string,
    batchSize int,
    oc *operator.Controller,
    changefeedDB *changefeed.ChangefeedDB,
    drainState *DrainState,
    nodeManager NodeManager,
) *drainScheduler {
    if batchSize <= 0 {
        batchSize = 1 // default to 1
    }
    return &drainScheduler{
        id:                 id,
        batchSize:          batchSize,
        operatorController: oc,
        changefeedDB:       changefeedDB,
        drainState:         drainState,
        nodeManager:        nodeManager,
    }
}

func (s *drainScheduler) Execute() time.Time {
    drainingTarget := s.drainState.GetDrainingTarget()
    if drainingTarget == "" {
        return time.Now().Add(time.Second)
    }

    // Get maintainers on draining capture
    maintainers := s.changefeedDB.GetByNodeID(drainingTarget)
    if len(maintainers) == 0 {
        // All maintainers migrated, check if all dispatchers are also migrated
        if s.allDispatchersMigrated(drainingTarget) {
            log.Info("drain complete, all maintainers and dispatchers migrated",
                zap.String("capture", drainingTarget.String()))
            s.drainState.ClearDrainingTarget()
        } else {
            log.Debug("waiting for dispatchers to migrate",
                zap.String("capture", drainingTarget.String()))
        }
        return time.Now().Add(time.Second)
    }

    // Get schedulable nodes (excludes draining/stopping nodes)
    schedulableNodes := s.nodeManager.GetSchedulableNodes()
    if len(schedulableNodes) == 0 {
        log.Warn("no schedulable nodes available for drain")
        return time.Now().Add(time.Second * 5)
    }

    // Count current in-progress operators for draining node
    inProgressCount := s.countInProgressOperators(drainingTarget)
    availableBatch := s.batchSize - inProgressCount
    if availableBatch <= 0 {
        // Wait for current batch to complete
        return time.Now().Add(time.Millisecond * 500)
    }

    // Generate MoveMaintainer operators in batches
    batchCount := 0
    for _, cf := range maintainers {
        if batchCount >= availableBatch {
            break
        }

        // Skip if operator already exists
        if s.operatorController.GetOperator(cf.ID) != nil {
            continue
        }

        // Select destination with lowest workload
        destNode := s.selectDestination(schedulableNodes)
        if destNode == "" {
            continue
        }

        op := operator.NewMoveMaintainerOperator(s.changefeedDB, cf, drainingTarget, destNode)
        if s.operatorController.AddOperator(op) {
            batchCount++
            log.Info("generated move maintainer operator for drain",
                zap.String("changefeed", cf.ID.String()),
                zap.String("from", drainingTarget.String()),
                zap.String("to", destNode.String()))
        }
    }

    return time.Now().Add(time.Millisecond * 500)
}

func (s *drainScheduler) countInProgressOperators(nodeID node.ID) int {
    count := 0
    for _, cf := range s.changefeedDB.GetByNodeID(nodeID) {
        if s.operatorController.GetOperator(cf.ID) != nil {
            count++
        }
    }
    return count
}

func (s *drainScheduler) allDispatchersMigrated(nodeID node.ID) bool {
    // Check if any changefeed still has dispatchers on the draining node
    // This information is reported via maintainer heartbeat
    for _, cf := range s.changefeedDB.GetAll() {
        if cf.GetDispatcherCountOnNode(nodeID) > 0 {
            return false
        }
    }
    return true
}

func (s *drainScheduler) selectDestination(nodes map[node.ID]*node.Info) node.ID {
    var minNode node.ID
    minCount := math.MaxInt

    nodeTaskSize := s.changefeedDB.GetTaskSizePerNode()
    for id := range nodes {
        if count := nodeTaskSize[id]; count < minCount {
            minCount = count
            minNode = id
        }
    }

    return minNode
}

func (s *drainScheduler) Name() string {
    return "drain-scheduler"
}
```


### 6. Heartbeat Extension

```go
// heartbeatpb/heartbeat.proto - 添加 draining_capture 字段

message MaintainerHeartbeat {
    // ... existing fields ...

    // draining_capture is the capture ID being drained, empty if no drain in progress
    // Maintainer should not schedule new dispatchers to this capture
    string draining_capture = N;
}
```

### 7. Maintainer Layer

新 Maintainer 启动后，通过 bootstrap 发现 draining 节点上的 dispatchers，并自动迁移：

```go
// maintainer/maintainer.go - 添加 drain 状态处理

type Maintainer struct {
    // ... existing fields ...

    // drainingCapture is the capture being drained, empty if no drain in progress
    drainingCapture atomic.Value // node.ID
}

func (m *Maintainer) SetDrainingCapture(captureID node.ID) {
    m.drainingCapture.Store(captureID)
    m.statusChanged.Store(true)
}

func (m *Maintainer) GetDrainingCapture() node.ID {
    if v := m.drainingCapture.Load(); v != nil {
        return v.(node.ID)
    }
    return ""
}

// OnDrainingCaptureNotified is called when maintainer receives draining notification via heartbeat
func (m *Maintainer) OnDrainingCaptureNotified(drainingCapture node.ID) {
    oldDrainingCapture := m.GetDrainingCapture()
    if oldDrainingCapture == drainingCapture {
        return // No change
    }

    m.SetDrainingCapture(drainingCapture)

    if drainingCapture == "" {
        log.Info("drain completed, clearing draining capture",
            zap.Stringer("changefeed", m.changefeedID))
        return
    }

    log.Info("received draining capture notification",
        zap.Stringer("changefeed", m.changefeedID),
        zap.String("drainingCapture", drainingCapture.String()))

    // Generate MoveDispatcher operators for dispatchers on draining capture
    m.controller.GenerateMoveDispatcherOperatorsForNode(drainingCapture)
}

// Called after bootstrap completes to check for dispatchers on draining node
func (m *Maintainer) checkAndMigrateDispatchersOnDrainingNode() {
    drainingCapture := m.GetDrainingCapture()
    if drainingCapture == "" {
        return
    }

    // Generate MoveDispatcher operators for dispatchers on draining capture
    m.controller.GenerateMoveDispatcherOperatorsForNode(drainingCapture)
}
```

### 8. Scheduler Exclusion Integration

```go
// coordinator/scheduler/basic.go - 更新 basicScheduler

func (s *basicScheduler) doBasicSchedule(availableSize int) {
    // Use GetSchedulableNodes instead of GetAliveNodes
    // This automatically excludes draining and stopping nodes
    schedulableNodes := s.nodeManager.GetSchedulableNodes()
    // ... rest of scheduling logic
}
```

```go
// coordinator/scheduler/balance.go - 更新 balanceScheduler

func (s *balanceScheduler) Execute() time.Time {
    // Use GetSchedulableNodes for balance decisions
    schedulableNodes := s.nodeManager.GetSchedulableNodes()
    // ... rest of balancing logic
}
```


## Data Models

### Drain Operation State (In-Memory)

```go
type DrainStatus int

const (
    DrainStatusNotStarted DrainStatus = iota
    DrainStatusMigratingMaintainers
    DrainStatusMigratingDispatchers
    DrainStatusCompleted
)
```

### Configuration

```go
// pkg/config/scheduler.go

type SchedulerConfig struct {
    // ... existing fields ...

    // DrainMaintainerBatchSize is the number of maintainers to migrate concurrently during drain
    // Default: 1
    DrainMaintainerBatchSize int `toml:"drain-maintainer-batch-size" json:"drain-maintainer-batch-size"`
}
```

### Metrics

```go
var (
    DrainCaptureGauge = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "ticdc",
            Subsystem: "coordinator",
            Name:      "drain_capture_status",
            Help:      "Status of drain capture operation (0=idle, 1=draining)",
        },
        []string{"capture_id"},
    )

    DrainCaptureMaintainerCount = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "ticdc",
            Subsystem: "coordinator",
            Name:      "drain_capture_remaining_maintainers",
            Help:      "Number of remaining maintainers on draining capture",
        },
        []string{"capture_id"},
    )

    DrainCaptureDispatcherCount = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "ticdc",
            Subsystem: "coordinator",
            Name:      "drain_capture_remaining_dispatchers",
            Help:      "Number of remaining dispatchers on draining capture",
        },
        []string{"capture_id"},
    )

    DrainCaptureDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: "ticdc",
            Subsystem: "coordinator",
            Name:      "drain_capture_duration_seconds",
            Help:      "Duration of drain capture operation",
            Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~17min
        },
        []string{"capture_id"},
    )
)
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system.*

### Property 1: Draining Node Exclusion from Scheduling

*For any* scheduling decision (maintainer or dispatcher), when a draining target is set, the draining node SHALL NOT be selected as a destination node.

**Validates: Requirements 2.7, 3.6, 4.2, 4.5, 5.2, 5.3, 5.4**

### Property 2: Single Draining Target Invariant

*For any* cluster state, there SHALL be at most one draining target at any time.

**Validates: Requirements 2.3, 2.4**

### Property 3: Maintainer-DDLSpan Co-location

*For any* maintainer, its associated Table Trigger Event Dispatcher (ddlSpan) SHALL always be on the same node as the maintainer.

**Validates: Architectural constraint**

### Property 4: Drain State Transition

*For any* drain operation, the node liveness SHALL transition from Alive → Draining → Stopping, and never back to Alive.

**Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5**

### Property 5: Batch Size Constraint

*For any* batch of migration operators generated by the drain scheduler, the number of concurrent in-progress operators SHALL NOT exceed the configured batch size.

**Validates: Requirements 3.3**

### Property 6: Drain Completion Condition

*For any* drain operation, the drain SHALL be marked complete only when both maintainer count AND dispatcher count on the draining node are zero.

**Validates: Requirements 2.9, 3.8**

### Property 7: Heartbeat Draining Notification Propagation

*For any* draining target set in coordinator, the draining capture ID SHALL be included in heartbeat messages to all maintainers.

**Validates: Requirements 10.1, 10.2**

### Property 8: Workload Balanced Distribution

*For any* set of maintainers being migrated from a draining node, the destination nodes SHALL be selected based on lowest workload.

**Validates: Requirements 2.8, 3.5**

### Property 9: Draining Node Cannot Become Coordinator (with exception)

*For any* coordinator election, nodes with Draining or Stopping liveness SHALL NOT be selected as coordinator candidates, EXCEPT when the draining node is the only node left in cluster.

**Validates: Requirements 9.6, 9.8**

### Property 10: Draining Node Crash Cleanup

*For any* draining node that crashes, the drain state SHALL be cleared and the maintainers SHALL be rescheduled by basic scheduler.

**Validates: Requirements 9.7**

### Property 11: Destination Node Failure Handling

*For any* destination node failure during maintainer migration, the MoveMaintainer operator SHALL either mark the maintainer as absent (if origin stopped) or convert to add operation (if origin not stopped).

**Validates: Requirements 8.2**


## Error Handling

### API Error Responses

| Condition | HTTP Status | Error Message |
|-----------|-------------|---------------|
| Target is coordinator | 400 | "cannot drain coordinator node" |
| Single node cluster | 400 | "at least 2 captures required for drain operation" |
| Drain already in progress | 409 | "another drain operation is in progress" |
| Capture not found | 404 | "capture not found" |
| Internal error | 500 | "internal server error: {details}" |

### Coordinator 选举约束

**Draining 节点不能成为 Coordinator**：
- 当节点处于 `Draining` 状态时，该节点不参与 coordinator 选举
- 这确保了 drain 操作的一致性：coordinator 负责协调 drain，不能自己 drain 自己

**特殊情况：集群只剩 Draining 节点**：
- 如果集群中只剩下一个节点，且该节点处于 `Draining` 状态
- 该节点可以将自己的状态从 `Draining` 重置为 `Alive`
- 然后成为 coordinator，恢复集群可用性
- 这是唯一允许从 `Draining` 回到 `Alive` 的场景

```go
// 在 coordinator 选举逻辑中
func (e *Election) TryBecomeCoordinator() bool {
    candidates := e.nodeManager.GetCoordinatorCandidates()

    // 如果没有候选节点，检查是否只剩自己（draining 状态）
    if len(candidates) == 0 {
        allNodes := e.nodeManager.GetAliveNodes()
        if len(allNodes) == 1 && allNodes[e.selfID] != nil {
            // 只剩自己，重置 draining 状态
            if e.nodeManager.GetNodeLiveness(e.selfID) == LivenessCaptureDraining {
                log.Warn("only draining node left in cluster, resetting to alive",
                    zap.String("node", e.selfID.String()))
                e.nodeManager.SetNodeLiveness(e.selfID, LivenessCaptureAlive)
                // 清理 drain 状态
                e.drainState.ClearDrainingTargetWithoutTransition()
                return true
            }
        }
        return false
    }
    // ... 正常选举逻辑
}
```

### Recovery Scenarios

1. **Coordinator Failover During Drain**
   - New coordinator detects nodes with Draining liveness
   - Drain operation continues from current state
   - No explicit state persistence needed, liveness state is sufficient
   - **注意**：Draining 节点不参与 coordinator 选举

2. **Destination Node Failure During Maintainer Migration**
   - MoveMaintainer operator 的 `OnNodeRemove` 被调用
   - 如果 origin 节点已停止：将 maintainer 标记为 absent，由 basic scheduler 重新调度
   - 如果 origin 节点未停止：将 move 操作转换为 add 操作，目标变为 origin 节点
   - 这是现有调度模块的标准行为，drain 场景复用此逻辑

3. **Destination Node Permanent Failure**
   - 与场景 2 相同，operator 会处理节点故障
   - Maintainer 被标记为 absent
   - Basic scheduler 会选择另一个可用节点重新调度
   - Drain 操作继续进行

4. **Draining Node Failure (Source Node Crash)**
   - Node failure detected by NodeManager
   - Maintainers on that node become absent
   - Basic scheduler reschedules them to other nodes
   - **Drain 操作自动完成**：节点已经不在了，无需继续 drain
   - Coordinator 清理 DrainState

5. **All Non-Draining Nodes Unavailable**
   - Drain scheduler detects no available destination nodes
   - Drain operation pauses (no new operators generated)
   - Warning logged
   - Resumes when nodes become available

6. **New Maintainer Bootstrap Failure**
   - If new maintainer fails to bootstrap, it will be marked as absent
   - Basic scheduler will reschedule to another node
   - Drain continues with remaining maintainers

7. **Two-Node Cluster: Coordinator Crash During Drain**
   - Draining 节点检测到自己是唯一存活节点
   - 重置自己的 Draining 状态为 Alive
   - 成为 coordinator，恢复集群可用性
   - Drain 操作被取消（因为原 coordinator 已经不在了）

8. **Only Draining Node Left in Cluster**
   - 当集群中只剩下 draining 节点时
   - 该节点重置状态为 Alive
   - 成为 coordinator
   - 这是唯一允许从 Draining 回到 Alive 的场景

## Testing Strategy

### Unit Tests

1. **Liveness Tests**
   - Test state transitions (Alive → Draining → Stopping)
   - Test IsSchedulable() returns false for Draining and Stopping

2. **DrainState Tests**
   - Test single draining target invariant
   - Test state transitions
   - Test concurrent access safety

3. **Scheduler Tests**
   - Test node exclusion logic
   - Test batch size constraint
   - Test destination node selection based on workload

4. **Maintainer Tests**
   - Test draining notification handling
   - Test dispatcher migration after bootstrap

### Integration Tests

1. **End-to-End Drain Test**
   - Set up multi-node cluster with multiple changefeeds
   - Trigger drain on one node
   - Verify maintainers migrate first
   - Verify dispatchers migrate after maintainer migration
   - Verify node transitions to Stopping
   - Verify data replication continues without interruption

2. **Concurrent Maintainer Migration Test**
   - Set up cluster with many maintainers on one node
   - Configure batch size > 1
   - Verify concurrent migration respects batch size
   - Verify all maintainers eventually migrate

3. **Failover During Drain Test**
   - Start drain operation
   - Kill coordinator
   - Verify new coordinator continues drain

4. **Node Failure During Drain Test**
   - Start drain operation
   - Kill destination node during migration
   - Verify maintainer is rescheduled to another node
