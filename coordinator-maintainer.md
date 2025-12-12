# 协调器 ↔ Maintainer Manager 交互逻辑

## Coordinator & Maintainer Manager

协调器（Coordinator）负责编排 changefeed、持久化状态与 checkpoint；Maintainer Manager 在每个 TiCDC 节点上托管按 changefeed 实例化的 Maintainer 工作进程。

- CoordinatorTopic、MaintainerManagerTopic，以及用于按 Maintainer 路由的 MaintainerTopic。

## controller

控制器是协调器的内部状态机与调度核心，负责解释来自消息总线的事件、推进 changefeed 状态与进度、以及生成对 Maintainer/Manager 的具体操作指令。

- 它通过明确的事件 → 状态转换规则和幂等的持久化操作，保证在故障与重复消息场景下维持一致性。

事件输入与分类

- 输入源：来自 CoordinatorTopic 的 TargetMessage（Heartbeat、BlockStatus、Bootstrap/PostBootstrap、CheckpointTs 等）。
- 分类处理：
  - 心跳事件：更新 changefeed 的运行态快照（resolvedTs/checkpointTs、阶段与健康信息）。
  - 阻塞/错误事件：记录 BlockStatus/错误摘要，评估 Severity 与 Retryable，可能触发状态转 Warning/Failed。
  - 引导完成事件：标记 Maintainer 已就绪，允许调度与进度推进。
  - 检查点事件：校验单调递增后更新本地与待持久化队列。

状态机与转换

- 核心状态遵循文档的“状态枚举与错误码”；控制器根据事件与策略执行以下转换：
  - Normal→Warning：心跳缺失/下游背压/可恢复错误；记录 Error 并触发降级或重试。
  - Warning→Normal：心跳恢复、下游恢复或人工干预；清理警告并恢复推进。
  - Warning→Failed：致命不可恢复错误、重试超限、维护进程崩溃；停止算子。
  - Any→Paused：接收到暂停请求时，协调优雅停止并冻结进度。
  - Paused→Normal：接收到恢复请求且资源与屏障就绪时恢复运行。

调度与指令生成

- 基于当前拓扑与节点负载选择目标 Manager/节点；对单 changefeed 生成以下指令：AddMaintainer、StartMaintainer、StopMaintainer、RemoveMaintainer、CheckpointTsMessage。
- 指令具备幂等标识（changefeedID、epoch、版本），重复发送不会产生副作用；失败则结合 Error.Retryable 策略进行退避重试。

进度与持久化队列

- 控制器维护待持久化队列（batch queue），按窗口合并各 changefeed 的 checkpointTs 以降低写放大。
- 定期将增量提交给协调器的持久化执行器（backend.UpdateChangefeedCheckpointTs）；提交前进行单调性与版本校验。

容错与恢复

- 心跳超时：标记失联并进入 Warning，尝试在新节点上重启 Maintainer；超过阈值转 Failed。
- 元存储冲突：采用重试与乐观并发控制（版本/epoch 校验），在冲突时回读并重算。
- 事件乱序/重复：使用事件序列号或时间戳做去重与抑制，确保状态推进单调。

与协调器的边界

- 控制器不直接进行持久化与 GC 更新；它产出变更与指令，由协调器执行持久化、GC 屏障更新与下发命令。
- 控制器仅关注“决策”，协调器负责“落实”。

## 引导流程（Bootstrap）

1. controller 向 Maintainer Manager 发送 CoordinatorBootstrapRequest。
2. Manager 更新 coordinatorID/版本，并以 CoordinatorBootstrapResponse 回复，包含所有 MaintainerStatus 快照。
3. 引导完成后，Manager 周期性发送 MaintainerHeartbeat，当状态变化或距离上次上报超过 1 秒时发送增量。

## Changefeed life cycle management

### Create changefeed

### Pause changefeed

### Resume changefeed

### Remove changefeed

changefeed 生命周期指令
- AddMaintainerRequest（Coordinator → Manager）：Manager 创建 Maintainer，推送 EventInit，启动工作进程。
- RemoveMaintainerRequest（Coordinator → Manager）：Manager 将请求路由到现有 Maintainer 或创建临时移除器（级联），推送 EventMessage 执行移除。
- Manager 可通过 MaintainerHeartbeat 返回包含单个 MaintainerStatus 的即时反馈。

事件流（运行时消息）
- 来自 Maintainer → Coordinator：HeartBeatRequest、BlockStatusRequest、MaintainerBootstrapResponse/PostBootstrapResponse、CheckpointTsMessage 由 Manager 路由到对应 Maintainer，并经 CoordinatorTopic 上报给协调器。
- 协调器在 CoordinatorTopic 上注册 recvMessages；收到的 TargetMessage 包装为 coordinator.Event，经 eventCh 转发给 Controller。
- Controller 处理事件，并在 changefeedChangeCh 上产生 changefeedChange（状态/TS）返回给协调器。

Checkpoint TS 处理
1. Controller 汇报各 changefeed 的最新 checkpointTs。
2. 协调器 saveCheckpointTs：
   - 若新值大于已保存的值，则批量在后端元存储执行 UpdateChangefeedCheckpointTs。
   - 执行 GC 过期检查（gc.Manager.CheckStaleCheckpointTs）；在 Warning/快速失败时，将状态变更入队到 changefeedChangeCh。
   - 对需要通知的 changefeed，通过 MessageCenter 向 Maintainer 发送 CheckpointTsMessage。

状态变更处理
- 协调器 handleStateChange 更新 ChangeFeedInfo.State/Error，并通过 backend.UpdateChangefeed 持久化。
- 典型状态迁移：
  - StateWarning：停止该 changefeed 的运行算子，递增 epoch，重新入队调度。
  - StateFailed/Finished：停止运行算子。
  - StateNormal：为创建/恢复清理并确保 PD 的 GC safepoint。

GC safepoint/屏障管理
- 周期性 ticker（默认 1 分钟）驱动 updateGCSafepoint：
  - NextGen：计算按 keyspace 的 GC 屏障并通过 gc.Manager.TryUpdateKeyspaceGCBarrier 更新。
  - Legacy：计算全局最小 checkpointTs（或在无 changefeed 时使用 PD 时间），并通过 TryUpdateGCSafePoint 更新。
- 在创建/恢复 changefeed 时，协调器立即更新对应屏障/安全点。

并发模型
- 协调器：
  - Goroutine：run（GC、checkpoint 持久化、状态变更）、runHandleEvent（事件分发到 Controller）、collectMetrics。
  - 事件通道：eventCh（来自 MessageCenter）、changefeedChangeCh（来自 Controller）。
- Manager：
  - msgCh 缓冲协调器命令；Run 循环消费 msgCh，发送周期性心跳，并清理已移除的 Maintainer。
  - dispatcherMaintainerMessage 将按 changefeed 的消息路由到对应 Maintainer。

主题与消息类型
- CoordinatorTopic：接收 Manager 的心跳与 maintainer 响应；协调器向 Maintainer 发送 CheckpointTsMessage。
- MaintainerManagerTopic：接收 Add/Remove/Bootstrap 请求。
- MaintainerTopic：按 Maintainer 的关闭及其它作用域响应，由 Manager 路由。

注意事项
- Manager 在执行命令之前验证 coordinatorID 与版本。
- 协调器优雅停止：注销处理器、等待守护、停止 controller、取消上下文、排空 eventCh。


新增：Maintainer Manager ↔ Maintainer 交互逻辑

概览
- Maintainer 是 changefeed 的执行单元，负责拉取增量、排序、编码、下游写入与本地进度管理。
- Maintainer Manager 负责生命周期管理（创建、启动、停止、销毁）、消息路由与心跳聚合。

消息类型（Manager ↔ Maintainer）
- EventInit：Manager 启动 Maintainer 前注入初始化事件（变更基本信息、下游配置、初始 checkpoint）。
- StartMaintainer：启动运行循环；维持内部 goroutine 与资源初始化。
- StopMaintainer：请求优雅停止；允许 flush 进度与释放资源。
- RemoveMaintainer：销毁实例；清理本地状态并上报最终状态。
- MaintainerHeartbeat：Maintainer 定期向 Manager 上报运行指标、错误摘要、阶段状态与进度（resolvedTs/checkpointTs）。
- BlockStatusRequest/Response：Maintainer 上报阻塞原因（例如下游背压、DDL 阻塞、网络异常），Manager 汇总后上行给协调器。
- PostBootstrapResponse：Maintainer 完成内部引导后回复，包含实际运行参数与能力协商结果。
- CheckpointTsMessage：Manager 从协调器转发的 checkpoint 通知，Maintainer 更新本地推进策略（例如控制拉取/写入速率）。
- CloseMaintainer：Manager 触发关闭，Maintainer 回复 CloseAck 并进入 terminal 状态。

时序图（ASCII）

Coordinator        Manager             Maintainer
    |                |                      |
    | AddMaintainer  |                      |
    |--------------->| create               |
    |                |----EventInit-------->|
    |                |----StartMaintainer-->|
    |                |                      | run loop
    |                |<---Heartbeat---------|
    |<---Heartbeat---|                      |
    | CheckpointTs   |                      |
    |--------------->|----CheckpointTs----->|
    |                |                      | apply
    | RemoveMaintainer                       |
    |--------------->|----StopMaintainer--->|
    |                |----RemoveMaintainer->|
    |                |<---CloseAck----------|
    |<---Heartbeat---|                      |

运行时细节
- 心跳聚合：Maintainer 以固定窗口（如 1s）上报，Manager 做去重与增量压缩后上行。
- 错误与告警：Maintainer 在遇到致命错误上报 errorCode/stack 摘要；Manager 将其映射为 StateWarning/Failed 并触发重试或移除策略。
- 速率控制：CheckpointTsMessage 可被 Manager 透传或合并，Maintainer 收到后调整内部拉取/写入的节流因子。


协调器上 create / pause / resume / remove changefeed 的详细执行逻辑

时间: 2025-12-12T15:26:20.049Z

通用原则
- 所有变更通过 Controller 驱动，持久化依赖 backend 元存储；必要时更新 GC safepoint/屏障并与 Maintainer 同步。
- 协调器以事件驱动：接收请求→生成状态变更事件→Controller 执行→结果通过 changefeedChangeCh 回传并持久化。

create（创建）
1. 参数校验与冲突检测：检查 changefeedID 唯一性、下游配置合法性、任务状态不在运行中。
2. 初始化元数据：创建 ChangeFeedInfo（包含配置、错误为空、State=Normal）、初始 checkpointTs/resolvedTs；写入 backend。
3. 更新 GC：为相关 keyspace/全局设置 GC 屏障/安全点以保护初始 checkpoint。
4. 启动流转：向 Maintainer Manager 发送 AddMaintainerRequest，附带 EventInit 与初始 checkpoint；等待 MaintainerHeartbeat 首次确认。
5. 记录运行态：收敛来自 Maintainer 的心跳，开始周期性持久化 checkpointTs；暴露指标。

pause（暂停）
1. 状态检查：仅允许在 State=Normal 或 Warning 时暂停；重复暂停短路返回。
2. 触发暂停：向 Maintainer Manager 发送 StopMaintainer（优雅停止），并更新 ChangeFeedInfo.State=Paused（原因可选）。
3. 持久化进度：在 CloseAck/最后心跳到达时写入最终 checkpointTs；停止调度与算子。
4. GC 维持：保留现有 GC 屏障，避免下游数据被回收；不推进 safepoint。

resume（恢复）
1. 状态检查：仅允许 Paused/Warning；读取最近一次持久化的 checkpointTs。
2. 更新 GC：根据 checkpointTs 重新设置 GC 屏障/安全点，确保可读取。
3. 重新启动：向 Maintainer Manager 发送 AddMaintainerRequest 或 StartMaintainer（视实现），携带 EventInit 与恢复点；将 ChangeFeedInfo.State=Normal。
4. 回到运行态：恢复周期性 checkpoint 持久化与调度；处理任何积压事件。

remove（移除/删除）
1. 前置检查：允许在任何非运行中的终态或运行态下删除；若运行中则先执行优雅停止。
2. 下发移除：向 Maintainer Manager 发送 RemoveMaintainer；接收 CloseAck。
3. 清理与持久化：从 backend 删除/标记删除 ChangeFeedInfo、运行时状态与检查点；移除关联调度与算子。
4. GC 回收：释放对应 keyspace 的 GC 屏障；在无其它 changefeed 时，回退全局 safepoint 到 PD 当前时间。

失败与重试策略
- 任一步骤失败会将 ChangeFeedInfo.State 置为 Warning，并附带 Error；协调器在定时任务中重试或等待人工干预。
- 对网络/下游瞬时错误采用指数退避；对配置/权限类硬失败，阻断并上报。


状态枚举与错误码

时间: 2025-12-12T15:27:50.963Z

状态枚举（ChangeFeedInfo.State）
- Normal：正常运行，允许推进 resolvedTs/checkpointTs 与调度算子。
- Paused：人为或策略暂停，维护当前 checkpoint，不推进。
- Warning：异常告警状态，需人工或自动恢复；可能降级/重试。
- Failed：致命失败，停止算子与维护；通常需要人工介入或删除。
- Finished：任务完成（例如全量/增量同步达成终点），停止推进。

错误码（示例，Error.Code）
- ErrConfigInvalid（1001）：配置非法（目标端点、过滤规则、协议不兼容）。
- ErrDuplicateChangefeedID（1002）：ID 冲突，已存在同名 changefeed。
- ErrDownstreamUnavailable（2001）：下游不可用/连接失败（网络、认证、权限）。
- ErrBackPressure（2002）：下游背压，写入受阻导致进度停滞。
- ErrDDLBlocked（2003）：DDL 事件导致阻塞（等待 schema 变更一致）。
- ErrPDUnavailable（3001）：PD/元数据服务不可用，无法获取 tso/更新 safepoint。
- ErrMetaStoreIO（3002）：后端元存储 I/O/事务失败（写入/读取/冲突）。
- ErrMaintainerCrash（4001）：Maintainer 进程崩溃或异常退出。
- ErrHeartbeatTimeout（4002）：Maintainer 心跳超时未达，判定失联。
- ErrCheckpointStale（5001）：checkpoint 过旧触发 GC 风险，需暂停或提升屏障。
- ErrUnsupportedProtocol（6001）：不支持的同步协议或版本不匹配。

错误字段
- Error.Message：人类可读摘要，含关键上下文（changefeedID、关键参数）。
- Error.Detail：结构化详细信息（栈、子模块、重试次数、最近 TSO）。
- Error.Severity：INFO/WARN/ERROR/FATAL，用于告警与处置分级。
- Error.Retryable：布尔或枚举（NONE/RETRY/LATER），指导协调器是否自动重试。

状态迁移约束
- Normal→Paused：允许，需优雅停止 Maintainer；保留屏障。
- Paused→Normal：允许，需重新设置屏障并恢复 Maintainer。
- Any→Warning：在可恢复异常下进入，自动/人工恢复后可回到 Normal。
- Warning→Failed：不可恢复致命错误或多次重试失败时进入。
- Failed→Removed：仅允许删除；不支持直接恢复到 Normal。