# Metrics 有效性与架构适配分析

Last updated: 2026-11-06
Scope: TiCDC 当前工作区（基于 HEAD `286f987bc`）与本地 TiFlow HEAD `24d2b6248` 的指标定义、注册、业务更新路径和相关 Grafana 查询。

## Background

排查代码中无法提供有效观测数据的指标，确认原始诊断意图，并结合 TiCDC 当前架构评估移除、补充采集或使用新指标替代的方向。

本文将 `~/go/tiflow` 中的 TiCDC 实现称为 TiFlow，将当前仓库实现称为 TiCDC。跨仓库源码链接使用本机相邻目录 `../../tiflow/`，用于本地阅读。

## 评估原则

指标在当前架构中没有意义时可以移除；诊断意图仍有意义时，需要设计符合 TiCDC 数据流和组件职责的观测方式。旧指标名和数量不构成保留要求，可以定义新指标替代。

下文的记录路径属于代码事实；处理方向属于建议，尚未实施。判定依据包括实际调用方、时间戳含义、观测对象、现有信号覆盖范围，以及新增标签和远程上报的成本。

## 检查结论

静态代码检查确认 27 个旧指标存在采集或暴露问题。数量按 collector 定义计数，Histogram 的 `_bucket`、`_sum`、`_count` 不单独计数。

| 情况                       | 数量 |
| -------------------------- | ---- |
| 只有定义和注册，没有业务更新 | 22   |
| 未接入 server registry      | 5    |

## Etcd 相关指标：5 个

以下 5 个指标保留了观测代码，但所属包的 `InitMetrics` 没有调用方，[server 注册入口](../server/register_metrics.go#L60) 也没有接入这两个包。根据注册和 HTTP 采集路径，可以推断 server 的 `/metrics` 无法输出这些指标。两个 EtcdWorker 事务指标还需要结合实际 worker 职责判断采集价值。

- `ticdc_etcd_request_count`：[pkg/etcd/metrics.go:18](../pkg/etcd/metrics.go#L18)。

- `ticdc_etcd_etcd_client`：[pkg/etcd/metrics.go:26](../pkg/etcd/metrics.go#L26)。

- `ticdc_etcd_worker_etcd_txn_size_bytes`：[pkg/orchestrator/metrics.go:19](../pkg/orchestrator/metrics.go#L19)。

- `ticdc_etcd_worker_etcd_txn_exec_duration`：[pkg/orchestrator/metrics.go:28](../pkg/orchestrator/metrics.go#L28)。

- `ticdc_etcd_worker_tick_reactor_duration`：[pkg/orchestrator/metrics.go:37](../pkg/orchestrator/metrics.go#L37)。

注册和更新证据：

- [etcd.InitMetrics](../pkg/etcd/metrics.go#L35) 定义了注册逻辑，但没有调用方。
- [orchestrator.InitMetrics](../pkg/orchestrator/metrics.go#L48) 定义了注册逻辑，但没有调用方。
- [etcd client](../pkg/etcd/client.go#L114) 创建请求计数句柄，并在 [健康检查路径](../pkg/etcd/client.go#L482) 更新 endpoints Gauge。
- [etcd worker](../pkg/orchestrator/etcd_worker.go#L123) 持有三个 Histogram 的句柄，供业务路径记录数据。
- [HTTP 路由](../api/http.go#L53) 使用 server 传入的 registry 作为 `prometheus.DefaultGatherer`。

`etcd.InitMetrics` 还有一个注册目标问题：`etcdStateGauge` 使用 `prometheus.MustRegister`，会注册到默认 registry；`etcdRequestCounter` 使用传入的 registry。补齐调用时，还需要将 `etcdStateGauge` 注册到 server 使用的 registry。

### TiFlow 的注册、记录与使用

TiFlow 在 [server init](../../tiflow/cdc/server/metrics.go#L39) 中调用两个包的 `InitMetrics`。请求 Counter 和三个 EtcdWorker Histogram 注册到独立的 server registry；[HTTP 路由](../../tiflow/cdc/http.go#L73) 从该 registry 采集。endpoint Gauge 注册到默认 registry，存在与 TiCDC 相同的注册目标问题。

- `ticdc_etcd_request_count`：Counter，次；按 `type` 区分的 etcd RPC 尝试。

- `ticdc_etcd_etcd_client`：Gauge，个；`type="endpoints"` 对应的健康 endpoint 数量。

- `ticdc_etcd_worker_etcd_txn_size_bytes`：Histogram，字节；EtcdWorker 提交的一批元数据变更的估算大小。

- `ticdc_etcd_worker_etcd_txn_exec_duration`：Histogram，秒；一次封装的 Txn 调用总耗时。

- `ticdc_etcd_worker_tick_reactor_duration`：Histogram，秒；一次 `reactor.Tick` 调用耗时。

具体统计口径：

- 请求计数在 [retryRPC](../../tiflow/pkg/etcd/client.go#L107) 每次 RPC 尝试结束时递增，包含失败和重试。一个 Txn 内包含多个 key 操作时计一次；Watch 没有接入。另有一个分类问题：[TimeToLive](../../tiflow/pkg/etcd/client.go#L212) 使用 `Revoke` 标签。
- endpoint Gauge 只在[非空健康 endpoint 集合发生变化时](../../tiflow/pkg/etcd/client.go#L427)更新。初始化后集合一直稳定时可能没有样本；全部不可用时不置零。
- 事务大小按 [batch.go](../../tiflow/pkg/orchestrator/batch.go#L85) 中的 `2 × key 长度 + value 长度` 累计估算，在[发送事务前](../../tiflow/pkg/orchestrator/etcd_worker.go#L476)记录，提交失败也有样本。
- 事务耗时包围整个[封装 Txn 调用](../../tiflow/pkg/orchestrator/etcd_worker.go#L477)，包含客户端重试和退避等待；错误和 CAS 条件不满足也会记录。
- Tick 耗时只覆盖 [reactor.Tick](../../tiflow/pkg/orchestrator/etcd_worker.go#L288)。TiFlow 的 [processor](../../tiflow/cdc/capture/capture.go#L415) 和 [owner](../../tiflow/cdc/capture/capture.go#L541) 都使用 EtcdWorker，进程内共用没有 role 标签的 Histogram。

TiFlow Grafana 使用了 [etcd 请求速率](../../tiflow/metrics/grafana/ticdc.json#L4585)和[三个 EtcdWorker Histogram](../../tiflow/metrics/grafana/ticdc.json#L15251)。未发现该 dashboard 使用 endpoint Gauge。

### TiCDC 的职责变化与处理建议

**请求负载诊断仍有意义，建议保留请求计数并补注册。** 当前 [retryRPC](../pkg/etcd/client.go#L134) 仍在记录请求尝试，[coordinator 元数据后端](../coordinator/changefeed/etcd_backend.go#L164) 仍通过 `ClientImpl.Txn` 写入。需要在 Help 和面板中说明重试口径，并修正 [TimeToLive](../pkg/etcd/client.go#L249) 的操作分类。

**endpoint 可用性诊断仍有意义，建议修复采集语义后保留或重命名。** 应在初始化和每轮健康检查后记录，包括零健康 endpoint 的情况，并注册到 server registry。存在多个逻辑 etcd client 时，需要明确实例归属，避免多个 client 覆盖同一时间序列。单纯补注册无法解决数值缺失和陈旧问题。

**节点发现 Tick 耗时仍有意义，建议按当前职责表达。** 当前唯一生产 EtcdWorker 调用链是 [NodeManager.Run](../server/watcher/module_node_manager.go#L161) → [RunEtcdWorker](../server/watcher/etcd_watcher.go#L48)。其 [Tick](../server/watcher/module_node_manager.go#L83) 查询 coordinator、更新节点视图并同步调用回调，因此耗时可用于发现节点管理循环阻塞。建议用 `ticdc_node_manager_tick_duration_seconds` 替代旧的泛化名称，计时边界覆盖完整 Tick；这一数值包含 Tick 内的 etcd 查询和回调执行。

**两个旧 worker 事务 Histogram 建议移除，元数据写入诊断改在实际写入边界采集。** NodeManager 的 watch 前缀是 `__cdc_meta__/capture`，Tick 返回原 state，不创建 patch；[GlobalReactorState.GetPatches](../pkg/orchestrator/reactor_state.go#L249) 从 pending patch 和 changefeed 状态取数据。根据这些路径，当前节点发现 worker 正常运行时没有事务写入样本。coordinator 的元数据事务直接调用 [ClientImpl.Txn](../pkg/etcd/client.go#L198)，不会经过旧 worker 的两个观测点。

替代方向是在 `ClientImpl.Txn` 边界增加事务耗时 Histogram，例如 `ticdc_etcd_txn_duration_seconds`，并用有限的结果分类区分 RPC 错误、CAS 失败和成功，明确是否包含重试。事务大小若仍需用于判断批量写入是否接近限制，应在该边界统一估算实际请求或序列化大小，使用明确的新名称和口径；旧 worker 的 patch 大小估算不覆盖这些写入。

## 只有定义和注册、没有业务更新的指标

以下 22 个指标在生产 Go 代码中只有定义和注册，没有更新调用或传递给业务使用方的路径。根据代码可以推断，普通 Gauge/Counter 会保持为 0，带标签的 Vec 不会创建时间序列。

### Slow table 指标：12 个

这些指标集中定义于 [maintainer/metrics.go:130](../maintainer/metrics.go#L130)，并在同文件的 [RegisterMetrics](../maintainer/metrics.go#L272) 中注册。

```text
ticdc_scheduler_slow_table_id
ticdc_scheduler_slow_table_checkpoint_ts
ticdc_scheduler_slow_table_resolved_ts
ticdc_scheduler_slow_table_stage_checkpoint_ts
ticdc_scheduler_slow_table_stage_resolved_ts
ticdc_scheduler_slow_table_stage_checkpoint_ts_lag
ticdc_scheduler_slow_table_stage_resolved_ts_lag
ticdc_scheduler_slow_table_stage_checkpoint_ts_lag_histogram
ticdc_scheduler_slow_table_stage_resolved_ts_lag_histogram
ticdc_scheduler_slow_table_region_count
ticdc_scheduler_slow_table_puller_resolved_ts
ticdc_scheduler_slow_table_puller_resolved_ts_lag
```

#### TiFlow 的上报和选取路径

TiFlow 中这一组指标具有完整的生产采集路径：

1. [CaptureManager.Tick](../../tiflow/cdc/scheduler/internal/v3/member/capture_manager.go#L183) 周期性设置心跳请求的 `CollectStats`；[默认配置](../../tiflow/pkg/config/scheduler_config.go#L79) 为 200 个 owner tick，约 10 秒。
2. [agent 处理心跳](../../tiflow/cdc/scheduler/internal/v3/agent/agent.go#L268) 时读取每个 span 的状态；[processor](../../tiflow/cdc/processor/processor.go#L358) 在需要采集时补充 `Stats`。
3. [processor 组装阶段进度](../../tiflow/cdc/processor/processor.go#L386)：`puller-ingress`、`puller-egress`、`sorter-ingress`、`sorter-egress`、`sink`，并附带 `RegionCount` 和 `BarrierTs`。
4. owner 的 ReplicationSet [保存非空 Stats](../../tiflow/cdc/scheduler/internal/v3/replication/replication_set.go#L1058)。阶段数据可以比普通 checkpoint 心跳更旧。
5. [replication manager](../../tiflow/cdc/scheduler/internal/v3/replication/replication_manager.go#L630) 选出 checkpoint 最小的 span 作为 `slowestSink`；checkpoint 相同时选 resolved ts 更小的 span。另按 `puller-egress` resolved ts 最小值独立选出 `slowestPuller`。
6. coordinator [每约 10 秒](../../tiflow/cdc/scheduler/internal/v3/coordinator.go#L445) 调用 [CollectMetrics](../../tiflow/cdc/scheduler/internal/v3/replication/replication_manager.go#L776)，并以当前 PD 时间计算 lag。

因此，`slow_table` 实际关联一个 span。表拆分时，table ID 无法独立标识该 span；`slowestSink` 和 `slowestPuller` 也可能属于不同的表。

#### 12 个指标的含义与用途

以下名称均省略共同前缀 `ticdc_scheduler_`。时间戳 Gauge 使用 TSO 的物理部分，单位为 Unix 毫秒；lag 使用秒。

- `slow_table_id`：`slowestSink` 的 table ID，用于定位拖慢下游进度的表。

- `slow_table_checkpoint_ts`：该 span 的 checkpoint，用于查看最慢 sink 的完成位置。

- `slow_table_resolved_ts`：同一 span 的 resolved ts，用于对照其已接收进度与完成进度。

- `slow_table_stage_checkpoint_ts`：该 span 各 stage 的 checkpoint，用于展示各阶段进度。

- `slow_table_stage_resolved_ts`：各 stage 的 resolved ts，另含 barrier，用于展示各阶段可推进上界。

- `slow_table_stage_checkpoint_ts_lag`：PD 时间减各 stage checkpoint，用于分析完成进度落后位置。

- `slow_table_stage_resolved_ts_lag`：PD 时间减各 stage resolved ts 或 barrier，用于分析接收进度或屏障约束。

- `slow_table_stage_checkpoint_ts_lag_histogram`：定期采样上述 checkpoint lag，用于观察历史最慢对象的 lag 分位数。

- `slow_table_stage_resolved_ts_lag_histogram`：定期采样上述 resolved lag，用于观察历史最慢对象的 lag 分位数。

- `slow_table_region_count`：`slowestSink` span 的 puller region 数，用于辅助判断订阅规模和拆分需求。

- `slow_table_puller_resolved_ts`：`slowestPuller` 的 puller-egress resolved ts，用于观察上游采集最慢进度。

- `slow_table_puller_resolved_ts_lag`：该位置相对 PD 时间的 lag，用于判断上游采集是否落后。

阶段进度本身也有口径差异：[puller Stats](../../tiflow/cdc/puller/multiplexing_puller.go#L526) 的 ingress checkpoint/resolved 使用同一个最大已收到 resolved ts，egress 两者使用同一个可输出 resolved ts；sorter-ingress checkpoint 使用最大已收到 commit ts。`barrier` 只更新 resolved 系列。各阶段的 lag 表示时间戳位置距离当前时间的差值，不能直接解释为该阶段处理一条事件的耗时。

两个 Histogram 每次采集只记录当时最慢 sink span 的阶段 lag。跨时间的样本可能来自不同 span，不能解释为所有表或所有事件的延迟分布。Gauge 的 stage label 也不包含 table ID。

TiFlow dashboard 使用了全部 12 个指标，例如 [ID 和进度](../../tiflow/metrics/grafana/ticdc.json#L10405)、[region 数和阶段进度](../../tiflow/metrics/grafana/ticdc.json#L11103)、[lag 详情](../../tiflow/metrics/grafana/ticdc.json#L11433)、[历史 lag 分位数](../../tiflow/metrics/grafana/ticdc.json#L11548)。注册链为 server → scheduler → v3 → replication。业务先计算进度再发布指标；没有发现调度决策读取这些 Prometheus collector。

#### TiCDC 当前已有的数据与观测信号

TiCDC 将上游订阅、缓存、扫描发送和下游应用拆到不同组件。当前 [TableSpanStatus](../heartbeatpb/heartbeat.proto#L457) 只有 dispatcher ID、状态、checkpoint、吞吐和 mode；没有逐 span resolved ts、stage checkpoint map 或 region 数。[DispatcherManager](../downstreamadapter/dispatchermanager/dispatcher_manager.go#L919) 能在本地读取每个 dispatcher 的 checkpoint/resolved，但向 maintainer 发送的 watermark 已聚合为节点级最小值。

可复用的信号及其边界：

- [maintainer 的 checkpoint/resolved 及 lag](../maintainer/maintainer.go#L874) 表示 changefeed 的安全全局进度。[计算过程](../maintainer/maintainer.go#L766) 同时受节点 watermark、调度和 barrier 约束，无法仅据此确定某个慢 span。
- [DispatcherManager 指标](../downstreamadapter/dispatchermanager/dispatcher_manager.go#L984) 表示节点内 changefeed 的聚合进度。两个最小值可能来自不同 dispatcher。
- [log coordinator](../logservice/coordinator/coordinator.go#L378) 已发布 `ticdc_owner_resolved_ts` 和 `ticdc_owner_resolved_ts_lag`。输入来自 [EventService 按 changefeed 上报的 receivedResolvedTs 最小值](../pkg/eventservice/metrics_collector.go#L345)，适合观察上游可用进度。lag 按各节点上报时刻计算，带有上报时间口径。
- [EventService](../pkg/eventservice/metrics_collector.go#L286) 已有 received/sent resolved lag。它还[记录慢 dispatcher 日志](../pkg/eventservice/metrics_collector.go#L302)，包含 checkpoint、received/sent resolved、table ID、dispatcher ID 和扫描状态；当前候选是 broker 范围 top 10，未按 changefeed 分组选取。
- [EventStore](../logservice/eventstore/event_store.go#L1305) 已按 subscription 采样 resolved lag，并记录慢 subscription 的 span 和进度；[puller debug 快照](../logservice/logpuller/debug.go#L49) 已包含慢 subscription、table ID、region 数、初始化状态和慢 region。
- [RegionCountRefresher](../maintainer/replica/region_count_refresher.go#L54) 根据 key range 查询 region cache，服务于拆分判断。该估算与 TiFlow puller 实际锁定的 region 数具有不同含义。

#### TiCDC 的处理建议

**这 12 个旧定义建议随替代信号和面板迁移一起移除。** 最慢对象定位、采集进度和阶段阻塞诊断仍有意义。建议按下列对应关系保留诊断意图：

- `slow_table_id`：使用慢 dispatcher 的结构化日志或诊断快照记录身份，同时给出 table ID、dispatcher ID、span、node、mode 和 epoch。这样可以表达表拆分后的具体慢对象。
- `slow_table_checkpoint_ts`、`slow_table_resolved_ts`：复用现有 maintainer/DispatcherManager 指标观察整体进度；需要关联同一个慢对象时，在 DispatcherManager 本地选出 checkpoint 最小的 dispatcher，从同一次采样记录其 checkpoint lag 和 resolved lag。建议新名为 `ticdc_dispatcher_slowest_checkpoint_lag_seconds`、`ticdc_dispatcher_slowest_resolved_lag_seconds`，按 keyspace、changefeed、mode 分组，由 scrape 的 instance 标识节点。两个 Gauge 必须来自同一候选。
- `slow_table_stage_checkpoint_ts`、`slow_table_stage_resolved_ts`、`slow_table_stage_checkpoint_ts_lag`、`slow_table_stage_resolved_ts_lag`：使用新架构的阶段划分。上游 received、扫描发送 sent、dispatcher received、sink completed 分别由实际持有者采集；DDL/barrier 和调度限制单独表达。全局各阶段最慢值可以用于初步定位；同一 span 的完整链路诊断需要带 dispatcher/epoch 的快照关联，不能拼接不同对象的最小值。
- `slow_table_stage_checkpoint_ts_lag_histogram`、`slow_table_stage_resolved_ts_lag_histogram`：建议移除。默认保留阶段 lag Gauge 的时间序列；需要百分位时，应先明确统计的是定时采样的最慢对象、所有活跃对象，还是事件处理耗时，再定义对应 Histogram。
- `slow_table_region_count`：建议移除旧定义，优先复用 puller debug 的实际 region 数据。需要关联下游慢 span 时按范围匹配上游 subscription，说明共享订阅和范围覆盖关系；RegionCountRefresher 的结果只能作为范围规模估算，不能冒充实际采集 region 数。
- `slow_table_puller_resolved_ts`、`slow_table_puller_resolved_ts_lag`：面板改用已有的 `ticdc_owner_resolved_ts`、`ticdc_owner_resolved_ts_lag` 观察 changefeed 上游进度，并联用 EventStore/puller 的日志和 debug 定位订阅。它们保留了上游最慢进度的诊断意图，观测边界和采样时点需在面板说明中标清。

实现同一慢 dispatcher 的观测时，优先扩展 [DispatcherManager 已有的心跳扫描](../downstreamadapter/dispatchermanager/dispatcher_manager.go#L948)，避免为 metrics 再次全量遍历或将所有阶段状态塞入 maintainer 的常规心跳。上游侧可复用 EventService 已有的扫描和按 changefeed 的聚合路径，按需增加低基数阶段 lag 指标。

候选应分别处理 default/redo，排除尚无有效进度的准备中对象，单独表示 DDL/scheduling 约束；进度缺失、重启或 epoch 变化时需要清理旧序列并暴露新鲜度。table ID、dispatcher ID、subscription ID 和 range 放在诊断详情中，常规指标保持低基数。新增时间戳使用明确的 seconds 单位，lag 统一按 PD 时间计算。

### 其他模块：10 个

- `ticdc_event_service_resolved_ts`：[pkg/eventservice/metrics.go:45](../pkg/eventservice/metrics.go#L45)。

- `ticdc_event_service_dropped_scan_task_count`：[pkg/eventservice/metrics.go:175](../pkg/eventservice/metrics.go#L175)。

- `ticdc_messaging_stream_gauge`：[pkg/messaging/metrics.go:52](../pkg/messaging/metrics.go#L52)。

- `ticdc_redo_fsync_duration_seconds`：[pkg/redo/writer/metrics.go:42](../pkg/redo/writer/metrics.go#L42)。

- `ticdc_dynamic_stream_add_path_num`：[utils/dynstream/metrics.go:35](../utils/dynstream/metrics.go#L35)。

- `ticdc_dynamic_stream_remove_path_num`：[utils/dynstream/metrics.go:43](../utils/dynstream/metrics.go#L43)。

- `ticdc_sink_txn_queue_duration`：[downstreamadapter/sink/metrics/metrics.go:26](../downstreamadapter/sink/metrics/metrics.go#L26)。

- `ticdc_sink_txn_sink_dml_batch_commit`：[downstreamadapter/sink/metrics/metrics.go:35](../downstreamadapter/sink/metrics/metrics.go#L35)。

- `ticdc_sink_txn_sink_dml_batch_callback`：[downstreamadapter/sink/metrics/metrics.go:44](../downstreamadapter/sink/metrics/metrics.go#L44)。

- `ticdc_sink_txn_prepare_statement_errors`：[downstreamadapter/sink/metrics/metrics.go:53](../downstreamadapter/sink/metrics/metrics.go#L53)。

#### EventService resolved ts：复用已有按 changefeed 的上游进度

`ticdc_event_service_resolved_ts` 是无标签的全局 Gauge，目前没有更新。TiFlow 没有对应的 EventService 组件；旧架构的上游进度来自前述按 span 上报的 puller Stats。

TiCDC 的 [EventService 快照](../pkg/eventservice/metrics_collector.go#L190) 同时计算 received/sent resolved 最小值，并记录 lag；[按 changefeed 上报路径](../pkg/eventservice/metrics_collector.go#L345) 已将 received 进度发送给 log coordinator。多个 changefeed、TiDB cluster 和 default/redo 的进度无法由一个无标签数值清楚表达。

建议移除旧 Gauge，复用已有的 `ticdc_owner_resolved_ts` 和 lag 观察 changefeed 的上游进度。需要发送阶段细分时，在实际 EventService owner 处按 changefeed、mode、received/sent 定义新的低基数指标，并在同一次快照中记录；避免给旧 Gauge 随意选择一个最小值后继续沿用模糊含义。

#### 丢弃扫描任务数：移除失去触发场景的定义

`ticdc_event_service_dropped_scan_task_count` 的 Help 明确描述“worker 队列满而丢弃任务”。当前 [scanTaskQueues](../pkg/eventservice/event_broker.go#L130) 是无界队列，[push](../pkg/eventservice/event_broker.go#L73) 没有返回满队列或丢弃结果；该指标没有可对应的容量拒绝分支。

建议移除该 Counter。积压诊断复用 [pending_scan_task_count](../pkg/eventservice/metrics_collector.go#L294) 和现有扫描耗时指标。未来若实现容量限制或主动丢弃，应在真实决策点定义带有限原因分类的新指标，明确取消、去重和容量拒绝的区别。

#### Messaging stream 数：保留连接健康诊断，替换模糊 Gauge

`ticdc_messaging_stream_gauge` 声明按 `from` 统计 stream，但没有更新。TiFlow 使用另一套 p2p 实现，定义了 [ticdc_message_server_cur_stream_count](../../tiflow/pkg/p2p/metrics.go#L24)，在 [服务端 stream 处理路径](../../tiflow/pkg/p2p/server.go#L707) 维护数量。当前实现需要按自己的 session 生命周期采集。

TiCDC 的 [remoteMessageTarget](../pkg/messaging/remote_target.go#L49) 按 event/command 保存双向 stream session；[初始条目](../pkg/messaging/remote_target.go#L209) 的值可以为 nil，[连接清理](../pkg/messaging/remote_target.go#L693) 也会将值清空。仅统计 map 条目数会把尚未建立或已断开的 stream 算进去。

建议移除旧定义，使用新的 Gauge，例如 `ticdc_messaging_active_streams{type="event|command"}`，统计实际存活的 session。采集归属 message center，可按受锁保护的状态做周期快照，或在连接建立、替换、断开处维护计数。需要区分已发现 peer 和可用 stream，并处理重连替换、关闭和节点移除；peer 级定位优先使用已有日志，常规指标按类型汇总。

#### Redo fsync 耗时：移除旧文件写入模型的指标

TiFlow 的 [file.Writer.flush](../../tiflow/cdc/redo/writer/file/file.go#L415) 在 `w.file.Sync()` 前后计时，记录 `ticdc_redo_fsync_duration_seconds`；它诊断本地 redo 文件持久化耗时。

TiCDC 当前的 [fileWorkerGroup.syncWriteFile](../pkg/redo/writer/file_worker.go#L267) 关闭内存编码 writer 后，通过 `extStorage.WriteFile` 或 multipart upload 写出。该边界没有可直接对应的 `file.Sync()`；已经用 `ticdc_redo_flush_all_duration_seconds` 记录写出耗时，DDL writer 也使用这组 flush 指标。

建议移除旧 fsync Histogram 和 [Grafana 引用](../metrics/grafana/ticdc_new_arch.json#L25256)，保留外部存储 flush 观测。若要细分编码收尾、等待写入许可或上传阶段，可在这些实际边界定义新指标；底层 storage 或 spool 的同步语义应由其所属组件描述。

#### Dynstream AddPath/RemovePath：用 Counter 表达成功操作次数

`ticdc_dynamic_stream_add_path_num` 和 `ticdc_dynamic_stream_remove_path_num` 都是没有更新的 Gauge。TiFlow 没有当前 dynstream 组件的对应实现。

TiCDC 已在 [AddPath](../utils/dynstream/parallel_dynamic_stream.go#L199) 和 [RemovePath](../utils/dynstream/parallel_dynamic_stream.go#L233) 维护累计成功操作次数，并通过 [GetMetrics](../utils/dynstream/parallel_dynamic_stream.go#L261) 返回 `AddPath`/`RemovePath`。重复添加或移除不存在对象会提前返回错误。

诊断 path 创建/销毁频率仍有意义，建议用一个 CounterVec 替换两个旧 Gauge，例如 `ticdc_dynamic_stream_path_operations_total{module,operation="add|remove"}`，在成功修改 path 集合时递增。相同 module 存在多个 stream 实例时需要正确汇总，不能让每个实例用 Set 覆盖同一序列。

如果目的是排查 path 泄漏，应另以活跃 path 集合大小做 Gauge，并在 Close 后清理；累计创建次数和活跃数量具有不同含义。内部 `_statAddPathCount` 同时用于选择 stream，不能随旧 collector 一起删除。

#### MySQL 入队耗时：保留诊断意图，在当前 worker 边界记录

TiFlow 的 [worker.onEvent](../../tiflow/cdc/sink/dmlsink/txn/worker.go#L158) 将 `time.Since(txn.start)` 记录到 `ticdc_sink_txn_queue_duration`，覆盖冲突等待和 worker 队列等待，用于区分执行前积压与数据库执行慢。

TiCDC 的 [ConflictDetector.Add](../downstreamadapter/sink/mysql/causality/conflict_detector.go#L100) 记录了从进入冲突检测到成功进入 txn cache 的时长；[runDMLWriter](../downstreamadapter/sink/mysql/sink.go#L293) 从 cache 取出批次，但没有记录各事件从进入 sink 到取出的完整等待时间。现有冲突时长和 flush 时长不能完整覆盖这个间隙。

建议恢复这一诊断能力，指标归属 MySQL sink。可以定义 `ticdc_sink_mysql_queue_duration_seconds`：在进入 `ConflictDetector.Add` 时记录单调时钟时间，在 worker 取出对应事件时 Observe，排除 SQL 执行和 callback 耗时；一个事件只记录一次。批量取出时保持逐事件采样，避免把一批等待混成一个未定义的值。旧定义和面板同步迁移。

#### MySQL batch commit/callback 耗时：按实际执行阶段替换

TiFlow 的 [mysql sink Flush](../../tiflow/cdc/sink/dmlsink/txn/mysql/mysql.go#L231) 先执行 `execDMLWithMaxRetries`，再执行 callbacks，分别记录 `ticdc_sink_txn_sink_dml_batch_commit` 和 `ticdc_sink_txn_sink_dml_batch_callback`。前者包含整个执行及重试过程。

TiCDC 的 [Writer.Flush](../pkg/sink/mysql/mysql_writer.go#L251) 也有数据库执行和同步 `event.PostFlush()` 两个阶段。当前 [workerFlushDuration](../downstreamadapter/sink/mysql/sink.go#L305) 包含 Flush 中的执行和回调，`workerBatchFlushDuration` 还覆盖 worker 取出的一组事件，已有指标可以观察粗粒度耗时，但无法单独识别回调阻塞。

建议移除两个旧定义，在 `Writer.Flush` 的实际边界用一个 HistogramVec 表达阶段耗时，例如 `ticdc_sink_mysql_batch_duration_seconds{phase="execute|callback"}`。execute 包含重试及重复键后的安全模式回退，callback 覆盖本次同步 PostFlush 调用；SQL 被过滤、DryRun 和失败路径需要明确是否采样。已有 worker 指标继续用于整体耗时和忙碌程度，避免把它们当成阶段指标的精确同义词。

#### Prepare statement 错误：恢复被吞掉的降级信号

TiFlow 在 [Prepare 失败分支](../../tiflow/cdc/sink/dmlsink/txn/mysql/mysql.go#L669) 递增 `ticdc_sink_txn_prepare_statement_errors`，清理缓存后退回普通 SQL 执行。它反映 Prepared Statement 缓存和下游资源限制导致的降级。

TiCDC 的 [Writer.sequenceExecute](../pkg/sink/mysql/mysql_writer_dml_exec.go#L151) 仍有同样的缓存和降级路径，但失败时只执行 `RemoveOldest()`。普通 SQL 执行成功时，最终执行错误计数不会体现这次 Prepare 失败。

建议将计数归属移到 `pkg/sink/mysql`，在实际 Prepare 返回错误时递增。可以保留旧名称，或使用 `ticdc_sink_mysql_prepare_statement_errors_total` 明确 Counter 语义。缓存命中不计失败，不能把成功降级后的 Prepare 错误混入最终 DML 失败；标签限于 keyspace、changefeed 和有限错误分类。

#### 面板迁移

当前 Grafana 仍引用 [sink queue duration](../metrics/grafana/ticdc_new_arch.json#L17270)、[DML batch commit duration](../metrics/grafana/ticdc_new_arch.json#L17676) 和前述 slow table 指标。替换方案实施时，需要同步检查 `metrics/grafana/` 和 `metrics/nextgengrafana/` 的查询、单位、分位数、说明及聚合维度。仅删除 collector 会继续留下无数据面板。

## 采集建议的验证要点

采用上述建议后，验证应覆盖实际诊断意图：

- 注册指标到 server 实际暴露的 registry，并从 `/metrics` 核对样本；Vec 注册成功不能证明业务会创建时间序列。
- 使用不同 checkpoint/resolved 排序的 span 验证最慢候选选择，确认关联指标来自同一对象；分别覆盖表拆分、default/redo 和 DDL/scheduling 阻塞。
- 验证初始化、无对象、对象移除、节点切换、重连和 epoch 变化后的序列清理与数据新鲜度；缺少有效样本时保持明确的缺失状态。
- 对队列、SQL 执行、callback 和 Prepare 降级分别构造场景，确认计时区间和事件计数不会重叠或漏记。
- 验证多实例共享同一 module/changefeed 标签时的汇总方式，检查新增基数和每次采集的遍历成本；阶段指标不要默认引入逐表常驻时间序列。
- 检查迁移后的 dashboard 使用与新统计口径一致的单位和聚合方式，并确认剩余查询不再引用已删除的名称。

## 验证方式与覆盖边界

本次结论来自静态代码检查：

- 使用 `rg` 检索指标定义、注册入口、更新调用、别名句柄和 Grafana 引用。
- 使用只读 Python 扫描脚本检查 274 处普通 Prometheus Gauge、Counter、Histogram、Summary 构造点，筛选缺少业务引用的定义，再核对代码。
- 使用 CodeGraph 查询 TiCDC 两个相关 `InitMetrics` 的调用方，结果均为空；TiFlow 的两个函数则由 server 初始化调用。
- 核对 server registry 到 HTTP `/metrics` 的连接关系。
- 对照 TiFlow 的 processor Stats、scheduler 聚合和 Grafana 使用情况，追踪 TiCDC 中 dispatcher、EventService、logservice 和 MySQL writer 的对应边界。

本轮仅更新文档，未修改生产代码或运行单元测试。本报告中的指标问题尚未通过启动服务或抓取实际 `/metrics` 验证；面板表现和运行时输出是基于代码路径的推断。
