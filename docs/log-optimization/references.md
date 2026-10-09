# 现状、热点与参考链接

## 当前主要问题模型

结合当前仓库内扫描结果，TiCDC 的主要问题可以归纳为五类：

### 1. 重复冗余日志

- 同一条错误链路在多层重复打印。
- 高频重试、轮询、关闭路径上反复打印相似日志。
- 同一问题缺少统一 suppression 和 summary 能力。

### 2. 无意义生命周期噪音

- 空集群启动和关闭时，默认日志充满模块 start、started、closing、closed。
- 大量日志只是在描述控制流，而不是记录诊断事实。

### 3. 语义密度不足

- `ignore it`
- `should not happen`
- `failed`
- `handle handshake event`

### 4. 大对象直接输出

- `zap.Any` 输出大对象、数组和清单。
- 单条日志成本高，AI 与人工读者都必须先人工提炼重点。

### 5. 高基数对象按对象逐条打印

默认日志在下面几类对象上容易按对象数线性膨胀：

- `changefeed`
- `dispatcher`
- `region`
- `subscription`

## 当前优先关注的代码入口

下面这些文件代表当前最值得优先治理的热点区域：

### 空集群启动和关闭

- `server/server.go`
- `server/module_http.go`
- `server/module_grpc.go`
- `server/module_election.go`

### Dispatcher 生命周期与高频等待

- `downstreamadapter/dispatchermanager/dispatcher_manager.go`
- `downstreamadapter/dispatchermanager/task.go`
- `downstreamadapter/eventcollector/dispatcher_stat.go`
- `downstreamadapter/eventcollector/event_collector.go`

### Changefeed 生命周期与 bootstrap、迁移、关闭

- `coordinator/changefeed/changefeed.go`
- `coordinator/changefeed/changefeed_db.go`
- `coordinator/operator/operator_add.go`
- `coordinator/operator/operator_move.go`
- `coordinator/operator/operator_stop.go`
- `maintainer/maintainer.go`
- `downstreamadapter/dispatcherorchestrator/dispatcher_orchestrator.go`

### Log puller、region、subscription

- `logservice/logpuller/subscription_client.go`
- `logservice/logpuller/region_request_worker.go`
- `logservice/logpuller/region_req_cache.go`
- `logservice/txnutil/lock_resolver.go`

### Maintainer 和周期任务

- `maintainer/maintainer.go`
- `maintainer/replica/region_count_refresher.go`
- `maintainer/operator/operator_controller.go`

### 公共能力

- `pkg/logger/log.go`
- `pkg/logger/log_metrics.go`

## 跟踪入口与参考改动

当前日志治理已存在的跟踪入口与参考改动如下：

- 总跟踪 issue：
  - `https://github.com/pingcap/ticdc/issues/4691`
- 已有 TiCDC 参考 PR：
  - `https://github.com/pingcap/ticdc/pull/4701`

跨仓参考可以保留，但当前文档不把它们作为本仓落地范围：

- TiFlow:
  - `https://github.com/pingcap/tiflow/pull/12591/changes`
- TiKV:
  - `https://github.com/tikv/tikv/pull/19250`

额外说明：

- TiKV 某些“集群初始化阶段日志暴增”问题可以作为治理思路参考，但如果日志根因来自 raft、region split、vote 等 TiKV 运行时层面，它并不等同于 TiKV-CDC 自身的日志治理范围。
