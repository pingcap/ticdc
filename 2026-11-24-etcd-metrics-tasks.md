# etcd metrics 接入剩余任务

## 指标含义

- `ticdc_etcd_request_count{type}`：每次 etcd RPC 尝试的计数，含失败与重试；`Delete` 只计一次，`Watch`、`NewSession` 不计。与 tiflow 同源同义，`TimeToLive` 也同 tiflow 一样误记为 `Revoke`。
- `ticdc_etcd_etcd_client{type="endpoints"}`：健康 endpoint 数，只在集合变化且非空时写入。定义与 tiflow 相同，但 tiflow 从未采集到它（注册在默认 registry，`/metrics` 用的是自定义 registry）；本分支修好注册后才真正暴露，多 client 覆盖与不归零的问题也随之可见。
- `ticdc_etcd_worker_etcd_txn_size_bytes`：EtcdWorker 每次提交事务携带的 patch 条数，不是字节数。定义与 tiflow 相同；tiflow 的 owner 与 processorManager 会写，TiCDC 只有 NodeManager，没有写入。
- `ticdc_etcd_worker_etcd_txn_exec_duration`：EtcdWorker 层 `client.Txn` 的耗时，含客户端内部的重试与退避。定义与 tiflow 相同，数据可得性与上一条相同。
- `ticdc_etcd_worker_tick_reactor_duration`：只计 `reactor.Tick` 的耗时，不含 applyUpdates、watch 等待与提交。测点与 tiflow 相同，但被测的是 `NodeManager.Tick`（内含 `GetOwnerID` 的 Get），与 tiflow 的 owner/processor tick 不是同一件事，数值不可跨版本比较。
- `ticdc_etcd_txn_duration_seconds{result}`（待加）：客户端层 `Txn` 的耗时，按 `success`、`cas_failed`、`error` 分。tiflow 没有对应指标，口径覆盖所有调用方，不要与上一条的 EtcdWorker 事务耗时混用。

## P0｜上报语义

- [ ] endpoint 健康数只由 server 的 etcd client 上报
  - 现状：`pkg/etcd/client.go:449` 在 `CreateRawEtcdClient` 里写 gauge，`pkg/upstream/upstream.go:204` 为非默认 upstream 也调用同一个函数，两个 client 写同一条 `type="endpoints"` 序列。
  - 方案：server 侧 client（`server/server_prepare.go:95`）上报，upstream 侧不上报；gauge 的 `type` 只有一个取值，改为无标签 `ticdc_etcd_healthy_endpoint_count`（此前从未暴露，无兼容负担）。
  - 完成条件：进程内存在多个 etcd client 时，gauge 始终等于 server etcd 的健康 endpoint 数。

- [ ] 每轮健康检查后无条件上报 endpoint 数
  - 现状：`pkg/etcd/client.go:468-479` 全部不可用时不上报，`pkg/etcd/client.go:481-486` 只在集合变化且非空时上报，gauge 会停在旧值。
  - 完成条件：停掉一个 endpoint 后，下一次检查 gauge 减少；全部停掉后 gauge 为 0。
  - 验证：在 `pkg/etcd/client_test.go` 用嵌入式 etcd 覆盖全健康与全不可用两种状态，或将上报逻辑抽成接收 `[]string` 的函数做单测。

- [ ] 修正 `TimeToLive` 的请求类型
  - 现状：`pkg/etcd/client.go:249` 用 `EtcdRevoke` 作标签，重试日志的 RPC 名也是 `Revoke`。
  - 方案：`pkg/etcd/client.go:48-53` 增加 `EtcdTimeToLive`，`retryRPC` 与 `WithLabelValues` 一并改用；`isRetryableError`（`pkg/etcd/client.go:345-356`）当前靠 `EtcdRevoke` 判断 lease 不存在属不可重试，新类型要一并匹配，否则会变成重试。
  - 完成条件：调用 `TimeToLive` 后 `ticdc_etcd_request_count{type="TimeToLive"}` 增加，`type="Revoke"` 不变。

- [ ] 明确 `ticdc_etcd_request_count` 的口径
  - 现状：`pkg/etcd/metrics.go:18-24` 的 Help 只有 `request counter of etcd operation`，从指标看不出是否含重试、哪些操作不计。
  - 完成条件：Help 写明「每次 RPC 尝试，含失败与重试；`Delete` 不重试只计一次；`Watch`、`NewSession` 不计；只统计 TiCDC 自身 etcd，非默认 upstream 的 client 不计」。

## P1｜替换没有数据来源的指标

- [ ] 删除 EtcdWorker 的 txn size 与 exec duration
  - 现状：生产只有 NodeManager 一个 EtcdWorker，`NodeManager.Tick`（`server/watcher/module_node_manager.go:83`）不产生 patch，`pkg/orchestrator/etcd_worker.go:489` 的 Txn 永不执行，`pkg/orchestrator/metrics.go:18-31` 两个无标签 Histogram 注册后只导出恒 0 的 bucket。
  - 改动：删定义与注册、`pkg/orchestrator/etcd_worker.go:82-85` 的字段、`:123-125` 的初始化、`:487` 与 `:495` 的 Observe；`getBatchChangedState` 算出的 size 仍用于分批，保留。
  - 完成条件：`/metrics` 不再出现 `ticdc_etcd_worker_etcd_txn_size_bytes` 和 `ticdc_etcd_worker_etcd_txn_exec_duration`。

- [ ] 在 `ClientImpl.Txn` 上报按结果分类的事务耗时
  - 现状：全进程的 etcd 事务只有请求计数，没有耗时；调用方包括 `coordinator/changefeed/etcd_backend.go:164,199,238,304,339,405,429,471,499`、`server/server.go:719`。
  - 改动：`pkg/etcd/client.go:198` 增加 `ticdc_etcd_txn_duration_seconds{result="success|cas_failed|error"}`，计时覆盖 `retryRPC` 的重试与退避；`resp.Succeeded == false` 记 `cas_failed`，`err != nil` 记 `error`。
  - 完成条件：成功、CAS 失败、RPC 错误三条路径各产生对应 result 的样本。

## P2｜面板与校验

- [ ] 在 dashboard 增加 etcd 面板
  - 改动：在 `metrics/grafana/ticdc_new_arch.json` 增加 etcd 行，包含按 `type` 的请求速率（标题注明含重试）、健康 endpoint 数、按 `result` 的事务耗时、node manager tick 耗时。
  - `metrics/nextgengrafana/` 下两个文件由 `make generate-next-gen-grafana` 从新架构文件生成，不手工编辑。
  - 完成条件：`make generate-next-gen-grafana` 与 `scripts/check-ticdc-dashboard.sh` 通过，新增 panel id 不重复。

- [ ] 覆盖 etcd 指标的注册与运行时行为
  - 现状：`server/register_metrics_test.go:27` 只 Gather 8 个 sink/build 指标（`:54`），etcd 指标注册错误不会被发现。
  - 完成条件：测试能 gather 到 `ticdc_etcd_request_count` 与 `ticdc_etcd_healthy_endpoint_count`；起 server 抓 `/metrics` 核对 6 个 `type` 有值、启动时 gauge 有值、全部 endpoint 不可用时为 0、新建 changefeed 后出现 `result="success"` 样本。

- [ ] 同步 `docs/metrics-audit.md`
  - 该文档仍把这 5 个 etcd 指标写成「无调用方/未接入」。
  - 完成条件：删除 etcd 小节，只保留仍未处理的指标。
