# Kafka Sink 基于 franz-go 的实现设计与实施计划

## Status

- Status: Proposed
- Date: 2026-01-09
- Owner: TiCDC Team

## Background / Context

TiCDC 的 Kafka sink 负责把上游产生的 DML/DDL/checkpoint 事件编码并写入 Kafka，写入成功后通过 callback 将“已落盘”信息回传给上游推进进度。当前实现分为两层：

- 使用层：`downstreamadapter/sink/kafka`（事件路由、编码流水线、发送调度）
- 客户端抽象层：`pkg/sink/kafka`（Factory、AdminClient、AsyncProducer、SyncProducer 以及 metrics 采集）

`pkg/sink/kafka` 当前以 Sarama 为主要实现（`sarama_factory.go` / `sarama_*_producer.go` / `admin.go` / `sarama_config.go`）。本设计的目标是引入并基于 `github.com/twmb/franz-go`（`kgo` + `kadm`）实现等价的 Kafka 客户端层，使 `downstreamadapter/sink/kafka` 的使用方式尽可能不变，并支持渐进式切换与回滚。

## Problem Statement

在不破坏既有 Kafka sink 行为与配置的前提下，引入 franz-go 作为 Kafka client 实现，满足：

- 兼容现有 sink-uri 参数（topic、partition-num、required-acks、compression、TLS、SASL 等）
- 兼容现有模块边界与接口：`pkg/sink/kafka/factory.go`、`pkg/sink/kafka/cluster_admin_client.go`
- 可灰度、可回滚（支持 Sarama 与 franz-go 并存，按配置选择）
- 性能与稳定性不劣于现有实现，并为后续优化留出空间

## Goals / Non-Goals

### Goals

- 在 `pkg/sink/kafka` 内新增 franz-go 实现：Factory、AdminClient、AsyncProducer、SyncProducer。
- `downstreamadapter/sink/kafka` 仅做最小化改动（最好只改 factory 选择逻辑）。
- 覆盖安全连接能力：TLS、SASL PLAIN / SCRAM / OAuth（与现有选项对齐）。
- 错误语义与诊断信息对齐：保留 `pkg/sink/kafka/logutil.go: AnnotateEventError(...)` 的日志上下文能力。
- 支持渐进式验证：单元测试 + 复用现有集成测试（通过切换参数跑两套）。

### Non-Goals

- 不修改事件编码协议与路由语义（`downstreamadapter/sink/eventrouter`、`pkg/sink/codec` 不在本设计范围）。
- 不实现 Kafka consumer 能力（仅生产端与 admin 能力）。
- 不在第一阶段追求 metrics 完全等价（可先保证功能正确，再补齐指标采集）。

## Current State (as-is)

### 关键接口与调用路径

- `pkg/sink/kafka/factory.go: type Factory`：为上层提供
  - `AdminClient(ctx)`
  - `AsyncProducer(ctx)`（DML）
  - `SyncProducer(ctx)`（DDL/checkpoint）
  - `MetricsCollector(adminClient)`
- `downstreamadapter/sink/kafka/helper.go: newKafkaSinkComponent(...)`：默认使用 `kafka.NewSaramaFactory`
- `downstreamadapter/sink/kafka/sink.go`：
  - DML：编码后调用 `AsyncProducer.AsyncSend(ctx, topic, partition, message)`，并在 `AsyncRunCallback` 中消费 ack/error
  - DDL/checkpoint：调用 `SyncProducer.SendMessage/SendMessages`
  - 心跳：每 5s 调用一次 `Producer.Heartbeat()`（DML 与 DDL 分别一个 ticker）
- Topic 管理依赖 admin：`downstreamadapter/sink/topicmanager/kafka_topic_manager.go`
- 配置自适应：`pkg/sink/kafka/options.go: adjustOptions(...)` 通过 `ClusterAdminClient` 读取 topic/broker 配置并调整 `MaxMessageBytes`、`PartitionNum`、`KeepConnAliveInterval` 等

## Proposed Design (to-be)

### 总体架构

保持 `downstreamadapter/sink/kafka` 逻辑基本不变，仅将 `pkg/sink/kafka` 的 Sarama 实现扩展为“多实现可选”：

```
downstreamadapter/sink/kafka
  └─ uses pkg/sink/kafka.Factory
        ├─ Sarama (existing): saramaFactory / saramaAdminClient / sarama{Async,Sync}Producer
        └─ Franz  (new):      franzFactory  / franzAdminClient  / franz{Async,Sync}Producer
```

### 组件与职责

#### 1) `franzFactory`（新增）

- 文件建议：`pkg/sink/kafka/franz_factory.go`
- 责任：
  - 从 `options` 构造 `kgo.Opt` 集合（seed brokers、TLS、SASL、超时、ack、压缩、producer 行为等）
  - 复用现有自适应逻辑：创建临时 admin client → 调用 `pkg/sink/kafka/options.go: adjustOptions(...)` → 关闭临时 admin → 保存调整后的 `options`
  - 提供 `AdminClient/AsyncProducer/SyncProducer/MetricsCollector` 的 franz-go 实现

#### 2) `franzAdminClient`（新增）

- 文件建议：`pkg/sink/kafka/franz_admin_client.go`
- 内部使用：
  - `kgo.Client`（底层连接与请求）
  - `kadm.Client`（admin API 封装）
- 需要实现 `pkg/sink/kafka/cluster_admin_client.go: ClusterAdminClient`：
  - `GetAllBrokers()`：`kadm.Client.ListBrokers(ctx)` 或 `BrokerMetadata(ctx)` 解析 broker id
  - `GetTopicsMeta(...)` / `GetTopicsPartitionsNum(...)`：`kadm.Client.Metadata(ctx, topics...)`
  - `CreateTopic(...)`：`kadm.Client.CreateTopics(ctx, partitions, rf, configs, topic)`；对 “topic already exists” 做兼容性忽略
  - `GetBrokerConfig(...)`：`kadm.Client.BrokerMetadata(ctx)` 获取 controller id，再 `DescribeBrokerConfigs(ctx, controllerID)`
  - `GetTopicConfig(...)`：`kadm.Client.DescribeTopicConfigs(ctx, topic)`
  - `Heartbeat()`：可实现为 no-op，依赖 `kgo` 的自动重连与 producer 的重试能力；必要时再引入 `Ping`（短超时）的实现以辅助排障

#### 3) `franzAsyncProducer`（新增，DML）

- 文件建议：`pkg/sink/kafka/franz_async_producer.go`
- 对齐上层语义：
  - `AsyncSend(ctx, topic, partition, message)`：调用 `kgo.Client.Produce`，record 的 `Topic/Partition/Key/Value` 来自现有路由与编码结果
  - `AsyncRunCallback(ctx)`：阻塞等待第一条 produce error 或 ctx.Done；对齐 Sarama 行为（发生错误导致 sink 退出重建）
  - `message.Callback`：在 produce 回调成功时执行（与 Sarama 成功通道消费一致）
  - 错误：立刻在边界处包装为带 stack 的错误，并通过 `AnnotateEventError(...)` 附带 message 的 `LogInfo`
  - `Heartbeat()`：可实现为 no-op；通过 `kgo.RecordRetries` 在网络抖动、连接被 broker 关闭等场景下提升鲁棒性

#### 4) `franzSyncProducer`（新增，DDL/checkpoint）

- 文件建议：`pkg/sink/kafka/franz_sync_producer.go`
- 对齐上层语义：
  - `SendMessage`：构造 1 条 record，`ProduceSync` 并返回错误
  - `SendMessages`：按 partitionNum 构造 N 条 record（与当前逻辑一致），`ProduceSync` 等待全部返回，聚合错误
  - `Heartbeat()`：可实现为 no-op

#### 5) 选择机制（灰度）

建议增加一个可选 sink-uri 参数来选择 Kafka client 实现，默认保持 Sarama：

- 新增参数：`kafka-client=sarama|franz`（默认 sarama）
- 影响范围：
  - `pkg/sink/kafka/options.go: urlConfig` 增加字段
  - `downstreamadapter/sink/kafka/helper.go: newKafkaSinkComponent(...)` 根据 options 选择 `kafka.NewSaramaFactory` 或 `kafka.NewFranzFactory`

该机制允许：

- CI/集成测试中在不改代码的情况下切换实现
- 线上灰度（按 changefeed 配置逐个切换）
- 快速回滚（改回 sarama）

## Detailed Design

### 配置映射（options → franz-go）

建议以“对齐现有行为”为优先原则，主要映射如下（示例为概念性描述，具体以实现为准）：

- Brokers：`options.BrokerEndpoints` → `kgo.SeedBrokers(...)`
- ClientID：`options.ClientID` → `kgo.ClientID(...)`
- Dial timeout：`options.DialTimeout` → `kgo.DialTimeout(...)`
- TLS：
  - `options.EnableTLS` / `options.Credential` / `options.InsecureSkipVerify`
  - → 构造 `tls.Config` 后 `kgo.DialTLSConfig(tlsConf)`
- SASL（注意能力差异）：
  - PLAIN / SCRAM：`kgo.SASL(...)`（基于 `github.com/twmb/franz-go/pkg/sasl/plain`、`.../scram`）
  - OAuth：基于 `github.com/twmb/franz-go/pkg/sasl/oauth`，把现有 token provider 适配为 franz-go 的 oauth provider
  - GSSAPI：franz-go 默认包不提供现成实现（当前 `pkg/sink/kafka/sarama_config.go: completeSaramaSASLConfig(...)` 支持）。第一阶段建议：若检测到 `sasl-mechanism=GSSAPI` 则强制走 Sarama，或直接返回“暂不支持”的显式错误。
- RequiredAcks：`options.RequiredAcks` →
  - `WaitForAll` → `kgo.RequiredAcks(kgo.AllISRAcks())`
  - `WaitForLocal` → `kgo.RequiredAcks(kgo.LeaderAck())`
  - `NoResponse` → `kgo.RequiredAcks(kgo.NoAck())`
- Compression：`options.Compression` → `kgo.ProducerBatchCompression(...)`（`SnappyCompression/GzipCompression/Lz4Compression/ZstdCompression/NoCompression`）
- MaxMessageBytes：`options.MaxMessageBytes` → `kgo.ProducerBatchMaxBytes(int32(...))`

### Producer 行为对齐（重试、顺序、幂等）

Sarama 现状（见 `pkg/sink/kafka/sarama_config.go`）：

- DML async：`Producer.Retry.Max = 0`，`Net.MaxOpenRequests = 1`（偏向“顺序安全 + fail fast”）
- DDL/checkpoint sync：`Producer.Retry.Max = 3`（偏向“关键控制面更稳健”）

franz-go 默认行为差异较大（默认 recordRetries 近似无限、默认开启幂等写），因此需要显式对齐：

- DML async（建议第一阶段）：
  - `kgo.DisableIdempotentWrite()`：避免引入 `IDEMPOTENT_WRITE` ACL 依赖，保持与 Sarama 默认一致
  - `kgo.MaxProduceRequestsInflightPerBroker(1)`：对齐顺序与可预期性
  - `kgo.RecordRetries(N)`：设置一个合理重试次数以提升鲁棒性（例如 N=3 或 5），并依赖 franz-go 的“gapless ordering”语义避免在单分区内越过失败记录继续成功写入
  - `kgo.ProduceRequestTimeout(...)`：与现有 `options.WriteTimeout/ReadTimeout` 对齐，避免重试导致长时间阻塞
  - `kgo.ProducerLinger(0)`：对齐“尽快 flush”
- DDL/checkpoint sync：
  - 可采用更保守的重试策略（例如 `RecordRetries(5)`），以提升控制面事件（DDL/checkpoint）的成功率
  - 需要用内部超时兜底，避免在 `SyncProducer` 接口缺少 ctx 的情况下无限阻塞

后续如需提升吞吐，可在不影响语义的前提下评估：

- 允许更大的 in-flight（可能导致乱序）
- 打开幂等写（需评估权限、配额与 broker 版本）
- 适度增加 linger（吞吐上升，延迟增加）

### 错误处理与诊断信息

边界层（franz-go → TiCDC）要做到：

- franz-go / kadm 返回的错误属于第三方错误：在最接近发生点立即用 TiCDC 的 errors 包装以获得 stack trace
- 附带事件上下文：使用 `pkg/sink/kafka/logutil.go: AnnotateEventError(...)` 把 `MessageLogInfo` 拼入错误，便于定位是哪类事件（dml/ddl/checkpoint）以及表信息、ts 等
- 上层 caller 对已包装错误不再重复 wrap（减少噪音与重复堆栈）

### Close 语义与资源管理

Sarama 版本中每个 producer/admin 都持有独立 client；close 顺序也写入了注释（先关 client 再关 producer，避免阻塞 flush）。franz-go 可以选择两种实现方式：

1) **与现状一致：每个组件一个 kgo.Client**（实现简单、行为可控，代价是连接数略多）
2) **同一个 factory 共享一个 kgo.Client**（连接更少、资源更省，但需要引用计数与更严格的 close 协议）

第一阶段建议采用方案 (1)，降低引入风险；后续可在确认稳定后再做共享优化。

## Performance Considerations

franz-go 的优势通常来自：

- 更紧凑的编码与更少的反射/分配
- 统一 client 能力（produce/admin/consume 一套基础设施）
- 可通过 hooks/telemetry 获取更丰富的请求级信息

但在 TiCDC Kafka sink 场景，真正的性能瓶颈往往在“上层编码与调度”，并非单纯 client 库。引入 franz-go 后仍需重点关注：

- `downstreamadapter/sink/kafka` 的无限队列与 per-row 分配（不在本设计范围，但可在后续优化）
- Producer 参数对吞吐/延迟/乱序的权衡（linger、batch、in-flight、retries）
- 若 franz 实现的 `Heartbeat()` 为 no-op，可考虑后续把上层 5s ticker 变为按需或配置化，减少无效调用

## Testing Strategy

### Unit Tests

- 配置映射测试：给定 `options`，断言构造出的 franz-go 配置与预期一致（acks/compression/TLS/SASL 等）。
- admin wrapper 行为测试：对 `GetTopicsMeta/GetTopicConfig/GetBrokerConfig/CreateTopic` 的错误处理、已存在 topic 的兼容性处理等。

### Integration / E2E

复用现有 Kafka 集成测试，通过 sink-uri 参数切换实现：

- 现有测试用例（示例）：
  - `tests/integration_tests/kafka_log_info/run.sh`（依赖 failpoint 注入错误与日志上下文）
  - `tests/integration_tests/mq_sink_error_resume/run.sh`（错误恢复）
- 新增运行方式：
  - 在 sink-uri 增加 `kafka-client=franz`，并确保 failpoint 名称在 franz 实现中兼容（或新增等价 failpoint）

### 性能回归

- A/B 对比：同一 workload 下对比 Sarama 与 franz-go 的吞吐、端到端延迟、CPU、内存、Kafka 请求数量。
- 关注场景：高并发 DML、批量 DDL、checkpoint 广播、topic 自动创建/metadata 刷新。

## Observability / Operations

- 日志：错误日志必须包含 changefeed 维度（keyspace/changefeed）和事件上下文（eventType/table/ts），但避免在日志文本中拼接函数名与多余格式噪音。
- Metrics（阶段性计划）：
  - 第一阶段：可先保持 `MetricsCollector` 为 no-op（功能优先）
  - 第二阶段：基于 franz-go hooks 或 client telemetry 把关键指标接入现有 Prometheus 指标体系（例如 request latency、in-flight、吞吐等）

## Rollout Plan

1) **实现与编译通过**
   - 新增 `kafka-client=franz` 选项，默认仍为 sarama
   - 引入 `NewFranzFactory` 与相关实现文件
2) **功能验证**
   - 单元测试覆盖关键映射与错误处理
   - 本地/CI 跑现有 Kafka 集成测试，分别用 sarama 与 franz-go 跑一遍
3) **灰度**
   - 选取少量 changefeed 开启 franz-go
   - 对比关键指标与故障率
4) **扩大与默认切换**
   - 确认稳定后逐步扩大覆盖面
   - 视情况将默认实现切换为 franz-go，并保留 sarama 回滚窗口

## Alternatives Considered

- 继续使用 Sarama：稳定但维护与性能空间受限，且部分行为（如 metadata/连接管理）需要更多定制补丁。
- 其他 Go Kafka client（如 kafka-go）：API/语义与现有实现差异较大，迁移成本与回归风险更高。

## Open Questions / Future Work

- SASL GSSAPI（Kerberos）在 franz-go 体系下的实现方案（自定义 sasl.Mechanism vs 继续走 Sarama）。
- franz-go metrics / hooks 与现有 `pkg/sink/kafka/metrics_collector.go` 指标体系的对齐方案与成本评估。
- 是否要在 factory 内共享 `kgo.Client`（资源更省）以及如何保证 close 语义与并发安全。

## References

- franz-go：`github.com/twmb/franz-go`（核心 `pkg/kgo`）
- kadm：admin 封装 `github.com/twmb/franz-go/pkg/kadm`
- 现有 TiCDC Kafka sink：
  - `downstreamadapter/sink/kafka/helper.go`
  - `downstreamadapter/sink/kafka/sink.go`
  - `pkg/sink/kafka/factory.go`
  - `pkg/sink/kafka/cluster_admin_client.go`
  - `pkg/sink/kafka/options.go`
  - `pkg/sink/kafka/sarama_factory.go`
  - `pkg/sink/kafka/sarama_config.go`
  - `pkg/sink/kafka/admin.go`
  - `pkg/sink/kafka/sarama_async_producer.go`
  - `pkg/sink/kafka/sarama_sync_producer.go`
  - `pkg/sink/kafka/logutil.go`
