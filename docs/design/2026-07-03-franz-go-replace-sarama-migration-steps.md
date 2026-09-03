# 使用 franz-go 替换 Sarama 的迁移步骤

## 目标

本文给出在 TiCDC Kafka sink 中用 `~/go/franz-go` 替换 Sarama 的迁移步骤。
配套功能审计见：

- `docs/design/2026-07-03-franz-go-replace-sarama-audit.md`

本文关注“怎么迁移”：分阶段实现、验证、灰度、切默认、回滚和清理。
迁移默认采用 expand-migrate-contract 方式：先新增 franz-go 实现并保持 Sarama 可用，
再灰度切流量，最后在兼容窗口结束后移除 Sarama。

## 迁移性质

| 项目 | 结论 |
| --- | --- |
| 状态源 | Kafka topic 中的 change event、TiCDC checkpoint/resolved 进度、changefeed config、Prometheus 指标。 |
| 外部可见面 | sink URI/config、Kafka 消息协议、topic/partition 顺序、ACL、metrics、日志、错误语义。 |
| 可逆性 | 分阶段可逆；切默认前可通过配置退回 Sarama；移除 Sarama 依赖后只剩版本回滚。 |
| 主要风险 | producer 默认行为差异、消息大小估算、callback 时机、metrics 兼容、SASL/GSSAPI、ACL 变化。 |
| 推荐策略 | 默认禁用 franz-go idempotency；保留 Sarama 回滚路径至少一个发布窗口。 |

## 总体阶段

1. 准备阶段：冻结兼容契约，补足 baseline 测试和 A/B 工具。
2. Expand：新增 franz-go 适配层和选择开关，默认仍使用 Sarama。
3. 功能迁移：先 admin，再 sync producer，再 async producer，再 TLS/SASL/metrics。
4. Shadow / 对照验证：同配置跑 Sarama 与 franz-go，确认功能、顺序、错误和指标。
5. 小流量灰度：按 changefeed 逐步启用 franz-go，保留即时回滚。
6. 切默认：把默认实现从 Sarama 切到 franz-go，但仍保留 `sarama` fallback。
7. Contract：兼容窗口结束后移除 Sarama 代码、依赖和文档残留。

## 阶段 0：准备和基线

### 0.1 明确兼容契约

产出：

- 一份确认过的兼容清单，至少覆盖：
  - sink URI/config key 和默认值。
  - Kafka 版本支持矩阵。
  - 最小 ACL 要求。
  - protocol 输出格式。
  - DDL/checkpoint/resolved broadcast 规则。
  - DML callback 时机。
  - Kafka producer metrics 名称和 label。
  - 错误类型、错误码和关键日志字段。

进入下一步前必须确认：

- 不公开改变 `required-acks`、`compression`、`max-retry`、`kafka-version`、
  `max-message-bytes` 等默认行为。
- 不复用已 deprecated 的 `enable-kafka-sink-v2` 作为 franz-go 开关。
- 如果要新增用户可见开关，例如 `kafka-client=franz|sarama`，需要单独做兼容评审。

### 0.2 建立 Sarama baseline

建议先在未引入 franz-go 的 `master` 上记录 baseline：

- unit tests：
  - `make unit_test_pkg PKG=./pkg/sink/kafka/...`
  - `make unit_test_pkg PKG=./downstreamadapter/sink/...`
- Kafka integration tests：
  - canal-json basic。
  - open-protocol basic。
  - avro / schema registry。
  - simple protocol。
  - large message / claim-check / handle-key-only。
  - dispatcher / dynamic topic。
  - mq sink error resume。
- 性能 baseline：
  - 小行高吞吐。
  - 大消息。
  - 多 partition。
  - 低流量长 idle。

记录内容：

- TiCDC commit、Go version、Kafka version、broker 配置。
- sink URI 和 changefeed config。
- rows/sec、bytes/sec、send latency p95/p99、CPU、heap、goroutine。
- Kafka producer request rate、response rate、in-flight、records/request、compression ratio。
- changefeed checkpoint lag / resolved ts lag。

退出条件：

- Sarama baseline 本身稳定。
- 已知 flaky test 单独记录，不和 franz-go 替换混在一起判断。

## 阶段 1：新增 franz-go 适配骨架

### 1.1 引入依赖

改动点：

- `go.mod` / `go.sum`
  - `github.com/twmb/franz-go/pkg/kgo`
  - `github.com/twmb/franz-go/pkg/kadm`
  - `github.com/twmb/franz-go/pkg/kmsg`
  - `github.com/twmb/franz-go/pkg/sasl/plain`
  - `github.com/twmb/franz-go/pkg/sasl/scram`
  - `github.com/twmb/franz-go/pkg/sasl/oauth`
  - `github.com/twmb/franz-go/pkg/sasl/kerberos`

注意：

- 不在这一阶段移除 Sarama。
- 不把 franz-go metrics plugin 直接作为最终指标方案，除非已决定迁移指标名。

### 1.2 保持现有接口

优先不改上层 sink，只新增实现：

- `pkg/sink/kafka/franz_factory.go`
- `pkg/sink/kafka/franz_config.go`
- `pkg/sink/kafka/franz_admin.go`
- `pkg/sink/kafka/franz_sync_producer.go`
- `pkg/sink/kafka/franz_async_producer.go`
- `pkg/sink/kafka/franz_metrics_collector.go`

保留接口：

- `Factory`
- `ClusterAdminClient`
- `AsyncProducer`
- `SyncProducer`
- `MetricsCollector`

退出条件：

- 新代码可编译。
- 默认路径仍是 Sarama。
- 没有任何用户在未显式启用时走 franz-go。

### 1.3 增加选择开关

推荐先用内部灰度开关，不立即变成公开文档承诺：

- 方案 A：内部 config/env/build tag，用于 CI 和受控灰度。
- 方案 B：公开 sink URI 参数 `kafka-client=sarama|franz`。

推荐顺序：

1. 第一阶段用内部开关验证。
2. 如果需要用户级灰度，再公开 `kafka-client`，并补充文档、测试和 release note。

实现要求：

- 默认值必须是 `sarama`。
- 无效值返回明确配置错误。
- 切换只影响 Kafka client 层，不影响 encoder、event router、topic manager。

回滚：

- 将开关改回 `sarama`。
- 如果开关在 changefeed config 中，回滚不应要求删除 changefeed。

## 阶段 2：迁移 admin client

先迁移 admin，是因为 `adjustOptions` 和 topic manager 依赖它，且不触碰数据写入。

### 2.1 实现 `franzAdminClient`

需要实现：

- `GetAllBrokers`
- `GetBrokerConfig`
- `GetTopicConfig`
- `GetTopicsMeta`
- `GetTopicsPartitionsNum`
- `CreateTopic`
- `Close`

映射建议：

- 使用 `kadm.Client` 做 topic metadata、create topic、describe config。
- `GetBrokerConfig` 要确认是否等价于当前 Sarama 从 controller broker 读取 config 的行为。
- `TopicAlreadyExists` 继续按成功处理。
- `UnknownTopicOrPartition` 在 `ignoreTopicError=true` 时跳过。
- `ErrKafkaConfigNotFound` 语义保持。

### 2.2 admin 单元测试

覆盖：

- topic 存在，partition 数读取正确。
- topic 不存在，ignore true/false 行为正确。
- create topic 成功。
- create topic 时 topic 已存在。
- invalid replication factor / policy violation / authorization error 保留原始诊断。
- broker/topic config 找不到时保留现有 fallback 和告警语义。
- Confluent Cloud 下 `min.insync.replicas` 不可见时保持允许启动但告警。

退出条件：

- `adjustOptions` 测试可在 franz admin wrapper 下通过。
- `topicmanager` 测试可在 franz admin wrapper 下通过。
- 默认 Sarama 测试未回归。

## 阶段 3：迁移 sync producer

sync producer 负责 DDL 和 checkpoint，流量低但正确性要求高。

### 3.1 实现 `franzSyncProducer`

要求：

- `SendMessage(topic, partitionNum, message)` 发送单条 record 到指定 partition。
- `SendMessages(topic, partitionNum, message)` 发送 `0..partitionNum-1` 每个 partition 一条。
- 使用 `ProduceSync(ctx, records...)` 或等价同步路径。
- 任一 partition 失败时返回错误。
- 错误必须通过 `AnnotateEventError` 附加 DDL/checkpoint log info。

配置必须显式设置：

- `RecordPartitioner(kgo.ManualPartitioner())`
- `RequiredAcks(...)`
- `ProducerBatchCompression(...)`
- `ProducerLinger(0)`
- `RecordRetries(options.MaxRetry)`
- `DisableIdempotentWrite()`
- `MaxProduceRequestsInflightPerBroker(1)`
- `ProducerBatchMaxBytes(...)`

### 3.2 sync producer 测试

覆盖：

- Canal-JSON DDL 到 partition 0。
- Open Protocol DDL broadcast 到全部 partition。
- checkpoint/resolved broadcast 到全部 partition。
- no table 时 checkpoint 发 default topic。
- 部分 partition 发送失败时返回错误。
- `required-acks=0/1/-1` 配置映射。

退出条件：

- DDL/checkpoint 单元测试通过。
- Kafka integration 中 DDL、checkpoint、resolved 语义和 Sarama 对齐。

回滚：

- `kafka-client=sarama`。
- 因为没有改变 Kafka 消息格式，已经写入的 DDL/checkpoint 可继续被消费者按原协议读取。

## 阶段 4：迁移 async producer

async producer 是 DML 主路径，必须最后接入，并且先在可回滚模式下运行。

### 4.1 实现 `franzAsyncProducer`

要求：

- `AsyncSend(ctx, topic, partition, message)`：
  - 构造 `kgo.Record{Topic, Partition, Key, Value}`。
  - record partition 必须来自 TiCDC event router，不能让 franz-go 重新 hash。
  - 发送前设置 message partition key，保持日志和统计语义。
- promise / callback：
  - `err == nil` 时执行 `message.Callback`。
  - `err != nil` 时不执行 callback。
  - promise 不做阻塞操作，不调用 `Flush` 或可能阻塞的 `Produce`。
- `AsyncRunCallback(ctx)`：
  - 等待首个 produce error 或 ctx done。
  - 首个 produce error 返回给 sink，让 sink 重建。
  - 返回错误带 TiCDC stack 和 `MessageLogInfo`。
- `Close()`：
  - 不在 sink 关闭路径无限等待 flush。
  - 明确是否允许未 ack record 后续由上游重放造成重复。

### 4.2 async producer 测试

覆盖：

- 成功后 callback 恰好执行一次。
- producer error 时 callback 不执行，`AsyncRunCallback` 返回错误。
- context cancel 时可退出。
- close 不阻塞。
- manual partition 生效。
- `required-acks=0` 下 callback 语义和 Sarama 对齐。
- `RecordRetries` 耗尽时同 partition 不越过失败 record。

退出条件：

- DML 单元测试通过。
- Kafka integration 中 DML 顺序、重复和恢复语义与 Sarama 对齐。
- `mq_sink_error_resume` 类场景通过。

## 阶段 5：配置、TLS、SASL 和版本映射

这一阶段不要改变用户配置名，只做映射。

### 5.1 producer option 映射

必须显式覆盖 franz-go 默认值：

| 配置 | franz-go 设置 |
| --- | --- |
| brokers | `kgo.SeedBrokers(...)` |
| client id | `kgo.ClientID(...)` |
| kafka version | `kgo.MaxVersions(...)` 或等价固定版本能力 |
| required acks | `kgo.RequiredAcks(...)` |
| compression | `kgo.ProducerBatchCompression(...)` |
| max message bytes | `kgo.ProducerBatchMaxBytes(...)` |
| retry | `kgo.RecordRetries(options.MaxRetry)` + backoff |
| linger | `kgo.ProducerLinger(0)` |
| partition | `kgo.RecordPartitioner(kgo.ManualPartitioner())` |
| idempotency | 默认 `kgo.DisableIdempotentWrite()` |
| inflight | `kgo.MaxProduceRequestsInflightPerBroker(1)` |
| buffer | 明确 `MaxBufferedRecords` / `MaxBufferedBytes` 策略 |

### 5.2 TLS 映射

覆盖：

- 系统 CA + `enable-tls=true`。
- 自签 CA + cert/key。
- cert/key/ca 不完整时仍报配置错误。
- `enable-tls=false` 但配置证书时仍报配置错误。
- `insecure-skip-verify` 只在 TLS 开启时生效。

### 5.3 SASL 映射

覆盖：

- PLAIN。
- SCRAM-SHA-256。
- SCRAM-SHA-512。
- OAUTHBEARER：
  - base64 secret 解码。
  - token URL。
  - scopes。
  - grant type。
  - audience。
- GSSAPI：
  - user/password auth。
  - keytab auth。
  - service name。
  - realm。
  - kerberos config path。
  - disable PAFXFAST。

注意：

- `pkg/security/sasl.go` 当前引用 Sarama 常量。只要 Sarama 还没移除，可以先保留；
  contract 阶段必须改成 TiCDC 自有常量。
- GSSAPI 不能只靠单元测试，至少需要一个可运行的 Kerberos/Kafka 集成验证或明确记录
  未覆盖风险。

### 5.4 Kafka version 映射

要求：

- 用户显式 `kafka-version` 必须生效。
- 无法解析版本仍返回 `ErrKafkaInvalidVersion` 或等价 TiCDC 错误。
- 未指定版本时可继续自动协商，但不能扩大产品支持承诺。
- 版本不匹配的告警体验尽量保留。

退出条件：

- 所有配置映射测试通过。
- 旧 sink URI/config 不修改即可加载。
- 官方最小 ACL 下可以启动并写入。

## 阶段 6：消息大小和大消息路径

这一步是切 DML 流量前的硬门槛。

### 6.1 替换 size accounting

当前 `Message.Length()` 使用 Sarama `MaxRecordOverhead`。迁移步骤：

1. 写一个 franz-go record batch size 估算 helper。
2. 用测试对照 franz-go 实际编码或 producer 拒绝边界。
3. 将 `Message.Length()` 或其调用方切到新的估算方式。
4. 保留或重新论证 `maxMessageBytesOverhead=128` safety margin。
5. 对 open-protocol batch splitter、large-message compression、claim-check 都加测试。

### 6.2 大消息测试

覆盖：

- 普通消息接近 `max-message-bytes`。
- 单行大于限制。
- Open Protocol batch 被拆分。
- message-level lz4/snappy compression 后不过限。
- `handle-key-only`。
- `claim-check`。
- `claim-check-raw-value`。
- broker/topic `message.max.bytes` 小于用户配置。

退出条件：

- 不出现“TiCDC 判定可发送但 franz-go/broker 拒绝”的边界误差。
- 不出现“TiCDC 过早 claim-check”的明显误差。
- `Message was too large` 错误仍可诊断。

## 阶段 7：metrics、日志和泄漏检查

### 7.1 metrics 兼容

默认要求保留现有指标名和 label：

- `ticdc_sink_kafka_producer_in_flight_requests`
- `ticdc_sink_kafka_producer_outgoing_byte_rate`
- `ticdc_sink_kafka_producer_request_rate`
- `ticdc_sink_kafka_producer_request_latency`
- `ticdc_sink_kafka_producer_compression_ratio`
- `ticdc_sink_kafka_producer_records_per_request`
- `ticdc_sink_kafka_producer_response_rate`

实现步骤：

1. 基于 franz-go hooks 实现 TiCDC collector。
2. 对齐 Sarama collector 的 label：`namespace`、`changefeed`、`broker`、`type`。
3. 明确 in-flight 的等价口径。
4. 在 changefeed stop/delete 后清理 label。
5. A/B 对比指标是否在同一数量级。

如果决定改指标名：

- 必须提供 dashboard / alert 迁移方案。
- 需要至少一个版本同时暴露新旧指标。
- release note 必须说明。

### 7.2 logger 和 leakutil

步骤：

- 给 franz-go 接 `kgo.WithLogger`。
- 保留 Kafka client 日志中的 keyspace/changefeed 上下文。
- 清理 `WithInitSaramaLogger` 的依赖路径，但不要在 Sarama fallback 存在期间破坏 Sarama。
- 更新 `pkg/leakutil/leak_helper.go`，不要继续用 Sarama goroutine ignore 掩盖新泄漏。

退出条件：

- `kafka_log_info` 类测试通过。
- goroutine leak 测试不需要新增宽泛 ignore。
- metrics cleanup 测试通过。

## 阶段 8：Shadow 和 A/B 验证

目标是确认 franz-go 在相同 TiCDC/Kafka 配置下不改变语义。

### 8.1 本地/CI 对照

对每组 case 跑两次：

- `kafka-client=sarama`
- `kafka-client=franz`

比较：

- 下游行数。
- DDL 顺序。
- partition 分布。
- row-level checksum。
- checkpoint/resolved 推进。
- 错误恢复后的重复消息是否仍可由 protocol 语义处理。
- Kafka producer metrics。

### 8.2 性能对照

至少覆盖：

- 无压缩。
- gzip/snappy/lz4/zstd。
- `required-acks=-1`。
- `required-acks=1`。
- TLS/SASL。
- 高 partition 数。
- 大消息。

退出条件：

- 正确性无差异。
- 性能无不可解释显著退化。
- 内存和 goroutine 无明显泄漏。
- 失败场景的恢复方式可解释。

## 阶段 9：小流量灰度

### 9.1 灰度前置条件

必须满足：

- 默认仍是 Sarama。
- 每个灰度 changefeed 可单独切回 Sarama。
- operator 知道回滚命令。
- dashboard 同时能看 Kafka sink lag、producer error、request latency、resource usage。
- Kafka ACL 是官方最小权限时，franz-go 已验证可写入。

### 9.2 灰度顺序

推荐顺序：

1. 内部测试环境，单 changefeed，单 topic，低流量。
2. 内部测试环境，多 topic / 动态 topic。
3. 预发环境，真实 schema，低写入。
4. 生产 canary，低风险 changefeed。
5. 生产扩大到 5%。
6. 生产扩大到 25%。
7. 生产扩大到 50%。
8. 切默认前维持观察窗口。

每一档观察：

- checkpoint lag / resolved lag。
- Kafka producer error rate。
- request latency p99。
- DML callback backlog。
- broker request/response rate。
- CPU、heap、goroutine。
- topic partition 写入分布。
- DDL 和 checkpoint 是否正常推进。

### 9.3 回滚动作

可回滚点：

- 切默认前：把 changefeed 的 Kafka client 选择改回 Sarama。
- 切默认后但保留 fallback：显式设置 `kafka-client=sarama` 或回滚默认配置。
- 移除 Sarama 后：只能回滚二进制版本。

回滚后验证：

- changefeed 恢复 running。
- checkpoint/resolved 继续推进。
- 下游消费者能处理可能重复的 at-least-once 消息。
- 大消息 claim-check 外部存储没有新增不可读 marker。
- metrics 回到 Sarama collector。

## 阶段 10：切默认

切默认的进入条件：

- franz-go 路径完成至少一个发布候选版本或一个充分观察窗口。
- 所有 P0 风险关闭。
- A/B 性能报告已归档。
- metrics 和日志兼容。
- 回滚路径演练过。
- 官方文档和 release note 已准备。

切默认步骤：

1. 将默认 Kafka client 从 Sarama 改为 franz-go。
2. 保留显式 `kafka-client=sarama` fallback。
3. release note 说明：
   - 默认 Kafka client 改变。
   - 配置兼容。
   - ACL 不需要新增 `IDEMPOTENT_WRITE`，因为默认禁用 idempotency。
   - 已知差异或调优建议。
4. 灰度发布。
5. 观察至少一个完整业务周期。

切默认后监控：

- Kafka sink error rate。
- changefeed restart count。
- Kafka produce latency。
- checkpoint lag。
- broker throttle。
- message too large。
- auth failures。
- metrics cardinality。

回滚：

- 优先配置回滚到 Sarama。
- 如果默认切换导致启动期失败，可回滚二进制。
- 不需要迁移 Kafka topic 数据，因为消息协议未改变。

## 阶段 11：Contract 和移除 Sarama

只有在兼容窗口结束后执行。

前置条件：

- 没有线上 changefeed 仍配置 `kafka-client=sarama`。
- 至少一个稳定版本周期内 franz-go 是默认实现。
- 没有未关闭的 franz-go P0/P1 correctness issue。
- 运营 dashboard 和 alert 不再依赖 Sarama-only 指标来源。

清理项：

- 删除 `sarama_factory.go`。
- 删除 `sarama_config.go`。
- 删除 `sarama_async_producer.go`。
- 删除 `sarama_sync_producer.go`。
- 删除或更新 Sarama 专属 mocks/tests。
- 将 `pkg/security/sasl.go` 中的 Sarama 常量改为 TiCDC 自有常量。
- 移除 `pkg/logger/log.go` 中 Sarama logger 初始化。
- 移除 `pkg/leakutil/leak_helper.go` 中 Sarama goroutine ignore。
- 删除 `go.mod` / `go.sum` 中不再需要的 Sarama 依赖。
- 更新 Kafka sink 文档中 Sarama client id 或 Sarama 行为描述。

Contract 阶段测试：

- `make unit_test_pkg PKG=./pkg/sink/kafka/...`
- `make unit_test_pkg PKG=./downstreamadapter/sink/...`
- `make unit_test_pkg PKG=./pkg/security/...`
- `make cdc`
- Kafka integration suite。
- `make check`，用于确认 go.mod、format、codegen 等。

回滚：

- Contract 后不能配置回滚到 Sarama，只能回滚二进制版本。
- 如果需要保留更强回滚能力，不要执行 Contract。

## 关键验收门槛

以下任一项未满足，不应切默认：

- DML callback 时机未被测试证明。
- 手动 partition 未被测试证明。
- 大消息 size accounting 未完成。
- 官方最小 Kafka ACL 下未验证。
- TLS/SASL/GSSAPI/OAuth 未覆盖。
- `required-acks=0/1/-1` 未覆盖。
- 既有 Kafka producer metrics 未兼容或未提供迁移方案。
- error resume / broken pipe / idle connection 场景未覆盖。
- 没有 Sarama fallback。
- 没有回滚演练。

## 推荐 PR 拆分

1. PR 1：新增 franz-go dependency、config builder skeleton、默认不启用。
2. PR 2：franz admin client + admin/topic manager tests。
3. PR 3：franz sync producer + DDL/checkpoint tests。
4. PR 4：franz async producer + callback/error/close tests。
5. PR 5：TLS/SASL/OAuth/GSSAPI mapping tests。
6. PR 6：message size accounting + large message tests。
7. PR 7：franz metrics collector + logger/leakutil。
8. PR 8：integration tests and A/B scripts。
9. PR 9：controlled gray switch documentation / release note。
10. PR 10：切默认，保留 Sarama fallback。
11. PR 11：Contract 移除 Sarama，需等兼容窗口结束。

## 运行手册摘要

启用 franz-go 前：

1. 确认 Kafka ACL 没有依赖 franz-go idempotency。
2. 确认 topic `max.message.bytes` 和 sink `max-message-bytes`。
3. 确认 TLS/SASL 配置在 franz-go 路径验证过。
4. 确认 dashboard 已能看 franz-go collector。
5. 确认回滚命令。

启用后观察：

1. 10 分钟内无 producer error spike。
2. checkpoint lag 不持续增长。
3. produce latency p99 不异常。
4. broker throttle 不异常。
5. consumer 没有解析错误。

触发回滚：

- Kafka auth/ACL error。
- message too large 明显增加。
- checkpoint lag 持续增长。
- DDL/checkpoint/resolved 不推进。
- producer goroutine 或 heap 持续增长。
- 下游消费者出现协议解析错误。

回滚后：

- 确认 changefeed running。
- 确认 checkpoint 继续推进。
- 确认消费者可处理重复消息。
- 保留 franz-go 错误日志、metrics 和 Kafka broker logs 供根因分析。
