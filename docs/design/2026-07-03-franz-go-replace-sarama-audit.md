# 使用 franz-go 替换 Sarama 的 Kafka sink 功能审计清单

## 背景

本文基于 `master` 分支代码和 TiCDC Kafka sink 官方文档，枚举用
`~/go/franz-go` 替换 Sarama 时必须保持、实现和验证的功能点。本文不是
PRD，也不是最终实现方案；它的目标是把替换边界、兼容性风险和验收项列清楚，
避免只替换 producer API 后遗漏 Kafka sink 对正确性、性能、可靠性和运维的
隐含约束。

代码阅读基准：

- `master` revision: `d2da619279f877a9964facdabebdc608044523cd`
- 本地 franz-go: `/Users/edison/go/franz-go`

官方文档阅读范围：

- [TiCDC 同步数据到 Kafka](https://docs.pingcap.com/zh/tidb/stable/ticdc-sink-to-kafka/)
- [TiCDC Changefeed 配置参数](https://docs.pingcap.com/zh/tidb/stable/ticdc-changefeed-config/)
- [TiCDC Open Protocol](https://docs.pingcap.com/zh/tidb/stable/ticdc-open-protocol/)
- [TiCDC Canal-JSON Protocol](https://docs.pingcap.com/zh/tidb/stable/ticdc-canal-json/)
- [TiCDC Avro Protocol](https://docs.pingcap.com/zh/tidb/stable/ticdc-avro-protocol/)
- [TiCDC Simple Protocol](https://docs.pingcap.com/zh/tidb/stable/ticdc-simple-protocol/)
- [TiCDC Debezium Protocol](https://docs.pingcap.com/zh/tidb/stable/ticdc-debezium/)
- [TiCDC 数据校验](https://docs.pingcap.com/zh/tidb/stable/ticdc-integrity-check/)
- [TiCDC 常见问题](https://docs.pingcap.com/zh/tidb/stable/ticdc-faq/)
- [TiCDC 故障处理](https://docs.pingcap.com/zh/tidb/stable/troubleshoot-ticdc/)

## 当前 master 上的 Kafka sink 结构

`downstreamadapter/sink/kafka` 是 Kafka sink 的业务层：

- `helper.go` 解析 sink URI、创建 encoder、event router、topic manager，并调用
  `kafka.NewSaramaFactory` 创建客户端层。
- `sink.go` 把 DML、DDL、checkpoint 分成三条路径：
  - DML 经过 event router 计算 topic/partition，encoder group 编码后用
    `AsyncProducer.AsyncSend` 发送。
  - DDL 用 `SyncProducer.SendMessage` 或 `SendMessages` 发送。Open Protocol 的
    DDL 需要广播到全部 partition，Canal-JSON 的 DDL 走 partition 0。
  - checkpoint/resolved message 广播到当前活跃 topic 的全部 partition。没有表时，
    发送到 default topic，以兼容旧行为。

`pkg/sink/kafka` 是客户端抽象层：

- `factory.go` 定义 `Factory`、`ClusterAdminClient`、`AsyncProducer`、
  `SyncProducer`、`MetricsCollector` 这些上层依赖的接口。
- `sarama_factory.go` 创建 Sarama admin、sync producer、async producer，并在
  producer 上挂 Sarama metrics registry。
- `sarama_config.go` 把 TiCDC Kafka options 转为 Sarama config，包括版本探测、
  TLS/SASL、acks、compression、manual partitioner、retry、timeout 和
  `Net.MaxOpenRequests=1`。
- `options.go` 合并 sink URI 和 changefeed config，并通过 admin 查询 topic/broker
  配置，调整 `max-message-bytes`、`partition-num` 和 `min.insync.replicas`。
- `admin.go` 封装 topic metadata、topic/broker config 和 create topic。
- `sarama_async_producer.go` 在 Sarama success channel 中执行 DML callback；遇到
  producer error 时返回错误，让 sink 重建。
- `sarama_sync_producer.go` 用于 DDL/checkpoint 的同步发送。
- `metrics_collector.go` 从 Sarama go-metrics registry 中采集并暴露 TiCDC 既有
  Prometheus 指标。

可替换边界相对清晰：优先保持 `pkg/sink/kafka/factory.go` 的接口稳定，在
`pkg/sink/kafka` 内新增 franz-go 实现。真正需要谨慎处理的是“默认行为差异”，
例如 franz-go 默认启用 idempotent write、默认 compression preference 包含
snappy、默认 linger 为 10ms、默认 record retries 近似无限，这些都不能直接沿用。

## 替换原则

1. 对用户可见的 sink URI、changefeed config、协议输出、错误语义和指标名称默认
   保持兼容。
2. 不因为换客户端而扩大官方支持矩阵。franz-go 自身支持更宽的 Kafka 版本，不代表
   TiCDC Kafka sink 的官方版本支持自动扩大。
3. 不默认引入新的 ACL 要求。特别是 franz-go 默认 idempotent write 在 Kafka 3.0
   以前通常需要 Cluster 级 `IDEMPOTENT_WRITE` 权限，而 TiCDC 文档当前最小 ACL
   没有列它。
4. TiCDC 的 at-least-once 语义、单行更新顺序、DDL/checkpoint/resolved 广播语义
   优先于吞吐优化。
5. 性能结论必须来自 TiCDC 场景 A/B 实测。franz-go README 的 benchmark 只能说明
   客户端潜力，不能直接作为 TiCDC 替换收益结论。

## 必须保持的用户配置面

以下配置在替换后必须继续支持，默认值、校验和配置文件/URI 覆盖关系也应保持：

| 类别 | 配置项 | 要求 |
| --- | --- | --- |
| 基础 | broker endpoints、topic、`protocol` | 保持 URI 语法和协议名不变。 |
| 版本 | `kafka-version` | 保留用户显式指定能力；保留版本错误诊断和文档中的兼容要求。 |
| producer | `partition-num`、`replication-factor`、`max-message-bytes`、`max-retry`、`required-acks` | 行为必须和 `options.go` 的校验/自适应一致。 |
| topic | `auto-create-topic` | true 时 TiCDC 创建 topic；false 且 topic 不存在时仍报配置错误。 |
| 压缩 | `compression=none,gzip,snappy,lz4,zstd` | 默认必须是 `none`，不能继承 franz-go 默认 snappy preference。 |
| TLS | `enable-tls`、`ca`、`cert`、`key`、`insecure-skip-verify` | 保持三证书校验和“配置了证书即启用 TLS”的现有行为。 |
| SASL | PLAIN、SCRAM-SHA-256、SCRAM-SHA-512、GSSAPI、OAUTHBEARER | 都需要映射；GSSAPI 需要用 franz-go `pkg/sasl/kerberos` 做专项验证。 |
| 超时 | `dial-timeout`、`write-timeout`、`read-timeout` | franz-go 没有完全同名语义，需要显式设计等价映射和测试。 |
| 协议扩展 | `enable-tidb-extension`、Avro decimal/bigint 模式、Debezium schema 开关、Simple codec config | 客户端替换不能改变 encoder 行为。 |
| 大消息 | `large-message-handle-*`、`claim-check-*` | 客户端替换不能改变大消息判定、外部存储写入和 Kafka marker 格式。 |

`enable-kafka-sink-v2` 在当前代码中已是 deprecated，并且仍使用默认 Kafka sink。
不要把它偷偷复用成 franz-go 开关。若需要灰度，建议新增内部开关或新的明确配置，
并为兼容性变更写单独方案。

## Kafka 版本支持

官方文档列出的 TiCDC Kafka sink 最低 Kafka 版本是产品承诺，替换后仍应遵守：

| TiCDC 版本 | Kafka 最低版本 |
| --- | --- |
| TiCDC >= v8.1.0 | Kafka >= 2.1.0 |
| v7.6.0 <= TiCDC < v8.1.0 | Kafka >= 2.4.0 |
| v7.5.2 <= TiCDC < v7.6.0 | Kafka >= 2.1.0 |
| v7.5.0 <= TiCDC < v7.5.2 | Kafka >= 2.4.0 |
| v6.5.0 <= TiCDC < v7.5.0 | Kafka >= 2.1.0 |
| v6.1.0 <= TiCDC < v6.5.0 | Kafka >= 2.0.0 |

当前 Sarama 实现的实际行为：

- `options.NewOptions` 默认 `Version` 为 `2.4.0`。
- `sarama_config.go` 的 `defaultKafkaVersion` 是 `2.0.0.0`，`maxKafkaVersion` 是
  `2.8.0.0`。
- 如果用户未指定 `kafka-version`，代码会通过 broker `ApiVersions` 中 Metadata
  API 的 max version 推断版本；失败时退回 `2.0.0.0`。
- 如果用户指定 `kafka-version`，解析失败返回 `ErrKafkaInvalidVersion`，并在
  指定版本和探测版本不一致时告警。

franz-go 侧能力：

- README 表示 franz-go 支持 Kafka 0.8.0 到 4.2+ 的协议范围。
- franz-go 默认会使用 ApiVersions 协商，也可通过 `kgo.MaxVersions` 固定协议版本。

替换要求：

1. 不能因为 franz-go 支持更高或更低版本就改变 TiCDC 文档承诺。
2. `kafka-version` 必须继续生效，建议映射为 `kgo.MaxVersions(...)` 或等价能力。
3. 当前“自动探测 + 指定版本告警”的诊断体验需要保留，至少不能退化为静默忽略。
4. 需要覆盖 Kafka 2.1、2.4、2.8、3.x、Confluent Cloud，以及如果仍支持则覆盖 KOP。
5. 若去掉 Sarama 的 `maxKafkaVersion=2.8.0` 上限，需要在 release note 中说明这只是
   客户端协议协商变化，不代表 TiCDC 扩大官方最低版本矩阵。

## ACL 和权限要求

官方文档列出的 Kafka 最小权限：

| Resource | Operation | 用途 |
| --- | --- | --- |
| Topic | Create | 自动创建 topic。 |
| Topic | Write | 写入变更事件。 |
| Topic | Describe | 启动和 topic metadata 查询。 |
| Cluster | DescribeConfigs | 读取 broker/topic 配置，如 `message.max.bytes`、`min.insync.replicas`。 |

如果 topic 已存在，文档说明可省略 Topic `Create`，但代码仍会读取 topic metadata 和
配置，因此 `Describe` / `DescribeConfigs` 的实际需求要按部署环境验证。

替换时的新增风险：

- franz-go 默认启用 idempotent write。Kafka 3.0 以前通常需要 Cluster 级
  `IDEMPOTENT_WRITE` 权限。当前 TiCDC 文档没有要求这个 ACL，Sarama 实现也没有开启
  idempotent producer。因此默认必须使用 `kgo.DisableIdempotentWrite()`，除非另开
  配置并同步更新文档、权限说明和回滚策略。
- 如果未来选择启用 idempotency，需要说明它改变的是客户端内部重试去重能力，不改变
  TiCDC 对外的 at-least-once 语义；TiCDC 仍可能在重启、故障恢复或上游重放后发送
  重复消息。
- Schema Registry、AWS Glue Schema Registry、claim-check 外部存储权限不是 Kafka
  ACL，但替换不能破坏其认证和错误诊断。

## Producer 行为映射

| TiCDC/Sarama 现状 | franz-go 替换要求 |
| --- | --- |
| 手动指定 partition，Sarama `NewManualPartitioner`。 | 使用 `kgo.RecordPartitioner(kgo.ManualPartitioner())`，所有 record 必须设置 `Topic` 和 `Partition`。 |
| `required-acks=-1/1/0` 映射到 Sarama RequiredAcks。 | 映射到 `kgo.AllISRAcks()`、`kgo.LeaderAck()`、`kgo.NoAck()`。 |
| 默认 `compression=none`。 | 显式 `kgo.ProducerBatchCompression(kgo.NoCompression())` 或等价配置。 |
| `Producer.Flush.*=0`，尽快 flush。 | 显式 `kgo.ProducerLinger(0)`，避免默认 10ms linger 改变延迟。 |
| `Producer.Retry.Max=o.MaxRetry`，默认 5，backoff 100ms。 | 显式 `kgo.RecordRetries(o.MaxRetry)`，并设置等价 backoff；不要继承 unlimited retries。 |
| `Net.MaxOpenRequests=1` 作为顺序保护。 | 禁用 idempotency 后保留 `MaxProduceRequestsInflightPerBroker(1)`；不要为吞吐随意调大。 |
| producer max message bytes 来自 `options.MaxMessageBytes`。 | 设置 `ProducerBatchMaxBytes`，并校准其“record batch pre-compression”语义和 TiCDC 大消息判定。 |
| async success 后执行 message callback。 | franz-go promise 只有 `err == nil` 才能执行 callback。promise 不得阻塞或调用可能阻塞的 Produce/Flush。 |
| async error 让 `AsyncRunCallback` 返回，sink 重建。 | 需要有中心错误通道/errgroup，把首个 produce error 转为 TiCDC error 并返回。 |
| sync DDL/checkpoint 用 `SendMessage` 或 `SendMessages`。 | 用 `ProduceSync` 实现；`SendMessages` 必须构造每个 partition 一条 record，并在任一失败时返回错误。 |

`required-acks=0` 必须保留，但需要明确：这本来就没有 broker durable ack 保证。替换后
不要在 callback 中假装获得了真实 broker ack；只能保持与现有“允许但风险自担”的语义。

## Admin 行为映射

当前 `ClusterAdminClient` 行为需要完整保留：

| 接口 | 当前用途 | franz-go/kadm 替换注意点 |
| --- | --- | --- |
| `GetAllBrokers` | metrics collector 获取 broker label。 | 可用 metadata/broker metadata。 |
| `GetBrokerConfig` | 读取 `message.max.bytes`、`min.insync.replicas`。 | 当前 Sarama 通过 controller broker 的 DescribeConfig 读取；kadm 实现要确认是否等价。 |
| `GetTopicConfig` | 读取 topic `max.message.bytes` 和 topic 级 `min.insync.replicas`。 | 需要兼容 KOP/不同 broker 返回 config entries 的形式。 |
| `GetTopicsMeta` | 判断 topic 是否存在、读取 partition 数。 | `UnknownTopicOrPartition` 在 ignore 模式下要跳过；其他错误不能吞。 |
| `GetTopicsPartitionsNum` | topic manager 定时刷新动态 topic partition 数。 | 返回值必须和当前 map 语义一致。 |
| `CreateTopic` | 自动创建 topic。 | `TopicAlreadyExists` 继续按成功处理；其他 policy/rf/auth 错误要保留。 |
| `Close` | 释放 admin client。 | 不能阻塞 sink 关闭路径。 |

`topicmanager/kafka_topic_manager.go` 的行为不要改：

- default topic 已存在且实际 partition 更多时，只使用配置中的 partition 子集。
- 用户指定 `partition-num` 大于实际 topic partition 数时返回错误，避免 dispatch 到
  不存在的 partition。
- 动态 topic 缓存刷新和 create-topic-then-wait-visible 逻辑继续由 topic manager 负责，
  不要转移到 producer 自动创建。

## Topic、partition 和顺序保证

官方文档和代码共同依赖以下顺序约束：

1. `index-value`、`columns`、`table/default` 这类 dispatcher 必须保证同一行的多次更新
   进入同一 Kafka partition。
2. `ts` dispatcher 可能把同一行不同版本发到不同 partition，消费者必须按 commitTs
   排序；客户端替换不能额外提供或破坏这个语义。
3. Open Protocol 的 DDL 和 Resolved Event 需要广播到所有 MQ partition，消费者用
   resolved ts 做多 partition 排序。
4. Canal-JSON DDL 发送到 partition 0；WATERMARK 只有在 `enable-tidb-extension=true`
   时输出。
5. Simple Protocol 的 WATERMARK 和 BOOTSTRAP 语义必须保持。客户端替换不能改变
   BOOTSTRAP 周期、发送分区和 DML/DDL 顺序关系。
6. DML callback 只能在 Kafka client 认为该 record 成功后执行，否则上游可能提前推进
   checkpoint，造成数据丢失。

franz-go 文档说明成功写入的 records 会按 partition 保持顺序；同时 `RecordRetries`
耗尽时会失败同 partition buffered records，避免跳过失败 record 后继续成功写入后续
record。替换实现必须利用这一点，而不是在 TiCDC 层自行绕过失败继续发送。

## 协议输出兼容性

客户端替换不应改 encoder，但实现和测试必须覆盖所有 Kafka 支持协议：

| 协议 | 必须保持的行为 |
| --- | --- |
| Open Protocol | Row Changed、DDL、Resolved 事件；batch key/value 格式；DDL/Resolved broadcast；`max-batch-size`。 |
| Canal-JSON | 一行一条 DML；DDL partition 0；`_tidb.commitTs`、WATERMARK、`content-compatible`。 |
| Avro | Confluent Avro wire format；每个 topic 只对应一张表；delete value 为 nil；Schema Registry / Glue 注册和错误处理。 |
| Debezium | 只输出 Row Changed Event，不输出 DDL/WATERMARK；schema 开关；TiDB 扩展字段。 |
| Simple | DDL、DML、WATERMARK、BOOTSTRAP；JSON/Avro codec；消费者 schema cache 依赖 BOOTSTRAP。 |

相关配置和限制也要覆盖：

- `delete-only-output-handle-key-columns`
- `only-output-updated-columns`
- `column-selectors`
- `enable-tidb-extension`
- `schema-registry` / AWS Glue schema registry
- row-level checksum：Kafka + Simple/Avro；Avro 需 TiDB extension 和 decimal/bigint string
  模式。

## 大消息处理和消息大小估算

这是替换中的高风险点。

当前 `pkg/sink/codec/common/message.go` 的 `Message.Length()` 使用：

```go
len(m.Key) + len(m.Value) + MaxRecordOverhead
```

其中 `MaxRecordOverhead` 的注释明确基于 Sarama 的 record batch 编码估算。这个值参与：

- producer `max-message-bytes` 前置检查；
- Open Protocol batch 拆分；
- large message compression 后是否进入 `handle-key-only` 或 `claim-check`；
- 报错 `Message was too large` 前的客户端侧保护。

franz-go 的 `ProducerBatchMaxBytes` 限制的是未压缩 record batch 上限。如果继续使用
Sarama overhead，可能出现两类问题：

- TiCDC 认为没超限，franz-go 或 broker 拒绝，造成 changefeed 报错。
- TiCDC 认为超限而提前 claim-check/handle-key-only，导致不必要的外部存储写入或消息降级。

替换要求：

1. 重新校准 Kafka record batch overhead。优先使用 franz-go 可复用的编码/估算能力；
   如果无法直接复用，使用保守上界并写明依据。
2. 覆盖 key/value 为空、key 大 value 小、value 大 key 小、header 为空、不同 compression
   的测试。
3. `large-message-handle-compression` 是 TiCDC 在消息级别先压缩再判断大小；producer
   `compression` 是 Kafka batch 压缩。两者不能混淆。
4. `claim-check` 和 `claim-check-raw-value` 的 Kafka marker 格式、外部存储路径、清理
   责任必须保持不变。
5. `max-message-bytes` 仍要和 broker/topic `message.max.bytes` / `max.message.bytes`
   通过 admin 自适应，并保留当前 `128` bytes safety margin 或给出替代依据。

## TLS、SASL 和认证

TLS 替换要求：

- 复用 `security.Credential.ToTLSConfig()`。
- 保留 TLS 1.2 minimum、证书文件校验、`insecure-skip-verify` 行为。
- franz-go 可用 `kgo.DialTLSConfig` 或自定义 dialer，具体选择要覆盖证书和系统 CA 两种路径。

SASL 替换要求：

| 机制 | franz-go 映射 | 注意点 |
| --- | --- | --- |
| PLAIN | `pkg/sasl/plain` | 用户名/密码为空时的现有错误行为要保持。 |
| SCRAM-SHA-256 | `pkg/sasl/scram` | 使用 SHA-256 mechanism；保持大小写和错误信息。 |
| SCRAM-SHA-512 | `pkg/sasl/scram` | 使用 SHA-512 mechanism。 |
| OAUTHBEARER | `pkg/sasl/oauth` 或自定义 provider | 复用现有 OAuth2 token provider 行为，包括 base64 secret、scope、grant type、audience。 |
| GSSAPI | `pkg/sasl/kerberos` | 需要把 `sasl-gssapi-*` 字段映射到 Kerberos client；user auth/keytab 两种都要集成测试。 |

`pkg/security/sasl.go` 当前直接引用 Sarama 常量。真正移除 Sarama 依赖时，需要先把这些
公共安全常量改成 TiCDC 自己的字符串常量，否则 Sarama 依赖会继续被保留。

## Metrics、日志和观测性

当前 TiCDC 暴露的 Kafka producer 指标名称和 label 是外部运维契约：

- `ticdc_sink_kafka_producer_in_flight_requests`
- `ticdc_sink_kafka_producer_outgoing_byte_rate`
- `ticdc_sink_kafka_producer_request_rate`
- `ticdc_sink_kafka_producer_request_latency`
- `ticdc_sink_kafka_producer_compression_ratio`
- `ticdc_sink_kafka_producer_records_per_request`
- `ticdc_sink_kafka_producer_response_rate`

这些指标目前来自 Sarama go-metrics registry。franz-go 可用 hook 或 `plugin/kprom`，但
`kprom` 的默认指标名不是 TiCDC 现有指标名。因此推荐实现 TiCDC 自己的 hook collector：

- `HookBrokerWrite` / `HookBrokerRead` / E2E hook：请求数、响应数、latency、broker label。
- `HookProduceBatchWritten`：records per request、compression ratio、outgoing bytes。
- 需要自行维护 in-flight gauge，或明确一个等价口径。
- collector cleanup 必须删除 `namespace/changefeed/broker/type` label，避免 changefeed
  删除后遗留时间序列。

日志相关替换点：

- `pkg/logger/log.go` 目前有 `WithInitSaramaLogger` 和 `sarama.Logger` hack。franz-go
  需要接入 `kgo.WithLogger` 或等价 logger adapter。
- `pkg/leakutil/leak_helper.go` 目前忽略 Sarama goroutine。替换后应删除或改成
  franz-go 相关 goroutine 的测试策略，不能永久掩盖泄漏。
- producer/admin 错误必须继续通过 `logutil.go` 附加 `MessageLogInfo`，否则
  `kafka_log_info` 类测试会退化。

## 正确性风险清单

P0 必须解决：

- DML callback 不得早于 Kafka 成功返回。
- 手动 partition 不得退化为客户端默认 partitioner。
- `required-acks`、`compression`、`max-retry`、`linger`、in-flight、idempotency 等默认值
  必须显式设置，不得使用 franz-go 默认值。
- DDL/checkpoint/resolved 的广播分区数必须来自 topic manager，而不是 producer metadata
  的临时结果。
- 大消息大小估算必须重新校准。
- `required-acks=-1` 时 `replication-factor >= min.insync.replicas` 的前置校验要保留。
- `UnknownTopicOrPartition`、`TopicAlreadyExists`、auth、policy、message too large 等错误
  要保持可诊断，不能被统一包装成无信息的 client error。

P1 需要验证：

- `required-acks=0` 下 promise/callback 语义和 Sarama 一致。
- broker idle connection 关闭后的 EOF/broken pipe 恢复行为。官方 FAQ 中曾提到 Sarama
  broken pipe；当前 master 已使用 bounded retry 和 Sarama fork ordering fix。franz-go
  是否改善该场景，需要专门压测。
- Confluent Cloud 中 `min.insync.replicas` 不可见时的容错告警仍然保留。
- KOP 对 DescribeConfig 返回项的兼容性。
- 动态 topic 表达式和多 topic checkpoint 广播。

## 可靠性和资源管理

需要明确设计：

- async producer close 是否等待 flush。当前 Sarama close 为避免阻塞，会异步关闭并接受
  可能重复数据。franz-go `Client.Close()` / cancel / `Flush` 的使用必须与这个策略一致，
  不能在 sink 关闭路径无限阻塞。
- producer promise 串行执行，不能在 promise 中执行阻塞操作。
- franz-go `MaxBufferedRecords` 默认 10000，`MaxBufferedBytes` 默认无限。TiCDC 上游已有
  unlimited channel，需要评估双重缓冲是否导致内存放大，并为高流量场景设置或暴露合理
  上限。
- admin、async producer、sync producer 是否共享同一个 `kgo.Client`。第一阶段建议和
  现状一致，每个组件独立 client，降低 close 生命周期复杂度；共享 client 可作为后续优化。
- request timeout、record delivery timeout、record retries 之间的关系必须有限制，避免
  transient error 下无限阻塞 changefeed。

## 性能对比和验收方法

不要在没有 TiCDC A/B 数据前宣称性能提升。建议基准如下：

环境变量：

- Go version、TiCDC commit、Kafka version、broker 数、topic partition 数、replication
  factor、`min.insync.replicas`。
- 是否启用 TLS/SASL。
- `required-acks`、producer compression、large-message compression、`max-message-bytes`。

workload：

- Canal-JSON 小行高吞吐。
- Open Protocol batch encode，覆盖 `max-batch-size`。
- Avro + Schema Registry / Glue。
- Simple JSON/Avro，覆盖 BOOTSTRAP。
- Debezium JSON/Avro。
- 大消息：普通超限、message-level compression、handle-key-only、claim-check、
  claim-check-raw-value。
- 多 topic 动态路由、单大表多 partition、高 partition 数。
- 低流量长 idle，验证连接保活和 broker idle close。

指标：

- rows/sec、bytes/sec。
- Kafka produce latency p50/p95/p99。
- CPU、heap、allocs/op、goroutine 数。
- producer request rate、response rate、in-flight、records/request、compression ratio。
- TiCDC changefeed checkpoint lag、resolved ts lag。
- 错误率、重试次数、重建次数。

验收标准：

- 功能正确性优先。性能不得显著退化；若有吞吐/延迟 trade-off，必须说明对应配置。
- metrics 名称和 label 兼容，或明确给出 dashboard/alert 迁移方案。
- A/B 报告要列出 Sarama 和 franz-go 的完整 producer 配置，避免比较默认值不同的结果。

## 测试计划

单元测试：

- options 合并和默认值：URI 覆盖 config、非法 client ID、非法 acks、非法 partition。
- franz-go option mapping：acks、compression、linger、retry、idempotency、manual partition、
  batch max bytes、buffer limits、timeouts。
- TLS/SASL：PLAIN、SCRAM、OAuth、GSSAPI user/keytab。
- admin wrapper：topic 存在/不存在、已存在 topic、invalid replication factor、policy
  violation、config not found、Confluent Cloud fallback。
- async producer：成功 callback、错误返回、context cancel、close、message log info。
- sync producer：DDL partition 0、Open Protocol broadcast、checkpoint broadcast、partial failure。
- message size：franz-go overhead 校准、large message option 触发点。
- metrics collector：hook 数据转换、label cleanup。

集成测试：

- 复用现有 Kafka integration cases，分别跑 Sarama 和 franz-go。
- 增加 ACL 测试：仅官方最小权限时 franz-go 默认配置必须能写入；若打开 idempotency，
  缺少 `IDEMPOTENT_WRITE` 要有明确错误。
- 增加 idle connection / broken pipe 场景。
- 增加 Kafka 2.1、2.4、2.8、3.x、Confluent Cloud 或兼容环境。
- 增加 TLS/SASL/GSSAPI/OAuth 覆盖。
- 增加 topic 已存在且 partition 数大于配置、partition 数小于配置、auto-create=false。

建议命令按改动范围选择：

- `make unit_test_pkg PKG=./pkg/sink/kafka/...`
- `make unit_test_pkg PKG=./downstreamadapter/sink/...`
- `make integration_test_kafka CASE=<case>`
- 最终切默认前跑完整 `make unit_test` 和 Kafka integration suite。

## 实施拆分建议

1. 增加 franz-go 依赖和内部 factory 实现，但默认仍走 Sarama。
2. 实现 `franzAdminClient`，让 `adjustOptions`、topic manager 测试先通过。
3. 实现 `franzSyncProducer`，先覆盖 DDL/checkpoint。
4. 实现 `franzAsyncProducer`，覆盖 callback、错误、close、backpressure。
5. 完成配置映射和 SASL/TLS/OAuth/GSSAPI。
6. 完成消息大小估算替换和大消息测试。
7. 完成 metrics collector 和 logger/leakutil 清理。
8. 增加灰度开关和 A/B 测试。
9. 满足功能、性能、可靠性验收后，再决定是否切默认并保留 Sarama 回滚窗口。
10. 最后移除 Sarama 依赖前，清理 `pkg/security/sasl.go`、logger、leak helper、mocks、
    go.mod/go.sum 和文档中的 Sarama 表述。

## 待决策项

- 默认是否禁用 idempotency：建议禁用，保持现有 ACL 和 Sarama 非幂等 producer 语义。
- `kafka-version` 是完全固定 franz-go MaxVersions，还是继续自动探测并只在用户显式指定时
  固定。
- message size overhead 使用精确编码计算还是保守上界。
- metrics 是自研 hook collector 还是迁移到 kprom 指标名。
- 是否需要公开 `kafka-client=franz|sarama` 灰度参数；如果公开，需要 API 兼容评审和文档。
- admin/sync/async 是否共享 `kgo.Client`。

## 结论

franz-go 替换 Sarama 的代码入口集中在 `pkg/sink/kafka`，但完整替换不是单纯把
`sarama.ProducerMessage` 换成 `kgo.Record`。必须显式复刻 TiCDC Kafka sink 的产品契约：
配置兼容、最小 ACL、协议输出、大消息处理、顺序保证、at-least-once、DDL/checkpoint
广播、metrics 和错误诊断。

最容易被遗漏、也最可能影响线上正确性的点是：

- franz-go 默认 idempotent write 带来的 ACL 和语义变化；
- franz-go 默认 snappy compression、10ms linger、unlimited record retries；
- Sarama record overhead 被用于 TiCDC 消息大小判断；
- 既有 Kafka producer Prometheus 指标来自 Sarama registry；
- GSSAPI、OAuth、Confluent Cloud、KOP 这类非本地单机 Kafka 场景。

这些点全部有测试和灰度证据后，才能考虑把 franz-go 设为默认实现。
