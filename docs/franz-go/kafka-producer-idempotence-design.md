# Kafka Producer Idempotence 设计

Last updated: 2026-09-03
Status: 待讨论
Scope: franz-go producer 的幂等写入
Related documents:

- [Milestone 1 TODO List](./milestone-1-todo-list.md)

## Background

TiCDC producer 会重试可恢复的 Kafka 写入错误。下面的故障会产生重复消息：

1. Producer 向 Broker 发送一个 record batch。
2. Broker 已经把 record batch 写入日志。
3. Broker 响应在网络中丢失，或者 Producer 等待响应超时。
4. Producer 无法判断 Broker 是否已经完成写入，因此重新发送相同数据。
5. Broker 把重试请求作为新数据再次写入。

当前 Sarama producer 没有启用幂等写入。franz-go 路径也显式配置了
[`DisableIdempotentWrite`](../../pkg/sink/kafka/franz_config.go)，因此两条路径都存在上述风险。

Kafka 幂等 producer 为每个 producer 分配 producer ID 和 epoch，并为每个 Topic partition
维护 sequence number。Broker 可以根据这些信息识别同一个 producer 重发的 record batch，
避免网络错误触发的 client 内部重试写入重复数据。Kafka 从 0.11 开始提供该能力，协议原理见
[Kafka Design](https://kafka.apache.org/41/design/design/)。

支持幂等写入可以减少 TiCDC 正常运行期间由 franz-go 内部重试产生的重复消息。TiCDC 的
对外投递语义继续保持 at-least-once。Producer 重建、TiCDC 重启以及从 checkpoint 重放的
消息会使用新的 sequence number，Kafka 无法把这些消息识别为同一次发送。

## Proposed Behavior

- 新增 `enable-idempotence` Kafka Sink 配置，默认值为 `false`。
- `enable-idempotence=false` 保持当前 producer 行为。
- `enable-idempotence=true` 只支持 franz-go，并要求 `required-acks=-1`。
- 用户同时配置 `enable-idempotence=true` 和 `required-acks=0` 或 `1` 时，TiCDC 在创建
  Changefeed 时返回配置错误。Kafka 要求幂等 producer 使用 `acks=all`，详见
  [Kafka Producer Configs](https://kafka.apache.org/41/configuration/producer-configs/)。
- TiCDC 不会在初始化失败后自动关闭幂等写入。权限、Broker 版本或消息格式不满足要求时，
  Changefeed 返回明确错误。
- TiCDC 启动日志记录最终是否启用幂等写入。

显式配置可以避免升级后自动增加 Kafka 权限要求。完成兼容性和故障测试后，可以单独讨论
是否修改默认值。

## Producer Configuration

启用幂等写入时：

- 不配置 `DisableIdempotentWrite()`。
- 不配置 `MaxProduceRequestsInflightPerBroker(1)`。franz-go 在幂等模式下自行选择在途请求
  数量；支持相应 Produce API 的 Broker 最多允许 5 个在途请求，并使用 sequence number
  保证同一 partition 的消息顺序。
- 保留 `RecordRetries(max-retry)` 和现有退避配置。
- 配置 `AllowIdempotentProduceCancellation()`，使 `max-retry` 耗尽和调用 context 取消仍能
  结束发送并释放 buffer。

关闭幂等写入时：

- 配置 `DisableIdempotentWrite()`。
- 保留 `MaxProduceRequestsInflightPerBroker(1)`，避免非幂等重试导致同一 partition 乱序。

`AllowIdempotentProduceCancellation()` 保留 franz-go 正常内部重试的 Broker 去重能力。
发送结果仍不确定时，如果 TiCDC 在收到最终错误后重新发送相同消息，Kafka 仍可能写入重复
数据。该行为符合 TiCDC 现有的 at-least-once 语义。franz-go 对取消行为的说明见
[`AllowIdempotentProduceCancellation`](https://github.com/twmb/franz-go/blob/v1.21.6/pkg/kgo/config.go#L1191-L1219)。

## Producer ID Initialization

一个 Changefeed 创建两个 franz-go producer client：

- async producer 发送 DML。
- sync producer 发送 DDL 和 checkpoint。

两个 client 分别持有 producer ID 和 sequence number。创建每个 producer client 后，TiCDC
调用 `client.ProducerID(ctx)`，在发送业务消息之前执行 `InitProducerID`：

- 返回错误时，producer 创建失败。
- producer ID 小于 0 时，producer 创建失败。
- producer ID 有效时，producer 创建成功。

franz-go 遇到不支持 `InitProducerID` 的旧 Broker 时，可能返回 `producer ID = -1` 和空错误，
然后以非幂等方式继续发送。TiCDC 必须同时检查错误和 producer ID，避免静默降低用户要求的
投递保证。

初始化需要使用有界 context。达到初始化 deadline 后，TiCDC 关闭该 client 并返回创建失败，
避免 Changefeed 创建过程无限等待 Kafka。

## Kafka Permissions

启用幂等写入会增加 Kafka 权限要求：

- Kafka 2.8 之前通常要求 producer principal 具有 Cluster 级 `IDEMPOTENT_WRITE`，同时具有
  目标 Topic 的 `WRITE` 权限。
- Kafka 2.8 及以上把 `InitProducerID` 权限放宽为对任意 Topic 具有 `WRITE` 权限。目标 Topic
  仍需要各自的 `WRITE` 权限。
- 自定义 Authorizer 需要正确实现 Kafka 2.8 引入的权限检查接口，否则升级后的 Broker 仍可能
  拒绝 `InitProducerID`。

权限变化和兼容场景见
[KIP-679](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/165221843/KIP-679%2BProducer%2Bwill%2Benable%2Bthe%2Bstrongest%2Bdelivery%2Bguarantee%2Bby%2Bdefault)。

TiCDC 不通过 Admin API 推测权限。`ProducerID(ctx)` 发出的真实 `InitProducerID` 请求作为权限
检查依据。`CLUSTER_AUTHORIZATION_FAILED` 等错误应保留 Kafka 错误原因，方便用户补充 ACL。

## Guarantee Boundaries

幂等写入只对同一个 producer ID、epoch 和 Topic partition 上的 client 内部重试去重。下面的
情况仍可能产生重复消息：

- TiCDC 在 Broker 写入成功但 callback 执行前退出，重启后从 checkpoint 重放消息。
- franz-go client 被关闭并重新创建，新 client 获得新的 producer ID。
- 发送结果不确定并达到取消或重试上限后，TiCDC 重新发送消息。
- sync producer 向多个 partition 发送 DDL 或 checkpoint，其中部分 partition 成功后整体操作
  返回失败。

幂等写入不提供跨 partition 原子性，也不为两个 producer client 提供共同的去重范围。实现不应
把该功能描述为 TiCDC 到 Kafka 的 exactly-once 投递。

## Risks

- 旧集群权限不足：现有 Topic 写入权限可能不足以执行 `InitProducerID`。
- Broker 或消息格式过旧：Kafka 0.11 之前的 Broker，以及使用 v2 之前消息格式的 Topic，不能
  接受幂等 record batch。
- 请求并发变化：幂等模式下 franz-go 可能把每个 Broker 的在途 Produce request 增加到 5，
  从而改变吞吐和故障期间的在途数据量。
- 取消后的重复：`AllowIdempotentProduceCancellation()` 保证有界退出，但取消后重新发送不能
  使用原来的 sequence number 去重。
- 部分成功：幂等写入不解决跨 partition 发送的部分成功。
- 首次发送延迟：两个 producer client 都需要执行一次 `InitProducerID`。

## Verification

- 覆盖 `enable-idempotence` 与 `required-acks=-1/1/0` 的配置组合。
- 验证关闭幂等写入时保留 `MaxProduceRequestsInflightPerBroker(1)`。
- 验证启用幂等写入时 sync 和 async producer 都取得有效 producer ID。
- 模拟 Broker 已写入但首次响应丢失，确认 franz-go 内部重试后 Kafka 中只有一份记录。
- 验证 `max-retry` 耗尽、调用 context 取消和 client 关闭都能结束 callback 并释放 buffer。
- 验证缺少 `IDEMPOTENT_WRITE` 权限时，Changefeed 初始化返回包含 Kafka 原因的错误。
- 覆盖 Kafka 0.11、Kafka 2.7、Kafka 2.8 及更高版本的版本和权限边界。
- 覆盖旧 Topic 消息格式被 Broker 拒绝的错误路径。
- 验证多个在途 batch 发生重试后，同一 partition 的消息顺序不变。
- 验证多 partition 发送部分成功时，TiCDC 仍按 at-least-once 语义处理。

## Open Questions

- `enable-idempotence` 是否只加入 Sink URI，还是同时加入 `SinkConfig`。
- 初始化 producer ID 使用哪个 deadline。
- 是否接受 `AllowIdempotentProduceCancellation()` 带来的取消后重复风险，或者选择严格幂等并
  接受单次发送可能超过 `max-retry` 和调用 context deadline。
- 完成兼容性测试后，是否把 `enable-idempotence` 的默认值改为 `true`。
