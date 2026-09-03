# Kafka Producer 大小限制

Last updated: 2026-09-03
Status: Produce request 和 producer buffer 已确定；消息与 record batch 的边界仍在讨论

## 背景

Kafka producer 处理四种不同对象：单条消息、record batch、Produce request 和内存 buffer。它们的限制作用在不同阶段。复用一个值会产生两个问题：Kafka Topic 配置可能意外放大进程内存，单条消息也可能通过 encoder 后被 producer 拒绝。

本文先说明每个参数的作用范围，再用具体场景说明参数之间的关系。本文使用 MiB，因为代码以二进制移位表示容量：`1 MiB = 1 << 20 bytes`。MB 表示十进制容量：`1 MB = 1,000,000 bytes`。

## 参数及作用范围

- `max-message-bytes`
  - 配置位置：Sink URI 或 SinkConfig。
  - 作用对象：encoder 生成的 `common.Message`。
  - 当前取值：默认 10 MiB。
  - 配置方式：用户可独立配置。

- Topic `max.message.bytes`
  - 配置位置：Kafka Topic。
  - 作用对象：broker 接受的 record batch。
  - 当前取值：TiCDC 读取 Kafka 配置。
  - 配置方式：由 Kafka 管理员独立配置。

- Broker `message.max.bytes`
  - 配置位置：Kafka Broker。
  - 作用对象：Topic 未覆盖时的 record batch。
  - 当前取值：TiCDC 读取 Kafka 配置。
  - 配置方式：由 Kafka 管理员独立配置。

- `ProducerBatchMaxBytes`
  - 配置位置：franz-go client。
  - 作用对象：未压缩的完整 record batch。
  - 当前取值：Kafka batch 上限与 100 MiB 的较小值。
  - 配置方式：TiCDC 内部计算，不对用户开放。

- `BrokerMaxWriteBytes`
  - 配置位置：franz-go client。
  - 作用对象：发给一个 broker 的完整 Produce request。
  - 当前取值：franz-go 默认 100 MiB。
  - 配置方式：不对用户开放。

- `MaxBufferedBytes`
  - 配置位置：franz-go client。
  - 作用对象：一个 client 中尚未完成的 record payload 总量。
  - 当前取值：固定 64 MiB。
  - 配置方式：不对用户开放。

- `MaxBufferedRecords`
  - 配置位置：franz-go client。
  - 作用对象：一个 client 中尚未完成的 record 数量。
  - 当前取值：franz-go 默认 10,000。
  - 配置方式：不对用户开放。

- `MaxProduceRequestsInflightPerBroker`
  - 配置位置：franz-go client。
  - 作用对象：每个 broker 同时在途的 Produce request 数量。
  - 当前取值：固定 1。
  - 配置方式：不对用户开放。

`max-message-bytes` 是当前唯一由 TiCDC 用户直接配置的大小参数。Produce request 和 producer buffer 使用固定值。Kafka record batch 的服务端上限由 Kafka 管理员控制。

## 数据经过哪些限制

```text
row events
    ↓ encoder
common.Message / Kafka record
    ↓ 按 topic-partition 组 batch
record batch
    ↓ 按目标 broker 组 request
Produce request
```

record 被 franz-go 接收后会计入 producer buffer，直到发送成功、发送失败或被取消。等待 metadata、等待组 batch、等待发送、等待 broker 响应和等待重试的 record 都占用 buffer。

## 已确定方案

### Produce request 固定为 100 MiB

TiCDC 不再设置 `BrokerMaxWriteBytes`，沿用 franz-go 的 100 MiB 默认值。该值对应 Kafka `socket.request.max.bytes` 的常见默认值。

Kafka Topic 的 `max.message.bytes` 不再放大 Produce request。franz-go 会把发往同一 broker 的多个 record batches 放入一个 request；达到 100 MiB 后创建下一个 request。

TiCDC 将 `ProducerBatchMaxBytes` 设置为 Kafka batch 上限与 100 MiB 的较小值。franz-go 随后按实际 client ID、Topic 名称和协议字段扣除 request envelope。

TiCDC 代码继续使用 `maxMessageBytes` 命名，与 Kafka `max.message.bytes` 和原 Sarama 配置保持一致。`ProducerBatchMaxBytes` 只作为 franz-go API 名称出现。

`MaxProduceRequestsInflightPerBroker(1)` 保持不变。一个 broker 同时最多有一个在途 Produce request。多个 broker 可以各有一个在途 request。

### Producer buffer 固定为 64 MiB

每个 franz-go client 设置：

```go
kgo.MaxBufferedBytes(64 << 20)
```

不增加 `max-buffered-bytes` Sink URI 参数。64 MiB 是每个 client 的 payload 上限，实际 RSS 还包括 record 对象、batch 编码、压缩空间、request 和网络 buffer。

`MaxBufferedBytes` 与默认的 `MaxBufferedRecords=10000` 同时生效。任一上限先达到，`Produce` 就会等待已有 record 完成或 context 被取消。

64 MiB 高于 TiCDC 默认的 10 MiB `max-message-bytes`，同时限制 10,000 条 record 可能占用的 payload 内存。该值会限制单条 record：当 key、value 和 headers 的总长度超过 64 MiB 时，franz-go 直接返回 `kerr.MessageTooLarge`。

`MaxProduceRequestsInflightPerBroker(1)` 无法替代 buffer 限制。它只限制在途 request 数量；等待 metadata、等待重试及发往其他 broker 的 records 仍可留在 buffer 中。

## 配置场景

### 场景一：普通消息，Kafka batch 上限约 1 MiB

配置示例：

```text
Sink URI 不设置 max-message-bytes，使用默认值 10485760（10 MiB）
Kafka Topic max.message.bytes = 1048576（1 MiB）
Produce request = 100 MiB
Producer buffer = 64 MiB
```

结果：

- encoder 的普通消息不能超过 Kafka record batch 实际可容纳的范围。
- 一个 Produce request 可以携带多个约 1 MiB 的 record batches。
- 如果平均 payload 为 1 MiB，buffer 大约容纳 64 条 record，随后开始反压。

这里的 `max-message-bytes`、Produce request 和 producer buffer 使用各自的值。Kafka record batch 上限仍会约束 encoder 的最终有效上限，具体 framing 预留量待确认。

### 场景二：普通消息按 1 MiB 聚合，允许 10 MiB 的单行大消息

配置示例：

```text
Sink URI: kafka://broker/topic?protocol=open-protocol&max-message-bytes=1048576
Kafka Topic max.message.bytes = 10485760
Produce request = 100 MiB
Producer buffer = 64 MiB
```

`max-message-bytes=1048576` 表示 1 MiB，Kafka 的 `10485760` 表示 10 MiB。

预期结果：

- Open Protocol 普通多行消息在约 1 MiB 时结束聚合。
- 单行大消息可以继续使用 Kafka record batch 提供的空间。
- franz-go 的 `ProducerBatchMaxBytes` 应接近 10 MiB，并为 batch framing 留出空间。
- 100 MiB request 和 64 MiB buffer 都能容纳这类消息。

这个场景体现 `max-message-bytes` 与 Kafka `max.message.bytes` 的独立用途：前者控制普通消息聚合，后者控制单条大消息和 record batch 的最终上限。

### 场景三：Kafka 允许 80 MiB batch，单条 payload 为 70 MiB

配置示例：

```text
Kafka Topic max.message.bytes = 83886080（80 MiB）
单条 record payload = 73400320（70 MiB）
Produce request = 100 MiB
Producer buffer = 64 MiB
```

结果：franz-go 会因为单条 payload 超过 `MaxBufferedBytes` 而拒绝该 record。Kafka Topic 和 Produce request 虽然具备足够空间，固定的 64 MiB buffer 仍形成了单条 record 上限。

当前方案因此不支持超过 64 MiB 的单条 payload。后续需要决定 encoder 是否提前按该限制报错，保证错误发生在 producer 之前。

### 场景四：Kafka batch 上限超过 100 MiB

配置示例：

```text
Kafka Topic max.message.bytes = 134217728（128 MiB）
Produce request = 100 MiB
ProducerBatchMaxBytes = 100 MiB
```

Kafka 允许管理员将 `max.message.bytes` 调到 100 MiB 以上。broker 的 `socket.request.max.bytes` 也必须相应增大，才能接收这种 batch。TiCDC 当前固定使用 100 MiB Produce request，因此 franz-go 的 batch 配置会限制为 100 MiB；franz-go 还会扣除 request envelope。

### 场景五：Kafka 暂时不可用

假设平均 payload 为 1 MiB：

- buffer 接受约 64 条 record 后达到 64 MiB。
- 后续 `Produce` 阻塞，反压逐步传回上游。
- Kafka 恢复后，已缓存 records 继续发送。
- context 取消时，阻塞中的调用退出。

假设平均 payload 为 4 KiB，10,000 条 record 约占 39 MiB。此时 `MaxBufferedRecords=10000` 先达到，record 数量限制触发反压。

### 场景六：Topic 分区分布在三个 broker

franz-go 最多可以同时存在三个在途 Produce requests，每个 broker 一个。所有等待中和在途的 records 共用同一个 64 MiB client buffer。

这个场景说明 `MaxProduceRequestsInflightPerBroker` 与 `MaxBufferedBytes` 可以分别设置。前者控制每个 broker 的请求并发，后者控制整个 client 的 payload 总量。

### 场景七：一个进程运行多个 Changefeed

Kafka sink 会为一个 Changefeed 创建 async 和 sync 两个 producer clients。每个 client 的上限都是 64 MiB，因此一个 Changefeed 的 producer payload 理论上限为 128 MiB。sync producer 主要发送低频 DDL，通常不会长期占满。

例如，一个进程运行 10 个 Changefeed，producer payload 的理论上限为：

```text
10 × 2 × 64 MiB = 1280 MiB
```

该数字仍未包含 encoder、event pipeline、batch、压缩和网络相关内存。

## 参数之间的关系

- `max-message-bytes` 与 Produce request 独立配置。修改 `max-message-bytes` 不会改变 100 MiB request。
- Kafka `max.message.bytes` 与 Produce request 独立配置。一个完整 batch 加 request envelope 必须小于 100 MiB。
- `ProducerBatchMaxBytes` 受 Kafka `max.message.bytes` 约束，不能超过 Kafka 接受的 batch。
- `ProducerBatchMaxBytes` 受 Produce request 约束，必须能装入一个 100 MiB request。
- 单条 record 受 `MaxBufferedBytes` 约束，payload 必须小于等于 64 MiB。
- `MaxBufferedBytes` 与 Produce request 独立配置，两者限制不同对象，数值无需相等。
- `MaxBufferedBytes` 与 `MaxBufferedRecords` 独立且同时生效，任一上限先达到就触发反压。
- `MaxBufferedBytes` 与每 broker 在途请求数独立。buffer 统计整个 client，在途请求数按 broker 统计。

## 待讨论项

- encoder 应为 franz-go record batch framing 预留多少空间。
- 单条 payload 超过固定 64 MiB 时，encoder 如何提前执行 claim-check、handle-key-only 或报错。
- Admin API 无法读取 Topic/Broker 配置时，应该使用哪个 fallback 上限。
- 动态 Topic 具有不同 `max.message.bytes` 时，是否需要 per-topic batch limit。

## 已落地的代码改动

`pkg/sink/kafka/franz_config.go`：

- 删除显式 `BrokerMaxWriteBytes`，使用 franz-go 默认的 100 MiB。
- 固定设置 `MaxBufferedBytes(64 << 20)`。
- 删除本地的 512 bytes 和 1 GiB batch 范围常量，不再静默提升 batch 上限。
- 将 Kafka batch 上限限制在 100 MiB 以内，再传给 franz-go。
- 保留 `MaxBufferedRecords=10000` 默认值。
- 保留 `MaxProduceRequestsInflightPerBroker(1)`。
- 代码注释说明 100 MiB request 和 64 MiB buffer 的作用范围及取值原因。

本次没有增加 Sink URI 参数。encoder 的消息边界将在上述待讨论项确认后实现。

## 验证

当前单元测试应确认：

- `BrokerMaxWriteBytes` 为 franz-go 默认的 100 MiB。
- `MaxBufferedBytes` 固定为 64 MiB。
- 修改 `max-message-bytes` 不会改变 request 或 buffer 上限。
- Kafka batch 上限超过 100 MiB 时，`ProducerBatchMaxBytes` 限制为 100 MiB。
- `MaxBufferedRecords` 仍为 10,000。

后续边界测试应覆盖 64 MiB 单条 payload、100 MiB request envelope、Kafka batch 上限超过 request 上限，以及 buffer 满后的阻塞和取消行为。
