# Kafka 消息大小检查口径说明

本文梳理 TiCDC Kafka sink 在 master 分支 Sarama 实现和当前 franz-go 实现里的
消息大小检查链路。重点是区分这些对象：

```text
TiCDC common.Message
  -> Kafka record
  -> Kafka record batch
  -> Kafka ProduceRequest
```

一次 `ProduceRequest` 按 topic 再按 partition 组织。现代 Kafka 版本下，每个
topic-partition 的 `Records` 字段承载一个 `RecordBatch`；一个 request 可以
包含多个 topic、多个 partition 的多个 record batches。就 Sarama 和 franz-go
这两个实现而言，一次 request 中同一个 topic-partition 只放一个 batch。

## Kafka 原始配置语义

Kafka broker/topic 层与消息大小相关的两个原始配置是
`message.max.bytes` 和 `max.message.bytes`。

- `message.max.bytes` 是 broker 级默认值。Apache Kafka 4.3 官方文档定义为
  Kafka 允许的最大 record batch size；如果启用了压缩，按压缩后的 batch 大小
  判断。它可以被 topic 级的 `max.message.bytes` 覆盖。默认值是 `1048588`。
- `max.message.bytes` 是 topic 级配置。它的语义同样是 Kafka 允许的最大
  record batch size；如果启用了压缩，按压缩后的 batch 大小判断。没有显式
  topic override 时，该 topic 使用 server default property，也就是
  broker 级的 `message.max.bytes`。默认值同样显示为 `1048588`。

所以，把它们口语化理解成“Kafka 单条消息大小上限”只在一个应用层 record 独占
一个 record batch 时近似成立。Kafka protocol 的精确对象是 record batch：
一个 batch 可以包含多条 records；broker/topic 限制校验的是这个 batch，而不是
单独某个应用层 key/value payload。

另一个容易混淆的 producer 侧参数是 `max.request.size`。Apache Kafka 4.3 官方
文档把它定义为 producer request 的最大大小，同时也是最大未压缩 record batch
size 的有效上限。server 侧仍然有自己的 record batch 上限，也就是上面的
`message.max.bytes` / `max.message.bytes`，并且这个 server 侧上限在启用压缩时
按压缩后大小判断。

相关官方文档：

- Apache Kafka 4.3 Broker Configs, `message.max.bytes`:
  https://kafka.apache.org/43/configuration/broker-configs/
- Apache Kafka 4.3 Topic Configs, `max.message.bytes`:
  https://kafka.apache.org/43/configuration/topic-configs/
- Apache Kafka 4.3 Producer Configs, `max.request.size`:
  https://kafka.apache.org/43/configuration/producer-configs/

## master 分支 Sarama 实现

master 分支的大小检查链路是：

```text
Kafka raw topic/broker limit
  -> TiCDC options.MaxMessageBytes
  -> encoder MaxMessageBytes
  -> open-protocol 单行/claim-check 检查
  -> open-protocol 多行 common.Message batching 检查
  -> Sarama ProducerMessage.ByteSize 检查
  -> Sarama produceSet batch / request rollover 检查
  -> broker 按 Kafka record batch limit 最终校验
```

### 1. TiCDC 从 Kafka raw config 折算 options.MaxMessageBytes

master 分支 `pkg/sink/kafka/options.go` 里有：

```go
maxMessageBytesOverhead = 128
```

topic 已存在时，`adjustOptions` 读取 topic 的 `max.message.bytes`，如果没有
topic override 则回退到 broker 的 `message.max.bytes`。随后使用：

```text
effective MaxMessageBytes = min(configured max-message-bytes, source max bytes - 128)
```

topic 不存在、需要 TiCDC 创建 topic 时，`adjustOptions` 读取 broker 的
`message.max.bytes`，也使用同样的 `source - 128` 折算。

因此，在 master 分支上，TiCDC 的 `options.MaxMessageBytes` 不是 Kafka
broker/topic raw value，而是一个扣掉 128 字节 safety margin 后的 TiCDC/Sarama
侧预算。

源码位置：

- `master:pkg/sink/kafka/options.go`：`maxMessageBytesOverhead = 128`
- `master:pkg/sink/kafka/options.go`：topic path 使用
  `topicMaxMessageBytes - maxMessageBytesOverhead`
- `master:pkg/sink/kafka/options.go`：broker path 使用
  `brokerMessageMaxBytes - maxMessageBytesOverhead`

### 2. Sarama producer 和 encoder 使用同一个 MaxMessageBytes

master 分支 `newSaramaConfig` 将：

```go
config.Producer.MaxMessageBytes = o.MaxMessageBytes
```

同时，`downstreamadapter/sink/helper/helper.go` 明确把 encoder 的
`MaxMessageBytes` 设置成 producer 的 `MaxMessageBytes`：

```go
encoderConfig = encoderConfig.WithMaxMessageBytes(maxMsgBytes)
```

这意味着 master 的意图是：encoder 不要生成超过 producer 预算的
`common.Message`。

源码位置：

- `master:pkg/sink/kafka/sarama_config.go`
- `master:downstreamadapter/sink/helper/helper.go`

### 3. open-protocol 单行编码与 claim-check 检查

open-protocol `batchEncoder.AppendRowChangedEvent` 会先把单行 RowEvent 编码成
key/value，并得到一个 `length`：

```go
key, value, length, err := encodeRowChangedEvent(...)
if length > d.config.MaxMessageBytes {
    ...
}
```

如果单行原始消息超过 `MaxMessageBytes`：

- large message handle disabled：直接返回 `ErrMessageTooLarge`。
- claim-check enabled：先把原始 key/value 写入外部存储，再重新编码一条
  claim-check location message。
- claim-check location message 仍超过 `MaxMessageBytes`：返回
  `ErrMessageTooLarge`。

这个检查发生在 Kafka producer 之前。

源码位置：

- `master:pkg/sink/codec/open/encoder.go`

### 4. open-protocol 多行 common.Message batching 检查

claim-check location message 单条通常很小，但 open-protocol 会继续把多条
row events 合并进一个 TiCDC `common.Message`。

`pushMessage` 里新加入一行时，计算：

```go
length := len(key) + len(value) + 16
```

然后用当前 TiCDC message 的 `Length()` 判断是否还能继续追加：

```go
latestMessage.Length() + length > d.config.MaxMessageBytes
```

`common.Message.Length()` 在 master 分支是：

```go
len(m.Key) + len(m.Value) + MaxRecordOverhead
```

其中：

```text
MaxRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1 = 36
```

也就是说，encoder 的 batching 口径和 Sarama
`ProducerMessage.ByteSize(2)` 的无 headers 估算口径一致：

```text
len(key) + len(value) + 36
```

源码位置：

- `master:pkg/sink/codec/open/encoder.go`
- `master:pkg/sink/codec/common/message.go`

### 5. Sarama AsyncSend 本身不做大小检查

TiCDC Sarama producer 的 `AsyncSend` 只是把 `common.Message` 转成
`sarama.ProducerMessage` 并写入 Sarama input channel：

```go
msg := &sarama.ProducerMessage{
    Topic: topic,
    Partition: partition,
    Key: sarama.StringEncoder(message.Key),
    Value: sarama.ByteEncoder(message.Value),
}
p.producer.Input() <- msg
```

TiCDC 这一层没有额外 size check。

源码位置：

- `master:pkg/sink/kafka/sarama_async_producer.go`

### 6. Sarama 单条 ProducerMessage 硬检查

Sarama 内部第一层硬检查在 `asyncProducer.dispatcher`：

```go
size := msg.ByteSize(version)
if size > p.conf.Producer.MaxMessageBytes {
    reject
}
```

Kafka `>= 0.11` 时，`version = 2`，`ProducerMessage.ByteSize(2)` 为：

```text
len(key) + len(value) + maximumRecordOverhead + headers estimate
```

没有 headers 时：

```text
len(key) + len(value) + 36
```

这一层是本地拒绝条件。

Sarama 源码位置：

- `/Users/edison/go/sarama/async_producer.go:365`
- `/Users/edison/go/sarama/async_producer.go:626`

### 7. Sarama produceSet rollover 检查

消息进入 broker producer 后，Sarama 调用 `produceSet.wouldOverflow(msg)`。
这里有三类检查：

```text
1. 整个 produce request 估算：
   ps.bufferBytes + msg.ByteSize(version) >= MaxRequestSize - 10KiB

2. 已存在 topic-partition batch 估算：
   set.bufferBytes + msg.ByteSize(version) >= Producer.MaxMessageBytes

3. Flush.MaxMessages 条数限制
```

这几类检查触发后，Sarama 会 `waitForSpace` / flush / rollover，而不是把当前
消息作为 `MESSAGE_TOO_LARGE` 直接失败。

关键细节：第二类 partition batch 检查只有在当前 topic-partition 的
`partitionSet` 已经存在时才执行。第一条 record 进入一个空的 partition batch
时，partition set 尚不存在，所以这个检查会跳过。

随后 `produceSet.add` 创建 batch：

```text
recordBatchOverhead = 49
```

并把本条消息加入 batch。此时 `partitionSet.bufferBytes` 会变成：

```text
49 + len(key) + len(value) + 36
```

即使这个值已经超过 `Producer.MaxMessageBytes`，第一条 record 也已经被接受。

Sarama 源码位置：

- `/Users/edison/go/sarama/produce_set.go:39`
- `/Users/edison/go/sarama/produce_set.go:303`
- `/Users/edison/go/sarama/async_producer.go:1188`
- `/Users/edison/go/sarama/async_producer.go:1328`

### 8. Sarama 例子：759 字节 claim-check batch

假设：

```text
Producer.MaxMessageBytes = 800
len(key) + len(value) = 759
headers = none
```

Sarama 单条硬检查：

```text
759 + 36 = 795 <= 800
```

所以能通过。

如果这是该 topic-partition 当前 batch 的第一条 record，partition batch
rollover 检查会跳过。加入后 Sarama 内部估算为：

```text
49 + 795 = 844
```

但这不是第一条 record 的拒绝条件。

如果具体 key/value 拆分为 `527 + 232`，实际 headerless record 编码是：

```text
record body:
  attributes             1
  timestamp delta         1
  offset delta            1
  key length varint       2
  key bytes             527
  value length varint     2
  value bytes           232
  headers count           1
  total                 767

record length varint      2
encoded record total    769
```

不启用 producer compression 时，Sarama 实际 `RecordBatch.encode` 大小约为：

```text
61 + 769 = 830
```

Sarama 本地仍然不会因为这个完整 encoded record batch 大于 800 而拒绝这条
空 batch 的第一条 record。

## 修正后的 franz-go 实现

修正后的当前分支大小检查链路是：

```text
Kafka raw topic/broker limit
  -> TiCDC options.ProducerBatchMaxBytes
  -> franz-go ProducerBatchMaxBytes
  -> broker 按 Kafka record batch limit 最终校验

用户配置 max-message-bytes / Kafka raw topic/broker limit
  -> TiCDC options.MaxMessageBytes
  -> encoder MaxMessageBytes payload 检查
  -> open-protocol 单行/claim-check 检查
  -> open-protocol 多行 common.Message batching 检查
  -> franz-go Produce(ctx, kgo.Record)
  -> franz-go buffered bytes/backpressure 检查
  -> franz-go recBatch.tryBuffer record batch 大小检查
  -> franz-go produceRequest request 总大小检查
```

这里刻意把两个值分开：

- `options.MaxMessageBytes`：TiCDC encoder 的 payload 预算，用于 open protocol
  自身分包以及 large message handle。
- `options.ProducerBatchMaxBytes`：franz-go producer 的 Kafka record batch 预算，
  直接来自 topic `max.message.bytes` 或 broker `message.max.bytes`。

### 1. 删除 maxMessageBytesOverhead，但不再混淆 producer batch 预算

当前分支删除 `maxMessageBytesOverhead`。`adjustOptions` 现在直接使用 Kafka raw
source limit 约束 encoder：

```text
effective MaxMessageBytes = min(configured max-message-bytes, kafka raw source max bytes)
```

topic 已存在时，source 是 topic `max.message.bytes`；topic 不存在时，source 是
broker `message.max.bytes`。

同时，`adjustOptions` 记录 Kafka raw source limit：

```text
ProducerBatchMaxBytes = kafka raw source max bytes
```

例如 integration test 里的：

```text
max-message-bytes=800
```

在 topic/broker raw limit 是 Kafka 默认值 `1048588` 时：

```text
options.MaxMessageBytes       = 800
options.ProducerBatchMaxBytes = 1048588
```

这正是 claim-check 场景需要的语义：`800` 只控制 TiCDC 何时把原始大消息转成
claim-check location message，不应该把 franz-go 的 record batch 上限也压成
`800`。

源码位置：

- `pkg/sink/kafka/options.go`
- `pkg/sink/kafka/options_test.go`

### 2. encoder 侧检查改为 payload 口径

encoder 仍然必须使用 `MaxMessageBytes` 做检查，原因是它承担两个 Kafka producer
之前的应用层功能：

- large message handle disabled 时，如果单行编码后的 payload 超过
  `MaxMessageBytes`，直接返回 `ErrMessageTooLarge`，不会等 producer/broker 拒绝。
- claim-check enabled 时，同一个检查点触发 claim-check：原始 key/value 写入外部
  存储，再生成一条 claim-check location message。
- open-protocol 会把多条 row events 合并成一个 Kafka record 的 key/value
  payload，因此 `pushMessage` 需要用同一预算决定是否开一个新的 TiCDC
  `common.Message`。

修正点是：`common.Message.Length()` 不再包含 Sarama 的 `MaxRecordOverhead = 36`，
而是返回：

```text
len(key) + len(value)
```

open-protocol 单行检查也不再额外加 `common.MaxRecordOverhead`。对 open protocol
来说，一条 row 在最终 Kafka record payload 里的真实应用层长度是：

```text
len(row key) + len(compressed row value) + 8(version) + 8(key length) + 8(value length)
```

后续追加一条 row 到同一个 `common.Message` 时增加：

```text
len(row key) + len(compressed row value) + 8(key length) + 8(value length)
```

因此，encoder 现在检查的是 TiCDC 实际生成的 key/value payload 大小，而不是
Sarama `ProducerMessage.ByteSize` 估算大小。

源码位置：

- `pkg/sink/codec/open/encoder.go`
- `pkg/sink/codec/open/codec.go`
- `pkg/sink/codec/common/message.go`

### 3. TiCDC franz-go AsyncSend 不做大小检查

当前分支 `kafkaAsyncProducer.AsyncSend` 把 `common.Message` 直接转成
`kgo.Record`：

```go
record := &kgo.Record{
    Topic: topic,
    Partition: partition,
    Key: message.Key,
    Value: message.Value,
}
p.client.Produce(ctx, record, promise)
```

TiCDC 这一层没有额外 size check。

源码位置：

- `pkg/sink/kafka/async_producer.go`

### 4. franz-go ProducerBatchMaxBytes 使用 Kafka raw source limit

修正前，当前分支在构造 franz-go producer options 时设置：

```go
kgo.ProducerBatchMaxBytes(int32(o.MaxMessageBytes))
```

franz-go 文档注释说明 `ProducerBatchMaxBytes` 限制的是 record batch 大小，
并且它 mirrors Kafka `max.message.bytes`。注释还明确说：record batch 是
topic-partition 维度，`ProduceRequest` 可以包含多个 topics 的多个 record
batches。

这一步是当前分支和 master/Sarama 行为不同的核心：master 的
`o.MaxMessageBytes` 进入 Sarama 后首先用于 `ProducerMessage.ByteSize` 单条估算；
当前分支的同一个值进入 franz-go 后用于 `ProducerBatchMaxBytes` record batch
上限。

修正后，franz-go producer 使用：

```go
kgo.ProducerBatchMaxBytes(int32(o.ProducerBatchMaxBytes))
```

也就是 topic/broker 的原始 record-batch 上限。`o.MaxMessageBytes` 继续传给
encoder，不再直接作为 franz-go record-batch 上限。

源码位置：

- `pkg/sink/kafka/client_options.go`
- `pkg/sink/kafka/kafka_factory.go`
- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/config.go:1255`

### 5. franz-go buffered bytes/backpressure 检查

franz-go `Produce` 开始时先计算：

```text
userSize = len(key) + len(value) + sum(header key/value)
```

如果配置了 `MaxBufferedBytes`：

```go
if maxBufferedBytes > 0 && userSize > maxBufferedBytes {
    MESSAGE_TOO_LARGE
}
```

随后还会检查客户端当前 buffered bytes 是否超过 `MaxBufferedBytes`。不过 TiCDC
当前没有设置 `kgo.MaxBufferedBytes`，所以这个检查通常不是本问题的来源。

franz-go 源码位置：

- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/record_and_fetch.go:157`
- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/producer.go:599`

### 6. franz-go recBatch.tryBuffer record batch 检查

franz-go 把 record 放入 topic-partition 的 `recBatch` 时，会先尝试加入当前最后
一个 batch；放不进去就创建新 batch 再试。

关键检查在 `recBatch.tryBuffer`：

```go
nums := b.calculateRecordNumbers(pr.Record)
batchWireLength, _, _ := b.wireLengthForProduceVersion(produceVersion)
newBatchLength := batchWireLength + nums.wireLength()

if b.frozen || newBatchLength > maxBatchBytes {
    return false, false
}
```

如果一个空的新 batch 也放不下这条 record，franz-go 会本地失败：

```go
MESSAGE_TOO_LARGE (uncompressed_bytes=...)
```

这是当前 claim-check case 的直接失败点。

franz-go 源码位置：

- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/sink.go:1640`
- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/sink.go:1958`
- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/sink.go:2320`
- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/sink.go:2413`

### 7. franz-go produceRequest 总大小检查

batch 准备写入 produce request 时，franz-go 还有 request 层检查：

```go
if p.wireLength + batchWireLength > p.wireLengthLimit {
    return false
}
```

这里的 `wireLengthLimit` 来源于 `maxBrokerWriteBytes`，默认对应 Kafka
`socket.request.max.bytes` 级别，默认约 100 MiB。它不是本次 800 字节失败的来源。

franz-go 源码位置：

- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/sink.go:2102`
- `/Users/edison/go/pkg/mod/github.com/twmb/franz-go@v1.21.4/pkg/kgo/sink.go:2250`

### 8. franz-go 例子：759 字节 claim-check payload

仍用同一个例子，sink URI 显式配置：

```text
max-message-bytes = 800
len(key) + len(value) = 759
key = 527
value = 232
headers = none
```

如果 Kafka topic/broker raw limit 是默认值：

```text
Kafka max.message.bytes / message.max.bytes = 1048588
```

修正后：

```text
options.MaxMessageBytes       = 800
options.ProducerBatchMaxBytes = 1048588
```

franz-go 对单条 record 的编码大小：

```text
record body:
  attributes             1
  timestamp delta         1
  offset delta            1
  key length varint       2
  key bytes             527
  value length varint     2
  value bytes           232
  headers count           1
  total                 767

record length varint      2
encoded record total    769
```

franz-go 新 batch 固定 wire length 是：

```text
record batch overhead = 65
```

所以空 batch 加入这条 record 后：

```text
65 + 769 = 834
```

修正前，因为代码设置：

```text
ProducerBatchMaxBytes = options.MaxMessageBytes = 800
```

于是：

```text
834 > 800
```

franz-go 在本地返回 `MESSAGE_TOO_LARGE (uncompressed_bytes=759)`。这里的
`uncompressed_bytes=759` 是用户 key/value payload 大小，不是完整 record batch
wire size。

修正后：

```text
834 <= ProducerBatchMaxBytes(1048588)
```

这条 claim-check location message 可以进入 producer，并交给 broker 按 Kafka
record batch 语义最终校验。

如果 Kafka topic 本身真的配置为：

```text
max.message.bytes = 800
```

那么完整 record batch wire size `834 > 800`，franz-go 本地拒绝是合理的。那表示
Kafka topic record-batch 上限确实放不下这条 claim-check location record，而不是
TiCDC 的 claim-check 阈值被误用为 producer batch 阈值。

## 当前结论

1. Kafka broker/topic 的 `message.max.bytes` / `max.message.bytes` 语义是
   record batch 上限，不是 TiCDC `common.Message` 上限。
2. master/Sarama 链路里，TiCDC encoder 和 Sarama 单条硬检查都主要使用
   `len(key) + len(value) + 36` 这一估算口径；Sarama 不会在空 batch 第一条
   record 时用完整 record batch wire size 拒绝消息。
3. franz-go 链路必须区分 TiCDC encoder payload 预算和 Kafka record batch 预算。
   `max-message-bytes=800` 应触发 open-protocol claim-check；Kafka producer 的
   `ProducerBatchMaxBytes` 应来自 topic/broker raw limit。
4. `common.Message.Length()` 不能继续携带 Sarama 的 36 字节 record overhead。
   修正后它表示 TiCDC 生成的 key/value payload 大小；Kafka record encoding 和
   record batch header 由 franz-go 在 producer 层按真实协议口径检查。
