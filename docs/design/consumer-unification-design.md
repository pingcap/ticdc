# TiCDC Consumer 统一实现设计

Last updated: 2026-10-08
Status: Implemented; full CI pending
Scope: 以单一 `cdc_consumer` 程序替代 Kafka、Pulsar 和 Storage 三个独立 consumer
Audience: TiCDC consumer、codec、sink、构建、测试与运维维护者

## Background

TiCDC 当前使用三个独立程序消费 Kafka、Pulsar 和 Storage。它们重复实现解码、写入、进度确认和进程生命周期，行为也在逐渐分化。

本设计使用一个 `cdc_consumer` 取代三个现有程序。consumer 实现放在 `cmd/consumer`，三个来源共享同一套处理流程，只保留各自必要的读取与确认代码。

`cdc_consumer` 服务于 TiCDC CI。设计优先保证行为一致、资源有界和失败后可恢复，不建设通用消费框架。

## Goals

- 提供一个 `cdc_consumer` 二进制，根据 upstream URI 使用 Kafka、Pulsar 或 Storage reader。
- 三个旧程序由统一实现替代；旧构建入口作为 CI 临时别名保留。
- 共享 DML 写入、flush、DDL 栅栏、完成记录、资源预算和生命周期；各 reader 保留自己的解码、顺序和确认规则。
- 支持现有协议的 DDL 与 watermark 分区布局，包括单 partition DDL、全 partition DDL、非分区 Pulsar control message 和 partitioned Pulsar control message。
- 来源位置对应的全部下游效果完成后，才推进 Kafka offset 或 Pulsar MessageID。
- 数据达到完整边界后及时形成 batch，限制无必要的内存等待。
- 所有队列、在途请求和内存占用都有明确上限。
- 保持现有消息格式、Storage 文件格式和 `sink.Sink` 接口不变。

## Non-goals

- 进程崩溃后的端到端 exactly-once。
- 一个进程同时消费多个独立 upstream。
- 重新定义 CDC 顺序语义。
- 提供通用 consumer SDK。
- 使用本地磁盘保存 DML。

## Confirmed external constraints

以下规则是本设计的输入，`cdc_consumer` 不负责解释或改变它们：

- checkpoint 是包含等号的完整边界。
- 来源配置决定有效的顺序域和 DDL 影响范围。
- control message 的 topic 与 partition 布局由 reader 类型和 codec 决定。
- Storage metadata、schema、index 和 DML 文件共同决定可见数据及顺序。
- 各 codec 所需的 extension、watermark 和 schema 信息必须完整。

reader 在启动时校验这些前置条件。条件不成立时直接返回配置错误或来源错误，公共处理流程只接收已经满足约束的输入。

## Design

### Program structure

```text
cmd/consumer/
├── main.go                    # 进程入口、参数与配置
├── reader.go                  # reader 接口、读取侧缓冲与内部完成状态
├── consumer.go                # 公共消费循环、sink 生命周期与进度确认
├── kafka.go                   # Kafka 读取、解码、顺序与 offset 提交
├── pulsar.go                  # Pulsar 读取、解码、顺序与 ACK
├── storage.go                 # Storage 文件读取、解码、排序与完成位置
└── writer.go                  # 共享 batch、flush、重放处理与 DDL 栅栏
```

所有文件属于同一个 `main` package，consumer 代码不进入 `pkg`。上游通过一个公共接口接入：

```go
type reader interface {
    Read(ctx context.Context) (*readResult, error)
    Confirm(ctx context.Context) error
    BufferedBytes() int64
    Close() error
}
```

`kafkaReader`、`pulsarReader` 和 `storageReader` 分别拥有 SDK、decoder、上游位置和完成状态。`Read` 交付已经满足来源顺序条件的 DML、规范化 DDL、进度及完成回调；`Confirm(ctx)` 检查 reader 内部状态，推进连续完成的消费位置。`Read` 与 `Confirm` 可以并发，位置队列受 reader 内部锁保护；`Close` 在两者停止后执行。

`consumer` 共享 sink 初始化与运行、消费循环、定时 flush、进度确认和退出收尾。`writer` 是公共下游写入实现，直接持有同一个 `sink.Sink`。它通过完成回调通知 reader，不感知上游位置或完成计数。

context 通过函数参数传递，consumer 的结构体不保存 context。

### Processing flow

```text
Kafka / Pulsar / Storage reader
               │
               ▼
        Read and decode
               │
               ▼
     ready batch + DDL fence
               │
               ▼
          bounded writer
               │
               ▼
              sink
               │
               ▼
    complete reader position
               │
               ▼
        reader confirmation
```

公共 consumer 用一个读取 goroutine 调用 `Read`，通过容量为 1 的队列交付结果。消费 goroutine 独立处理结果、完成回调和 flush 定时器，上游暂时没有数据也不会阻止 partial batch 提交。所有显式 goroutine 都受 context 和 wait group 管理。

reader 按自己的顺序单元串行解码；只有满足顺序条件的事件才进入 writer。DDL 到达 writer 后先提交此前 ready DML，等待受影响的在途 batch，再调用 `FlushDMLBeforeBlock` 和 `WriteBlockEvent`。

多个 DML batch 可以有界并发写入。每个来源只保留自己的位置和连续完成状态，使后续 batch 先完成时也不会越过前面未完成的输入。

### Completion and confirmation

reader 为每条消息或文件维护内部未完成计数和资源计费。创建时保留一个解码引用；每个 DML、DDL 或需等待写入的 watermark 增加一个引用。解码结束后释放解码引用，下游成功后的完成回调释放写入引用。`Confirm(ctx)` 只推进引用归零的连续位置，因此一个文件跨多个 batch 也不会在仍可解码出新行时被提前确认。公共 consumer 和 writer 不访问这些计数。

Kafka/Pulsar 的物理 partition 和 Storage 的文件组由各 reader 自己表示；公共 consumer 不需要理解这些顺序单元。后续输入即使先完成，也不能越过 reader 中未完成的连续位置。

Kafka 确认连续完成位置之后的 next offset。Pulsar 只对连续完成前沿执行 cumulative ACK。Storage 没有上游确认能力，确认只更新当前进程的文件完成位置并释放资源，重启仍重新枚举文件。

sink 已成功、来源确认失败时，该位置仍会再次投递。consumer 只在 sink 完成后推进来源确认，并保留 at-least-once 语义。MySQL sink 使用默认的 batch DML 路径，consumer 不强制 safe mode，也不关闭 batch DML。

### DDL fence

写入逻辑对 DDL 的目标范围建立栅栏：

1. 停止该范围内 DDL 之后的 DML 进入 `writer`。
2. 等待完整边界覆盖 DDL，并完成该范围内 commit-ts 小于或等于 DDL 的 DML。
3. 调用 `FlushDMLBeforeBlock` 和 `WriteBlockEvent`。
4. DDL 完成后调用 reader 提供的完成回调，消费循环继续处理后续输入。

CREATE SCHEMA 和没有已有表依赖的 CREATE TABLE 可在收到后初始化下游。CREATE TABLE LIKE 等有已有表依赖的 DDL 仍等待完整边界与对应 DML 完成。

每个来源负责把 DDL 整理成一条规范化输入。sink 写入逻辑不感知 partition 副本、控制消息或文件组织方式。

### Decoder correctness and replay handling

[PR #6314](https://github.com/pingcap/ticdc/pull/6314) 的 decoder 和 schema 修复随 master 合并保留。consumer 直接使用解码后的完整 `TableInfo` 和行事件：列 offset、primary index、index state、handle-key flags 及 update/delete 行身份。Open Protocol 的 metadata 按 DDL 边界匹配事件；Avro key-only delete 使用完整 schema；缺少 before image 的 update 解码为 keyed DELETE 和 INSERT。

consumer 使用 MySQL sink 的默认 batch DML 路径，所有 reader 共用 writer 的重放处理。保留的状态有界，sink 写入失败时保留未确认位置。该处理保证 at-least-once 重放下 batch DML 的正确性。

共享写入路径使用 table ID、commit-ts、变更类型和完整 handle key 识别并过滤同一行变更的副本。UPDATE 使用旧行的 handle key，复合键使用长度前缀编码。没有 commit-ts 或可用 handle key 的行保留原有消费语义。

同 batch 的副本随该 batch 完成；跨 batch 的副本等待原 batch 成功后才能完成来源位置。写入前沿越过 commit-ts 后释放对应身份，当前边界仍保留身份以处理等 ts 重放。部分重复的行事件只重建保留行，并保留原事件的解码资源回收回调。

### Bounded rereaders

每个来源对以下对象统一计费：

- reader client 持有的 input payload；
- 解码与 Restore 临时对象；
- ready queue 和 sink in-flight batch；
- 为 batch DML 正确性保留的 consumer 状态；
- decoder 保留的 schema 版本与 table metadata；
- 来源位置与连续完成状态；
- batch、恢复任务和 goroutine 数量。

reader 与 writer 共用一份原子资源预算，解码后的 payload 从读取侧交付 writer 时只转移归属，不重复预留。writer 在 batch 完成后释放 payload，reader 在连续确认后释放原始输入。Kafka 在高水位暂停快 partition；下游的有界在途 batch 和读取结果队列形成反压。单条输入或无法闭合的窗口超过硬上限时返回资源错误，保持该位置未确认。

字节额度是 consumer 的资源计费预算，包含 input、chunk、重放身份和 metadata 的保守估算；它不等同于进程 RSS。reader SDK 同时限制 fetch 大小与并发 fetch 数。schema cache 具有独立的数量和字节上限，超限时返回资源错误。

Kafka 在内存高水位继续读取最慢 partition 或缺 schema 的 partition，以便收到能关闭当前窗口的控制消息；这些输入仍计入总硬上限。不能闭合且超过额度的窗口返回资源错误，不绕过顺序条件。

正常路径不写本地 DML spool。进程退出后，未确认数据从来源重新读取。

### Flush policy

reader 与 writer 中的 DML 分成两种状态：

- waiting：保留在 reader 中，来源的 watermark、DDL 或文件组条件尚未满足；
- ready：reader 已确认顺序条件，交付 writer 后可以写入 sink。

Kafka/Pulsar waiting DML 在读取边界覆盖后交付；Storage 普通文件按既有顺序及时交付，跨节点文件组完整解码并排序后交付。定时器和内存压力不能绕过 reader 的条件。

ready DML 按目标范围和顺序组成 batch。出现以下任一条件时，`writer` 立即提交当前 batch：

- 行数达到 batch 上限；
- 字节数达到 batch 上限；
- 最早 ready DML 达到 max linger；
- Storage 文件组已经完整交付；
- 内存达到高水位；
- 即将执行 DDL 或关闭进程。

reader 交付 ready DML 后，公共 consumer 立即提交已满 batch。最后一个 partial batch 最多等待 max linger；Storage 文件组结束、DDL 到达、内存达到高水位或进程关闭时立即提交。它无需等待下一个 watermark。低流量由 max linger 限制等待时间，高流量由行数、字节数和最大在途 batch 数维持批量与并发。

`writer` 不等待一个 DML batch 完成后再提交下一个 batch。最大在途 batch 数和最大在途字节数构成硬上限；DDL 只等待其影响范围内已经提交的 batch 完成。batch 和在途上限由 consumer 内部固定，不提供外部配置。

commit-ts 超过当前 `globalWatermark` 的 DML 必须留在 waiting 状态。consumer 不使用超时推测完整性；完整边界长期不前进时，reader 接收反压。吞吐依靠足够大的 batch 和有界的多 batch 在途能力，max linger 只限制 partial batch 的等待时间。

## Sources

### Kafka

Kafka 实现维护每个 `(topic, partition)` 的 decoder、watermark、未完成记录和已提交 offset。

Kafka reader 支持以下 control message 布局：

| Protocol | DDL placement | Watermark placement | Required configuration |
| --- | --- | --- | --- |
| Open Protocol | routed topic 的全部 partition | 每个 active topic 的全部 partition | 无额外要求 |
| Canal JSON | routed topic 的 partition 0 | 每个 active topic 的全部 partition | `enable-tidb-extension=true` |
| Avro | routed topic 的全部 partition | 每个 active topic 的全部 partition | `enable-tidb-extension=true`、`avro-enable-watermark=true` |
| Simple | routed topic 的全部 partition | 每个 active topic 的全部 partition | 无额外要求 |
| Debezium JSON | routed topic 的全部 partition | 每个 active topic 的全部 partition | `enable-tidb-extension=true`、`debezium-disable-schema=false` |
| Debezium Avro | routed topic 的全部 partition | 每个 active topic 的全部 partition | `enable-tidb-extension=true`、`avro-enable-watermark=true`、`debezium-disable-schema=false` |

每个 Kafka topic 使用一个 `cdc_consumer` 进程消费全部 partition。进程从 upstream URI 获取 topic，并发现其完整 partition 集合。使用 topic route 时，每个 routed topic 启动一个进程。

Canal JSON 的 DDL 必须来自 partition 0。其余协议按 `(commit-ts, schema, table)` 匹配 DDL，收齐全部 partition 的副本后，使用 partition 0 中的 DDL 向 sink 提交一次写入。

reader 合并同一 DDL 的全部副本，并通过一次完成回调更新各副本的内部状态。writer 写入成功后调用该回调。reader 保留已交付 DDL 的身份，读取边界越过其 commit-ts 后释放身份；后续同身份副本不重复交付。

DDL 执行还需等待 `globalWatermark` 覆盖其 commit-ts。该边界已经覆盖 DDL、预期副本仍未收齐时返回协议错误，对应记录保持未确认。

每个 partition 独立记录 watermark。`globalWatermark` 取全部 partition watermark 的最小值。

Kafka offset 的初始化与恢复沿用现有 consumer 的配置。

Avro schema registry 通过 upstream URI 的 `schema-registry` 或配置文件指定。需要查询上游 TiDB 的解码场景使用 URI 中的 `upstream-tidb-dsn`；Kafka reader 创建该连接并在初始化失败或进程关闭时释放。

### Pulsar

Pulsar reader 保留 Canal JSON 和 Exclusive subscription。一个进程订阅指定 topic，接收其全部物理 partition 的消息。

reader 按现有 topic 级 control message 布局处理 DDL 和 watermark。DDL 按身份归一化，结合完整边界建立其影响范围的写入栅栏。

所有 partition 的消息使用同一个 decoder 串行处理。收到 watermark 后，按 topic 单调更新 `globalWatermark`，提交其覆盖的 DML 并处理满足条件的 DDL。

每个 partition 独立维护消息位置和连续完成前沿。其来源位置对应的全部下游效果完成后，才能推进该 partition 的 cumulative ACK。

watermark 消息还需等待其边界内的 DML 和 DDL 写入完成，之后才能完成自身来源位置。ACK 使用 broker 响应确认，失败时保留对应记录。

subscription name 通过 `--consumer-id` 显式配置。

### Storage

Storage reader 继续读取现有 metadata、schema、index 和 DML 文件，并把 ready group 交给 sink 写入逻辑。文件可见性、排序和跳过规则沿用现有 Storage consumer 的约束。

扫描结果按 `CompareDMLPathKey` 排序，同一版本的 schema 先于 DML 文件处理。已执行 DDL 的版本用于过滤过期 DDL 和 DML；RENAME TABLE 同时更新旧表名的版本。reader 完整交付一个文件组后发出强制 flush 和该物理表的进度，再读取后续 schema DDL；writer 保证受影响表的写入完成后执行该 DDL。

CSV 直接使用 schema 文件提供的完整 metadata 和主键标记，并沿用列选择配置。Canal JSON 使用现有事务 decoder，按文件所属的 table version 标记 metadata。两种协议都保留物理 partition 对应的 table ID。

普通文件中满足顺序条件的 DML 可以及时形成 batch。跨节点输出先收齐同一文件组中的消息，按 commit-ts 排序后写入。文件读取、解码队列和 schema/index 缓存受内部资源额度限制，文件组超过硬上限时返回资源错误。

Storage 的表进度只用于释放已完成的重放身份，不当作 MQ 完整 watermark。未读取的跨节点文件组仍可能包含较小 commit-ts 的有效行，writer 不据此丢弃这些行。

Storage 没有持久化的 consumer 完成位置。进程重启后重新枚举可见文件，已经写入下游的效果可能再次执行。

## Failure and shutdown

| Scenario | Behavior | Data boundary |
| --- | --- | --- |
| 来源暂时不可用 | reader 重连，读取和确认暂停 | 确认前沿不前进 |
| sink 变慢 | 反压传到 reader | 已接收数据保持在额度内 |
| sink 失败或重试耗尽 | 进程返回非零 | 未确认位置在重启后重放 |
| Kafka control message 未覆盖预期 partition | `globalWatermark` 停止前进并返回来源错误 | waiting DML 不进入 sink |
| 输入超过硬上限 | 返回资源错误 | 不确认该位置 |
| Storage consumer 重启 | 重新枚举可见文件 | 已落库效果可能重放 |

SIGINT 或 SIGTERM 触发受控关闭。consumer 停止接收新输入，按来源约束结束已接收数据的处理，关闭 reader 与 sink，并等待资源释放。所有后台 goroutine 接收进程 context，由 wait group 统一等待。用户信号导致的正常取消返回 0；初始化错误、来源错误、sink 错误、资源错误和内部不变量失败返回非零。

关闭时的写入等待限制为五秒。Kafka 和 Pulsar 提交已有 ready DML，等待在途 batch 完成并确认连续完成的位置；waiting DML 保持未确认。Storage 同样提交已交付的 ready DML 并等待在途 batch；部分解码文件和未完整排序的跨节点文件组仍不确认，重启后重新读取。超时会取消 sink 的初始化 context，以终止正在执行的 DDL 和 DML；未成功完成的位置在重启后重放。

## CLI and migration

### New command

`make consumer` 生成 `bin/cdc_consumer`。

统一镜像使用 `deployments/consumer.Dockerfile`，入口为 `/cdc_consumer`。

```text
cdc_consumer \
  --upstream-uri='kafka://broker/orders?protocol=canal-json' \
  --downstream-uri='mysql://root@127.0.0.1:4000/' \
  --consumer-id='orders-test' \
  --config='consumer.toml'
```

URI scheme 选择对应 reader：

- `kafka`、`kafka+ssl`：Kafka；
- `pulsar`、`pulsar+ssl`、`pulsar+http`、`pulsar+https`：Pulsar；
- `file`、`s3`、`gcs`、`gs`、`azblob`、`azure`：Storage。

公共 flags 包括 `--upstream-uri`、`--downstream-uri`、`--config`、`--consumer-id`、`--tz`、`--log-file`、`--log-level` 和 `--enable-profiling`。Kafka 和 Pulsar reader 必须显式指定 `--consumer-id`；Storage reader 不使用该参数。来源专用配置放入 upstream URI 或配置文件，认证材料不得写入日志。

CI 通过 `tests/integration_tests/_utils/run_consumer WORK_DIR UPSTREAM_URI [CONFIG] [LOG_SUFFIX]` 启动程序。该入口使用 MySQL sink 默认 batch DML 路径，日志写入 `cdc_consumer[LOG_SUFFIX].log` 和 `cdc_consumer_stdout[LOG_SUFFIX].log`。需要跟踪子进程 PID 的用例直接启动同一二进制。

### CI build compatibility

`make kafka_consumer`、`make pulsar_consumer` 和 `make storage_consumer` 都依赖 `make consumer`，生成 `cdc_consumer`，并为对应旧产物名创建指向它的相对符号链接。现有 CI 的文件检查和缓存继续使用旧名字；仓库测试通过 `cdc_consumer` 启动统一程序。缓存只恢复旧链接时，缺失的目标文件使 CI 文件检查失败，随后通过旧 Make target 重建统一程序。

以下独立实现及源码目录已移除：

```text
cdc_kafka_consumer       cmd/kafka-consumer
cdc_pulsar_consumer      cmd/pulsar-consumer
cdc_storage_consumer     cmd/storage-consumer
```

镜像、发布产物、仓库测试脚本和部署示例使用 `cdc_consumer`。本分支 CI 跑通后，外部流水线统一构建入口与缓存产物名，再移除三个临时 Make target 和旧产物链接。

旧 Kafka group ID 或 Pulsar subscription name 作为新程序的 `--consumer-id` 使用。迁移时先停止旧 consumer，再启动 `cdc_consumer`；回退时执行相反顺序。新旧程序不得使用同一 consumer ID 并行写入同一目标。

## Logging

consumer 使用结构化日志记录启动、关闭和退出原因。消费循环和 batch 等待路径每五秒最多输出一条进度汇总，帮助定位 CI 中的消费或写入停滞：

- 已接收输入、解码行数、成功写入行数和已完成输入数量；
- 待写 DML 数量、在途 batch 数量和字节数；
- 未完成输入数量、reader 缓冲字节数和共享预算字节数。

读取与确认计数由 reader 原子更新，行数和 batch 计数由消费 goroutine 更新。日志直接写在相关处理路径中，不输出 URI 凭据、消息原文、证书内容或认证密钥。

## Verification

`cdc_consumer` 用于执行 TiCDC CI。现有 Kafka、Pulsar 和 Storage consumer 场景及受支持的 control message 布局全部迁移到新程序后，仓库 CI 全部通过即表示本设计满足正确性要求。

单元测试只覆盖无法由类型和局部代码结构保证的最小约束：

- 一个来源位置的全部效果完成后，连续确认位置才能推进。
- Kafka reader 收齐 DDL 副本后只执行一次 DDL，成功后才完成各副本的来源记录；完整边界已覆盖 DDL、副本仍缺失时返回错误。
- Kafka 的 `globalWatermark` 取全部 partition watermark 的最小值。
- Pulsar 按 topic 单调更新 watermark，覆盖的 DML 和 DDL 完成后才能确认对应 watermark 消息；每个 partition 独立推进连续完成位置。
- DDL 栅栏保证受影响范围内的 DML 与 DDL 顺序。
- waiting、ready 和 in-flight batch 的额度满足守恒关系。
- decoder 保留 batch DML 所需的完整行身份，consumer 对重复或冲突变更的处理不会丢失有效变更。

## Completion criteria

- `cdc_consumer` 可以根据 upstream URI 启动 Kafka、Pulsar 或 Storage reader。
- 全部 consumer 实现位于 `cmd/consumer`，仓库不再包含三个旧 consumer 程序。
- 镜像、发布清单、仓库测试脚本和部署文档使用 `cdc_consumer`；外部 CI 通过临时构建别名使用相同实现。
- Kafka 和 Pulsar 的确认前沿不会越过未完成的下游效果。
- Kafka 支持本文列出的全部协议与 control message 分区布局。
- Pulsar 支持非分区与 partitioned topic 的现有 DDL 和 watermark 布局。
- ready DML 在 batch 满、达到 max linger、Storage 文件组结束、DDL 到达、内存达到高水位或进程关闭时提交。
- 多个 DML batch 可以有界并发，DDL 只等待其影响范围内的 batch。
- 三类 reader 都保留 decoder 修复后的完整行身份，并能正确处理 at-least-once 重放下的 batch DML。
- MySQL sink 使用默认 batch DML 路径，CI 不强制 safe mode 或禁用 batch DML。
- 正常路径不写本地 DML 临时文件，所有队列和状态都有硬资源上限。
- 仓库 CI 全部通过。
