# Storage Sink Design

> 本文是 storage sink 相关内容的唯一设计文档。
> 配套文档：
> - `docs/plans/storage-sink-requirements.md`
> - `docs/plans/storage-sink-task-list.md`

## 1. 当前分支真实设计范围

当前分支相对 `master` 保留下来的 storage sink 改动只有三类：

1. 新增 `downstreamadapter/sink/cloudstorage/spool/*`
2. 把 spool 接进 cloudstorage `dmlWriters -> writer` 主链路
3. 新增 spool 相关 metrics
4. 新增 `spool-disk-quota` 配置入口

## 2. 模块划分

### 2.1 Spool 本体

- `codec.go`
  - 负责把消息批次序列化到字节流
  - 负责把字节流还原成消息批次
- `manager.go`
  - 负责内存承载
  - 负责磁盘 segment 管理
  - 负责 wake suppression
  - 负责生命周期清理

### 2.2 配套模块

- `downstreamadapter/sink/cloudstorage/dml_writers.go`
  - 创建共享 spool manager
  - 把同一个 spool 注入所有 writer shard
- `downstreamadapter/sink/cloudstorage/writer.go`
  - 在 pending batch 阶段先把 encoded messages 放进 spool
  - flush 时从 spool 读取、回调并释放
- `pkg/config/sink.go`
  - 暴露 `spool-disk-quota` 配置项
- `pkg/sink/cloudstorage/config.go`
  - 提供默认值、合并逻辑和校验逻辑
- `downstreamadapter/sink/metrics/cloudstorage.go`
  - 注册 spool 指标

## 3. 数据模型

```text
writer
  -> Enqueue(messages, onEnqueued)
  -> Entry
     - memoryMsgs
     - pointer(segmentID, offset, length)
     - callbacks
     - accountingBytes
     - fileBytes
  -> writer batch keeps Entry handles
  -> Load(entry)
  -> Release(entry)
```

### 3.1 `Entry`

`Entry` 表示一次入队结果，有两种存储形态：

- 内存态
  - `memoryMsgs != nil`
- 磁盘态
  - `pointer != nil`

无论哪种形态，callback 都会从原始消息中摘出来，统一挂在 `Entry.callbacks` 上，由调用方在后续消费流程中继续使用。

### 3.2 `segment`

磁盘溢写使用 append-only segment 文件：

- 每个 segment 有唯一 id
- `pointer` 记录 segment id、offset、length
- `refCnt` 用来判断 segment 何时可以删除

## 4. 入队与溢写流程

### 4.1 接入点

当前实现里，spool 接在 writer 的 pending-batch 阶段：

```text
dmlWriters
  -> encoderGroup
  -> writer.enqueueTask
  -> writer.genAndDispatchTask
  -> spool.Enqueue
  -> flush trigger
  -> spool.Load
  -> external storage write
  -> callback
  -> spool.Release
```

### 4.2 内存优先

`Manager.Enqueue` 先累计本批消息的 `accountingBytes`。如果：

- `memoryBytes + entryBytes <= memoryQuotaBytes`

则本批消息直接保留在内存里。

### 4.3 超出内存层后写磁盘

如果超过内存层阈值，则：

1. 把消息序列化
2. 写入当前可写 segment
3. 记录 `pointer`
4. 递增 `diskBytes`

segment 大小达到阈值后，manager 会 rotate 到新的文件。

### 4.4 wake suppression

manager 维护基于总占用的高低水位：

- 超过 high watermark 时，开始抑制新的 `onEnqueued`
- 回落到 low watermark 时，批量恢复之前延迟的 callback

这里控制的是 wake 节奏，不是 durable 语义。

## 5. 读取、释放与清理

### 5.1 `Load`

- 如果 entry 仍在内存里，直接返回内存消息
- 如果 entry 已经 spill，则按 `pointer` 从 segment 文件 `ReadAt`
- 反序列化后恢复消息内容和 row count

writer flush 时按 entry 顺序调用 `Load`，再把取回的消息写成 data / index 文件。

### 5.2 `Release`

释放时按 entry 形态回收资源：

- 内存态：递减 `memoryBytes`
- 磁盘态：递减 `diskBytes`，并递减 segment `refCnt`

当某个旧 segment 的 `refCnt == 0` 且它不是当前 active segment 时，文件会被关闭并删除。

当前实现里，`Release` 发生在两种场景：

- writer 成功写完 data / index 文件并执行 callback 后
- writer 判断该批消息属于旧 schema version、选择忽略时

### 5.3 `Close`

关闭时会：

- 关闭所有 segment 文件
- 把内部占用归零
- 清空 pending wake
- 删除 spool 自己创建的 segment 文件
- 删除对应的 metrics label values

这里不会删除 `RootDir` 下的其他文件。当前实现只清理 spool 自己管理的 `segment-*.log`，避免误删同目录下与 spool 无关的内容。

## 6. 配置与指标

### 6.1 `spool-disk-quota`

当前分支新增了 `spool-disk-quota`：

- 默认值：10 GiB
- 可从 sink URI 读取
- 可从 replica config 读取
- URI 优先级高于 replica config
- 非法值会报错

### 6.2 当前配置语义

虽然配置名叫 `spool-disk-quota`，但当前实现里它承担的是 spool 总容量基线：

- 用来推导 memory tier 大小
- 用来推导高低水位

当前分支没有把它实现成“超过后立即拒绝新消息”的硬上限，也没有把它实现成“只约束磁盘字节数”的纯磁盘配额。

这意味着当前实现只能表达“什么时候开始 spill、什么时候抑制 wake”，还不能表达“达到上限后 storage sink 应该如何处理后续流量”。这个运行时契约已经明确记录下来，留待下周继续收口。

### 6.3 指标

当前分支新增 4 个指标：

- `cloud_storage_spool_memory_bytes`
- `cloud_storage_spool_disk_bytes`
- `cloud_storage_spool_total_bytes`
- `cloud_storage_wake_suppressed_total`

这些指标都以 changefeed 维度打标签。

## 7. 当前已知缺口

### 7.1 `spool-disk-quota` 还不是严格 quota

当前代码把 `spool-disk-quota` 当作总容量基线，用它推导内存层比例和高低水位，但不会在运行时强制拦截后续写入。因此，下游长期卡住时，本地磁盘仍然可能继续增长。

这和“quota”这个名字并不完全一致。下周需要继续明确：

- 它到底是软基线、总配额，还是磁盘配额
- 超过后应该阻塞、报错，还是只做背压控制
- 配置名、实现、测试和文档如何保持一致

### 7.2 外部删除 spool 目录时，当前错误暴露时机偏晚

当前实现已经明确不做自愈。运行中如果 spool 根目录被外部删除，storage sink 不会立刻报错，而是通常要等到下一次创建新 segment 时才会返回错误。

原因是当前 active segment 的文件句柄在进程里仍然打开时，已有 segment 还可能继续读写；真正需要重新打开新文件时，目录缺失才会暴露出来。这个行为是当前真实语义，但还不是最终想要的运行时契约。

下周需要继续明确：

- 这类外部破坏是否应尽早 fail fast
- 如果要 fail fast，最早应该在哪个路径上暴露错误
- 对应测试应该保护哪一种契约，而不是只验证当前实现细节

### 7.3 选项默认值与校验仍需要再收口

当前 `withDefaultOptions` 会先填默认值，再进入校验逻辑。这样一来，一些显式传入的零值会在校验前被默认值覆盖，导致本应报错的非法配置没有被识别出来。

这个问题不会改变 spool 主链路的基本工作方式，但会影响配置契约的一致性和可预期性。下周需要把默认值注入和参数校验的职责重新理顺。

## 8. 当前分支的明确边界

本文特意不再声称当前分支已经完成这些内容：

- 没有改 `path.go`
- 没有改 dispatcher / block event / barrier 语义
- 没有改 storage consumer
- 没有改 schema / data / index 文件布局

因此，当前设计文档描述的是“当前分支已经落地的 spool 原语、它在 writer 中的接入方式，以及它的直接配套”，不是完整的 storage sink 最终形态。

## 9. 模块映射

- spool 编码与反编码：`downstreamadapter/sink/cloudstorage/spool/codec.go`
- spool 生命周期与存储管理：`downstreamadapter/sink/cloudstorage/spool/manager.go`
- spool 接入点：`downstreamadapter/sink/cloudstorage/dml_writers.go`
- writer 中的 spool 消费：`downstreamadapter/sink/cloudstorage/writer.go`
- spool 指标：`downstreamadapter/sink/metrics/cloudstorage.go`
- spool 配置入口：`pkg/config/sink.go`
- cloudstorage spool 配置应用：`pkg/sink/cloudstorage/config.go`
