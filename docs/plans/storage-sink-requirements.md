# Storage Sink Requirements

> 本文是 storage sink 相关内容的唯一需求文档。
> 配套文档：
> - `docs/plans/storage-sink-design.md`
> - `docs/plans/storage-sink-task-list.md`

## 1. 背景

当前分支相对 `master` 实际保留下来的 storage sink 改动，已经收敛到一个更小的范围：

- 新增 cloudstorage spool 能力本体
- 把 spool 接进 cloudstorage 主写出链路
- 新增 spool 配置入口
- 新增 spool 相关指标

本文只描述这一轮分支上真实存在的需求边界，不再覆盖已经从当前分支中移除的 path、barrier、dispatcher、consumer 等主题。

## 2. 本轮范围

### 2.1 包含的内容

- 为 cloudstorage sink 引入本地 spool 组件
- 让 cloudstorage writer 在 flush 前先通过 spool 承载 pending batch
- 让 spool 能以“内存优先、磁盘溢写”的方式承载编码后的消息
- 为 spool 暴露最小配置入口
- 为 spool 暴露最小可观测性

### 2.2 不包含的内容

- storage consumer
- dispatcher / barrier / block event 语义
- schema / data / index 路径与文件布局改造
- DDL 顺序控制与 checkpoint 语义改造
- 其他 sink 类型的行为调整

## 3. 目标

### 3.1 功能目标

- storage sink 需要有一个可复用的本地 spool，专门承载编码后的消息。
- cloudstorage sink 需要把编码后的消息先交给 spool，再由 flush 路径统一加载、写出和释放。
- spool 需要支持两层承载：
  - 优先放在内存
  - 当内存层超过阈值时，把消息落到本地 segment 文件
- 调用方需要能在后续重新取回消息，并在使用结束后显式释放占用。

### 3.2 资源目标

- spool 需要有一个统一的容量基线，用于推导内存层占比与高低水位。
- spool 需要在 changefeed 维度管理自己的本地目录与临时文件。
- 关闭时，spool 产生的本地临时文件必须能回收。

### 3.3 可观测性目标

- 需要能直接观察当前 spool 占用的内存字节数。
- 需要能直接观察当前 spool 占用的磁盘字节数。
- 需要能直接观察当前 spool 总占用。
- 需要能观察 wake callback 被抑制的次数。

## 4. 需求约束

### 4.1 当前分支只引入 spool 原语，不扩散改造面

本轮需求要求把改动控制在 spool 本体及其直接配套内容内。任何不属于 spool 的路径、顺序、DDL、consumer、dispatcher 语义，都不应该被顺带带入当前分支。

### 4.2 spool 只负责承载，不宣称 durable

spool 的职责是承载与转运消息，不是对外宣布数据已经 durable。文档和实现都不能把“进入 spool”描述成“已经安全写出”。

### 4.3 当前容量配置是软控制基线

本轮配置项用于决定内存层比例和 wake 抑制水位。当前分支不要求把它实现成严格拒绝写入的硬上限，也不要求把它描述成磁盘绝对容量上限。

### 4.4 生命周期必须自洽

spool 创建的目录、segment 文件和指标标签，都必须在关闭时清理干净，不能留下长期漂移状态。

### 4.5 已知缺口需要显式记录

本轮先把已经确认的问题记录清楚，不继续扩展实现范围。`spool-disk-quota` 的运行时契约、spool 本地目录被外部破坏后的失败语义，以及相关配置校验细节，都要在文档里写明当前状态，并在后续任务中继续收口。

## 5. 功能性需求

### 5.1 消息承载

- spool 必须接收编码后的消息批次。
- spool 必须保留每条消息的 key、value、row count 和 callback 关联关系。
- 当消息被重新读取后，调用方看到的内容必须与入队前等价。

### 5.2 内存与磁盘分层

- 当消息量仍在内存层范围内时，spool 应保留内存副本。
- 当消息量超过内存层阈值时，spool 应把消息序列化后写入本地 segment 文件。
- 被磁盘溢写的消息需要可重新读取。

### 5.3 释放与回收

- 调用方释放 entry 后，spool 必须回收相应的内存或磁盘占用。
- 不再被引用的旧 segment 文件必须可以删除。
- 关闭 spool 后，整个本地 spool 目录必须可以删除。

### 5.4 wake 抑制

- 当总占用越过高水位时，spool 可以延迟 on-enqueued callback。
- 当总占用回落到低水位后，被延迟的 callback 需要恢复执行。
- 这套机制只解决 wake 节奏问题，不改变 durable 语义。

### 5.5 配置入口

- 用户需要能够通过 sink URI 或 replica config 提供 `spool-disk-quota`。
- 当未显式提供时，系统需要使用默认值。
- 非法配置值必须被识别并返回错误。

## 6. 非功能性需求

### 6.1 可测试性

- 需要有单测覆盖 wake 抑制 / 恢复行为。
- 需要有单测覆盖 spill 到磁盘并读回的行为。
- 需要有单测覆盖 `spool-disk-quota` 的默认值、URI 覆盖和 replica config 覆盖。
- 需要有测试覆盖 writer 已经通过 spool 持有未 flush 的消息。

### 6.2 可维护性

- spool 本体、配置、指标应分别收敛在各自文件中。
- 文档必须明确哪些内容已经在当前分支里，哪些还没有进入当前分支。

## 7. 验收标准

### 7.1 功能验收

- spill 到磁盘的消息能够读回，内容与 row count 保持一致。
- callback 在 entry 生命周期中不会丢失。
- release 后，占用能够下降，旧 segment 能被清理。

### 7.2 配置验收

- `spool-disk-quota` 默认值稳定。
- URI 参数可以覆盖 replica config。
- 非法值会返回错误。

### 7.3 可观测性验收

- metrics 中可以看到 spool 的内存、磁盘、总占用和 wake suppression 计数。

### 7.4 分支范围验收

- 当前分支的 projected diff 只保留 spool、storage sink 对 spool 的接入、本轮 spool 配置入口和 spool 指标。
