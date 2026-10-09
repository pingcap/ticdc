# Storage Sink 半成功写入问题记录

对应 issue：

- https://github.com/pingcap/ticdc/issues/4530

## 背景

当前 storage sink 写一个 DML batch 时，顺序是：

1. 写 data file
2. 写 index file

相关代码在：

- `downstreamadapter/sink/cloudstorage/writer.go`
- `pkg/sink/cloudstorage/path.go`

这个顺序意味着存在一条半成功路径：data file 已经成功写入远端存储，但 index file 写入失败。

## 当前行为

当前 `writer.writeDataFile` 的行为是：

1. 先把当前 batch 的内容写到 data file
2. 再把这个 data file 名写入 index file
3. 如果 index file 写失败，整个 flush 返回错误

问题在于，此时远端状态已经发生了变化：

- data file 已经存在
- index file 仍然是旧的，或者根本不存在

后续恢复时，`FilePathGenerator.GenerateDataFilePath` 会先从 index file 推导下一个文件号。如果 index file 是旧的，它会生成一个实际上已经存在的 data file 路径。当前实现发现文件已存在后，会删除内存里的 `fileIndex`，然后递归重新生成路径。

但是远端状态并没有改变。如果 index file 依旧是旧值，递归重试仍然会拿到同一个冲突结果。这意味着系统可能持续在同一个 stale 状态里打转，而不是继续向前生成新的 data file。

## 为什么这是问题

这个问题不是普通的写失败，而是“部分外部副作用已经成功”的失败：

- 已写出的 data file 不能简单当成没发生过
- 不能覆盖已经存在的数据文件
- 也不能因为 index 落后而一直卡住，导致恢复后无法继续写入

如果这条路径不被明确处理，storage sink 在重试或重启后可能出现：

- 反复尝试生成已存在的 data file 名
- 无法继续向前推进
- 把一次本可恢复的半成功写入放大成持续故障

## 影响范围

问题主要涉及两块：

- `downstreamadapter/sink/cloudstorage/writer.go`
  这里定义了 data file 和 index file 的写入顺序，以及 index file 失败时 writer 如何退出。
- `pkg/sink/cloudstorage/path.go`
  这里定义了恢复后如何决定下一个 data file 路径。

## 期望的修复目标

后续修复应当把这条路径收口成一个稳定的恢复契约：

- 当 data file 已经成功、index file 失败时，系统重试或重启后仍然可以继续推进
- 已存在的 data file 不会被覆盖
- index file 落后不会导致递归卡住
- 系统能够基于远端真实状态重新对齐下一个 data file 序号

更直白地说，如果 `CDC_xxx_000001.json` 已经存在，但 index file 还停在空值或旧值，恢复后应当继续生成 `CDC_xxx_000002.json`，而不是反复撞到 `000001`。

## 可能的修复方向

当前还没有最终方案，但可行方向大致有两类：

1. 以远端真实存在的 data file 为准，重新找出当前最大文件号，然后修正本地状态
2. 重新定义 index file 的恢复逻辑，让 stale index 能够被显式重建，而不是只靠递归重试

无论采用哪种方案，都需要先明确恢复契约，再让 `writer` 和 `FilePathGenerator` 的行为与契约一致。

## 为什么本 PR 不解决

本 PR 的目标是引入 spool，并接入 storage sink 主链路。

“data file 成功、index file 失败”的恢复语义虽然属于 storage sink correctness，但它不是 spool 接入本身引入的新行为，而是 storage sink 既有写入协议中的一个独立问题。这个问题需要单独定义恢复契约、补测试、再做实现，更适合后续单独 PR 处理。
