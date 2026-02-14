# Storage Sink / Consumer 总览（唯一架构文档）

> 目标：把 storage sink 与 storage consumer 的关键设计、语义约束、性能与运维信息收敛到一处，减少分散文档阅读成本。
>
> 文档数量约束（storage sink 相关仅保留 3 份）：
> 1) 本文（总体设计与运维）  
> 2) `docs/plans/2026-02-04-cloudstorage-sink-ga-task-breakdown.md`（任务清单与完成状态）  
> 3) `docs/plans/2026-02-14-cloudstorage-sink-spool-review-recommendations.md`（评审清单）

## 1. 范围与核心目标

### 1.1 范围
- 生产端：`downstreamadapter/sink/cloudstorage` + `pkg/sink/cloudstorage`
- 消费端：`cmd/storage-consumer`
- 外部系统：`storage.ExternalStorage`（本地文件、S3、GCS、Azure Blob）

### 1.2 核心目标
- 解决低流量多 dispatcher 场景下吞吐被 `flush-interval` 锁死的问题。
- 避免 sink 内无界内存增长（OOM 风险可控）。
- 保证 DDL / DML 顺序正确性不被 early-wake 破坏。
- 支持大规模 dispatcher（目标口径：1000）长时间稳定运行。

### 1.3 非目标
- 不改变 checkpoint 推进语义（仍然必须远端写成功后推进）。
- 不改其他 sink（kafka/mysql/pulsar）的行为。
- 不引入大量面向用户的新参数；复杂控制尽量内置。

## 2. 端到端数据流（sink + consumer）

### 2.1 DML/DDL 写出链路（sink）
```text
Dispatcher
  -> Sink.AddDMLEvent / PassBlockEvent / WriteBlockEvent
  -> encoding group (task/future pipeline)
  -> spool (memory + disk spill)
  -> writer flush (data/index/schema)
  -> PostFlush callback
```

### 2.2 回放链路（consumer）
```text
WalkDir 扫描 schema/index
  -> 解析路径与 file index
  -> 读取 data file 解码
  -> 聚合并投递到下游 sink
  -> 等待 PostFlush 完成
```

## 3. 必须保持的语义不变量

### 3.1 DML two-stage ack
- **enqueue ack（用于 wake）**：数据进入 sink 内可控承载后触发（用于解除 dynstream path 阻塞）。
- **flush ack（用于 checkpoint）**：只有远端写成功后才触发 `PostFlush`。
- 结论：wake 可以提前，checkpoint 不能提前。

### 3.2 DDL 顺序正确性
- 对 DDL(commitTs=T)，必须保证 `<T` 的 DML 已落远端后，才能写 DDL schema 文件。
- 通过 `PassBlockEvent + DrainMarker` 覆盖 encoding queue -> spool -> writer flush 全链路。

### 3.3 路由与顺序
- 同 dispatcher 路由稳定，避免写出顺序抖动。
- 同 output shard 单消费者，保证 shard 内顺序可推理。
- 同 index file 不能被多个 writer 并发写。

### 3.4 AddDMLEvent 约束
- 入口保持 non-blocking，避免反向堵塞 dispatcher hot path。

## 4. Spool 机制（为什么要有 memory quota）

### 4.1 设计动机
如果仅靠内存积压“待写远端数据”，远端变慢时会出现：
- 队列持续增长
- GC 放大
- 最终 OOM

因此需要 spool 做“可控承载 + 背压信号”，而不是把内存当无底洞。

### 4.2 语义边界
- spool 是内部缓冲层，不是 checkpoint 边界。
- 数据进入 spool 只能用于 wake 相关逻辑，不代表可推进 checkpoint。

### 4.3 memory quota 与水位控制
- 按 changefeed 独立记账（包括 memory 与 spill 状态）。
- 通过高/低水位（hysteresis）控制 wake：
  - 超过高水位：抑制 wake，把背压留在上游调度层；
  - 低于低水位：恢复 wake。

### 4.4 disk spill、segment、rotate、delete
- spill 采用 segment 追加写，减少小文件和随机写开销。
- 一个 segment **可以混合多个 dispatcher（也可能包含多表数据）**，这是有意的文件数控制策略。
- 快速读取不靠扫描文件，而靠每条记录的 `diskPtr`（segmentID + offset + length）直接定位。
- rotate：当前 segment 达到阈值后切换到新 segment 写入。
- delete：当旧 segment 中所有记录都被消费并释放后，删除该 segment 文件。

## 5. 文件布局与契约（sink 与 consumer 共识）

### 5.1 schema/data/index
- schema：`<schema>/<table>/meta/schema_{version}_{checksum}.json`
- data：`.../CDC{index}.ext` 或 `.../CDC_{dispatcher}_{index}.ext`
- index：`.../meta/CDC.index` 或 `.../meta/CDC_{dispatcher}.index`

### 5.2 关键契约
- consumer 必须按路径中的 schema version 对应地读取与回放。
- index 文件与 data 文件命名必须严格匹配，避免“读到错误文件序号”。
- 路径解析逻辑已经就近放到 consumer 侧（`cmd/storage-consumer/path_key.go`），避免 `pkg` 承担仅 consumer 使用的职责。

## 6. DDL 行为矩阵（精简版）

### 6.1 单表 DDL
- 未拆表：通常可不经 barrier，单 dispatcher 处理。
- 拆表后：同表多个 dispatcher，需要 barrier 协调 write/pass。

### 6.2 多表 / DB / All DDL
- 走 barrier，由 maintainer 协调，通常一个 dispatcher write，其余 pass。

### 6.3 早唤醒下的额外要求
- 不管是否 barrier，只要 DDL 会写 schema 文件，都要先完成 `PassBlockEvent` 对应 drain。

## 7. 性能瓶颈与优化优先级

### P0（必须优先）
- flush 作为放行边界导致吞吐节拍化。
- 背压失控导致内存高水位与 OOM 风险。
- DDL drain 覆盖不完整导致顺序风险。

### P1（本轮建议）
- index/schema 路径解析热路径开销（已做无正则化）。
- consumer flush 错误链路必须完整上抛（已修复）。
- consumer 状态（`tableDefMap` / `tableDMLIdxMap`）回收策略。

### P2（后续）
- 大目录下全量 `WalkDir` 的增量化扫描。
- consumer 模块进一步拆分（scan / decode / apply / state）。

## 8. 监控与排障（最小集）

### 8.1 先看吞吐是否被 interval 主导
- flush reason：`size` vs `interval` 比例。
- flush file size 分布（是否长期偏小）。

### 8.2 再看是否内存/背压问题
- spool bytes / items
- wake suppressed 次数
- writer 输入积压长度

### 8.3 DDL 风险定位
- `ddl_drain_duration_seconds`
- DDL 前后 `<T` DML 的远端可见性

## 9. 代码布局（当前）

### 9.1 sink 侧
- 入口与生命周期：`downstreamadapter/sink/cloudstorage/sink.go`
- DML pipeline：`downstreamadapter/sink/cloudstorage/dml_writers.go`
- 编码组：`downstreamadapter/sink/cloudstorage/encoding_group.go`
- 写出：`downstreamadapter/sink/cloudstorage/writer.go`
- spool：`downstreamadapter/sink/cloudstorage/spool/*`
- 路径与清理：`pkg/sink/cloudstorage/path.go`

### 9.2 consumer 侧
- 主流程：`cmd/storage-consumer/consumer.go`
- 发现/扫描：`cmd/storage-consumer/discovery.go`
- 路径键与解析：`cmd/storage-consumer/path_key.go`
- 启动：`cmd/storage-consumer/main.go`

## 10. 仍需持续优化的点（执行入口）

- 具体任务与完成状态：`docs/plans/2026-02-04-cloudstorage-sink-ga-task-breakdown.md`
- 评审重点与测试建议：`docs/plans/2026-02-14-cloudstorage-sink-spool-review-recommendations.md`
