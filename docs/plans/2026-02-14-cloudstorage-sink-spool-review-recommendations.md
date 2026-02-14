# Cloud Storage Sink Spool Review 建议（按优先级）

> 文档定位（已整合）
> - 类型：代码评审清单（优先级/风险/测试建议）
> - 总览文档：`storage-sink.md`
> - 说明：本文件只维护 review 视角，不重复维护实现方案与任务状态。

## 目标与范围

- 目标：给出一份可执行、可验收的 review 清单，覆盖 cloud storage sink 的 spool + task pipeline + DDL drain + 观测性改造。
- 范围：
  - `downstreamadapter/sink/cloudstorage/*`
  - `downstreamadapter/sink/cloudstorage/spool/*`
  - `downstreamadapter/dispatcher/basic_dispatcher.go`
  - `pkg/common/event/dml_event.go`
  - `pkg/sink/cloudstorage/path.go`
  - `downstreamadapter/sink/metrics/cloudstorage.go`

## 优先级与关键点

### P0（必须优先通过）

1. **语义锚点：checkpoint 不提前**
   - 关键点：`PostEnqueue` 仅用于 wake；`PostFlush` 仍只在远端写成功后触发。
   - 结论标准：压测下 checkpoint 推进速率不快于远端真正落盘速率。

2. **DDL 顺序正确性**
   - 关键点：`PassBlockEvent` + `DrainMarker` 必须覆盖 encoding queue → spool → writer flush 全链路。
   - 结论标准：任何 DDL(commitTs=T) 写 schema 前，`<T` DML 已全部远端可见。

3. **spool 生命周期安全**
   - 关键点：`Enqueue/Load/Release/Close` 不丢 callback、不重放 callback、segment 不泄露且不提前删除。
   - 结论标准：长时间运行后 spool 目录与引用计数稳定，无异常增长。

4. **路由保序与稳定性**
   - 关键点：同 dispatcher 永远映射到同 output shard；route cache 命中后无重复 hash/分配开销。
   - 结论标准：同 dispatcher 的 task 顺序稳定，跨 shard 并行不互相阻塞。

### P1（建议本轮覆盖）

1. **模块边界清晰**
   - 关键点：channel 不跨组件裸传递；`encodingGroup`、`writer` 仅暴露方法，不暴露内部 channel。

2. **状态回收机制有效**
   - 关键点：`FilePathGenerator` 不再按 `VersionedTableName` 无界累积状态；TTL 回收与版本复用生效。

3. **观测性可用于排障**
   - 关键点：至少可回答三类问题：
     - flush 是 `interval` 还是 `size` 触发？
     - 每次 flush 大小与耗时是否异常？
     - DDL drain 是否变慢？

### P2（可并行补强）

1. **关闭路径一致性**
   - 关键点：writer input 关闭时剩余 batch 会 `reason=close` flush，不丢尾部数据。
2. **性能细节**
   - 关键点：route cache、path state cleanup、spool rotate 在高并发下锁竞争可控。

## 风险点清单

1. **回调错配风险（高）**
   - 风险：spool 条目回调与原始 event 回调混用时可能漏调或重调，影响 table progress。

2. **DDL drain 漏覆盖风险（高）**
   - 风险：若某条路径未被 marker 串联，schema file 可能先于旧版本 DML 可见。

3. **状态回收误删风险（中高）**
   - 风险：TTL 回收若触发过早，会导致 file index 重置或路径状态抖动。

4. **指标误读风险（中）**
   - 风险：flush 原有总时延与分 reason 时延并存，dashboard 若未区分可能误判瓶颈。

## 测试项（意图 / 价值 / 建议）

| 测试项 | 测试意图 | 测试价值 | 测试建议 |
| --- | --- | --- | --- |
| `TestTaskIndexerRouteOutputShardCached` | 验证 dispatcher -> output shard 路由缓存 | 锁定“同 dispatcher 同 shard”与 cache 行为 | 保持为快速单测，防止后续回归为重复 hash |
| `TestTaskIndexerRouteOutputShardStable` | 验证 route 范围与稳定性 | 保证路由边界正确 | 可补充随机 dispatcher 批量覆盖 |
| `TestEncodingGroupRouteByDispatcher` | 验证 encoding 后按 output shard 消费且保序 | 保障 defragmenter 移除后的核心语义 | 增加多 shard 并发 case |
| `TestWriterDrainMarker` | 验证 DDL drain 会先 flush 再 ack marker | 直接覆盖 DDL 顺序关键路径 | 增加 marker 前后混合多 table case |
| `TestPassBlockEventRecordDrainDurationMetric` | 验证 DDL drain duration 指标打点 | 保障线上可观测性基础 | 可补充慢路径阈值告警回归 |
| `TestPathStateCleanup` | 验证 path state TTL 回收与状态复用 | 防止长期运行 map 膨胀 | 增加多 dispatcher + 多版本 churn case |
| `spool/manager` 现有单测 | 验证 spill/load/release/wake 抑制恢复 | 锁定 spool 正确性 | 建议补多 segment rotate + 大 payload case |

## 建议评审顺序

1. 先看语义锚点：`pkg/common/event/dml_event.go`、`downstreamadapter/dispatcher/basic_dispatcher.go`。
2. 再看主链路：`downstreamadapter/sink/cloudstorage/dml_writers.go` → `encoding_group.go` → `writer.go`。
3. 再看资源与状态：`downstreamadapter/sink/cloudstorage/spool/manager.go`、`pkg/sink/cloudstorage/path.go`。
4. 最后看观测性：`downstreamadapter/sink/metrics/cloudstorage.go` 与对应打点调用点。
