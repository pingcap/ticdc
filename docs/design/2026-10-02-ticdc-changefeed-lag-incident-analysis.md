# 2026-10-02 TiCDC changefeed 延迟持续上升故障分析

Last updated: 2026-10-08

Status: Root cause identified; recovery mechanism pending 13:00–14:00 log confirmation

Scope: 2026-10-02 `cdc-recover-saas02-*` changefeed 延迟故障，重点覆盖 06:45–11:29 的四节点日志与 09:30–12:40 的监控截图

Audience: TiCDC runtime owner、Event Collector / Dynamic Stream 维护者、值班与 SRE 工程师

Related documents:

- [TiCDC 规模化故障日志增强设计](../log-optimization/incident-observability-enhancement-design.md)
- [TiCDC flow control design](2024-12-20-ticdc-flow-control.md)

## Background

2026-10-02，以下三个 changefeed 的 checkpoint lag 持续线性增长：

- `cdc-recover-saas02-1700-1999`
- `cdc-recover-saas02-2000-2399`
- `cdc-recover-saas02-2500-2999`

其中 `cdc-recover-saas02-2000-2399` 约有 75,700 个 span。06:52–06:57 期间各 CDC 节点完成重启；08:46–08:58 期间三个 changefeed 先后执行 pause / resume，`cdc-recover-saas02-2000-2399` 曾短暂以 sync point 开启状态恢复，随后再次 pause，并以 sync point 关闭状态恢复。关闭 sync point 后 MySQL sink 流量曾短暂上升，之后再次下降，checkpoint lag 继续增长。

四份节点日志覆盖 06:45–11:29，总体积约 16 GiB。事故二进制的 Git hash 为 `452a3eacd01c3544dcc53612b4fc03c39d3a3774`，与本仓库调查时的代码版本一致，因此本文的源码机制分析对应事故现场实际运行代码。

当前仍缺少两项原始证据：

- 13:00–14:00 暂停全部 changefeed、逐个恢复并最终追平期间的节点日志。
- 事故时各节点的 CPU profile、goroutine dump 和 Dynamic Stream feedback channel 长度。

## 结论摘要

本次故障的主要故障点位于 **Event Collector 的 Dynamic Stream 内存释放反馈环**。高基数 changefeed 同时 bootstrap，并叠加 sync point 历史 block event 的创建、reset 和重发，使 `10.107.15.206` 与 `10.107.16.194` 上的 Dynamic Stream pending memory 达到约 1 GiB。此后释放控制环持续运行约 2.5 小时，pending bytes 基本没有下降。

释放反馈中持续包含已经从 Event Collector 移除数小时的 dispatcher。源码允许以下状态同时出现：

1. 多个释放周期并发生成重复 feedback batch。
2. 内存控制 area 继续跟踪已移除 path。
3. feedback 被同步写入容量为 1024 的 channel，释放循环可能长时间阻塞。
4. feedback 到达后，如果主 path map 已不存在该 dispatcher，`Release` 静默返回，无法释放 pending bytes，也没有空操作指标或告警。

这一失去进展的反馈环解释了现场的核心现象：Event Service 持续收到上游 resolved-ts，部分节点却无法向下游阶段推进 resolved-ts；MySQL sink worker 保持较低忙碌度；checkpoint 取所有 dispatcher 进度的最小值，因此少量或一组卡住的 dispatcher 足以让整个 changefeed 的 lag 按墙钟时间线性增长。

故障触发条件为大量 dispatcher 同时 bootstrap，以及 sync point 历史事件带来的额外 block/reset/handshake 压力。`cdc-recover-saas02-2000-2399` 在 08:56 短暂以 sync point 开启状态 resume，放大了这一波控制事件。三个受影响 changefeed 共用节点级 Event Collector Dynamic Stream，故障能够跨 changefeed 传播。

结论置信度：

- **高置信**：延迟主阻塞点在 Event Store 输入之后、sink 写入之前；Dynamic Stream 内存释放环已失去进展。
- **高置信**：已移除 dispatcher 的释放反馈持续数小时，是释放环无法收敛的直接证据。
- **中高置信**：后续暂停全部 changefeed 再逐个恢复，通过清空共享队列、回收旧 path 状态并降低同时 bootstrap 压力，最终恢复 checkpoint；该阶段缺少节点日志，机制仍需补证。

## 影响范围与节点

| 节点 | 节点 UUID | 关键现场状态 |
| --- | --- | --- |
| `10.107.15.204:8300` | `15da6cad-c283-4d6c-91e5-120ca8123816` | 10:28 时 `2000-2399` Event Collector used 约 574 MiB；09:00 后未持续出现同量级 release 日志风暴 |
| `10.107.15.206:8300` | `8f479b7b-09ba-461c-801b-d31c83ee26cd` | `2000-2399` pending memory 长期约 1 GiB；11:29 仍是 1,908 个 add operator 的目标节点 |
| `10.107.16.194:8300` | `5113f1fe-5dad-43d4-9e0d-13b2a573a738` | `2000-2399` pending memory 长期约 1 GiB；记录 maintainer 和 operator 停滞状态 |
| `10.107.17.183:8300` | `1d6023f2-ac18-408a-a833-fb003b1772dd` | 承载 changefeed 生命周期操作；09:00 后未持续出现同量级 release 日志风暴 |

调度日志包含 `cdc-recover-saas02-2000-2399` 向四个节点的 bind 记录。监控中只有两个节点持续承担主要工作量，来源更接近 dispatcher 已进入工作状态与实际流量分布，并不能证明调度器只向两个节点发出过分配。11:29 时，发往 `.206` 的 1,908 个 add operator 仍未完成，说明“已计划分配”和“已成功进入工作状态”之间存在大规模缺口。

## 时间线

| 时间 | 事件 | 证据与解释 |
| --- | --- | --- |
| 06:52–06:57 | CDC 节点重启 | `.206` 于 06:52:16 退出并在 06:52:31 启动；`.204` 于 06:57:08 退出并在 06:57:22 启动，06:57:24 完成初始化 |
| 08:46:54 | `1700-1999` resume，sync point 关闭 | final resume 配置为 `syncPoint=false`，随后开始大规模 bootstrap |
| 08:56:06 | `2000-2399` resume，sync point 开启 | 日志记录 `syncPoint=true`；历史 sync point block event 很快进入新建 dispatcher |
| 08:57:19 | `2500-2999` resume，sync point 关闭 | 随后开始 bootstrap |
| 08:58:00 左右 | `2000-2399` 再次 pause，旧 dispatcher 被移除 | 样本 dispatcher 在 08:58:00 完成 remove |
| 08:58:27 | `2000-2399` final resume，sync point 关闭 | 新 maintainer 于 08:58:28 初始化；旧 manager 清理已在约 08:58:02 完成 |
| 09:00 以后 | `.206`、`.194` 的 pending memory 固定在约 1 GiB | release feedback 日志持续高频出现，pending bytes 几乎不下降 |
| 09:30–12:40 | 三个 changefeed checkpoint lag 线性增长 | Event Service received resolved-ts lag 约 2–3 秒；部分 sent resolved-ts lag 达数小时；sink worker 未饱和 |
| 11:29 | 日志采集结束时故障仍在持续 | `.206` 目标节点仍有 1,908 个 add operator，最老约 2 小时 30 分钟 |
| 13:00–14:00 | 暂停全部 changefeed 后逐个恢复，checkpoint 最终恢复 | 用户确认的操作结果；当前日志集合未覆盖该时段 |

## 数据路径定位

监控把阻塞范围缩小到了 Event Service 输入和 sink 之间：

1. `Changefeed Resolved Ts Lag` 约为 2.9 秒，说明上游 resolved-ts 仍在正常生成。
2. Event Service 面板中，各节点 `received-resolvedts` 约为 2 秒；受影响节点的 `sent-resolvedts` 达到 5–6 小时，说明 Event Service 已收到时间推进，却无法持续把进度交付到后续 dispatcher 路径。
3. 三个 changefeed 的 maintainer、dispatcher manager resolved-ts lag 与 checkpoint lag 同步增长。
4. MySQL sink 的 worker busy ratio 在故障稳定期大多约 20%–35%，full flush duration 约 0.5 秒，worker input rows/s 较低。
5. 下游 MySQL 集群已由现场确认不存在写入瓶颈。

因此，sink 吞吐下降是上游 dispatcher 路径无法持续供给事件的结果。关闭 sync point 后出现的短时流量上升，说明额外 block event 压力曾被移除；Dynamic Stream 中已经形成的积压与失效反馈仍然存在，吞吐随后再次下降。

## 关键证据

### Dynamic Stream pending bytes 长期不下降

`cdc-recover-saas02-2000-2399` 对应的 Dynamic Stream area GID 为：

```text
131837078342867588713483817240584975288
```

`.206` 节点：

- 09:00:07：`totalPendingSize=1083555349`，`sizeToRelease=433422139`
  `logs/ticdc_10.107.15.206_8300.log:11565900`
- 11:29:58：`totalPendingSize=1083598669`
  `logs/ticdc_10.107.15.206_8300.log:21186138`

`.194` 节点：

- 09:03:33：`totalPendingSize=1073742219`
  `logs/ticdc_10.107.16.194_8300.log:13783138`
- 11:29:56：`totalPendingSize=1074105616`
  `logs/ticdc_10.107.16.194_8300.log:24237225`

两个节点都在反复执行 release，约 2.5 小时后的 pending bytes 仍停留在原量级。这是内存释放控制环失去进展的直接证据。

### 已移除 dispatcher 仍持续收到 release feedback

`.206` 上的样本 dispatcher：

```text
46309077457825334044444067147109924778
```

其生命周期为：

- 08:57:29：为 `cdc-recover-saas02-2000-2399`、table `741822` 创建。
- 创建后收到历史 sync point 事件。
- 08:58:00.563：remove 完成，见 `logs/ticdc_10.107.15.206_8300.log:10569933`。
- 09:00:07.400：仍收到 release feedback，见 `logs/ticdc_10.107.15.206_8300.log:11568971`。
- 11:29:58.703：仍收到 release feedback，见 `logs/ticdc_10.107.15.206_8300.log:21189194`。

final resume 后没有找到该 dispatcher ID 的重新 add 记录。`.194` 上的 dispatcher `845026021071138061916874440829272205464` 也呈现相同模式：08:58:01 已移除，release feedback 持续到 11:29。

这组证据表明，释放反馈中存在陈旧 path，且陈旧反馈不会促成 pending memory 下降。

### release 日志风暴集中在两个异常节点

四个日志文件中，`release dispatcher memory in DS` 出现次数为：

| 节点 | 次数 |
| --- | ---: |
| `.204` | 1,059,188 |
| `.206` | 10,588,242 |
| `.194` | 11,644,533 |
| `.183` | 1,987,747 |
| 合计 | 25,279,710 |

09:00 后，`.206` 和 `.194` 每 15 分钟各产生约 90–100 万条 release feedback 日志。重复 dispatcher ID 大约每 5 秒出现一次，每个异常节点涉及约 5,000 个持续重复的 path。日志量和 pending memory 平台期在节点维度吻合。

### 调度请求已发往四个节点，`.206` 的 add operator 大量停滞

调度日志中可以找到 CF `2000-2399` 向四个节点的 bind 记录，例如：

- `.204`：`logs/ticdc_10.107.16.194_8300.log:3666019`
- `.183`：`logs/ticdc_10.107.16.194_8300.log:2524997`
- `.194`：`logs/ticdc_10.107.16.194_8300.log:2524999`
- `.206`：`logs/ticdc_10.107.16.194_8300.log:2524993`

11:29 这一分钟内，日志出现 3,816 条 `operator is still in running queue`，对应 1,908 个唯一 add operator，即两轮完整告警。它们的目标节点均为 `.206`。最老 operator 的年龄约 `2h30m11s`，见 `logs/ticdc_10.107.16.194_8300.log:24242805`。

该证据说明 scheduler 已生成跨节点分配意图，`.206` 上的 add 流程长期无法完成。监控所见“两节点承担主要流量”与节点执行状态停滞一致。

### 日志放大量级

除 2,527 万条 release 日志外，四份日志还包含以下高频事件：

| 事件 | 约计数量 |
| --- | ---: |
| `operator is still in running queue` | 651,162 |
| out-of-order | 461,794 |
| reset dispatcher | 1,177,620 |
| sync point receive/set | 1,508,931 |
| resend | 418,285 |

这些事件共同反映出大量 dispatcher 在 block、reset、重发和 add 等控制阶段反复循环。完整日志治理和增强方案见[关联设计文档](../log-optimization/incident-observability-enhancement-design.md)。

## 源码机制

事故版本的关键调用链如下：

```text
Dynamic Stream pending memory 超过 area quota
  -> memoryControl.releaseMemory
  -> 对 area.pathMap 中每个 path 同步发送 Feedback
  -> EventCollector.processDSFeedback
  -> DynamicStream.Release(path)
  -> path 已不存在时静默返回
```

具体机制：

1. [`utils/dynstream/memory_control.go`](../../utils/dynstream/memory_control.go) 的 `releaseMemory` 先通过 `lastReleaseTime.Load` / `Store` 控制时间间隔。该组合没有 CAS 或 singleflight 语义，多个调用者可以同时通过检查并生成重复 batch。
2. 同一函数对 `area.pathMap` 建立全量快照，再逐 path 同步写入 feedback channel。path 数达到数千时，单次 release 会生成数千条反馈。
3. `removePathFromArea` 只减少 pending size 和 path count，没有从 `area.pathMap` 删除对应项。内存控制视图因此能够继续枚举已经移除的 dispatcher。
4. [`utils/dynstream/parallel_dynamic_stream.go`](../../utils/dynstream/parallel_dynamic_stream.go) 中 feedback channel 容量为 1024；大量同步写入会形成反压。
5. 同一文件的 `Release` 在主 path map 找不到 path 时直接返回。陈旧 feedback 变成静默空操作，当前指标无法观测其数量。
6. `releaseMemory` 在发送完 feedback 后无条件更新 `lastSizeDecreaseTime`，即使 feedback 最终全是空操作、pending bytes 没有实际下降。后续 deadlock 判断会把一次“请求释放”当成近期已有内存进展。
7. [`downstreamadapter/eventcollector/event_collector.go`](../../downstreamadapter/eventcollector/event_collector.go) 的 `processDSFeedback` 对每条 feedback 写一条 INFO 日志，然后调用 `Release`。每节点只有一个共享 Event Collector Dynamic Stream，异常会影响多个 changefeed。

这些行为可以形成自我维持的失效循环：area 保留陈旧 path，释放周期不断为其产生 feedback；主 path map 已删除对应 dispatcher，消费端执行空操作；pending bytes 仍不下降，下一个释放周期继续生成同一批反馈。

当前日志无法精确区分每条反馈是新生成、重复并发生成，还是在 channel 中延迟已久的预计算 batch。该不确定性不影响“释放反馈持续发生且 pending memory 无进展”的故障结论。

## master 分支核查

核查使用的官方 master revision 为 `006b1df37a241e1e960d36c4395c7f01d76254a2`，本地 `master` 与 `git ls-remote upstream refs/heads/master` 返回值一致。事故版本 `452a3eacd01c3544dcc53612b4fc03c39d3a3774` 来自独立的 `release-8.5-20260808-v8.5.4` 分支，两者没有祖先关系。

master 已通过 commit `836bc7701bd7f25a185a42e30a1d663a08a79353`（`dynstream: fix removed-path memory accounting after RemovePath`）切断本次事故最关键的陈旧 path 循环：

- `removePathFromArea` 使用 `path.pendingSize.Swap(0)` 结算被移除 path 的 pending bytes。
- `removePathFromArea` 调用 `area.pathMap.Delete(path.path)`，后续 `releaseMemory` 快照不再枚举该 dispatcher。
- `appendEvent` 检查 `path.removed`，拒绝 remove 期间到达的晚到事件，避免被删除 path 再次污染 area pending bytes。
- 对应测试 `TestRemovePathLateAppendDoesNotPolluteAreaPendingSize` 覆盖 remove 与晚到 append 的内存统计一致性。

事故分支不包含该 commit。事故版本中的 `removePathFromArea` 只减少 pending size 和 path count，导致已删除 dispatcher 长期留在 `area.pathMap`。因此，master 不具备本次事故中“同一个已删除 dispatcher 被持续数小时重新加入 release batch”的源码条件。

已经进入 `releaseMemory` 快照的 path 仍可能在并发 remove 后产生有限数量的陈旧 feedback。后续 release 周期不会再次从 `area.pathMap` 选中该 path，反馈数量应随在途 batch 排空而收敛。

master 仍保留以下控制环风险：

- `lastReleaseMemoryTime` 使用独立的 `Load` 和 `Store`，多个 stream worker 可以同时通过一秒门控并生成重复 release batch。
- release feedback 逐 path 同步写入容量为 1024 的 channel，大规模 batch 可以阻塞生成 feedback 的 stream worker。
- `releaseMemory` 在发送完 feedback 后无条件更新 `lastSizeDecreaseTime`，此时 pending bytes 可能尚未实际下降。
- `Release` 找不到主 path map 中的 path 时静默返回，缺少 no-op 计数和 generation 校验。
- Event Collector 仍为每条 release feedback 输出 INFO 日志，反馈风暴会放大日志 I/O。

这些风险可以在大量活跃且 blocking 的 dispatcher 下产生重复 release、channel 反压和高日志量。源码分析尚不能证明它们会在 master 上形成与本次事故相同的数小时 checkpoint 停滞。风险等级判断如下：

- 已删除 dispatcher 被无限重复 release：关键缺陷已修复，源码路径已切断。
- 活跃 dispatcher 在高内存压力下出现 release 风暴：仍有中等风险。
- master 在 75,000 span、同时 bootstrap 和 sync point 历史事件下的整体进展性：需要规模化压力测试确认。

commit `836bc7701b` 的三个文件补丁已对事故 revision 执行 `git apply --check --verbose`，检查通过，未发现文本冲突。这只证明补丁可以干净应用；release 分支仍需完成编译、单元测试和规模化回归测试。

本次尝试从 master 导出源码并运行以下目标测试：

```bash
go test ./utils/dynstream \
  -run 'TestMemControlAddRemovePath|TestRemovePathLateAppendDoesNotPolluteAreaPendingSize|TestReleaseMemory' \
  -count=1
```

测试环境在编译 TiDB 依赖时因 `checkMapABI` 未定义而失败，目标测试没有实际执行。当前结论来自 master 源码、提交历史和现有测试内容的静态核查，未将该命令计为通过验证。

## sync point 的作用

`cdc-recover-saas02-2000-2399` 于 08:56:06 以 `syncPoint=true` resume。日志显示新建 dispatcher 随即收到历史 sync point block event，包括 checkpoint 起点及其后续间隔点。这一阶段产生了大规模 block、reset、handshake 和 resend 控制事件。

08:58:27 final resume 时 sync point 已关闭。关闭配置阻止了后续新 sync point 事件继续产生，因此 sink 流量一度回升。08:56–08:58 已经进入共享 Dynamic Stream 的事件与 release feedback 仍需处理；旧 dispatcher 被移除后，其陈旧 path 又继续出现在 release batch 中，所以单独关闭 sync point 没有让控制环恢复进展。

sync point 是本次故障的重要压力放大因素。根本失效机制仍是 Dynamic Stream 对高基数 path 生命周期和释放反馈缺乏可收敛保证。

## MySQL sink 与 `ticdc_sink_batch_row_count` 排除项

`ticdc_sink_batch_row_count` 记录成功写入 batch 的行数。其 histogram bucket 为 `1, 2, 4, ..., 128, 256, ...`。Grafana 上 p999 接近 250，表示分位值通过 `(128, 256]` bucket 插值得到，没有 250 行的硬上限。

事故配置中的 `max-txn-row=32` 控制多个 DML event 合并 batch 时的阈值。单个 DML event 自身超过 32 行时仍会整体 flush，因此 p999 接近 250 与该参数并不冲突。可结合 `ticdc_sink_txn_worker_event_row_count` 判断是否存在大事务事件。

该指标、sink worker busy ratio、flush duration、input rows/s 以及现场对 MySQL 集群的确认共同支持：MySQL sink 未达到写入能力上限，无法解释 checkpoint lag 的线性增长。

## 重启和单个 changefeed pause / resume 未能持续恢复的原因

### 重启 CDC 节点

重启会清除进程内队列和 path 状态，所有大 changefeed 随后从持久化 checkpoint 同时 bootstrap。高基数 dispatcher 创建、历史事件扫描、block event 处理和跨节点 add 再次集中发生，现场条件随启动过程重新形成。重启带来的恢复窗口因此很短。

### 单独 pause / resume changefeed

单独 pause 能移除该 changefeed 的 dispatcher，节点级 Event Collector Dynamic Stream 仍由其他 changefeed 共用。已经生成或排队的 feedback 也没有 generation、batch ID 或 cancel 语义，旧生命周期的反馈可以在新 maintainer 启动后继续被消费。

`2000-2399` 的样本 dispatcher 已在 08:58 删除，其 feedback 仍持续到 11:29，直接证明旧生命周期工作没有随 pause 完整失效。final resume 又立即创建约 75,700 个 span，使共享控制环缺少自然排空窗口。

### 暂停全部 changefeed 后逐个恢复

现场确认 13:00–14:00 执行“全部暂停，再逐个恢复”后，所有 checkpoint lag 最终恢复。结合已确认机制，最符合证据的解释为：

1. 全部暂停停止了节点级 Event Collector 的新增输入，为旧 feedback 和事件队列提供排空时间。
2. 所有 dispatcher 被移除后，共享 path 状态得到更完整的回收机会。
3. 逐个恢复限制了同时 bootstrap 的 dispatcher 数量和瞬时内存压力。
4. sync point 保持关闭，避免重新引入历史 block event 波峰。

这部分属于高置信机制推断。补充 13:00–14:00 日志后，应验证 pending bytes 是否降至低水位、陈旧 dispatcher feedback 是否停止、add operator 是否逐批清零。

## 根因链路

```text
多个高基数 changefeed 同时 bootstrap
  + 2000-2399 短暂开启 sync point，历史 block event 集中进入
  -> dispatcher add / reset / resend 与 Event Collector event 激增
  -> .206、.194 的 Dynamic Stream pending memory 达到约 1 GiB
  -> releaseMemory 为数千 path 反复生成 feedback
  -> 已移除 path 仍被枚举，Release 对缺失 path 静默空操作
  -> pending bytes 长期无下降，feedback 与 INFO 日志持续放大
  -> dispatcher add 和事件消费无法完成
  -> sent resolved-ts、dispatcher checkpoint 停滞
  -> changefeed checkpoint 受最慢 dispatcher 限制，lag 线性增长
  -> sink 缺少输入，写入流量下降
```

## 修复与验证建议

### P0：修复进展性和 path 生命周期

1. `removePathFromArea` 同步删除 `area.pathMap` 中的 path，并增加断言或一致性指标，确保 `tracked path count` 能回落。
2. `releaseMemory` 使用 CAS、singleflight 或明确的 in-flight 状态，保证同一 area 同一时刻只有一个 release batch。
3. release feedback 携带 batch ID、path generation 和预期释放字节；已删除或 generation 不匹配的反馈计为空操作，并聚合上报。
4. feedback 发送采用有界、可取消的批处理；避免在持有关键状态或扫描全量 path 时同步阻塞。
5. 仅在 pending bytes 实际下降后更新 `lastSizeDecreaseTime`；将请求释放时间与确认释放进展时间拆成两个状态。
6. 当连续多个 release 周期没有降低 pending bytes 时，进入明确的 stalled 状态并触发告警。

### P0：建立可验证指标

至少增加以下低基数指标：

- Dynamic Stream `pending_bytes`、`active_path_count`、`tracked_path_count`。
- release batch 的 requested / applied / no-op path 数与实际释放字节。
- feedback channel length、最老 feedback age、in-flight batch 数。
- dispatcher operator 按 changefeed、目标节点、阶段聚合的 pending count 与 oldest age。
- Event Service quota skip 次数、持续时间和受影响 changefeed 数。

### P1：安全运行策略

- 超大 changefeed 恢复时分批或错峰 bootstrap，限制每节点同时创建的 dispatcher 数。
- pause / resume 后等待旧 manager、operator、path 和 feedback 全部清零，再启动下一批。
- 关闭 sync point 的变更应记录 operation ID、旧值、新值、生效时间和剩余历史 block 数。
- 临时止损时采用“全部暂停、确认共享队列排空、逐个恢复”的顺序，并观察 pending bytes 和 operator backlog。

### 验证标准

修复后的压力测试应覆盖 75,000 级 span、四节点、同时 resume、sync point 历史事件与 pause / resume 交错。通过标准为：

- pending memory 达到高水位后能在有限时间内持续下降。
- dispatcher remove 后不再产生该 generation 的有效 release。
- release no-op 比例接近零，且异常时可直接观测。
- feedback backlog 有界，最老 age 不持续增长。
- 任一节点负载异常时，operator backlog 能定位到阶段并最终清零。
- sink 仍有容量时，checkpoint lag 不随墙钟时间持续线性增长。

## 证据限制与待补充项

- 13:00–14:00 恢复阶段缺少日志，当前只能基于操作结果和前序机制解释恢复过程。
- 未采集 CPU profile 和 goroutine dump，无法量化 feedback 日志、channel 阻塞和实际事件处理各自消耗的 CPU / goroutine 时间。
- bind 日志包含重试和重绑，不能直接用累计 bind 次数计算最终 dispatcher 分布。
- 当前日志没有 release batch ID，无法精确拆分重复生成和队列延迟的占比。
- Event Service `skip scan due to quota` 需要结合新增的 changefeed 维度指标，才能量化它在三个 changefeed 中的相对贡献。

## 附带安全发现

事故日志包含由 MySQL sink 配置输出的完整 sink URI。源码位于 [`pkg/sink/mysql/config.go`](../../pkg/sink/mysql/config.go)，当前使用 `sinkURI.String()` 写日志，可能暴露用户名、密码和连接参数。本文没有复制任何凭据。该日志应改为经过脱敏的 endpoint 与非敏感配置摘要，历史事故日志应按敏感数据处理并完成凭据轮换评估。
