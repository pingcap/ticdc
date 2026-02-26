<!-- Copyright 2026 PingCAP, Inc. -->

# Recover Dispatcher 需求文档

- Date: 2026-02-25
- Status: Draft
- Scope: Kafka sink transient error 的 dispatcher 级恢复

## 1. 背景

当前 Kafka sink 在遇到部分临时性错误时，容易被放大到 changefeed 级别重启，导致影响面过大、恢复时间长。  
本需求希望把可局部自愈的问题收敛到 dispatcher 级别处理。

### 1.1 现状错误传播链路（归并）

当前错误放大链路可抽象为：
1. `sarama async producer` 返回发送错误；
2. `kafka sink` 的 callback goroutine 返回 error，触发 `errgroup` 收敛退出；
3. `DispatcherManager` 将 sink 退出当作 changefeed 级错误上报；
4. `maintainer/coordinator` 进入 changefeed 级 backoff/restart。

需求目标是把“可恢复 transient error”从这条放大链路中剥离出来，仅触发 dispatcher 级恢复。

## 2. 目标

1. 将可恢复的 Kafka transient error 优先收敛到 dispatcher 重启。
2. 降低影响面：单 dispatcher 异常不应直接拖垮整条 changefeed。
3. 保证可终止：recover 请求若未被有效处理，必须有确定兜底（changefeed 重启）。
4. 保证可观测：链路关键节点有明确日志与指标。

## 3. 非目标

1. 本轮不改动 coordinator 调度语义。
2. 本轮不做 sink 全类型统一接入（先保证 Kafka 场景正确）。
3. 本轮不追求“零额外重启次数”，优先保证正确性与鲁棒性。

## 4. 功能性需求

### R1 上报语义

Kafka producer 识别 recoverable transient error 后，上报 dispatcher 集合与对应 epoch。

### R2 去重语义

同一 dispatcher 在同一 epoch 只允许触发一次 recover 上报。  
同一批次输入若包含重复 dispatcher，按最大 epoch 归并。

### R3 DM pending 语义

DispatcherManager 对同一 dispatcher 同时最多保留一条 pending recover 流程。

### R4 重试语义

pending recover 在未进入终态前需周期重发；不可因偶发发送失败而静默丢失。

### R5 回执语义

Maintainer 对每个 recover identity 都要给出明确响应状态（ACCEPTED/RUNNING/SUPERSEDED/FAILED）。

### R6 清理语义

pending 清理必须由确定事件驱动：
1. remove + recreate 生命周期事件；
2. FAILED 终态；
3. timeout 最后兜底。

### R7 maintainer 切换语义

maintainer 切换后，DM 需要同步 maintainer ID 和 epoch，并使 pending recover 可继续被新 maintainer 正确处理。

### R8 来源校验语义

recover response 必须校验消息来源是“当前 maintainer”，否则丢弃。

### R9 epoch 稳定性语义

recover identity 使用 `(dispatcherID, dispatcherEpoch, maintainerEpoch)`，其中 dispatcherEpoch 对同 dispatcher 必须具备单调语义（跨重建不回退）。

### R10 兜底语义

若 recover 链路无法在超时时间内完成，必须触发 changefeed 级 retryable 重启路径，不能卡死。

## 5. 非功能性需求

1. 正常路径开销可控：不影响主吞吐路径。
2. 内存可控：pending/reporter 状态空间可预估。
3. 可维护性：状态机与责任边界清晰，避免隐式时间推断。

## 6. 验收标准

1. 同 epoch 连续 transient error 不会产生重复 recover 请求。
2. maintainer 切换后 recover 不会提前停止重试。
3. stale maintainer response 不会污染 pending 状态。
4. merge/split/move 并发下 pending 可被正确清理。
5. recover 失败可确定触发 changefeed 级兜底。
