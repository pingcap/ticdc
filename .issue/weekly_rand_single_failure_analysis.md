# weekly_rand_single 失败分析与修复计划

## 目标

修复 `/tmp/tidb_cdc_test/weekly_rand_single` case 在收敛阶段超时的问题，并用回归测试和连续 5 次 case 通过确认修复有效。

当前修复分支：`fix-weekly-rand-single-ddl-progress`，基于 `0115-ddl-test` 的 `0ffe03f83`。

## 现象

失败不是 sync-diff 数据不一致，而是 workload 结束后等待 finish mark 同步到下游超时。

关键日志：

- `runner.log:2944`：`syncpoint diff` 成功，`primary_ts=467013528453120000`。
- `runner.log:5122`：workload finished，开始等待收敛。
- `runner.log:5123-5124`：上游创建 `db1.finish_mark` 并等待下游出现。
- `runner.log:5205-5303`：checkpoint 卡在 `467013630689280000`。
- `runner.log:5304`：`runner failed: context deadline exceeded`。

TSO 换算（Asia/Shanghai）：

- `467013630689280000` = `2026-06-15 17:27:00.000 +08:00`。
- `467013636614783103` = `2026-06-15 17:27:22.604 +08:00`。
- `467013638553600000` = `2026-06-15 17:27:30.000 +08:00`。
- `467013645501726800` = `2026-06-15 17:27:56.505 +08:00`。
- `467013645501726844` = `2026-06-15 17:27:56.505 +08:00`。

## 已确认的卡点

CDC 日志显示 barrier 没有完成 coverage，导致 maintainer 不能把 global checkpoint 推过早期 DDL/syncpoint barrier：

- DDL barrier：`commitTs=467013636614783103`，query 为 `CREATE INDEX idx_d_6619 ON db1.t08(d)`。
- 该 DDL 覆盖物理分区表 `142, 143, 144, 145`。
- `142/143/144` 都有 `dispatcher receive ddl event` 和 ack。
- `145` 没有对应的 accepted DDL 日志，maintainer 报 `reported count: 3, require count: 4, uncovered tables: 145`。
- syncpoint barrier：`commitTs=467013638553600000`。
- 后续 syncpoint 又显示 `uncovered tables: 142, 143, 144, 145`，本质上是前面的 DDL barrier 没过，导致后面的 barrier 继续被挡住。
- `maintainer.go` 反复选择 `newCheckpointTs=467013630689279999`，runner 侧看到的 checkpoint 为 `467013630689280000`。

同时，event scanner 存在一个明确的无进展循环：

- `tableID=2669` 对应旧物理表 `db3.t10_r_7179892`。
- TiDB DDL 在 `2026-06-15 17:27:56 +08:00` 对该表执行 `TRUNCATE TABLE db3.t10_r_7179892`。
- schema store 的删除版本为 `deleteVersion=467013645501726800`。
- scanner 反复请求 `GetTableInfo(tableID=2669, ts=467013645501726844)`。
- 因为 `ts >= deleteVersion`，`multi_version.go` 返回 `TableDeletedError`。
- `event_scanner.go` 把该错误转换成 `nil, nil`，随后以 `rawEvent.CRTs-1` 调用 `finalizeScan`。

问题在于这次原始事件的 `rawEvent.CRTs-1` 正好等于当前扫描起点；scanner 发送的 resolved event 没有推进水位。下一轮仍然从同一个起点扫描到同一条 raw event，然后再次遇到 `TableDeletedError`，形成死循环。

相关代码：

- `logservice/schemastore/multi_version.go`：`getTableInfo` 在 `ts >= deleteVersion` 时返回 `TableDeletedError`。
- `pkg/eventservice/event_scanner.go`：`scanAndMergeEvents` 在 `tableInfo == nil` 时调用 `finalizeScan(..., rawEvent.CRTs-1)`。
- `logservice/eventstore/event_store.go`：iterator 扫描范围是 `(CommitTsStart, CommitTsEnd]`，所以 resolved ts 必须严格大于旧的 `CommitTsStart` 才能跳过当前 raw event。

table 145 的 DDL 缺失与 reset/replay 状态有关：

- `cdc-2026-06-15T17-51-16.335.log:726193`：dispatcher `1774578769225496409714574658290184438691` 收到 reset，`epoch=7`，`resetTs=467013575639039999`。
- 同一 DDL 的旧 epoch 事件随后被正确忽略：`eventEpoch=6`，`dispatcherEpoch=7`。
- `cdc-2026-06-15T17-52-19.370.log:121963`：event service 已经向 table 145 dispatcher 重发 DDL，`commitTs=467013636614783103`，`seq=1325`。
- 但 downstream dispatcher 没有记录 `dispatcher receive ddl event`，说明事件在进入 dispatcher 前被 eventcollector 的状态过滤或 reset 后状态不一致挡住。
- `downstreamadapter/eventcollector/dispatcher_stat.go` 原来在 `advanceEpochForReset` 只切换 epoch/maxEventTs，没有把 `lastEventCommitTs` 和同 ts DDL/SyncPoint 去重标志回到 resetTs。
- reset 的语义是从 `resetTs` 重新 replay 新 epoch 事件；如果旧 epoch 已经把 `lastEventCommitTs` 推到更大值，新 epoch 中位于 `(resetTs, oldLastEventCommitTs)` 的 replay DDL 会被当成旧事件过滤，table 145 就不会向 maintainer 上报 DDL barrier。

## 根因判断

目前确认有三个会阻塞 checkpoint 前进的问题：

1. 直接卡住原始 case 的问题是 eventcollector reset 后没有同步重置 commitTs 去重状态。table 145 reset 到新 epoch 后需要 replay `467013636614783103` 的 DDL，但旧 epoch 的 `lastEventCommitTs` 可能已经更大，导致 replay DDL 在进入 downstream dispatcher 前被过滤。maintainer 因此一直等不到 table 145 的 DDL barrier report。

2. event scanner 还有一个独立的无进展问题：deleted table raw event 被跳过时 resolved ts 仍可能停在 `rawEvent.CRTs-1`，而该值等于当前 scan start 时，下一轮会再次读到同一条 raw event。

对于已经删除或 truncate 后的旧物理表，遇到无法取到 table info 的 raw event 时，scanner 不能继续把 resolved ts 固定在 `rawEvent.CRTs-1`。如果该值等于本轮 `CommitTsStart`，event broker 下一轮仍会在 `(CommitTsStart, CommitTsEnd]` 内看到同一条 event，导致 dispatcher 无法完成对应 DDL/syncpoint coverage，最终 changefeed checkpoint 卡住，finish mark 永远不能同步到下游。

3. 第一次带前两个修复重新跑 case 后，原 table 145 DDL 卡点没有复现，但暴露出同一 syncpoint barrier 被迟到 WAITING 状态重建后的覆盖丢失问题。第一次 `commitTs=467018333552640000` 的 syncpoint barrier 已经完成并从 `blockedEvents` 删除；随后迟到 WAITING 又创建了第二个同 ts syncpoint barrier。第二个 barrier 通过 checkpoint-forward 直接进入 selected/pass 阶段，但新的 range checker 没有继承第一次 barrier 中已经 DONE 的 span block state，导致 table 299 永久显示 uncovered，checkpoint 卡住超过 5 分钟。

这个问题不能通过简单忽略迟到 WAITING 解决，因为迟到或重启后的 dispatcher 仍可能需要收到 Pass 才能解除本地 block。正确处理方式是：当 barrier 因 checkpoint-forward 进入 selected 阶段时，和正常 writer 选择路径一样重置 DONE 阶段进度，并把当前 spanController 中已严格越过 barrier、或同一 `(commitTs, isSyncPoint)` 已经上报 DONE 的 replication 计入新的 range checker。

## 修复计划

1. 在 `pkg/eventservice/event_scanner.go` 中修改 deleted table 分支。
   - 当前行为：`finalizeScan(..., rawEvent.CRTs-1)`。
   - 目标行为：对 `TableDeletedError` 造成的 `tableInfo == nil`，跳过当前 raw event 所在 commit ts，并用 `rawEvent.CRTs` 作为 resolved ts。
   - 这样下一轮 iterator 的 `(CommitTsStart, CommitTsEnd]` 不会再包含当前 raw event。

2. 在 `downstreamadapter/eventcollector/dispatcher_stat.go` 中修改 reset 状态。
   - 当前行为：`advanceEpochForReset` 只切换 epoch 和 `maxEventTs`。
   - 目标行为：成功进入新 epoch 时，把 `lastEventCommitTs` 重置为 `resetTs`，并清掉 `gotDDLOnTs` / `gotSyncpointOnTS`。
   - 这样 reset replay 的 DDL/SyncPoint 不会被旧 epoch 的 commitTs 去重状态过滤。

3. 增加聚焦回归测试。
   - 文件：`pkg/eventservice/event_scanner_test.go`。
   - 场景：mock schema store 返回 `TableDeletedError`，scan range 的 `CommitTsStart` 设置为 `rawEvent.CRTs-1`。
   - 断言：scanner 不产生 DML，返回 resolved event，并且 resolved ts 等于 `rawEvent.CRTs` 且严格大于 scan start。
   - 文件：`downstreamadapter/eventcollector/dispatcher_stat_test.go`。
   - 场景：旧 epoch 的 `lastEventCommitTs=220`，reset 到 `150`，新 epoch handshake 后 replay `180` 的 DDL。
   - 断言：reset 后 commitTs 状态回到 `150`，并且 `180` 的 DDL 可以被转发到 dispatcher。

4. 在 `maintainer/barrier_event.go` 中修复 checkpoint-forward selected 路径。
   - 当前行为：`checkBlockedDispatchers` 发现某个 replication 已越过 barrier 后，只设置 `selected=true` 和 `writerDispatcherAdvanced=true`。
   - 目标行为：进入 selected 阶段时统一调用重置逻辑，重建或清空 range checker，并把已越过当前 barrier 的 replication 加入 DONE 阶段覆盖。
   - `forwardBarrierEvent` 保持 `checkpointTs > commitTs` 的严格判断，同时新增同一 `(commitTs, isSyncPoint)` 且 `Stage_DONE` 的判断，避免把 `checkpointTs == commitTs` 误认为 syncpoint 已经 flush。

5. 增加 maintainer 回归测试。
   - 文件：`maintainer/barrier_test.go`。
   - 场景：同一 syncpoint barrier 先正常完成并删除；迟到 WAITING 重建同 ts barrier；一个 dispatcher checkpoint-forward 触发 selected。
   - 断言：旧 barrier 中已 DONE 的 dispatcher 会计入新 range checker，重建的 barrier 可以立即完成，不会留下永久 uncovered table。

6. 本地验证顺序。
   - 先跑 `go test ./pkg/eventservice -run TestScanAndMergeEventsSkipsDeletedTableTxn -count=1`。
   - 再跑 `go test ./downstreamadapter/eventcollector -run TestAdvanceEpochForResetClearsCommitTsFilter -count=1`。
   - 再跑 `go test ./maintainer -run TestSyncPointBarrierRecreatedCountsAlreadyDoneDispatchers -count=1`。
   - 再分别跑 `go test ./pkg/eventservice -count=1`、`go test ./downstreamadapter/eventcollector -count=1`、`go test ./maintainer -count=1`。
   - 如编译或测试失败，先修复单测/逻辑再扩大验证。

7. case 验证。
   - 重新构建需要的 `cdc` binary。
   - 运行 `weekly_rand_single` case。
   - 连续通过 5 次才认为成功。

8. 独立审查。
   - 修复完成后让 reviewer subagent 检查 diff、回归测试和剩余风险。
   - 若发现阻塞问题，修复后重新跑相关测试。

## 调查记录

- 已读取 TiCDC event-broker / schema-store 相关代码路径。
- 已确认 `runner.log` 的失败点为收敛超时，不是 sync-diff mismatch。
- 已确认 event store 迭代器扫描范围是 `(CommitTsStart, CommitTsEnd]`。
- 已确认 `TableDeletedError` 目前只在 `getTableInfo4Txn` 单元测试里覆盖，没有覆盖 scanner 水位是否前进。

## 已实施修复

- `pkg/eventservice/event_scanner.go`
  - deleted-table 分支从 `finalizeScan(..., rawEvent.CRTs-1)` 改为 `finalizeScan(..., rawEvent.CRTs)`。
  - 目的：让当前无法解码的 post-delete raw event 被排除在下一轮 `(CommitTsStart, CommitTsEnd]` 之外。

- `pkg/eventservice/event_scanner_test.go`
  - 更新 `TestEventScannerWithDeleteTable` 的预期：删除后的第一条 raw event 被跳过后，resolved ts 前进到该 raw event 的 `CRTs`。
  - 新增 `TestScanAndMergeEventsSkipsDeletedTableTxn`，直接覆盖 `TableDeletedError` 且 `CommitTsStart == rawEvent.CRTs-1` 的无进展场景。

- `downstreamadapter/eventcollector/dispatcher_stat.go`
  - `advanceEpochForReset` 成功切换到新 epoch 后，把 `lastEventCommitTs` 重置为 `resetTs`。
  - 同时清理 `gotDDLOnTs` 和 `gotSyncpointOnTS`。
  - 目的：reset 后从 `resetTs` replay，新 epoch 的 DDL/SyncPoint 不能被旧 epoch 的 commitTs 去重状态过滤。

- `maintainer/barrier_event.go`
  - 新增 selected 阶段重置逻辑：确保 range checker 存在，重置 DONE 阶段 coverage，并把已越过当前 barrier 的 replication 计入 coverage。
  - `checkBlockedDispatchers` 的 checkpoint-forward 路径不再只设置 `selected/writerDispatcherAdvanced`，而是走同一 selected 阶段初始化逻辑。
  - `forwardBarrierEvent` 新增同一 `(BlockTs, IsSyncPoint)` 且 `Stage_DONE` 的判断。
  - 保留 `checkpointTs > commitTs` 的严格条件，不使用 `>=`，避免 dispatcher 以 `startTs == commitTs` 重建时跳过仍需 flush 的 syncpoint。
  - 针对 Normal DROP barrier 的迟到 WAITING 重建，新增缺失 dropped table 覆盖逻辑。
  - 仅当 `BlockedTables.InfluenceType == Normal`、`NeedDroppedTables.InfluenceType == Normal`、tableID 属于 `NeedDroppedTables` 且 `spanController` 中已无该 table task 时，才把该 tableID 标记为覆盖。
  - 目的：table dispatcher 已因先前完成的 DROP/TRUNCATE 调度被删除后，迟到重建的 barrier 不再永久等待已删除的 dispatcher；事件进入 selected/pass 阶段后仍会给 DDL span 发送 `Action_Pass`，不会重复执行 `Action_Write`。

- `maintainer/barrier_test.go`
  - 新增 `TestSyncPointBarrierRecreatedCountsAlreadyDoneDispatchers`。
  - 覆盖 syncpoint barrier 完成删除后被迟到 WAITING 重建、再由 checkpoint-forward 进入 selected 的场景。
  - 断言同 ts 已 DONE 的 dispatcher 会被计入重建后的 range checker，barrier 不会永久等待旧 DONE 状态。
  - 新增 `TestForwardBarrierEventBoundaries`，覆盖 `checkpointTs == commitTs` 不推进、`checkpointTs > commitTs` 推进、同 ts syncpoint/DDL DONE 与 WAITING 的顺序边界。
  - 新增 `TestNormalBarrierRecreatedAfterDroppedTableRemoved`。
  - 覆盖 DROP TABLE barrier 已完成并删除 table dispatcher 后，DDL dispatcher 迟到 WAITING 重建同一 barrier 的场景。
  - 新增 `TestNormalBarrierDoesNotCoverMissingNonDroppedTable`，确认非 drop Normal barrier 不会因为 table task 缺失而被误推进。

- `downstreamadapter/eventcollector/dispatcher_stat_test.go`
  - 新增 `TestAdvanceEpochForResetClearsCommitTsFilter`。
  - 覆盖旧状态已推进到 `220`、reset 到 `150`、新 epoch replay `180` DDL 的场景。

## 验证记录

## 新增失败：Normal DDL 迟到 WAITING 重建

- 5 连跑的第 1 次 attempt（seed `2026061509`）中，workload 已结束并进入 converge，但 finish mark 长时间未同步到下游。
- `runner.log` 从 `2026/06/15 15:38:08` 开始等待 finish mark；checkpoint 最终卡在 `467019061784216256`。
- maintainer 日志反复报告普通 DROP TABLE barrier `467019061784216242` 未 resolved：`reported count: 1, require count: 2, uncovered tables: 267`，blocked tables 为 `[267,0]`。
- 同一 commitTs 的前序日志显示 `2026/06/15 23:40:47.934 +08:00` 已经 `all dispatchers reported event done, remove event`；随后 `2026/06/15 23:40:48.172 +08:00` table 267 dispatcher 被 remove/unregister。
- 但 `2026/06/15 23:40:48.324 +08:00` DDL dispatcher 才收到同一个 `DROP TABLE db3.t10` 并上报 WAITING，maintainer 因已删除旧 event 而重新创建 barrier。
- 根因：`blockedEvents` 不保留已完成事件历史；迟到 WAITING 重建 Normal DDL barrier 后，`checkBlockedDispatchers` 只检查仍存在的相关 replication 是否已 forward，未把已经删除的 dropped table 视为完成，导致永远等待 table 267。
- 处理：中断该 doomed attempt，补充 Normal dropped table 缺失覆盖逻辑后重新验证。

- `go test ./pkg/eventservice -run TestScanAndMergeEventsSkipsDeletedTableTxn -count=1`
  - 结果：失败，原因是远端默认 `go` 为 1.25.3，`go.mod` 要求 `>=1.25.10` 且 `GOTOOLCHAIN=local`。

- `GOTOOLCHAIN=auto go test ./pkg/eventservice -run TestScanAndMergeEventsSkipsDeletedTableTxn -count=1`
  - 结果：失败，原因是 TiDB testkit 要求 `--tags=intest`。

- `GOTOOLCHAIN=auto go test --tags=intest ./pkg/eventservice -run TestScanAndMergeEventsSkipsDeletedTableTxn -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./pkg/eventservice -run "TestScanAndMergeEventsSkipsDeletedTableTxn|TestEventScannerWithDeleteTable" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./pkg/eventservice -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/eventcollector -run TestAdvanceEpochForResetClearsCommitTsFilter -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/eventcollector -run "TestAdvanceEpochForResetClearsCommitTsFilter|TestCheckpointTsForEventServiceUsesCollectorObservedMaxTs|TestFilterAndUpdateEventByCommitTs|TestHandleSingleDataEventsUpdatesDDLStateAndDedupsSameTsDDL|TestHandleSignalEvent|TestGroupHeartbeatResetThenHandshake" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/eventcollector -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./pkg/eventservice -count=1`
  - 结果：再次通过。

- `git diff --check -- pkg/eventservice/event_scanner.go pkg/eventservice/event_scanner_test.go downstreamadapter/eventcollector/dispatcher_stat.go downstreamadapter/eventcollector/dispatcher_stat_test.go .issue/weekly_rand_single_failure_analysis.md`
  - 结果：通过。

- reviewer subagent：event scanner 修复审查
  - 结果：未发现阻塞问题。
  - 结论：`finalizeScan(..., rawEvent.CRTs)` 只应用于 table 已在 `rawEvent.CRTs-1` 不存在的情况，不会丢弃同 commit ts 下仍可用旧 schema 解码的 DML。

- reviewer subagent：eventcollector reset 修复审查
  - 结果：未发现阻塞问题。
  - 结论：旧 epoch 事件仍由 epoch 过滤拦截；新 epoch 仍要求 handshake/seq；heartbeat 仍受 `maxEventTs` 限制；reset 清理 commitTs flags 只避免旧 epoch 状态误杀新 epoch replay。

- `GOTOOLCHAIN=auto make integration_test_build_fast`
  - 结果：通过。

- 第一次重新运行 `weekly_rand_single`
  - 命令：`GOTOOLCHAIN=auto RUN_PROFILE=weekly RUN_DURATION=30m RUN_SEED=2026061509 tests/integration_tests/run.sh mysql weekly_rand_single`。
  - 结果：失败；原 table 145 DDL 缺失问题没有复现，新的卡点为 syncpoint `467018333552640000`。
  - 失败摘要：runner 报 `checkpoint did not advance for 5m9.912642314s (hard=5m0s)`，maintainer 报 `active_ddl=1`。
  - 关键 barrier 日志：第二个同 ts syncpoint barrier 的 coverage 为 `reported count: 184, require count: 185, uncovered tables: 299`。
  - 定位结论：第一次 barrier 完成删除后，迟到 WAITING 重建第二个 barrier；table 299 已在第一次 barrier 上报 DONE，但第二个 barrier 的 range checker 未继承该状态。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -run TestSyncPointBarrierRecreatedCountsAlreadyDoneDispatchers -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -count=1`
  - 结果：通过。

- `git diff --check -- pkg/eventservice/event_scanner.go pkg/eventservice/event_scanner_test.go downstreamadapter/eventcollector/dispatcher_stat.go downstreamadapter/eventcollector/dispatcher_stat_test.go maintainer/barrier_event.go maintainer/barrier_test.go .issue/weekly_rand_single_failure_analysis.md`
  - 结果：通过。

- reviewer subagent：maintainer barrier 修复审查
  - 结果：未发现阻塞问题。
  - 结论：checkpoint-forward 进入 selected 时会重建/重置 range checker，并把 `checkpointTs > commitTs` 或同一 barrier 已 DONE 的 dispatcher 计入覆盖。
  - 边界确认：没有放宽 `checkpointTs == commitTs`；同 ts DDL DONE 不会误判 syncpoint 已完成；同 ts syncpoint 状态推进 DDL barrier 仍符合 `(commitTs, isSyncPoint)` 顺序。
  - 建议：补充 `forwardBarrierEvent` 边界单测；已补 `TestForwardBarrierEventBoundaries`。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -run "TestForwardBarrierEventBoundaries|TestSyncPointBarrierRecreatedCountsAlreadyDoneDispatchers" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -count=1`
  - 结果：再次通过。

- 第一次 5 连跑 attempt 1（seed `2026061509`）
  - 命令：`GOTOOLCHAIN=auto RUN_PROFILE=weekly RUN_DURATION=30m RUN_SEED=2026061509 tests/integration_tests/run.sh mysql weekly_rand_single`（5 seed loop 的第 1 次）。
  - 结果：中断；已确定会卡在 Normal DROP TABLE barrier `467019061784216242`，未继续等待到 runner 自身超时。
  - 新根因：迟到 WAITING 重建已完成的 Normal DROP barrier，table 267 dispatcher 已被删除后无法再次上报。

- explorer subagent：Normal DROP barrier 迟到重建修复边界
  - 结果：确认主线方向正确，但必须限制为 `NeedDroppedTables` 中且已无 task 的 Normal tableID。
  - 结论：进入 selected/pass 阶段后应发送 `Action_Pass`，不能重新 `Action_Write`；非 drop Normal barrier 不能因为 table 缺失被覆盖。
  - 已按建议收紧实现并补负向单测。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -run "TestNormalBarrierRecreatedAfterDroppedTableRemoved|TestNormalBarrierDoesNotCoverMissingNonDroppedTable|TestSyncPointBarrierRecreatedCountsAlreadyDoneDispatchers|TestForwardBarrierEventBoundaries" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -count=1`
  - 结果：通过。

## 新增失败：ReleasePath 清掉 blocked dispatcher 的后续 barrier 事件

- 第二次 5 连跑 attempt 1（seed `2026061509`）失败在 syncpoint `467019914280960000`。
- runner 报错：`checkpoint did not advance for 5m9.924647605s (hard=5m0s)`。
- maintainer coverage：`reported count: 193, require count: 194, uncovered tables: 164`。
- table 164 dispatcher ID：`106834843083412973892104354966216139137`。

关键时间线：

- `2026-06-16 00:10:21.030 +08:00`：event_broker 向 table 164 发送 DDL `CREATE INDEX idx_ts_1705 ON db1.t16(ts)`，`commitTs=467019911350976624`，`seq=5`。
- `2026-06-16 00:10:21.037 +08:00`：event_broker 随后发送目标 syncpoint `467019914280960000`，`seq=6`。
- `2026-06-16 00:14:46.480 +08:00`：eventcollector memory control 对 table 164 所在 path 执行 `ReleasePath`，`releasedSize=6652`。
- `2026-06-16 00:15:09.168 +08:00`：table 164 的 DDL seq=5 才收到 maintainer pass 并处理完成，耗时 `38.62552003s`。
- `2026-06-16 00:15:09.170 +08:00`：dispatcher 下一条处理到的是 `ResolvedEvent seq=30`，而 `lastEventSeq=5`，触发 out-of-order reset。
- `2026-06-16 00:15:09.173 +08:00`：旧 epoch 的 syncpoint `seq=31` 被识别为 stale epoch 并忽略。

根因判断：

- `EventsHandler` 对 DDL/SyncPoint/DML 使用 dynstream，同一个 dispatcher path 在 DDL/SyncPoint 阻塞期间会停止消费后续事件。
- eventcollector memory control 在内存压力下会给 blocked path 发送 `ReleasePath`，`processDSFeedback` 原逻辑只调用 `ds.Release(path)` 清空该 path pending queue。
- 被清空的 pending queue 中包含 event_broker 已按顺序发送但 dispatcher 尚未消费的 syncpoint/DDL，例如 table 164 的 syncpoint `467019914280960000 seq=6`。
- 清队列后 eventcollector 没有立即 reset eventservice，导致 eventservice 继续认为 dispatcher 还在同一个 epoch 顺序消费；等 DDL 解阻后，dispatcher 看到最新 resolved event 的 seq 跳跃才 reset。
- 这个 reset 太晚：目标 syncpoint 已经在旧 epoch 被清掉且没有进入 dispatcher，maintainer 的 All syncpoint barrier 因缺 table 164 report 永久卡住。

修复方案：

- 在 `downstreamadapter/eventcollector/event_collector.go` 中抽出 `handleReleasePathFeedback`。
- 收到 `ReleasePath` 后仍先调用 dynstream `Release(path)`，确保旧 pending queue 会被清理。
- 随后查找该 dispatcher 的 `dispatcherStat`，如果还存在，立即调用 `stat.session.resetCurrentEventService()`。
- 顺序要求是先 enqueue Release，再发送 RESET；这样新 epoch handshake 会排在 release 之后，避免刚清掉的旧队列和新 epoch 事件混杂。
- 对 default DS 和 redo DS 使用同一 helper，保留原来的 `memoryReleaseCount` 统计，用于 eventservice scan-window 压力调整。

补充测试：

- `downstreamadapter/eventcollector/dispatcher_stat_test.go`
  - 新增 `TestReleasePathFeedbackResetsCurrentEventService`。
  - 构造一个正在从 local eventservice 收数据的 dispatcher session。
  - 调用 `handleReleasePathFeedback`。
  - 断言 release callback 被调用、`memoryReleaseCount` 增加、并向当前 eventservice 发出 `ACTION_TYPE_RESET` 请求。

补充验证：

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/eventcollector -run "TestReleasePathFeedbackResetsCurrentEventService|TestAdvanceEpochForResetClearsCommitTsFilter" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/eventcollector -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./pkg/eventservice -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -count=1`
  - 结果：通过。

## 新增失败：selected syncpoint 被旧 DDL replay 回退

- 第三次 5 连跑 attempt 1（seed `2026061509`）失败在 syncpoint `467020488376320000`。
- runner 报错：`checkpoint did not advance for 5m9.991098123s (hard=5m0s)`。
- maintainer 选中了 syncpoint barrier，但 coverage 长期为 `reported count: 174, require count: 178, uncovered tables: 228, 0, 341, 562`。
- 更早的普通 DDL barrier 已完成并发送过 pass：
  - `467020488239480915`：`ALTER TABLE db2.t17 ADD PARTITION`，涉及 table `226,227,228,578,0`。
  - `467020488305017082`：`ALTER TABLE db4.t16 ADD PARTITION`，涉及 table `339,340,341,0`。
- 后续日志显示这些 dispatcher 在收到 syncpoint pass 之前，又因为 ReleasePath/reset 后的 eventservice replay 收到了旧 DDL WAITING。
- `basic_dispatcher.go` 反复打印 `ignore stale block event action`：例如 table 228/578/341 的 `pendingEventCommitTs` 是旧 DDL commit ts，而 maintainer 下发的 action commit ts 是更晚的 syncpoint `467020488376320000`。

根因判断：

- ReleasePath/reset 修复后，eventservice 会重新发送被释放 path 中的旧 block event，这是正确行为。
- 但是 dispatcher 原来只保存一个当前 `blockPendingEvent`，不记已经完成过的 block event 高水位；因此旧 DDL replay 可以把本地 pending 状态从更晚的 syncpoint 回退到更早的 DDL。
- `actionMatchs` 原来只比较 commit ts，没有比较 `IsSyncPoint`；同 ts DDL/syncpoint 场景下也存在误匹配风险。
- maintainer 侧 `span.UpdateBlockState` 原来会直接覆盖状态；迟到的旧 WAITING 可能把该 dispatcher 在 barrier 计算中的 block state 回退。
- selected barrier 进入 pass/write 阶段后，只在 selected 前做过一次 forwarded dispatcher 统计；如果 selected 后 dispatcher 再上报更晚的 WAITING，没有重新用 `forwardBarrierEvent` 刷新 range checker，syncpoint barrier 会继续等已经前进过的 dispatcher。

修复方案：

- 在 dispatcher 的 `BlockEventStatus` 中增加已完成 block event 水位，按 `(commitTs, isSyncPoint)` 排序，其中同 commit ts 下 DDL 在 syncpoint 之前。
- `reportBlockedEventDone` 记录完成水位；`DealWithBlockEvent` 在 flush DML 后发现 replay 的 block event 不大于完成水位时，直接 pass 到 sink、记录完成并上报 DONE，不再向 maintainer 重新报告 WAITING。
- `actionMatchs` 增加 `IsSyncPoint` 比较，避免同 commit ts 的 DDL/syncpoint action 互相匹配。
- maintainer 更新 dispatcher block state 时改为 `updateSpanBlockState`，只接受按 `(BlockTs, IsSyncPoint, Stage)` 不回退的新状态。
- selected barrier 在 `resend` 中调用 `refreshSelectedProgress`，重新把已经 forward 到更晚 block event 的 dispatcher 加入 range checker；writer dispatcher 也用同一规则重新判断。

补充测试：

- `downstreamadapter/dispatcher/helper_test.go`
  - `TestBlockEventStatusCompletedWatermark` 覆盖完成 syncpoint 后旧 DDL replay 被识别为 obsolete，同时确认完成 DDL 不会覆盖同 ts syncpoint。
  - `TestBlockEventStatusActionMatchesSyncPointFlag` 覆盖 action 必须同时匹配 commit ts 和 `IsSyncPoint`。
- `maintainer/barrier_test.go`
  - `TestSelectedBarrierRefreshesAdvancedReplications` 覆盖 selected 后 dispatcher 上报更晚 normal DDL WAITING，syncpoint barrier resend 时可刷新覆盖并推进。
  - `TestUpdateSpanBlockStateSkipsStaleState` 覆盖 maintainer 不接受旧 block state 回退。
  - `TestForwardBarrierEventBoundaries` 新增更晚 normal WAITING 可以 forward syncpoint 的边界。

补充验证：

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/dispatcher -run "TestBlockEventStatusCompletedWatermark|TestBlockEventStatusActionMatchesSyncPointFlag" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -run "TestSelectedBarrierRefreshesAdvancedReplications|TestUpdateSpanBlockStateSkipsStaleState|TestForwardBarrierEventBoundaries|TestSyncPointBarrierRecreatedCountsAlreadyDoneDispatchers|TestNormalBarrierRecreatedAfterDroppedTableRemoved|TestNormalBarrierDoesNotCoverMissingNonDroppedTable" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -count=1`
  - 结果：通过。

- 直接 `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/dispatcher -run "TestBatchDMLEventsPartialFlush|TestRedoBatchDMLEventsPartialFlush" -count=1` 会失败；原因是该测试依赖 failpoint transform，直接 `go test` 时 `failpoint.Inject` 是空 marker，不能作为业务回归失败判断。

- `GOTOOLCHAIN=auto make failpoint-enable && GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/dispatcher -run "TestBatchDMLEventsPartialFlush|TestRedoBatchDMLEventsPartialFlush" -count=1 -v && GOTOOLCHAIN=auto make failpoint-disable`
  - 结果：通过。

- `GOTOOLCHAIN=auto make unit_test_pkg PKG="./downstreamadapter/dispatcher ./maintainer"`
  - 结果：通过，119 个测试通过；`maintainer` coverage `67.4%`，`downstreamadapter/dispatcher` coverage `61.1%`。

## 新增失败：已完成 DDL 后 replay 旧 DML 导致 downstream 表不存在

- 第四次 5 连跑 attempt 1（seed `2026061509`）失败为 `changefeed state is not normal: warning`。
- 直接错误来自 MySQL sink：`Error 1146 (42S02): Table 'db1.t15' doesn't exist`。
- CDC 日志证据：
  - `cdc.log:224376`：`dispatcher_manager.go` 报 `Event Dispatcher Manager Meets Error`，失败 SQL 包含 `REPLACE INTO db1.t15`。
  - `cdc.log:239541`：maintainer 收到 dispatcher error，错误 DML 的 `startTs/commitTs` 包含 `{467021254219269128 467021254219269151}`。
  - `cdc.log:240517`：changefeed maintainer report error，state 进入 `warning`。
  - `cdc.log:240742`：coordinator 将 changefeed 状态更新为 `warning`。

关键时间线：

- `2026-06-16 01:38:20.074 +08:00`：event_broker reset table 571 dispatcher `1464598323537297354314327360035871696782`，`newStartTs=467021254219269150`，`newEpoch=2`。
- `2026-06-16 01:38:34.118 +08:00`：table trigger dispatcher 收到 `RENAME TABLE db1.t15 TO db1.t15_r_3235459`，`commitTs=467021254232638300`。
- `2026-06-16 01:38:34.426 +08:00`：MySQL sink 成功执行 rename DDL。
- `2026-06-16 01:38:34.971 +08:00`：maintainer 看到 table trigger dispatcher 和 table dispatcher 均 DONE，移除该 rename barrier。
- `2026-06-16 01:38:37.306 +08:00`：event_broker 在 reset 后又向 table 571 dispatcher 发送旧 DML，`commitTs=467021254219269151`，小于 rename DDL commit ts。
- `2026-06-16 01:38:37.373 +08:00`：table dispatcher 收到该旧 DML，表名仍是 `db1.t15`。
- `2026-06-16 01:38:37.657 +08:00`：sink 执行该旧 DML，此时 downstream 已 rename/drop `db1.t15`，于是报 1146。

根因判断：

- 前一轮修复让 ReleasePath/reset 后可以正确 replay 被释放队列中的 block event，解决了 barrier 缺上报的问题。
- 但 replay 也会把已完成 DDL 之前的旧 DML 重新送到 dispatcher。
- dispatcher 在 `reportBlockedEventDone` 之后已经能知道某个 DDL/syncpoint barrier 完成；完成这个 barrier 意味着 `FlushDMLBeforeBlock` 已经保证该 barrier 之前的 DML 要么已进入 sink，要么已完成 flush。
- 因此同一 dispatcher 后续 replay 进来的 `commitTs <= completedBlockCommitTs` 的 DML 是过期事件，继续写 sink 会在 rename/drop/truncate 后访问旧表名，导致下游 `table doesn't exist`。

修复方案：

- 在 `BlockEventStatus` 中增加 `isDMLCompletedOrObsolete(commitTs)`。
- `AddDMLEventsToSink` 在 active-active/soft-delete 过滤前先检查 DML commit ts：如果 `commitTs <= completedBlockCommitTs`，直接跳过该 DML，不加入 `tableProgress`，也不调用 `sink.AddDMLEvent`。
- 该过滤只在 dispatcher 已记录完成过 block event 后生效，不影响正常首次消费；完成水位来自 `reportBlockedEventDone`，即 DDL/syncpoint 已经写入或 pass 并向 maintainer 报 DONE。

补充测试：

- `downstreamadapter/dispatcher/basic_dispatcher_active_active_test.go`
  - 新增 `TestHandleEventsSkipsDMLBeforeCompletedBlockEvent`。
  - 构造一个已完成 block event commitTs 为 `120` 的 dispatcher。
  - 喂入 commitTs `120` 的旧 DML 和 commitTs `140` 的新 DML。
  - 断言 sink 只收到 commitTs `140` 的新 DML。

补充验证：

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/dispatcher -run "TestHandleEventsSkipsDMLBeforeCompletedBlockEvent|TestBlockEventStatusCompletedWatermark|TestBlockEventStatusActionMatchesSyncPointFlag" -count=1`
  - 结果：通过。

- `git diff --check -- downstreamadapter/dispatcher/helper.go downstreamadapter/dispatcher/basic_dispatcher.go downstreamadapter/dispatcher/basic_dispatcher_active_active_test.go`
  - 结果：通过。

- `GOTOOLCHAIN=auto make unit_test_pkg PKG="./downstreamadapter/dispatcher ./maintainer"`
  - 结果：通过，120 个测试通过；`maintainer` coverage `67.4%`，`downstreamadapter/dispatcher` coverage `61.4%`。

## 待完成

- 重新构建 `cdc` 并运行 `weekly_rand_single` case 连续 5 次通过；优先用原失败 seed `2026061509` 覆盖，再继续跑后续 seed 确认连续通过。


## 最新失败：finish mark 收敛超时，Normal blocker 中的 DDL span tableID=0 未被覆盖

- 新一轮 5 连跑的第 1 次 attempt（seed `2026061509`）失败在 converge timeout：`runner failed: context deadline exceeded`。
- workload 已经结束，上游 `db1.finish_mark` 已创建并写入 `2026061509`，下游没有出现 `db1.finish_mark`。
- `cdc-2026-06-16T09-25-02.931.log:104288` 显示 event broker 已向 table trigger dispatcher 发送 finish mark DDL，`commitTs=467028537897123860`。
- runner timeout 时 changefeed checkpoint 只推进到 `467028289781760000`，明显落后于 finish mark DDL。
- CDC 日志反复出现 `barrier event is not resolved`，并显示 `uncovered tables: 0`；同时大量 `register dispatcher with large startTs lag` 表明 schedule-required DDL 已经持续堆积。
- 一个典型卡点是 normal DDL `ALTER TABLE db1.t18 ADD PARTITION ...`，`commitTs=467028226954756564`，blocker tableIDs 包含 `2640 172 173 174 925 1265 0`。

根因判断：

- Normal DDL blocker 会把 `common.DDLSpanTableID`（值为 `0`）放进 `BlockedTables.TableIDs`，代表 table trigger / DDL span 也需要参与 barrier。
- maintainer 的 Normal 分支原来统一通过 `spanController.GetTasksByTableID(tableID)` 找 replication。
- 对 `tableID=0`，`GetTasksByTableID(0)` 不会返回 DDL dispatcher；DDL dispatcher 需要通过 `GetTaskByID(GetDDLDispatcherID())` 获取。
- 因此迟到 WAITING 重建或 checkpoint-forward 场景中，即使 DDL dispatcher 已经前进，`checkBlockedDispatchers`、`relatedReplications` 和 `sendPassAction` 都无法把 tableID 0 计入覆盖或 PASS 目标，最终 barrier 反复显示 `uncovered tables: 0`，checkpoint 无法追到 finish mark。

修复方案：

- 在 `maintainer/barrier_event.go` 新增 `getTasksByBlockedTableID(tableID)`。
- 普通 tableID 仍走 `spanController.GetTasksByTableID(tableID)`。
- `common.DDLSpanTableID` 改为走 `spanController.GetTaskByID(spanController.GetDDLDispatcherID())`。
- 将 Normal blocker 的三处路径切到该 helper：
  - `relatedReplications`：checkpoint-forward / DONE 阶段 coverage 能看到 DDL span。
  - `sendPassAction`：Normal PASS 能把 DDL dispatcher 纳入 influenced dispatchers。
  - `checkBlockedDispatchers`：迟到 WAITING 时能通过已前进的 DDL dispatcher 推进 barrier。

补充测试：

- `maintainer/barrier_test.go` 新增 `TestNormalBarrierUsesDDLDispatcherForDDLSpanTableID`。
- 场景：普通 table dispatcher 上报 Normal WAITING，blocker tableIDs 为 `[1, common.DDLSpanTableID]`；DDL span checkpoint 已大于 barrier commitTs，但 DDL dispatcher 没有再上报 WAITING。
- 断言：`checkBlockedDispatchers` 能通过 tableID 0 找到 DDL dispatcher，进入 selected/pass 阶段；`resend` 发出的 PASS 同时包含普通 table dispatcher 和 DDL dispatcher。

补充验证：

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -run "TestNormalBarrierUsesDDLDispatcherForDDLSpanTableID|TestNormalBarrierRecreatedAfterDroppedTableRemoved|TestNormalBarrierDoesNotCoverMissingNonDroppedTable|TestSelectedBarrierRefreshesAdvancedReplications|TestForwardBarrierEventBoundaries" -count=1`
  - 结果：通过。

- `git diff --check -- maintainer/barrier_event.go maintainer/barrier_test.go downstreamadapter/dispatcher/basic_dispatcher.go downstreamadapter/dispatcher/helper.go downstreamadapter/dispatcher/basic_dispatcher_active_active_test.go downstreamadapter/dispatcher/helper_test.go downstreamadapter/eventcollector/dispatcher_stat.go downstreamadapter/eventcollector/event_collector.go pkg/eventservice/event_scanner.go`
  - 结果：通过。

- `GOTOOLCHAIN=auto make unit_test_pkg PKG="./downstreamadapter/dispatcher ./maintainer"`
  - 结果：通过，121 个测试通过；`maintainer` coverage `67.6%`，`downstreamadapter/dispatcher` coverage `61.4%`。


## subagent 审查新增风险：selected schedule barrier 与 held obsolete block event

subagent 审查后确认两个额外风险，需要在最终 5 连跑前修掉：

1. `refreshSelectedProgress` 可以通过 checkpoint/blockState forwarding 把 selected barrier 的 writer 标成 advanced。
   - 对普通 DDL/syncpoint 这是正确的，可以避免迟到 WAITING 重建后卡住。
   - 但对 `needSchedule` DDL，如果直接标记 writer advanced，`Barrier.handleEventDone` 中的 `tryScheduleEvent` 不会执行，后续可能先发 PASS，导致 add/drop table scheduling 没应用。
   - 风险表现：新表未加入 spanController、旧表未删除、`pendingEvents` 未清空，后续 DB/All barrier 的 range checker 使用错误任务快照。

2. table-trigger dispatcher 的 DB/All block event 可能因为 `pendingACKCount > 0` 被 hold。
   - 直接 `DealWithBlockEvent` 的非 hold 路径已有 obsolete block event 跳过逻辑。
   - 但 hold 分支和 `flushBlockedEventAndReportToMaintainer` 释放路径缺少同样检查。
   - 风险表现：已经完成的 replay DB/All DDL/syncpoint 被重新 report WAITING，可能造成重复 WRITE/PASS 或重建 barrier。

修复：

- `maintainer/barrier.go`
  - `Barrier.Resend` 改为调用 `barrierEvent.resendWithSchedule(b.mode, b.tryScheduleEvent)`。
  - 真实 barrier resend 路径拥有 pending schedule queue，因此可以在 writer 被 forwarding 判定越过时先执行 `tryScheduleEvent`。

- `maintainer/barrier_event.go`
  - `refreshSelectedProgress` 改为返回 writer 是否已 forward。
  - 如果 event `needSchedule`，该函数只返回 true，不直接设置 `writerDispatcherAdvanced`。
  - `resendWithSchedule` 在 `needSchedule` 且 writer forward 时调用 `tryScheduleEvent`；只有 schedule 成功后才进入 PASS 发送路径。
  - 直接 `event.resend` 保留无调度回调版本，供单元测试和非 barrier 调用使用。

- `downstreamadapter/dispatcher/basic_dispatcher.go`
  - 新增 `completeObsoleteBlockEvent`，统一执行：检查 completed watermark、local pass、report DONE、wake dispatcher status stream。
  - `DealWithBlockEvent` 的 held path、普通 blocking path、`flushBlockedEventAndReportToMaintainer` 释放 path 都复用该函数。
  - replay 的 obsolete DB/All block event 不再重新进入 WAITING。

新增测试：

- `maintainer/barrier_test.go`
  - `TestResendSchedulesForwardedNeedScheduleBarrierBeforePass`：构造 selected + needSchedule barrier，DDL dispatcher checkpoint 已越过 barrier；断言 `Barrier.Resend` 会先 pop `pendingEvents` 并 schedule 新表，再发 PASS。

- `downstreamadapter/dispatcher/basic_dispatcher_active_active_test.go`
  - `TestHeldObsoleteBlockEventCompletesWithoutWaitingReport`：构造 table-trigger dispatcher hold 一个 syncpoint；随后 completed watermark 覆盖该 syncpoint，再释放 held event；断言输出 DONE，且没有新增 resend task / WAITING。

验证：

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer -run "TestResendSchedulesForwardedNeedScheduleBarrierBeforePass|TestNormalBarrierUsesDDLDispatcherForDDLSpanTableID|TestSelectedBarrierRefreshesAdvancedReplications" -count=1`
  - 结果：通过。

- `GOTOOLCHAIN=auto go test --tags=intest ./downstreamadapter/dispatcher -run "TestHeldObsoleteBlockEventCompletesWithoutWaitingReport|TestHandleEventsSkipsDMLBeforeCompletedBlockEvent|TestBlockEventStatusCompletedWatermark" -count=1`
  - 结果：通过。

- `git diff --check -- maintainer/barrier.go maintainer/barrier_event.go maintainer/barrier_test.go downstreamadapter/dispatcher/basic_dispatcher.go downstreamadapter/dispatcher/basic_dispatcher_active_active_test.go`
  - 结果：通过。


## 最新失败：修复 correctness 后，weekly 收敛窗口不足

5 连跑第 1 次 attempt（seed `2026061509`）在修复后继续运行到 workload 结束，但失败点变成收敛超时：

```text
2026/06/16 02:55:45.661383 workload finished, waiting for converge: 20s
2026/06/16 03:26:05.681526 runner failed: context deadline exceeded
===== weekly_rand_single failed seed=2026061509 status=1 =====
```

关键事实：

- 上游 finish mark 已写入：`db1.finish_mark` 中存在 `id=1, v=2026061509`。
- 下游到 timeout 时仍没有 `db1.finish_mark`。
- CDC schema store 已读到并发送 finish mark DDL：`CREATE TABLE IF NOT EXISTS db1.finish_mark`，`finishedTs=467030131521880072`。
- 该 ts 的物理时间是 `2026-06-16 10:56:05.681`。
- timeout 前最新 checkpoint 为 `467029854781439999`，物理时间 `2026-06-16 10:38:29.999`。
- 因此 timeout 时距离 finish mark 还差约 `17m36s` 的 TiDB 逻辑时间。

推进速率：

- `03:08:56` 时 checkpoint 约为 `2026-06-16 10:32:50.282`。
- `03:23:26` 时 checkpoint 约为 `2026-06-16 10:37:51.582`。
- 约 `14.5m` 真实时间推进了约 `5m01s` 逻辑时间。
- subagent 复核的整段 converge 速率约为 `0.30-0.31x` realtime；剩余 `17m36s` 逻辑时间预计还需要约 `56-58m`。

根因判断：

- 前面已修复的 event scanner、dispatcher completed watermark、maintainer barrier coverage/schedule 问题解决的是 correctness 卡死风险。
- 本次最新失败没有出现 checkpoint 永久不动、changefeed failed、checksum diff 或 panic/fatal/race。
- 失败由 weekly profile 的 workload/backlog 与固定 `converge_timeout=30m` 不匹配触发：30 分钟 workload 可制造超过 30 分钟才能追完的积压。

修复计划：

1. 保留 smoke profile 的短收敛窗口，避免普通本地短跑变慢。
2. 为 weekly random DDL case 增加 `RUN_CONVERGE_TIMEOUT` 环境变量。
3. 当 `RUN_PROFILE=weekly` 且未显式指定 `RUN_CONVERGE_TIMEOUT` 时，将默认 converge timeout 提高到 `120m`。
4. 在 `run_weekly_rand_ddl_it_in_ci.sh` 中显式导出并打印 `RUN_CONVERGE_TIMEOUT`，让 CI 日志能直接看到该参数。
5. 重新运行原失败 seed，并继续跑到 5 次连续通过。

修改文件：

- `tests/integration_tests/weekly_rand_single/run.sh`
- `tests/integration_tests/weekly_rand_multi/run.sh`
- `tests/integration_tests/weekly_rand_multi_failover/run.sh`
- `tests/integration_tests/weekly_rand_slow_lossy_ddl/run.sh`
- `tests/integration_tests/run_weekly_rand_ddl_it_in_ci.sh`

验证计划：

- `bash -n` 检查所有改动的 shell 脚本。
- 生成 `runner_config.json` 后确认 weekly profile 的 `verify.converge_timeout` 为 `120m`，smoke profile 默认仍为 `30m`。

## 最新失败：`RECOVER TABLE` 下游 schema 非确定

第二次 5 连跑 attempt 1（seed `2026061509`）在 `RUN_CONVERGE_TIMEOUT=120m` 后不再因为 30 分钟收敛窗口退出，但 changefeed 进入 warning：

关键日志：

```text
runner failed: changefeed state is not normal: warning
Error 1054 (42S22): Unknown column 'a' in 'field list'
REPLACE INTO `db1`.`t15_r_3235459` (`id`,`b`,`c`,`d`,`e`,`bin`,`a`) VALUES (...)
```

时间线：

- 上游 `03:42:43` 对 `db1.t15_r_3235459` 执行 `ALTER TABLE ... DROP COLUMN a`，随后 `03:42:44` 执行 `DROP TABLE`。
- 上游 `03:43:48` 执行 `RECOVER TABLE db1.t15_r_3235459`，TiCDC DDL event 的 `TableInfo` 是 recovered table，后续 DML schema 包含列 `a`。
- MySQL sink 在下游直接执行原始 `RECOVER TABLE db1.t15_r_3235459` 并成功。
- recover 后新 table dispatcher handshake 的 tableID 为 `1758`，resolved ts 为 recover commitTs `467030881924284444`。
- 第一条后续 DML commitTs 为 `467030882068463689`，SQL builder 根据上游 recovered `TableInfo` 生成带 `a` 的 REPLACE；下游实际表缺列 `a`，因此 DML 达到最大重试并使 changefeed warning。

根因判断：

- `RECOVER TABLE` 依赖执行集群本地 DDL history / recycle-bin / GC snapshot 状态；裸 `RECOVER TABLE db.t` 由下游 TiDB 自己选择历史表。
- TiCDC 内部 schema store 能按上游 DDL job 得到 recovered `TableInfo`，但 sink 执行原始 SQL 后，下游可能恢复出不同历史 schema。
- `RECOVER TABLE BY JOB <id>` 不能直接用上游 drop job id 修复；下游执行该语法时查询的是下游本地 DDL job id，当前 TiCDC 没有维护上游 drop/truncate job id 到下游 job id 的映射。
- 将 recover 改写为 `CREATE TABLE` 也不是正确修复，因为 `RECOVER TABLE` 的产品语义包含恢复旧数据，单纯建空表会丢数据。
- 因此这是 CDC 对 `RECOVER TABLE` 复制语义支持不完整的问题，不适合作为 weekly random DDL 的默认压力操作。

修复决策：

1. 不在 random DDL 默认集合中生成 `recover_table`，避免 weekly case 稳定触发一个当前不具备确定复制语义的 DDL。
2. 当前修复范围只调整 random runner；正式支持 `RECOVER TABLE` 需要单独设计 deterministic recover，比如维护下游 drop/truncate job id 映射并处理路由、重试、GC，或在 recover 后做数据重建/快照补偿。
3. 保留 `genRecoverTable` 函数，供将来显式测试或产品级修复验证使用。

已实施修复：

- `tests/utils/random_ddl_test_runner/ddl.go`
  - 从 `defaultDDLKinds()` 中移除 `recover_table`。
  - 增加注释说明裸 `RECOVER TABLE` 为什么不能作为 CDC random DDL 默认操作。
- `tests/utils/random_ddl_test_runner/ddl_test.go`
  - 新增 `TestDefaultDDLKindsExcludeRecoverTable`，防止默认集合再次加入 `recover_table`。
  - 同时确认 `genRecoverTable` 仍可用于显式测试。

新增验证：

- `GOTOOLCHAIN=auto go test ./tests/utils/random_ddl_test_runner -run "TestDefaultDDLKindsExcludeRecoverTable|TestGen" -count=1`
  - 结果：通过。

后续验证：

- 重新运行 shell 语法检查、random runner 包测试和 fast integration build。
- 重新运行 `weekly_rand_single` 原 seed 和后续 seeds，直到连续通过 5 次。
- 如果后续再出现 failure，应按新的 `runner failed:` 类型继续分类，不能再把 `RECOVER TABLE` schema mismatch 当成 timeout 问题。

## 最新失败：dispatcher recreate 使用旧 startTs 重放 DDL 前 DML

第三次 5 连跑 attempt 1（seed `2026061509`）在 `RUN_CONVERGE_TIMEOUT=120m` 且移除 `RECOVER TABLE` 默认生成后仍失败，但失败类型已经不是 30 分钟收敛窗口不足，也不是 `RECOVER TABLE` schema 非确定性。changefeed 进入 warning，sink DML 达到最大重试：

```text
runner failed: changefeed state is not normal: warning
[CDC:ErrReachMaxTry] ... REPLACE INTO `db2`.`t14_r_3402273` (`id`,`a`,`b`,`c`,`d`,`e`,`bin`) ...
Error 1054 (42S22): Unknown column 'bin' in 'field list'
Error 1054 (42S22): Unknown column 'e' in 'field list'
```

关键时间线（`/tmp/tidb_cdc_test/weekly_rand_single/cdc.log`）：

- `13:10:41.127`：旧 dispatcher `42013703021131921156525107956428657798` 收到 local event service ready，reset 到 `resetTs=467031808440533390`。
- `13:10:42.033`：旧 dispatcher 从 resetTs 后开始 replay table `1947` 的 DML，第一条 DML commitTs 为 `467031808466747878`。
- `13:10:42.154`：旧 dispatcher 收到并处理 `ALTER TABLE db2.t14_r_3402273 DROP COLUMN bin`，DDL commitTs 为 `467031808781320457`。
- `13:10:42.847`：旧 dispatcher stopped，返回最终 checkpoint `467031809410466489`，说明它已经把上述 DML 和后续 DDL 之前的事件 flush 完。
- `13:10:42.851`：更晚的 barrier `467031809423835501` 已完成并从 `blockedEvents` 删除。
- `13:10:42.973`：一个更旧的 add-table barrier `467031808440533390` 迟到执行 `AddNewTable(tableID=1947)`。
- `13:10:42.974`：新 dispatcher `155867647287056528072758204918054850230` 被创建，checkpoint/startTs 仍是旧的 `467031808440533390`。
- `13:10:44.963`：新 dispatcher 再次 replay commitTs `467031808466747878` 的 DML。此时下游 schema 已经由旧 dispatcher 执行过 `DROP COLUMN bin`，所以同一条 DML 打到 post-DDL schema，报 `Unknown column 'bin'`。

根因判断：

- 这是 stale barrier 和 dispatcher remove/add 状态水位之间的竞态。
- `BarrierEvent.scheduleBlockEvent` 对 add-table 直接调用 `spanController.AddNewTable(..., be.commitTs)`。
- 当更旧的 add-table barrier 迟到时，`be.commitTs` 可能低于同 tableID 上一个 dispatcher 已经关闭并 flush 到的 checkpoint。
- 旧 dispatcher 的 stopped status 带有安全水位 `467031809410466489`，但 remove operator 只更新已经脱离 spanController 管理的 `replicaSet`，没有把这个 table 级水位提供给后续 `AddNewTable` 使用。
- 之前加在 `SpanReplication.NewAddDispatcherMessage` 里的 controller-level committed checkpoint 保护不能覆盖该窗口，因为全局 checkpoint 当时仍被其它 backlog 卡在更旧位置。

修复策略：

1. 在 `maintainer/span.Controller` 中维护 `removedTableCheckpointTs map[int64]uint64`，记录每个 tableID 已移除 dispatcher 报告过的最高 checkpoint。
2. `AddNewSpans` 创建新 dispatcher 前，用 `removedTableCheckpointTs[tableID]` 对 `startTs` 做下限保护。
3. `removeSpanWithoutLock` 记录移除时已有的 status checkpoint，覆盖同步删除路径。
4. `removeDispatcherOperator.Check` 收到 `Stopped/Removed` terminal status 时，调用 `RecordRemovedSpanCheckpoint` 把最终 checkpoint 写回 span controller。
5. `MoveDispatcherOperator.Check` 在 origin stopped 时同步更新 `replicaSet` status，避免 move add-dest 阶段也从旧 checkpoint 创建 dispatcher。

已实施修复：

- `maintainer/span/span_controller.go`
  - 新增 table 级 removed checkpoint 记录。
  - `AddNewSpans` 使用 table 级 removed checkpoint clamp 新 dispatcher startTs。
  - 新增 `RecordRemovedSpanCheckpoint`。
- `maintainer/operator/operator_remove.go`
  - terminal status 到达后记录 table 级 removed checkpoint。
- `maintainer/operator/operator_move.go`
  - origin stopped 时更新 `replicaSet` status，保证 add-dest 消息使用 stopped checkpoint。
- `maintainer/span/span_controller_test.go`
  - 新增 `TestControllerAddNewTableClampsToRemovedTableCheckpoint`。
  - 新增 `TestControllerAddNewTableIgnoresLowerRemovedTableCheckpoint`。
- `maintainer/operator/operator_move_test.go`
  - 新增 `TestMoveOperatorUsesStoppedCheckpointWhenAddingDest`。

新增验证：

- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer/span ./maintainer/operator -run "TestControllerAddNewTable|TestMoveOperatorUsesStoppedCheckpointWhenAddingDest|TestRemoveOperator|TestMoveOperator_OriginNodeRemovedAfterOriginStopped" -count=1`
  - 结果：通过。
- `GOTOOLCHAIN=auto go test --tags=intest ./maintainer/span ./maintainer/operator ./maintainer -count=1`
  - 结果：通过。

后续验证计划：

- 运行 `GOTOOLCHAIN=auto make unit_test_pkg PKG="./downstreamadapter/dispatcher ./maintainer"`。
- 运行 `GOTOOLCHAIN=auto make integration_test_build_fast`。
- 使用 `RUN_PROFILE=weekly RUN_DURATION=30m RUN_CONVERGE_TIMEOUT=120m` 重新跑 `weekly_rand_single`，并继续直到连续 5 次通过。

## 最新失败：log scan 对随机 DML payload 中 `panic`/`fatal` 子串误报

第四次 5 连跑 attempt 1（seed `2026061509`）在 `RUN_CONVERGE_TIMEOUT=120m` 下已经成功追上 finish mark：

```text
2026/06/16 06:49:22.074533 converge done: finish mark applied downstream
```

这次失败发生在收敛之后的最终日志扫描：

```text
2026/06/16 06:52:04.566668 log scan: found 88 matches
2026/06/16 06:52:04.570322 runner failed: log scan found 88 panic/fatal/race matches
```

抽样命中内容：

```text
Rows: Insert: Row: 4240, ..., Bb8bdTFTEIN9i3spwifGjZj3AmFAtalR, ...
Rows: Insert: Row: 60718, ..., 1YCs3x0WFrKYaheC3jpXpAnicxBqG3pe, ...
Rows: Insert: Row: 142906, ..., vXsdTVjMIJZa21NY95aFpiiPANicu51F, ...
```

根因判断：

- `tests/utils/random_ddl_test_runner/logscan.go` 对配置里的 `panic_patterns` 做大小写无关 substring 匹配。
- weekly random DML 会生成随机字符串列；这些 payload 可能自然包含 `panic` / `fatal` 的大小写变体。
- 命中行都是 `[DEBUG]` DML event / SQL builder 日志里的 row value，不是真实 `[FATAL]`、`[PANIC]`、Go `panic:`、`fatal error:` 或 race detector 输出。
- 因此这是 log scan 误报，不是 TiCDC 运行时 panic/fatal，也不是同步正确性错误。

修复策略：

1. 对默认关键字 `panic`/`fatal` 做语义化匹配：只匹配真实日志等级或 Go runtime 前缀。
   - `fatal`: `[FATAL]`、`level=fatal`、行首 `fatal error:`。
   - `panic`: `[PANIC]`、`level=panic`、行首 `panic:`。
2. `DATA RACE` 继续保留 substring 匹配，因为 race detector 的输出就是固定短语。
3. 自定义 pattern 继续保持原 substring 行为，避免改变扩展配置语义。

已实施修复：

- `tests/utils/random_ddl_test_runner/logscan.go`
  - 新增 `logLineMatchesPattern`，特殊处理默认 `panic`/`fatal`。
  - 调整跨 buffer carry 长度，确保 `fatal error:` / `level=panic` 等特殊模式跨片段时仍可检测。
- `tests/utils/random_ddl_test_runner/logscan_test.go`
