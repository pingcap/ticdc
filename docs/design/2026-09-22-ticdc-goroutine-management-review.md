# TiCDC Goroutine 管理审查

Last updated: 2026-09-22
Status: 审查完成，问题未修复
Scope: 以生产代码（`*.go`，排除 `tests/`、`tools/` 与 `*_test.go`）为主，另含测试与工具代码的统计；第三方库内部 goroutine 不在范围内
Audience: TiCDC runtime owner 与后续修复者

## Background

本审查回答两个问题：生产代码里有没有不受 WaitGroup/errgroup 管理的 goroutine，有没有按周期或按需临时启动、但没有确定性收尾的 goroutine。本文给出问题清单与修复方向，用于排定修复顺序，不实施修复。

审查方式：用 `rg` 全量提取 `go` 语句（含跨行写法与 `go pkg.fn(...)` 限定名调用），再逐个核对 `Wait()`、`Close()`、`context` 的收尾链；另外单独扫描了 `time.AfterFunc` 和"构造即起 goroutine"的 `chann` 抽象。行号对应 commit `93cd55259`。

"已管理"在本文中的含义：启动方在关闭路径上能确定性地等待其退出（WaitGroup/errgroup），或该 goroutine 的退出由所依赖的 ctx/channel 生命周期保证。

规模统计：

- 生产代码直接 `go` 语句 66 处。
- 隐式启动 7 处：6 处 unbounded `chann` 构造（`utils/chann/chann.go:102`）+ 1 处 `time.AfterFunc`。
- 测试与工具代码 323 处：`*_test.go` 282 处，`tests/`、`tools/` 41 处；其中约 17 处附近有 wg/errgroup，其余是测试并发用例的 fire-and-forget。

## 确定泄漏与缺失的等待

### `NewAutoDrainChann` 从不 `CloseAndDrain`

`utils/chann/chann.go:102` 在 unbounded `Chann` 构造时启动 `unboundedProcessing`，该 goroutine 只会在 `ch.close` 被触发后退出；`DrainableChann` 只暴露 `CloseAndDrain`，没有 `Close`。以下 5 个对象在关闭路径上都没有调用它，每个对象泄漏 1 个永久阻塞的 goroutine。

- `maintainer/maintainer.go:207`（`eventCh`）— 影响最大：每个 changefeed 一个 maintainer，反复创建/销毁会持续累积泄漏。
- `downstreamadapter/eventcollector/event_collector.go:161`（`dispatcherMessageChan`）
- `downstreamadapter/eventcollector/log_coordinator_client.go:50`（`logCoordinatorRequestChan`）
- `logservice/eventstore/event_store.go:317`（`subscriptionChangeCh`）
- `logservice/coordinator/coordinator.go:98`（`requestChan`）

对照：`coordinator/coordinator.go:119` 的 `eventCh` 在 `coordinator/coordinator.go:469` 正确调用了 `CloseAndDrain`，可作为修复模板。

修复方向：优先改用 `chann.NewUnlimitedChannel`（基于 cond，不启动 goroutine）；若保留 unbounded `Chann`，则必须在关闭路径补 `CloseAndDrain`。maintainer 需要先把 4 个常驻 goroutine 纳入 WaitGroup，等读侧退出后再 Drain，否则 Drain 会与 `runHandleEvents` 抢读同一个 channel。

### eventStore 写 worker 的 WaitGroup 只 Add 不 Wait

- `logservice/eventstore/event_store.go:263` 定义 `wg sync.WaitGroup`，`:366` 执行 `p.store.wg.Add(p.workerNum)`，worker 在 `defer p.store.wg.Done()`，但全包没有任何 `Wait()`（`gc.go:292` 的 `wg.Wait()` 是 `gcManager.run` 的局部变量，覆盖不了它）。
- worker 实际只依赖 `eventStore.Run` 里 errgroup 派生 ctx 的取消退出，`writeTaskPool.run` 立刻返回，所以 `eg.Wait()` 并不等待 worker。
- 影响：`eventStore.Close()`（`:466`）直接关闭 Pebble DB 时，写 worker 可能仍在写，属于 close 后又写已关闭 DB 的竞态。

修复方向：让 `writeTaskPool.run` 在返回前等待自己启动的 worker，或在 `Close` 关闭 DB 之前 `e.wg.Wait()`。

## 只 cancel、不 join

这些 goroutine 都有 ctx 或停止 channel，但没有 WaitGroup，关闭路径返回时不保证它们已经退出，停机期间可能与资源释放并发。

- `maintainer/maintainer.go:266/267/269/274`：4 个常驻 goroutine（`runHandleEvents`、`calCheckpointTs`、`handleRedoMetaTsMessage`、`refresher.Run`）。maintainer 包内没有 WaitGroup 或 errgroup，`Maintainer.Close()` 只调用 `cancel()`。
- `logservice/schemastore/gc_keeper.go:107`：2 分钟周期的 safepoint 刷新。它不在 `store.runWg` 内（该 wg 只覆盖 `logservice/schemastore/schema_store.go:688` 的 resolvedTs ticker），`keyspaceSchemaStore.close()` 取消 ctx 后不等待 keeper。
- `pkg/etcd/client.go:450/493`：etcd 健康检查与 endpoint 刷新 ticker。`ClientImpl.Close()` 只调用 `cli.Close()`，不等待这两个 goroutine。
- `pkg/sink/mysql/mysql_writer.go:134`：每个 DML writer 一个 `runDMLConnLoop` ticker（会话保活/active-active stats）。`Writer.Close()` 只调用 `cancel()`。
- `downstreamadapter/sink/topicmanager/kafka_topic_manager.go:94`：每个 sink 一个 metadata 刷新 ticker。`Close()`（`:339`）只调用 `cancel()`。
- `downstreamadapter/dispatcher/redo_dispatcher.go:106`：redo meta 的 `PreStart`/`Run` 常驻 goroutine。`RedoMeta.Run` 内部用 errgroup 管理自己的子 goroutine，但这一层只由 `rd.cancel` 结束，调用方不等待。
- `pkg/sink/spool/spool.go:386`：segment 回收循环。`Spool.Close()`（`:750`）只 `close(reclaimStopCh)`，随后立即 `RemoveAll` 工作目录并关闭文件句柄，回收循环可能仍在执行。
- `pkg/notify/notify.go:128`：每个 `Receiver` 一个 tick goroutine，靠 `closeCh`/`Notifier.Close` 退出，无 wg。
- `pkg/etcd/client.go:262`：`Watch()` 每次调用启动一个 goroutine 转发 watch 事件，靠 ctx 与 `close(outCh)` 退出，无 wg。生产代码中只有 `pkg/orchestrator/etcd_worker.go:156` 使用。

## 临时与异步启动、无 join

这些是 per-object 或 per-request 启动的 goroutine，设计上就是异步的，但没有任何机制让调用方等待其结束。

- `pkg/upstream/manager.go:130`：每次新增 upstream 启动一个初始化 goroutine。`Manager.Close()` 只 cancel 并关闭各 upstream，不等待初始化结束。
- `downstreamadapter/dispatchermanager/dispatcher_manager.go:1069/1082`：`go e.close()` 与 `go e.finishClose()`，只靠 `closing`/`closed` 原子标志协调。`finishClose` 内部会 `e.wg.Wait()`，但调用方无法等待这次异步关闭完成。
- `downstreamadapter/dispatchermanager/dispatcher_manager.go:1220`：remove-changefeed 清理 goroutine，由 CAS 标志保证只跑一个，但不在 `e.wg` 内。
- `server/module_grpc.go:73` 与 `server/module_http.go:81`：`Serve` goroutine 通过一个无缓冲 channel 回传错误；ctx 先结束时 `Run` 直接返回，该 goroutine 会永久阻塞在 `ch <- err`。
- `server/server.go:403/412/417/693`：分别是 session watchdog 哨兵、关机超时哨兵、`g.Wait()` 转发 goroutine、`closePreServices` 内置 goroutine。`:417` 启动的转发 goroutine 在超时分支胜出后会一直阻塞到所有模块退出（通常就是进程退出）；`:693` 启动的 goroutine 在超时后不会被 join。`:636` 使用了 `closeGroup`，是正确写法。
- `pkg/diff/diff.go:279`（chunk 生产者，靠 defer 关闭 worker channel 收尾）与 `:517/:520`（每个源/目标连接一个 `getChecksum`，靠收满 N 个结果同步，无 wg）。注意 `pkg/diff` 只被 `tests/integration_tests/util/db.go` 引用，属测试工具。
- `logservice/logpuller/region_failure_handler.go:163`：`time.AfterFunc` 延迟重试，内部有 ctx 与 `stopped` 检查，但不属于任何 goroutine 生命周期。
- `coordinator/controller.go:570`：仅 failpoint `DelayCaptureWriteLeaseResponse` 触发时启动的延迟发送 goroutine。

## 进程级与有意为之

以下不需要修复，列出以免后续审查重复标记。

- `cmd/util/util.go:115` 信号处理；`cmd/storage-consumer/main.go:91` 与 `cmd/cdc/redo/apply.go:96` pprof server；`cmd/cdc/server/server.go:160` 关机包装。均为进程级或 CLI 级生命周期。
- `pkg/logger/log_file_monitor.go:49`：`sync.Once` + 30s ticker，ctx 来自 server，随进程退出。
- `pkg/sink/kafka/sarama_async_producer.go:44`：代码注释中明确接受 goroutine 泄漏，以换取 Kafka 异常时不卡住 processor tick。

## 已正确管理（对照）

以下位置经核对具备确定性等待，不需要改动。

- `manager.wg`：`downstreamadapter/dispatchermanager/dispatcher_manager.go:394/402/409/416` 与 `dispatcher_manager_redo.go:111/150`，`dispatcher_manager.go:1191` 处 Wait。
- `c.wg`：`downstreamadapter/dispatchermanager/heartbeat_collector.go:95/103`，`Close()`（`:304`）处 Wait。
- `s.wg`：`utils/dynstream/stream.go:174/178`，`stream.close()` 处 Wait；`parallelDynamicStream.Close()` 逐个关闭 stream。
- `utils/threadpool`：`thread_pool.go:44`、`wait_reactor.go:102/103`，`Stop()` 等待两个 wg。
- `utils/chann`：`coordinator/coordinator.go:119` → `:469` `CloseAndDrain`。
- 其他：`pkg/fsutil/file_allocator.go:56`、`pkg/chdelay/channel_delayer.go:64`、`pkg/encryption/manager.go:478`、`pkg/pdutil/clock.go:86`、`pkg/upstream/upstream.go:252`（`up.wg.Wait()`）、`pkg/messaging/helper.go:50`（`stop()` 内 Wait）、`logservice/eventstore/gc.go:194/273`、`logservice/schemastore/schema_store.go:688`、`logservice/schemastore/persist_storage.go:289/290`、`logservice/schemastore/validator.go:80`、`downstreamadapter/dispatcher/block_event_executor.go:53`。
- errgroup 用法（server 模块、redo writer/reader、applier、workerpool、tcpserver、eventservice、message center、cmd 各 consumer）均已 Wait。

## 测试与工具代码

`*_test.go` 282 处、`tests/` 与 `tools/` 41 处 goroutine 启动点中，仅约 17 处附近存在 wg/errgroup，其余为测试并发用例的 fire-and-forget。这些不影响生产，但会让 goroutine 泄漏类问题在测试中不被发现，因为测试进程结束时无法区分"本应退出的 goroutine 没退出"。

## 局限

- 只覆盖仓库自身代码的 goroutine；clientv3、sarama、pebble、grpc 等第三方库内部启动的 goroutine 未纳入。
- `tests/`、`tools/` 下 323 处测试 goroutine 只做了统计与抽样，未逐条给出生命周期结论。
- 结论基于静态阅读，未通过运行时 goroutine dump 或泄漏检测工具验证。
