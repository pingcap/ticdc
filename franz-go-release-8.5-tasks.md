# Franz-go Kafka sink 回移 release-8.5 任务清单

所有改动统一收进 [#6411](https://github.com/pingcap/ticdc/pull/6411)。清单保留未完成工作，下一项始终置于首位。实现完成后移除对应任务，剩余验证另建任务；受阻任务后移并注明解除条件。按用户要求，不在本地运行测试；测试和构建验证交给 CI。

- [ ] 1. 在 CI 验证最终组合。
  - 来源：#6411 的当前提交；目标分支基线为 `de31ad8261a43ebaade6ca2b222028c02987b942`（#6408）。
  - 执行：由 CI 完成仓库检查、构建及 Kafka、topic manager、codec、认证、监控和 redo 相关测试；逐项记录通过、失败、未执行及阻塞原因。
  - 完成标准：最终提交的检查和测试通过；ABI 阻塞的检查留在末项，TiDB 依赖就绪后补跑。历史提交或隔离环境的结果不代表最终提交验证通过。

- [ ] 2. franz-go 提供请求批次数 hook 后接入 batches/request 监控。
  - 来源：[#6358](https://github.com/pingcap/ticdc/pull/6358)。
  - 位置：`pkg/sink/kafka/franz_metrics_hook.go:24`。
  - 阻塞：当前锁定的 franz-go v1.22.2 没有 `HookProduceRequestEncoded` 或 `OnProduceRequestEncoded` 调用点；源 PR 的直接方法调用测试无法证明客户端会触发该 hook。当前提交移除无法采集的指标和面板。
  - 完成标准：有受支持的客户端 API 后接入指标，配齐注册、清理和 dashboard；在 CI 通过真实 producer/fake broker 链路确认请求触发采样。

- [ ] 3. TiDB release 依赖就绪后补跑 ABI 阻塞的 CI 验证。
  - 位置：`go.mod:68` 的 TiDB 依赖。
  - 保留 release-8.5 的 TiDB 依赖与 ABI 校验。当前依赖仍限制 Go 1.25；TiDB #66254 已合入 master，但 release-8.5 尚无 `map_abi_go126.go`。Go 1.26 下的已知阻塞见 [TiFlow #12877 的说明](https://github.com/pingcap/tiflow/pull/12877#issuecomment-6143013862)；运行验证需要 TiDB release 依赖包含 [#66254](https://github.com/pingcap/tidb/pull/66254) 的兼容实现。
  - 完成标准：TiDB 侧兼容实现就绪后在 CI 补跑受阻检查，更新验证结果和 PR 说明。
