# Franz-go Kafka sink 回移 release-8.5 任务清单

所有改动统一收进 [#6411](https://github.com/pingcap/ticdc/pull/6411)。清单保留未完成工作，下一项始终置于首位。实现完成后移除对应任务，剩余验证另建任务；受阻任务后移并注明解除条件。按用户要求，不在本地运行测试；测试和构建验证交给 CI。

- [ ] 1. 在 CI 验证最终组合。
  - 来源：#6411 的当前提交；目标分支基线为 `de31ad8261a43ebaade6ca2b222028c02987b942`（#6408）。
  - 已有结果：`d7345f2c3` 的仓库 Check、Build Classic CDC 和 Check Next Gen Grafana 通过；Classic Unit Tests 在初始化时触发 TiDB 的 Go 1.25 ABI 限制。恢复 master 指标后的提交需要重新确认 CI；E2E Test Group 的成功只代表分组任务完成。
  - 执行：由 CI 完成仓库检查、构建及 Kafka、topic manager、codec、认证、监控和 redo 相关测试；逐项记录通过、失败、未执行及阻塞原因。
  - 完成标准：最终提交的检查和测试通过；ABI 阻塞的检查留在末项，TiDB 依赖就绪后补跑。历史提交或隔离环境的结果不代表最终提交验证通过。

- [ ] 2. 补齐 kafka_dump 的 Avro decoder（按用户要求后续处理）。
  - 来源：#6411 静态审查；用户要求保留问题。
  - 位置：`tests/utils/kafka_dump/main.go:40`。
  - 问题：代码引用 `avroMessageDecoder`，当前 release 回移缺少其定义，kafka_dump 无法编译。master 的实现位于 `tests/utils/kafka_dump/avro.go`；该文件在 #4167 之前已存在，因此源 PR 的差异没有带入它。
  - 完成标准：回移 decoder 和相关覆盖，在 CI 确认工具可编译，且 Schema Registry 查询、schema 缓存、合法和无效 Avro 消息处理符合 master。

- [ ] 3. 避免 factory 关闭删除其他客户端的指标（按用户要求后续处理）。
  - 来源：#6411 静态审查；用户要求保留问题。
  - 位置：`pkg/sink/kafka/franz_factory.go:124`、`pkg/sink/kafka/franz_metrics_hook.go:97`。
  - 问题：同一 keyspace/changefeed 的多个 factory 共用指标标签；关闭其中一个会按标签删除全部序列。仍运行的 hook 缓存已删除的指标句柄，后续更新无法导出。
  - 完成标准：只在最后一个同标签客户端关闭时清理指标；在 CI 覆盖 factory 重叠、创建失败和重复关闭，确认活跃客户端的指标持续导出。

- [ ] 4. 为 OAuth token 请求设置默认超时（按用户要求后续处理）。
  - 来源：#6411 静态审查；用户要求保留问题。
  - 位置：`pkg/sink/kafka/oauth2.go:44`、`pkg/sink/kafka/oauth2.go:87`。
  - 问题：默认 HTTP client 和自定义 CA client 均未设置请求超时；token source 使用创建时的 context，单次认证的 context 无法限制请求耗时。token 服务迟迟不响应时可能阻塞认证。
  - 完成标准：为未设置超时的 client 提供默认上限，保留调用方已有的正超时、transport 和 context 取消能力；在 CI 覆盖超时、取消及自定义 CA。

- [ ] 5. 接通 batches/request 的真实采集。
  - 来源：[#6358](https://github.com/pingcap/ticdc/pull/6358)。
  - 位置：`pkg/sink/kafka/franz_metrics_hook.go:113`。
  - 问题：[引入提交](https://github.com/pingcap/ticdc/commit/1f87bccedef18ee6c53a254c9bd027533f7ef9b3) 只在 TiCDC 添加 `OnProduceRequestEncoded`。franz-go v1.21.7 和 v1.22.2 均没有对应接口或调用点；master 仍保留该方法，且使用 v1.22.2、没有 franz-go replace，因此 master 上该指标也无法采样。回移保留源 PR 的指标、注册、清理、方法和面板，以与 master 一致。
  - 替代方案：现有 `OnProduceBatchWritten` 可统计成功批次；`OnBrokerWrite` 可按 API key 0 单独统计 Produce 请求。二者的时间窗比值只能提供近似均值，错误、重试和窗口边界会影响结果，无法提供逐请求分布与 P99。精确实现需要客户端暴露编码请求的批次数。
  - 完成标准：确定采用精确 hook 或重新定义近似指标；在 CI 通过真实 producer/fake broker 链路确认客户端触发采样。直接调用 hook 的单测仅验证记录逻辑。

- [ ] 6. TiDB release 依赖就绪后补跑 ABI 阻塞的 CI 验证。
  - 位置：`go.mod:68` 的 TiDB 依赖。
  - 保留 release-8.5 的 TiDB 依赖与 ABI 校验。当前依赖仍限制 Go 1.25；TiDB #66254 已合入 master，但 release-8.5 尚无 `map_abi_go126.go`。Go 1.26 下的已知阻塞见 [TiFlow #12877 的说明](https://github.com/pingcap/tiflow/pull/12877#issuecomment-6143013862)；运行验证需要 TiDB release 依赖包含 [#66254](https://github.com/pingcap/tidb/pull/66254) 的兼容实现。
  - 已有结果：[Classic Unit Tests 日志](https://github.com/pingcap/ticdc/actions/runs/38572648662/job/115881571873) 确认 `d7345f2c3` 在包初始化时触发上述 ABI panic。
  - 完成标准：TiDB 侧兼容实现就绪后在 CI 补跑受阻检查，更新验证结果和 PR 说明。
