# Franz-go Kafka sink 回移 release-8.5 任务清单

所有改动统一收进 [#6411](https://github.com/pingcap/ticdc/pull/6411)。清单保留未完成工作，下一项始终置于首位。整理回移时引入的遗漏和行为回退直接恢复；源代码已有、核对时发现的问题单独记录。实现完成后移除对应任务，剩余验证另建任务。按用户要求，不在本地运行测试；最终 CI 验证放在末尾。

- [ ] 1. 为 OAuth token 请求设置默认超时（源代码已有问题，后续处理）。
  - 历史来源：
    - 2023-05-25，[TiFlow #8938](https://github.com/pingcap/tiflow/pull/8938) 首次加入 Kafka OAuth 支持，直接使用 `cfg.TokenSource(ctx)`，没有设置独立的 HTTP 请求超时。
    - 2024-09-10，[TiCDC #274](https://github.com/pingcap/ticdc/pull/274) 带入该实现，沿用相同超时行为。
    - 2026-09-01，[#6140](https://github.com/pingcap/ticdc/pull/6140) 增加 OAuth 私有 CA 支持，新建的 HTTP client 同样没有设置 `Timeout`。
    - 2026-09-22，[#4167](https://github.com/pingcap/ticdc/pull/4167) 提取共享 helper，Sarama 和 franz-go 共用它；缺少默认超时的行为继续保留。已核对当前 master 提交 `835726c4bf`，仍是此实现。
  - 位置：`pkg/sink/kafka/oauth2.go:46`、`pkg/sink/kafka/oauth2.go:124`。
  - 问题：默认路径使用的 `http.DefaultClient` 和自定义 CA client 均未设置独立的请求总时限。token source 使用创建时的 context，其取消和 deadline 仍然有效；没有这些限制时，token 服务持续不响应可能长时间阻塞认证。Kafka broker 的读写超时不会为该 HTTP 请求设置时限。
  - 完成标准：为未设置超时的 client 提供默认上限，保留调用方已有的正超时、transport 和 context 取消能力；在 CI 覆盖超时、取消及自定义 CA。

- [ ] 2. 接通 batches/request 的真实采集（源代码已有问题，后续处理）。
  - 来源：[#6358](https://github.com/pingcap/ticdc/pull/6358)。
  - 位置：`pkg/sink/kafka/franz_metrics_hook.go:113`。
  - 问题：[引入提交](https://github.com/pingcap/ticdc/commit/1f87bccedef18ee6c53a254c9bd027533f7ef9b3) 只在 TiCDC 添加 `OnProduceRequestEncoded`。franz-go v1.21.7 和 v1.22.2 均没有对应接口或调用点；#6541 对应的 master 提交 `835726c4bf` 仍保留该方法，且使用 v1.22.2、没有 franz-go replace，因此该源版本也无法采样。回移保留源 PR 的指标、注册、清理、方法和面板，以与 master 一致。
  - 替代方案：现有 `OnProduceBatchWritten` 可统计成功批次；`OnBrokerWrite` 可按 API key 0 单独统计 Produce 请求。二者的时间窗比值只能提供近似均值，错误、重试和窗口边界会影响结果，无法提供逐请求分布与 P99。精确实现需要客户端暴露编码请求的批次数。
  - 完成标准：确定采用精确 hook 或重新定义近似指标；在 CI 通过真实 producer/fake broker 链路确认客户端触发采样。直接调用 hook 的单测仅验证记录逻辑。

- [ ] 3. TiDB release 依赖就绪后补跑 ABI 阻塞的 CI 验证。
  - 位置：`go.mod:68` 的 TiDB 依赖。
  - 保留 release-8.5 的 TiDB 依赖与 ABI 校验。当前依赖仍限制 Go 1.25；TiDB #66254 已合入 master，但 release-8.5 尚无 `map_abi_go126.go`。Go 1.26 下的已知阻塞见 [TiFlow #12877 的说明](https://github.com/pingcap/tiflow/pull/12877#issuecomment-6143013862)；运行验证需要 TiDB release 依赖包含 [#66254](https://github.com/pingcap/tidb/pull/66254) 的兼容实现。
  - 已有结果：`034536215` 的 [Classic Unit Tests 日志](https://github.com/pingcap/ticdc/actions/runs/38576303294/job/115942459619) 中的 panic 均来自上述 ABI 限制。
  - 完成标准：TiDB 侧兼容实现就绪后在 CI 补跑受阻检查，更新验证结果和 PR 说明。

- [ ] 4. 在 CI 验证最终组合。
  - 来源：#6411 的最终提交；目标分支基线为 `de31ad8261a43ebaade6ca2b222028c02987b942`（#6408）。代码核对覆盖 admin 接口及调用方、topic 可见性与错误分类、大小写参数、消息与批次大小、写入围栏、关闭顺序、认证、模块文件及工具；最终运行行为仍需 CI 确认。
  - 基线回归依据（已核对合并状态及提交祖先关系）：
    - [#5883](https://github.com/pingcap/ticdc/pull/5883) 的独立回移 [#6527](https://github.com/pingcap/ticdc/pull/6527) 已关闭，改动由已合并的 [#6405](https://github.com/pingcap/ticdc/pull/6405) 带入；保留客户端无关 admin 接口、元数据可见性重试及错误处理。
    - [#6257](https://github.com/pingcap/ticdc/pull/6257) 的独立回移 [#6407](https://github.com/pingcap/ticdc/pull/6407) 已关闭，改动由已合并的 [#6408](https://github.com/pingcap/ticdc/pull/6408) 带入；保留 changefeed 级大小写参数。
    - [#5420](https://github.com/pingcap/ticdc/pull/5420) 经 [#5772](https://github.com/pingcap/ticdc/pull/5772) 回移，随后被 [#6010](https://github.com/pingcap/ticdc/pull/6010) 回退，最终由 [#6400](https://github.com/pingcap/ticdc/pull/6400) 恢复拆分；后三个 PR 均已合并到 release-8.5。保留独立的 `MaxMessageBytes` 和 `MaxBatchedBytes`。
  - 已有结果：`034536215` 的仓库 Check、Build Classic CDC 和 Check Next Gen Grafana 通过；Classic Unit Tests 受上一项 ABI 限制阻塞。集成检查和 error log review 显示失败，具体原因尚未确认。E2E Test Group 的成功只代表分组任务完成。
  - 执行：在恢复 Avro decoder 和 OAuth 错误脱敏后，由 CI 验证最终提交的构建、工具、Kafka、topic manager、codec、认证、监控和 redo 相关测试；重点确认 decoder 可编译、Schema Registry 解码与缓存正常、OAuth 错误不含凭据。逐项记录通过、失败、未执行及阻塞原因，并检查集成检查和 error log review 的失败日志。
  - 完成标准：最终提交的检查和测试通过；ABI 阻塞的检查在 TiDB 依赖就绪后补跑。历史提交或隔离环境的结果不代表最终提交验证通过。
