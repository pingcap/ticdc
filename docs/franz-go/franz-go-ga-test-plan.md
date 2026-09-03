# TiCDC Kafka Sink franz-go GA 测试计划

Last updated: 2026-09-02
Status: 评审稿
Scope: franz-go Kafka Sink 的正确性、故障恢复、性能和可观测性验证
Related documents:

- [执行计划](https://pingcap.feishu.cn/wiki/YK6UwCWn0iNvlfkAGDncAOK2nWh)
- [Milestone 1 TODO List](./milestone-1-todo-list.md)

## 1. 测试原则

- 测试以最终行为为准，不限定 SDK、caselib 或 Test Plan 的具体实现。
- 现有 testcase 能覆盖的场景，使用指定的 franz-go TiCDC image 直接运行，不改造 test-infra。
- 只有现有 test-infra 无法构造或验证的场景才新增代码。
- correctness testcase 负责消费和数据一致性校验；专项 testcase 只验证对应能力，不重复完整业务流程。

## 2. 正确性

执行方式：使用指定的 franz-go TiCDC image 运行现有 Kafka testcase，不需要修改 test-infra。

- [ ] 覆盖初始同步、增量同步、支持的协议和 dispatcher。
- [ ] 覆盖 message-size 边界、large message、claim-check 和 handle-key-only。
- [ ] 覆盖 Topic 自动创建、已有 Topic、partition 变化、Topic 配置和 Schema Registry 正常与异常路径。
- [ ] 覆盖多 Changefeed、扩缩容和 HA，并校验 callback、checkpoint、消息顺序和最终消费结果。

通过标准：所有 correctness testcase 通过，不存在数据丢失、重复 callback 或 checkpoint 提前推进。

## 3. 鲁棒性与故障恢复

执行方式：复用现有 Kafka chaos testcase；只为缺失场景新增 test-infra 代码。

- [ ] 补充多 broker 故障、滚动升级、metadata 变化、request timeout、retry、idle connection 和 broken pipe 场景。
- [ ] 补充 controller 和 broker 的网络延迟、丢包及更多网络分区组合。
- [ ] 验证 broker 长时间不可用时内存有界，取消和关闭能够解除等待。
- [ ] 验证恢复期间 partition 内消息顺序、callback 和 retry 行为。
- [ ] 使用 Kafka 集群状态、checkpoint 和数据一致性判断恢复结果，不使用固定 sleep。
- [ ] 失败时保存 TiCDC、Kafka、consumer 日志、关键 metrics、集群状态和测试时间范围。

通过标准：每个故障用例都能命中目标节点；故障解除后 Kafka 集群恢复可用，checkpoint 最终追平，消费结果与上游一致，资源使用保持有界。

## 4. 性能

执行方式：新增三 broker 性能 testcase。

- [ ] Kafka Topic replication factor 为 3，`min.insync.replicas` 为 2。
- [ ] 固定 Kafka 集群、Topic、partition、TiCDC 规格、workload、预热方式、数据规模和重复次数。
- [ ] 覆盖 sysbench、bank、jitu，单表和多表，以及 table 和 index-value dispatcher。
- [ ] 执行 Changefeed create、pause、workload、resume、catch-up 和一致性校验。
- [ ] 记录吞吐、catch-up 时间、p99、TiCDC/Kafka CPU 和内存、GC、goroutine、heap、batch、buffer、retry、error 和 consumer lag。
- [ ] 保存测试参数、代码版本、资源规格和原始时间序列，并复验超出阈值的结果。

通过标准：所有场景完成一致性校验，吞吐、延迟、CPU 和内存满足确定的阈值。

## 5. 可观测性

执行方式：新增 Prometheus 查询和 Dashboard 断言 testcase。

- [ ] 验证 franz-go logger、metrics collector 和敏感信息脱敏。
- [ ] 覆盖吞吐、request/response rate、latency、retry、error、batch、buffer 和 broker 指标。
- [ ] 验证 metrics label 不包含非预期高基数值，Changefeed 关闭后对应 series 被清理。
- [ ] 分别产生正常写入、retry、broker 故障和恢复流量，验证指标随场景变化。
- [ ] 验证 Dashboard PromQL 可执行，并保存 Prometheus 原始结果、Dashboard 定义和对应日志。

通过标准：每个 franz-go Dashboard panel 都有自动查询断言，日志和指标足以诊断正常写入、重试、错误和恢复。

## 6. 代码变更验证

仅在新增或修改 test-infra 代码时执行：

- [ ] 在仓库根目录运行 `go test ./common/model/resource/...`。
- [ ] 在 `sdk` 模块运行 `go test ./resource/impl/k8s/...`。
- [ ] 在 `caselib` 模块运行 `go test ./pkg/model/kafka/... ./pkg/steps/...`。
- [ ] 运行新增端到端 Test Plan，并保存资源状态和诊断产物。

## 7. 阻塞输入

- [ ] 确定性能场景的数据量、partition 数、重复次数以及吞吐、延迟、CPU 和内存阈值。
- [ ] 确定 franz-go 指标名、label 和 Dashboard 定义。

## 8. 执行阶段

- 开发与 PR：运行改动涉及的单元测试和集成测试。
- Nightly：运行完整 correctness regression、故障恢复和可观测性测试。
- Release：在 Nightly 范围上增加完整性能和稳定性验证。

GA 前不得遗留阻塞发布的正确性、稳定性、资源、性能或可观测性问题。
