# TiCDC Consumer 统一实现任务

本清单按依赖顺序执行。完成一个任务后删除对应条目；所有任务完成后删除本文件。实现阶段只做编译检查，单元测试与 CI 在 T16 统一运行。设计约束以 [consumer-unification-design.md](./consumer-unification-design.md) 为准。

读取侧拥有解码、顺序、位置和完成计数。共享 writer 在写入成功后调用完成回调，公共 consumer 通过 `Confirm(ctx)` 推进来源位置。最小单元测试位于 `cmd/consumer/consumer_test.go` 和 `cmd/consumer/writer_test.go`。

- [ ] **T16 执行最终 CI 并关闭迁移工作**

  - 入口：
    - [统一 CI 启动脚本](../../tests/integration_tests/_utils/run_consumer#L1)。
    - [Pulsar 基础场景](../../tests/integration_tests/canal_json_basic/run.sh#L31)。
    - [consumer 镜像](../../deployments/consumer.Dockerfile#L1)。
  - 完成条件：仓库 CI 全部通过；Kafka、Pulsar 和 Storage consumer 场景全部由 `cdc_consumer` 执行；设计文档的 completion criteria 逐项满足；仓库中不留下临时兼容分支、旧进程名或未跟踪的后续工作。
  - 验证：运行 consumer 最小单元测试、`make check`、`make integration_test_build` 和仓库完整 CI；构建 consumer 镜像并检查启动命令。保存 Kafka、Pulsar、Storage 三类 integration test 结果及失败重试记录，覆盖多 topic、DDL、失败恢复和认证场景。最小约束包括 Kafka DDL 副本覆盖、DDL 成功后各副本记录才能完成、Kafka 全部 partition watermark 的最小值、Pulsar topic 级 watermark 与下游完成后的累计确认、DDL 影响范围、连续完成位置、批次组装取消后的状态回滚和资源上限。CI 必须以 MySQL sink 默认 batch DML 路径覆盖复合主键、key-only delete、缺少 before image 的 update、同 batch 重放、在途重放和边界重放。完整 CI 通过后删除本任务文件。
