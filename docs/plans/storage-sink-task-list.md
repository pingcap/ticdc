# Storage Sink Task List

> 本文是 storage sink 相关内容的唯一任务清单文档。
> 配套文档：
> - `docs/plans/storage-sink-requirements.md`
> - `docs/plans/storage-sink-design.md`

本文只列“当前分支真实代码状态”之后的剩余任务。本周已经把 review 里确认的问题沉淀到这里，剩余收口工作留到下周继续做。

当前分支已经有的内容是：

- `spool.Manager` 与它的序列化 / spill / release 逻辑
- cloudstorage writer 已经通过 spool 承载 pending batch
- `spool-disk-quota` 配置入口
- spool 基础指标

当前分支还没有的内容是：

- 把 quota 语义做成明确的运行时契约
- 把 spool 本地目录被外部破坏时的失败语义收口成一致契约
- 把 options 默认值和参数校验的职责边界理顺
- 围绕真实接入后的链路补完回归测试

## Tasks

- [ ] Task 1: Clarify and implement quota contract
  Goal: 把 `spool-disk-quota` 的实际语义说清楚，并在实现、测试和文档里保持一致。
  Scope: `pkg/config/sink.go`、`pkg/sink/cloudstorage/config.go`、`downstreamadapter/sink/cloudstorage/spool/quota.go`、`downstreamadapter/sink/cloudstorage/spool/manager.go` 及相关文档。
  Done when: 可以明确回答这个配置到底是软基线、总配额还是磁盘配额；达到阈值后的行为也有稳定契约。
  Verify: 配置解析测试，阈值行为测试，文档与实现对齐检查。

- [ ] Task 2: Define runtime contract for external spool path damage
  Goal: 把“运行中外部删除 spool 根目录”这类操作错误收口成稳定语义。
  Scope: `downstreamadapter/sink/cloudstorage/spool/manager.go`、相关测试，以及设计文档中的运行时说明。
  Done when: 可以明确回答 storage sink 应该何时报错、如何暴露错误，以及 changefeed 应如何恢复；测试保护的是这份契约，而不是当前偶然行为。
  Verify: 针对外部删除 spool 目录的单测，必要时补接入层错误传播测试。

- [ ] Task 3: Clean up options defaulting and validation
  Goal: 让默认值注入和参数校验的职责边界清晰，不再掩盖显式非法配置。
  Scope: `downstreamadapter/sink/cloudstorage/spool/manager.go`、`downstreamadapter/sink/cloudstorage/spool/quota.go` 及相关测试。
  Done when: 显式传入的非法零值或越界值不会被默认值静默覆盖；校验逻辑可读且可预测。
  Verify: options 单测，非法配置测试，默认值覆盖测试。

- [ ] Task 4: Complete runtime observability after integration
  Goal: 让 spool 在接入 sink 主链路后具备可用于排障的运行时观测。
  Scope: `downstreamadapter/sink/metrics/cloudstorage.go` 与 cloudstorage 接入点上的打点调用。
  Done when: 能从实际运行指标看出 spool 当前占用、spill 情况和 wake suppression 行为；指标不会出现空闲自增、重复注册或 label 泄漏。
  Verify: 指标注册测试，接入路径打点测试，close 后 label 清理测试。

- [ ] Task 5: Add integration-focused regression coverage
  Goal: 围绕已经接入 storage sink 的 spool 主链路补完关键回归测试。
  Scope: `downstreamadapter/sink/cloudstorage/*_test.go` 与必要的 `intest` 用例。
  Done when: 至少覆盖内存路径、spill 路径、callback 生命周期、close 清理、quota 相关行为，以及外部路径异常的错误传播。
  Verify: 轻量单测覆盖 spool 行为，必要时用 `-tags=intest` 补接入场景。

## Dependency Order

- Task 1、Task 2、Task 3 都是运行时契约收口，建议下周优先处理。
- Task 4 和 Task 5 适合在前 3 个边界稳定后继续推进。

## Document Rule

从现在开始，storage sink 只保留这 3 份文档：

- 需求：`docs/plans/storage-sink-requirements.md`
- 设计：`docs/plans/storage-sink-design.md`
- 任务清单：`docs/plans/storage-sink-task-list.md`

新的分析、review、troubleshooting 草稿不再单独长期保留；需要沉淀时，分别吸收到上述三类文档中。
