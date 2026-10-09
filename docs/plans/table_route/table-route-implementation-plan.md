# Table Route 实施计划

- 状态：进度更新，待补齐 conflict 检测与 Pulsar 验证
- 最后更新：2026-05-19
- 基线：`upstream/master`
- 关联文档：`docs/plans/table_route/table-route-requirements.md`、`docs/plans/table_route/table-route-design.md`
- 说明：本文是基于需求文档和设计文档派生出的执行计划。若执行细节与需求或设计冲突，以需求和设计文档为准。

## 1. 实施目标

实现按阶段推进，但共享模型按终态一次收口。总体目标只有五条：

1. 用户可以通过 `target-schema` / `target-table` 配置下游目标名。
2. dispatch 继续按上游名计算。
3. 已生效 sink 对外一致地输出目标名。
4. 非法配置、静态冲突和运行时冲突都按契约失败。
5. steady-state DML row path 不重复做 route 计算。

## 2. 当前分支进度摘要

核对依据：

- 当前分支代码实现。
- 本地 git 提交历史中带 PR 号的 table route 合入记录。

已合入的 table route 相关 PR：

1. `#4654`：配置契约与 API 字段，commit `91fd9855b`。
2. `#4658`：事件模型 target 字段，commit `b2b596352`。
3. `#4659`：routing 核心、DDL rewrite 与 dispatcher 接入，commit `29a85764e`。
4. `#5006`：MySQL / TiDB sink 与 sqlmodel 接入，commit `21f52e04a`。
5. `#5053`：redo 持久化与 replay 保留 target 名，commit `e9c24f63d`。
6. `#5071`：storage sink path / metadata / codec 接入，commit `a70cbecd6`。
7. `#5084`：Kafka sink 与 MQ codec 接入，commit `b2a9a57ef`。

当前完成情况：

- `V1` MySQL / TiDB sink：核心链路已完成；配置、共享模型、router、DML、DDL、rename DDL 和集成测试已落地。
- `V1.1` Redo：redo 持久化和 redo apply 使用 target 名的链路已落地，并有 `redo_apply_table_route` 集成测试。
- `V2` Kafka / Pulsar：Kafka 已完成多协议 codec 与端到端集成测试；Pulsar 复用 shared codec 和 sink 的 source dispatch 路径，但当前没有 table route 专项集成测试。
- `V3` Storage：storage 的 DML 文件路径、schema 文件、表定义和 `canal-json` / `csv` 集成测试已落地。

当前主要缺口：

- 静态 route conflict 检查尚未落地。
- per-changefeed `TargetTableRegistry` 和运行时 route conflict 检查尚未落地。
- `DDLEvent.GetEvents()` 对 routed multi `RENAME TABLE` 的 fan-out 仍需要专项修复和测试，Kafka / Pulsar / Storage sink 都会调用该方法。
- Pulsar 需要补充 table route 端到端集成测试。
- 发布说明、用户文档、Support FAQ、性能验证和 conflict 场景验证还没有完成到 launch-ready 状态。

## 3. 交付阶段

- `V1`：MySQL / TiDB sink，已完成。
- `V1.1`：Redo，已完成。
- `V2`：Kafka / Pulsar sink，Kafka 已完成；Pulsar 需要补专项验证。
- `V3`：Storage sink，已完成。

`V1` 必须完成的共享基础设施：

- `DispatchRule.TargetSchema` / `TargetTable`：已完成。
- `TableName.TargetSchema` / `TargetTable`：已完成。
- routed `TableInfo`：已完成。
- routed `DDLEvent`：已完成。
- shared router：已完成。
- 静态冲突检查：未完成。
- `TargetTableRegistry`：未完成。

未进入当前阶段的 sink 继续遵循“接受配置但不生效”的契约。

## 4. 工作拆解

### 4.1 配置契约

- [x] 在 `pkg/config.DispatchRule` 中增加 `TargetSchema` / `TargetTable`
- [x] 增加占位符和表达式校验
- [x] 在 create / update changefeed 路径统一执行 table route 通用校验
- [x] 保持非 MQ sink 配置裁剪时不误删 table route 规则

建议涉及文件：

- `pkg/config/sink.go`
- `pkg/config/sink_test.go`
- `pkg/config/changefeed.go`
- `pkg/config/changefeed_test.go`
- `api/v2/model.go`

### 4.2 路由核心与共享模型

- [x] 新增 `downstreamadapter/routing`
- [x] 编译 `matcher`，展开 `TargetSchema` / `TargetTable`
- [x] 提供统一 route 计算入口；当前对外通过 `ApplyToTableInfo` / `ApplyToDDLEvent` 消费，`RouteName` 未单独导出
- [x] 提供 routed `TableInfo`
- [x] 提供 routed `DDLEvent`
- [x] 为 `TableName` 增加 `TargetSchema` / `TargetTable`
- [x] 为 `TableInfo` 提供 `CloneWithRouting`
- [x] 为 `DDLEvent` 增加 runtime-only target 访问接口

建议涉及文件：

- `downstreamadapter/routing/*`
- `pkg/common/table_name.go`
- `pkg/common/table_info.go`
- `pkg/common/event/ddl_event.go`

### 4.3 冲突检测

- [ ] 在配置阶段实现基于当前对象集合的静态 route conflict 检查
- [ ] 引入 per-changefeed `TargetTableRegistry`
- [ ] 在表进入复制范围、离开复制范围、rename、drop 等边界注册或释放 target 占用关系
- [ ] 运行时冲突通过既有错误链路让 changefeed fail

建议涉及文件：

- `downstreamadapter/routing/*`
- `downstreamadapter/eventcollector/*`
- `downstreamadapter/dispatchermanager/*`

### 4.4 DML / DDL 接入边界

- [x] 在 `TableInfo` 初始化和 table version 更新边界应用 route
- [x] 将 routed `TableInfo` 缓存到 dispatcher/table-version 相关边界
- [x] steady-state DML 只复用 routed `TableInfo`
- [x] DDL 进入 sink 前统一构造 routed DDL 视图
- [x] rename DDL 正确处理 old / new 两组 target 名
- [ ] DDL 与 `TargetTableRegistry` 协同更新 target 占用关系
- [ ] 单独修复 `DDLEvent.GetEvents()` 对 routed multi `RENAME TABLE` 的拆分：router 可能产出单条逗号形式 SQL，但 `MultipleTableInfos` 有多项；当前 `SplitQueries` 只返回 1 条 statement，使用 `GetEvents()` fan-out 的 sink 会在数量不一致时 panic

建议涉及文件：

- `downstreamadapter/eventcollector/*`
- `downstreamadapter/dispatcher/*`
- `pkg/common/event/ddl_event.go`
- `downstreamadapter/routing/*`

### 4.5 Sink 分阶段接入

`V1` MySQL / TiDB：

- [x] DML SQL builder 读取 routed `TableInfo`
- [x] DDL 执行路径读取 routed schema 和 rewritten query

`V1.1` Redo：

- [x] redo 持久化读取 target 名
- [x] redo replay 后继续看到 target 名

`V2` Kafka / Pulsar：

- [x] payload codec 读取 target 名
- [x] DDL 相关消息中的名称字段与 SQL 文本同步体现 target 名
- [x] `topic` / `partition` 继续读取上游名
- [ ] 补充 Pulsar table route 专项端到端验证

`V3` Storage：

- [x] 路径中的 schema / table 命名读取 target 名
- [x] schema 文件、表定义文件或同类 metadata 读取 target 名

建议涉及文件：

- `pkg/sink/mysql/*`
- `downstreamadapter/sink/mysql/*`
- `downstreamadapter/sink/redo/*`
- `pkg/applier/redo.go`
- `pkg/sink/codec/*`
- `downstreamadapter/sink/kafka/*`
- `downstreamadapter/sink/pulsar/*`
- `pkg/sink/cloudstorage/*`
- `downstreamadapter/sink/cloudstorage/*`

### 4.6 文档、发布与验证

- [ ] 发布说明只声明当前阶段已生效的 sink
- [ ] FAQ 和用户文档明确未生效 sink 的行为
- [x] 集成测试覆盖 MySQL / TiDB sink 的 DML / DDL rename 场景
- [ ] 补齐性能和冲突处理验证结果

## 5. 推荐 PR 拆分顺序

1. 配置契约与 API 字段
2. `TableName` / `TableInfo` / `DDLEvent` 的共享模型改动
3. routing 核心
4. 静态冲突检查
5. `TargetTableRegistry`
6. DML 边界接入
7. DDL routed 视图与 rename 语义
8. MySQL / TiDB sink 接入
9. `V1` 测试、文档与发布说明
10. Redo 接入
11. Kafka / Pulsar 接入
12. Storage 接入

## 6. 分阶段验证

### 6.1 `V1`

- [x] 配置 `target-schema` / `target-table` 后，MySQL / TiDB sink 得到正确目标名
- [x] DML 与 DDL 对外一致地体现目标名
- [x] rename DDL 的 old / new 语义正确
- [x] `topic` / `partition` 等 dispatch 结果不受影响
- [x] 非法占位符、非法表达式在 create / update 阶段失败
- [ ] 静态 route conflict 在 create / update 阶段失败
- [ ] 运行时新增表或 rename 导致的冲突会让 changefeed fail
- [ ] 持续 DML 负载场景没有明显性能回退

### 6.2 `V1.1`

- [x] redo 持久化和 replay 都体现目标名

### 6.3 `V2`

- [x] Kafka payload 中的 schema / table 字段体现目标名
- [ ] Pulsar payload 中的 schema / table 字段通过专项集成测试验证
- [x] `topic` / `partition` 继续基于上游名

### 6.4 `V3`

- [x] Storage 路径、schema 文件和表定义中的命名体现目标名

## 7. 还需要做的工作

1. 实现静态 route conflict 检查。
   - create / update changefeed 时，基于当前对象集合计算 source 到 target 映射。
   - 同一个 `(targetSchema, targetTable)` 被不同 source table 占用时直接报错。
   - 错误信息需要包含冲突 source、target 和命中的 rule 信息。
2. 实现运行时 route conflict 检查。
   - 引入 per-changefeed `TargetTableRegistry`。
   - 在新表进入复制范围、drop、rename、table version 更新等边界注册或释放 target。
   - 冲突通过既有 dispatcher / maintainer 错误链路让 changefeed fail。
3. 修复并测试 `DDLEvent.GetEvents()` 的 routed multi `RENAME TABLE` fan-out。
   - 当前 Kafka / Pulsar / Storage 都会在 DDL 发送或写 schema 文件时调用 `GetEvents()`。
   - 需要覆盖 `ActionRenameTables` 的逗号形式 SQL、`MultipleTableInfos` 多项和 routed old / new target 名。
4. 补 Pulsar table route 端到端验证。
   - 复用 `tests/integration_tests/table_route` 的 workload。
   - 覆盖 Pulsar 当前支持的 `canal-json` 协议和 source topic / partition 语义。
5. 补齐发布和运维材料。
   - 发布说明明确 MySQL / TiDB、redo、Kafka、storage 的当前支持状态，以及 Pulsar 验证状态。
   - 用户文档、FAQ 和 Support 说明补齐冲突处理、未生效/未验证 sink、升级与回退限制。
6. 补性能和 conflict 场景验证。
   - 持续 DML workload 下比较开启/关闭 table route 的吞吐和延迟。
   - 增加静态冲突、运行时新表冲突、rename 冲突的单元或集成测试。

## 8. 风险与关注点

- route 逻辑如果分散到 sink / codec，会导致命名口径分叉
- DDL rewrite 如果不是 all-or-nothing，容易出现 query 与结构化字段不一致
- runtime conflict 如果没有独立注册表，wildcard 新表和 rename 场景会漏检
- 未生效 sink 的行为必须保持稳定，否则“先保存配置、后续启用”这条升级路径会失效
