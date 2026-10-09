# Table Route PR2 Common Model 评审

## 范围

分支：`table-route-pr2-common-model`

本次 review 使用的 diff 基线：`upstream/master...HEAD`

这个分支的实际改动范围不大，而且非常集中。总共只改了 10 个文件，全部位于：

- `pkg/common/`
- `pkg/common/event/`

这是一组纯粹的 common model 层改动，还没有把完整的 routing 行为一路接通到所有 downstream sink。

## 改了什么

### 1. 表名模型新增 source/target 双字段

文件：

- `pkg/common/table_name.go`
- `pkg/common/table_name_gen.go`

改动：

- `common.TableName` 新增 `TargetSchema` 和 `TargetTable`
- 新增 `GetSchema()`、`GetTable()`、`GetTargetSchema()`、`GetTargetTable()`
- 新增 `QuoteTargetString()`
- 更新 msgpack 生成代码，确保新字段在持久化 / 传输过程中不会丢失

为什么需要这样做：

- table routing 需要一个结构化的位置来携带 routed name
- 把 source name 和 target name 放在同一个值对象里，可以避免把零散的 route 结果通过很多无关 API 传来传去
- common model 支持必须先于 downstream wiring 落地

### 2. TableInfo 具备 route 感知能力，但不直接修改共享的 schema-store 对象

文件：

- `pkg/common/table_info.go`
- `pkg/common/table_info_test.go`

改动：

- `InitPrivateFields()` 现在会用 target name 构造预生成 SQL
- 新增 `CloneWithRouting(targetSchema, targetTable)`
- 新增 `GetTargetSchemaName()` / `GetTargetTableName()`
- 保持原有的 `Schema/Table` 字段不变

为什么需要这样做：

- DML 热路径不能接受每一行都做一次 route rewrite
- routing 应该发生在 table-version 边界上，然后复用 routed `TableInfo`
- schema-store 里的 `TableInfo` 是共享对象，所以 routing 必须是 copy-on-write，而不是原地修改

### 3. DDLEvent 增加显式的 routed-name 槽位

文件：

- `pkg/common/event/ddl_event.go`
- `pkg/common/event/ddl_event_test.go`

改动：

- `DDLEvent` 增加私有 routed-name 字段：
  - `targetSchemaName`
  - `targetTableName`
  - `targetExtraSchemaName`
  - `targetExtraTableName`
- 新增 `GetTargetSchemaName()` / `GetTargetTableName()` / `GetTargetExtra*()`
- `GetEvents()` 现在会在拆分 multi-table DDL 时尽量保留 routed name
- 新增 `NewRoutedDDLEvent()`，用于构造 routed DDL，同时不修改原始 source event

为什么需要这样做：

- DDL 比单纯的表级 routing 更复杂，尤其 rename 语义同时包含 old name 和 new name
- downstream 代码需要一个稳定 API 来获取 routed name，而不是到处重新解析 query 字符串
- routing 不应该破坏 source identity，因为 dispatch/topic matching 仍然依赖 source name

### 4. Batch DML 组装逻辑在本地快速路径上可以替换为 routed TableInfo

文件：

- `pkg/common/event/dml_event.go`

改动：

- `BatchDMLEvent.AssembleRows()` 现在即使在 `Rows` 已经存在时，也会重新绑定 `TableInfo`

为什么需要这样做：

- same-node 的 batch DML 路径会跳过 row decode
- 如果没有这次 rebind，本地 batch DML 将继续携带 source `TableInfo`
- 那样 routed SQL / codec 输出在本地路径和远端路径上就会不一致

### 5. Redo 持久化 routed name

文件：

- `pkg/common/event/redo.go`
- `pkg/common/event/redo_gen.go`
- `pkg/common/event/redo_test.go`

改动：

- redo DML 写入 routed table name
- redo DDL 写入 routed table name
- redo decode 从这个 routed view 还原 DDL/DML
- 测试断言 redo round-trip 会保留 routed naming 和列元数据

为什么需要这样做：

- redo replay 是面向 downstream 的恢复路径
- 如果 redo 保存的是 source name，而 sink 执行的是 target name，replay 语义就会漂移

## 建议阅读路径

如果你只有 10 分钟，建议只看这三个位置：

1. `pkg/common/table_info.go:143`
2. `pkg/common/event/ddl_event.go:252`
3. `pkg/common/event/dml_event.go:281`

推荐的完整阅读顺序：

1. `pkg/common/table_name.go`
   - 先理解最基础的 source/target 命名契约。

2. `pkg/common/table_info.go`
   - 看 `CloneWithRouting()` 和 `InitPrivateFields()`。
   - 确认 routing 是 copy-on-write，且预生成 SQL 已经切换到 target name。

3. `pkg/common/event/ddl_event.go`
   - 看 target getter 的语义。
   - 看 `GetEvents()` 对 `CreateTables` 和 `RenameTables` 的拆分行为。
   - 看 `NewRoutedDDLEvent()` 是否保持了 source 字段的稳定性。

4. `pkg/common/event/dml_event.go`
   - 看 `BatchDMLEvent.AssembleRows()` 在本地 batch 路径上的正确性。

5. `pkg/common/event/redo.go`
   - 确认 redo 是否按预期持久化 routed name。

## 主要 Review 关注点

### 关注点 1

文件：

- `pkg/common/event/dml_event.go:281`

关注点：

- `BatchDMLEvent.AssembleRows()` 现在新增了一个 `b.Rows != nil` 分支，会在本地 batch 快速路径上把 `TableInfo` 重新绑定到 routed table info。
- 这里最关键的契约不是“是否一定要报错”，而是“只有在 routed rebind 场景下才做版本一致性检查”。

为什么这很重要：

- 调用方是在消费时读取当前 dispatcher 的 `tableInfo`
- `Rows != nil` 说明 rows 已经 materialize 完成，这时不能无条件拿当前 `tableInfo` 去做版本比较
- 正确的语义应该是：
  - 非 routed 场景直接返回，不做重绑
  - routed 重绑场景才检查 `UpdateTS`
  - routed clone 因为保持同一个 schema version，所以应当允许通过

建议的 review 问题：

- 当前实现是否严格满足“只在 routed rebind 时校验版本一致性”这个约束？

## 问题：`GetEvents()` 里的这些 target 赋值有必要吗？

讨论中的代码：

```go
targetSchemaName: info.GetTargetSchemaName(),
targetTableName:  info.GetTargetTableName(),
```

以及：

```go
if model.ActionType(d.Type) == model.ActionRenameTables {
    event.ExtraSchemaName = d.TableNameChange.DropName[i].SchemaName
    event.ExtraTableName = d.TableNameChange.DropName[i].TableName
    targetExtraSchemaName, targetExtraTableName := extractRenameTargetExtraFromQuery(queries[i])
    event.targetExtraSchemaName = targetExtraSchemaName
    event.targetExtraTableName = targetExtraTableName
}
```

我的结论：

### 1. 拆分后的 child DDL 上，`targetSchemaName` / `targetTableName` 是有逻辑必要性的

前提是：`GetEvents()` 返回的 child `DDLEvent` 需要是“自包含”的，并且同时保留：

- source new name
- routed target new name

原因：

- child event 上的 `SchemaName` / `TableName` 是刻意保持为 source name 的
- 如果不设置这两个 target 字段，那么 `child.GetTargetSchemaName()` 和 `child.GetTargetTableName()` 就会回退到 source name
- 这样一来，拆分后的 child event 会在 `DDLEvent` API 这一层丢掉 routed new-name 信息

如果没有这两行，还会剩下什么：

- `child.TableInfo` 里可能仍然携带 target name

为什么这还不够：

- `DDLEvent` 的调用方不应该被迫去翻 `TableInfo` 才能拿到 routed name
- 一旦 `DDLEvent` 已经暴露了 `GetTarget*()` API，拆分后的 child event 就应该保证这些 API 的返回值是可信的

实践层面的补充：

- 在当前这个分支上，生产代码暂时还没有实际消费这些 target getter
- 所以今天它更多是在保证 common model 内部语义一致、以及为后续 wiring 提前铺路，而不是已经支撑了某条现有行为

### 2. 对 `RenameTables` 来说，`targetExtraSchemaName` / `targetExtraTableName` 更有必要

这两行比前面两行更站得住脚。

原因：

- rename 同时包含两组名字：
  - new table name
  - old table name
- 拆分之后，`ExtraSchemaName` / `ExtraTableName` 是刻意保持为 source old name 的
- routed old name 否则只会存在于改写后的 SQL 字符串里

如果没有这些赋值：

- `GetTargetExtraSchemaName()` / `GetTargetExtraTableName()` 会退回到 source old name
- 结构化访问 routed old name 的能力会丢失
- 后续任何消费者都必须再次去解析 `Query`

所以对于 rename 语义来说，这里是唯一一个结构化保留以下四组信息的位置：

- source old name
- source new name
- target old name
- target new name

## 设计解释

这里其实有两种可能的模型：

### 模型 A：source 字段始终保持 source，target 字段只是 overlay

这个分支目前对普通 `DDLEvent` 基本就是沿着这个模型在实现。

如果这是预期契约，那么 `GetEvents()` 里的这些 target 赋值就是正确的，而且应该保留。

### 模型 B：拆分后的 routed child DDL 直接变成 canonical target view

在这个模型下，拆分后的 child event 会直接把 `SchemaName/TableName/ExtraSchemaName/ExtraTableName` 全部改写成 target name，而不再需要私有 target 字段。

这个模型更简单，但它不是当前这个分支在其他位置所采用的做法。

所以在当前设计下，答案是：

- `GetEvents()` 里的 `targetSchemaName/targetTableName` 为了保持一致性是有必要的
- rename split 里的 `targetExtraSchemaName/targetExtraTableName` 也是有必要的，而且必要性更强

## 这个分支新增的测试

相关新增测试：

- `pkg/common/table_info_test.go`
- `pkg/common/event/ddl_event_test.go`
- `pkg/common/event/redo_test.go`

这些测试提供的覆盖：

- routed `TableInfo` 的 clone 语义
- routed `DDLEvent` 的 source/target 保留语义
- rename split 对 source 和 target 的 old/new name 的保留
- redo round-trip 对 routed name 的保留

关于测试手段的判断：

- `NewEventTestHelper` 在这里是值得保留的。它生成的数据更接近真实 DDL / schema / 列元信息，能减少手写 fixture 和真实行为脱节的问题。
- 对这个分支来说，优先使用 `NewEventTestHelper` 验证 common model 在真实输入下的行为，是合理且更推荐的做法。

仍然缺少的覆盖：

- 本地 batch DML 快速路径在 `Rows != nil` 且 routed 的正向 rebind 场景
- `TableName` 上新增的 target getter / target quote 行为仍然缺少更直接的单测
