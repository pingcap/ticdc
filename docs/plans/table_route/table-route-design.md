# Table Route 设计文档

- 状态：待内部评审
- 最后更新：2026-04-16
- 基线：`upstream/master`
- 对齐文档：`docs/plans/table_route/table-route-requirements.md`、`docs/plans/table_route/table-route-prfaq.md`
- 说明：本文只回答实现设计问题：数据模型怎么承载、route 在哪里计算、不同 sink 要改哪些消费点、为什么还需要运行时冲突检测。

## 1. 配置入口与所有权

`table route` 继续复用 `sink.dispatchers`。在 `pkg/config.DispatchRule` 中新增：

- `TargetSchema string 'toml:"target-schema" json:"target-schema"'`
- `TargetTable string 'toml:"target-table" json:"target-table"'`

规则约束：

- `matcher` 继续匹配上游 schema / table，沿用现有 dispatch matcher 的 table-filter 语义，支持 `sales.*`、`*.orders`、`sales.order_*`、`*.*` 这类通配写法，也支持 `!sales.tmp_*` 这类排除写法
- `TargetSchema` / `TargetTable` 只描述下游目标名
- 只有配置了 `TargetSchema` 或 `TargetTable` 的 rule 才参与 table route
- 多条 route rule 同时命中时，按 `dispatchers` 顺序取首条

`table route` 的计算 owner 是统一 router，而不是各 sink。原因只有一个：规则匹配、占位符展开和 rename DDL rewrite 只能有一个口径；如果把这套逻辑散到 MySQL、MQ、Storage 各自实现，很快就会出现 DML、DDL、payload、path 不一致。

## 2. 共享数据模型与应用路径

### 2.1 DML：`TableName` / `TableInfo`

`TableName` 继续作为名字承载结构，在现有字段基础上新增：

```go
type TableName struct {
    Schema string
    Table string
    TableID int64
    IsPartition bool

    TargetSchema string
    TargetTable string
}
```

字段语义：

- `Schema` / `Table` 始终表示上游名
- `TargetSchema` / `TargetTable` 表示 route 结果

`TableInfo` 不再额外新增一组 top-level target 字段，而是继续通过内部的 `TableName` 承载目标名。这样做的原因是：

- 现有名字相关 helper、quoted name 生成和 SQL builder 都围绕 `TableName` 工作
- 如果把 target 放到 `TableInfo` 顶层，就会出现两套名字入口：`TableInfo.Target*` 和 `TableInfo.TableName.*`
- 结果是 helper、SQL builder、codec 很容易各读各的，名字语义会在 `TableName` 和 `TableInfo` 两层分叉

因此 DML 路径的约束是：

- route 在 `TableInfo` 创建或 table version 更新时计算一次
- 命中 route 时，通过 `CloneWithRouting(targetSchema, targetTable)` 构造 routed `TableInfo`
- clone 只改 `TableName.TargetSchema` / `TargetTable`
- `columnSchema`、`View`、`Sequence` 等只读元数据继续共享

这意味着 steady-state DML row path 不再重复做规则匹配和占位符展开。这里的 steady-state row hot path 指持续复制期间每一行 DML 编码、分发和发送的主循环；它应该只复用已经算好的 routed `TableInfo`。

### 2.2 DDL：`DDLEvent`

DDL 不能只依赖 `TableInfo` 承载 route 结果，因为 rename DDL 需要同时表达：

- source old name
- source new name
- target old name
- target new name

建议在 `pkg/common/event/DDLEvent` 上增加 runtime-only target 字段：

```go
type DDLEvent struct {
    SchemaName string
    TableName string
    ExtraSchemaName string
    ExtraTableName string
    Query string

    targetSchemaName string
    targetTableName string
    targetExtraSchemaName string
    targetExtraTableName string
}
```

字段语义：

- `SchemaName` / `TableName`：source new name
- `ExtraSchemaName` / `ExtraTableName`：rename 场景下的 source old name
- `target*`：对应的 routed target name

`target*` 只在 downstream adapter 内部构造和消费，不进入 `DDLEvent` 的序列化格式。这里是兼容性硬约束，不是“尽量降低风险”：

- 当前 `DDLEvent` 已经有版本化 wire format，并且 mixed-version 场景依赖现有 payload 兼容
- 如果把 `target*` 写进当前版本 payload，rolling upgrade 期间就必须定义新旧节点如何读写和解释这些字段
- 本次方案要求现有 `DDLEventVersion1` payload 零变更，因此 target 只能保持 runtime-only

sink 通过 accessor 读取输出名，例如：

- `GetTargetSchemaName()`
- `GetTargetTableName()`
- `GetTargetExtraSchemaName()`
- `GetTargetExtraTableName()`

DDL 进入 sink 之前，router 必须一次性构造 routed DDL 视图。改写对象是整组，而不是单个字段：

- `SchemaName` / `TableName`
- `ExtraSchemaName` / `ExtraTableName`
- `Query`
- `TableInfo`
- `MultipleTableInfos`
- `BlockedTableNames`

要求是 all-or-nothing：要么整组都改好，要么整个 DDL 失败，不能出现 query 已改而结构化字段未改，或者反过来的情况。

V1 不为 TiCDC 本地定义的 parser 外 DDL action 做 table routing 改写：

- `ActionAddFullTextIndex`
- `ActionCreateHybridIndex`

这两个 action 当前不是 TiDB 原生 `model.ActionType`，而是 TiCDC 根据原始 `job.Query` 识别出的本地 action。schemastore 对它们保留原始 SQL，因为当前 TiDB parser 不能稳定识别这些语法。table route 的 DDL rewrite 以 parser AST 为边界，V1 不引入针对这些语法的字符串级 SQL 改写。未配置 table route 时，它们的既有同步行为不受影响。

输入：

```sql
RENAME TABLE sales.temp_table TO sales.renamed_table
```

若 route 规则为：

- `target-schema = "archive"`
- `target-table = "{table}_routed"`

则 routed 结果必须是：

- source old：`sales.temp_table`
- source new：`sales.renamed_table`
- target old：`archive.temp_table_routed`
- target new：`archive.renamed_table_routed`
- rewritten query：`RENAME TABLE archive.temp_table_routed TO archive.renamed_table_routed`

### 2.3 Router

统一 router 放在 `downstreamadapter/routing`，它是 table route 的唯一计算入口，负责：

- 编译 `matcher`
- 编译并展开 `TargetSchema` / `TargetTable`
- 计算 `RouteName(sourceSchema, sourceTable)`
- 构造 routed `TableInfo`
- 构造 routed `DDLEvent`
- 统一执行 DDL query rewrite

所有 sink 只消费 routed `TableInfo` / `DDLEvent`，不再自行实现 matcher 匹配、占位符展开或 rename rewrite。

## 3. Route Conflict 与 `TargetTableRegistry`

`route conflict` 的定义是：两张不同的上游表，经 table route 计算后得到同一个 `(targetSchema, targetTable)`。

create / update changefeed 阶段，基于当前对象集合能直接判断出的冲突必须静态报错。但仍然有一类冲突只能在运行时暴露：它依赖运行中的对象集合变化。

例子一：新建表后才暴露冲突

```toml
[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "archive"
target-table = "{table}"

[[sink.dispatchers]]
matcher = ["hr.*"]
target-schema = "archive"
target-table = "{table}"
```

创建 changefeed 时，如果上游只有：

- `sales.orders -> archive.orders`
- `hr.employees -> archive.employees`

则静态阶段没有冲突。

但运行中如果新增：

- `hr.orders -> archive.orders`

它就会和 `sales.orders -> archive.orders` 冲突。这个冲突依赖“未来才出现的一张表”，只能在运行时识别。

例子二：rename DDL 后才暴露冲突

```toml
[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "archive"
target-table = "{table}"

[[sink.dispatchers]]
matcher = ["hr.*"]
target-schema = "archive"
target-table = "{table}"
```

创建 changefeed 时，如果上游是：

- `sales.orders -> archive.orders`
- `hr.customers -> archive.customers`

同样没有冲突。

但运行中如果执行：

```sql
RENAME TABLE hr.customers TO hr.orders
```

rename 后 route 结果变成：

- `hr.orders -> archive.orders`

于是与 `sales.orders -> archive.orders` 冲突。这个冲突依赖运行中的对象名变化，也只能在运行时识别。

因此需要一个 changefeed 级的 `TargetTableRegistry`：

```go
type TargetKey struct {
    Schema string
    Table  string
}

type RouteBinding struct {
    SourceTableID int64
    Target        TargetKey
}

type TargetTableRegistry struct {
    mu      sync.Mutex
    owners  map[TargetKey]int64
    reverse map[int64]TargetKey
}

func (r *TargetTableRegistry) Upsert(binding RouteBinding) error
func (r *TargetTableRegistry) Remove(sourceTableID int64)
func (r *TargetTableRegistry) ReplaceBindings(removes []int64, adds []RouteBinding) error
```

设计说明：

- `SourceTableID` 直接使用上游 `TableID`
- `owners` 记录某个 target 当前被哪张 source 表占用
- `reverse` 让 rename、drop、table version 更新可以先找到旧 target，再原子替换
- `Upsert` 用于表进入复制范围或 table version 更新
- `Remove` 用于表离开复制范围、drop、rename away
- `ReplaceBindings` 用于 rename / drop / add 这类需要“先检查新 target，再替换占用关系”的路径
- 同一个 target 被同一张 source 表再次注册：允许覆盖
- 同一个 target 被另一张 source 表注册：判定为 route conflict，并让 changefeed fail

`TargetTableRegistry` 只维护运行时 target 占用状态；它不重新做 route 计算，也不替代静态校验。

## 4. 各 Sink 的改动点

### 4.1 MySQL / TiDB sink

DML：

- `pkg/sink/mysql/sql_builder.go` 已经围绕 `TableInfo` 生成目标表名
- table route 必须在进入 sink 之前完成
- sink 不需要知道 `TableInfo` 是否命中过 route，只需要按统一契约读取输出名

DDL：

- `pkg/sink/mysql/mysql_writer_ddl.go` 对部分 DDL 会先执行 `USE <db>`
- 这里必须读取 routed schema
- 真正执行的 query 必须是 rewritten query

结论：

- MySQL / TiDB sink 不再做第二套规则匹配
- 它只稳定消费 routed `TableInfo` 和 routed `DDLEvent`

### 4.2 Kafka / Pulsar sink

改动点在两层：

- `downstreamadapter/sink/kafka/*`、`downstreamadapter/sink/pulsar/*`
- `pkg/sink/codec/*`

要求：

- payload 中对外暴露 schema / table 名的位置统一读取 target accessor
- DDL 消息中的名称字段与 SQL 文本一起体现 target 名
- event router、`topic`、`partition` 继续按上游名工作

### 4.3 Storage sink

Storage sink 不是只改 encoder 字段，还要同时改 path。至少包括：

- `downstreamadapter/sink/cloudstorage/dml_writers.go`
- `pkg/sink/cloudstorage/path.go`
- `pkg/sink/cloudstorage/table_definition.go`

要求：

- 文件内容中的 schema / table 字段使用 target 名
- 路径中的 schema / table 命名使用 target 名
- schema metadata / table definition 使用 target 名

### 4.4 Redo

Redo 的目标是让“持久化看到什么，回放后就还原什么”。因此：

- redo 持久化时读取 target 名
- redo replay 后继续看到 target 名
- redo 不再额外保存一套 source + target 双轨输出视图

## 5. 校验与测试重点

配置阶段：

- 占位符和表达式校验
- 显式空字符串校验
- 基于当前对象集合的静态 route conflict 校验

运行阶段：

- wildcard 新表导致的冲突
- rename DDL 导致的冲突
- DDL rewrite 失败时不能静默回退到 source query
- parser 外 DDL action 暂不做 table routing 改写，避免引入脆弱的字符串级 SQL rewrite

测试重点：

- `DispatchRule` 的配置和校验
- `TableName` / `TableInfo` 的 source / target 语义
- `CloneWithRouting()` 的共享与缓存语义
- `DDLEvent` 的 target accessor 与 rewritten query
- `TargetTableRegistry` 的注册、释放和 rename 冲突
- MySQL / TiDB sink 的 DML / DDL 输出
- Kafka / Pulsar 的“source dispatch + target payload”
- Storage 的“target path + target metadata”
- Redo 的“target persist + target replay”
