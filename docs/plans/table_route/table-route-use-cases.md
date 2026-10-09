# Table Route 适用场景分析

- 状态：草案
- 最后更新：2026-05-21
- 读者：产品、架构、开发、测试、文档、支持团队
- 范围：分析 `table route` 的用户价值、适用场景、边界和落地建议；

## 1. 核心结论

`table route` 解决的是 CDC 链路里的对象命名适配问题：上游仍按原 schema/table 采集和调度，下游按用户声明的 target schema/table 输出。

如果用户只是想改变下游对象名，且每张 source table 仍然一对一写入一张唯一 target table，`table route` 是合适的；

它最适合以下场景：

1. 下游命名规范和上游命名规范不同，但表结构和数据语义不变。
2. 多个源库、多个业务域或多个环境同步到同一个下游系统，需要 target namespace 隔离。
3. 迁移、容灾、归档、灰度或影子链路中，需要把数据写到不同 schema/table，避免覆盖已有对象。
4. 数据湖、MQ、分析库消费者要求稳定的目标表名或路径名，而上游名不适合直接暴露。
5. 未来新增表或 rename 表也要按同一套命名规则自动进入目标命名空间。

它不适合以下场景：

1. 多张上游表合并成一张下游表。

## 2. 市场功能对照

主流 CDC、迁移和数据集成产品都提供某种形式的对象命名映射，但范围差异很大。

| 产品 | 类似能力 | 对 TiCDC 的启示 |
| --- | --- | --- |
| Oracle GoldenGate | `TABLE` / `MAP` 用于选择对象，并用 `MAP source_table, TARGET target_table` 指定 source-target 映射。 | 企业 CDC 用户长期需要显式对象映射。映射是复制链路的基础配置，不是边缘功能。 |
| AWS DMS | transformation rules 支持 rename schema/table、前后缀、大小写转换等；修改已有任务里的 schema-altering transformation 可能需要重启任务。 | 用户需要 schema/table 级重命名和批量命名规则；运行中修改 route 规则必须有清晰失败语义和操作边界。 |
| Apache Flink CDC | `route` 用 source-table 匹配 sink-table，支持正则和 pattern replacement，并把分库分表合并作为典型场景。 | 市场上存在强烈的分库分表归并诉求；TiCDC 当前版本不支持多 source 到同一 target，因此必须明确冲突检测和边界。 |
| Debezium | topic routing SMT 可把多张物理表的事件路由到同一 topic，并提供 key uniqueness 机制。 | 一旦允许多 source 合流，就必须处理 key 唯一性和来源标识。TiCDC 当前 table route 不做合流是合理收敛。 |
| Confluent JDBC Sink | sink 可通过 topic 和 `table.name.format` 推导目标表名；官方提示长 topic/table 被截断后可能造成 table name collision。 | target name collision 是真实生产风险，不能只依赖用户自觉避让。静态和动态 conflict 检测是必要能力。 |
| Striim | 支持 table mapping 验证，会检查 source 是否映射、target 是否存在、列数量和类型兼容性；多表 source 可用 `MAP` 分流到不同 stream。 | 生产用户需要在部署前知道映射是否可执行。TiCDC 至少应在名字层面做确定性校验。 |
| Fivetran | 数据库连接器倾向一对一同步，只提供有限命名约定，且初始同步后不能修改命名选择。 | 受管产品常用保守命名策略降低复杂度。TiCDC 暴露 route 配置后，需要更强的校验、文档和运维指引。 |

市场趋势可以概括为三类：

1. 保守一对一命名：Fivetran 代表这类产品，强调低配置和低出错率。
2. 一对一 rename / prefix / suffix：AWS DMS、GoldenGate 覆盖这类迁移和复制需求。
3. 多对一合流：Flink CDC、Debezium topic routing 覆盖分库分表或 logical table 诉求，但需要额外处理 key、schema、DDL 和来源标识。

TiCDC 当前 `table route` 应定位在第二类：可配置、可批量、可覆盖 DML/DDL 的一对一 target name 改写。不要在同一阶段扩展到第三类。

## 3. 生产需求来源

### 3.1 目标库命名规范与源库不一致

很多企业下游数据库、数仓或数据湖有统一命名规范。例如：

- 业务库：`sales.orders`
- 下游 ODS：`ods_sales.orders`
- 下游归档库：`archive.sales_orders`
- 下游审计库：`audit.orders_cdc`

如果没有 table route，用户只能在下游建和上游同名的对象，或者在 TiCDC 后面再接一层转换任务。这样会增加链路延迟、故障点和运维成本。

适用条件：

1. source table 和 target table 一对一。
2. target 表结构与 routed 后的 DDL/DML 兼容。
3. 下游消费者需要看到 target 名，而不是 source 名。

示例：

```toml
[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "ods_sales"
target-table = "{table}"
```

### 3.2 多环境或多集群写入同一类下游

生产中常见多个 TiDB 集群、多个环境或多个业务线写入同一个下游实例。直接使用 source schema/table 名容易冲突。

例子：

- `prod.orders`
- `staging.orders`
- `eu.orders`
- `us.orders`

如果这些链路都写到同一个 MySQL、Kafka payload namespace 或 storage path，就需要在 target 名里带上环境或业务域。

示例：

```toml
[[sink.dispatchers]]
matcher = ["orders.*"]
target-schema = "prod_orders"
target-table = "{table}"
```

使用建议：

1. 用 literal `target-schema` 表达集群、环境、业务域。
2. 多 changefeed 写同一 downstream 时，route 命名约定必须在 changefeed 之外统一管理。
3. 不要让两个 changefeed 在同一 downstream 写同一 target，TiCDC 单个 changefeed 内的 conflict 检测不能替代跨 changefeed 的全局治理。

### 3.3 多租户 schema 需要落到统一命名空间

SaaS 或多租户系统常见每个租户一个 schema：

```text
tenant_001.orders
tenant_002.orders
tenant_003.orders
```

下游可能不希望创建大量业务同名表，也可能需要把租户信息编码进表名或 schema 名。

安全写法：

```toml
[[sink.dispatchers]]
matcher = ["tenant_*.*"]
target-schema = "tenant_mirror"
target-table = "{schema}_{table}"
```

结果：

```text
tenant_001.orders -> tenant_mirror.tenant_001_orders
tenant_002.orders -> tenant_mirror.tenant_002_orders
```

危险写法：

```toml
[[sink.dispatchers]]
matcher = ["tenant_*.*"]
target-schema = "tenant_mirror"
target-table = "{table}"
```

这个规则会把多个 `orders` 路由到同一个 `tenant_mirror.orders`，属于多 source 到同一 target。当前 table route 不支持这种合并，必须在 create / update 或运行时检测出 conflict 并失败。

### 3.4 数据迁移、灰度和影子链路

迁移和灰度场景常要求把源库数据同步到同一实例里的新 schema 或临时表：

```text
source: app.orders
target: app_shadow.orders
```

用途：

1. 预热新系统。
2. 对比新旧链路数据。
3. 执行 cutover 前校验 DDL/DML 兼容性。
4. 保留可回滚的数据副本。

示例：

```toml
[[sink.dispatchers]]
matcher = ["app.*"]
target-schema = "app_shadow"
target-table = "{table}"
```

注意：

1. 如果 source 和 target 在同一个数据库实例，必须避免把数据写回原表。
2. 下游 DDL 权限、表名长度、大小写规则需要提前检查。
3. cutover 后是否继续保留 route，需要作为发布流程的一部分明确。

### 3.5 下游消费者要求稳定逻辑名

上游表名可能频繁调整，或者包含内部实现细节，但下游 BI、审计、风控、数据产品希望消费稳定逻辑名。

例子：

```text
order_service.order_v2 -> dwd.orders
crm.customer_base     -> dwd.customers
```

这种场景适合显式逐表 route：

```toml
[[sink.dispatchers]]
matcher = ["order_service.order_v2"]
target-schema = "dwd"
target-table = "orders"

[[sink.dispatchers]]
matcher = ["crm.customer_base"]
target-schema = "dwd"
target-table = "customers"
```

适用条件：

1. 每个 source 对应唯一 target。
2. 下游消费者愿意以 target 名作为长期契约。
3. 上游 rename 时需要评估是否继续保持 target 名稳定。

### 3.6 数据湖和对象存储路径治理

Storage sink 会把 schema/table 名体现在路径或 metadata 中。上游名直接进入路径时，常见问题包括：

1. 不符合数据湖分层规范。
2. 不希望暴露源库内部命名。
3. 多业务写同一 bucket 时路径冲突。
4. 下游 catalog 需要统一 schema/table 命名。

示例：

```toml
[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "ods_sales"
target-table = "{table}"
```

结果是 storage 输出和 schema metadata 使用 routed target name。这样可以减少下游 Spark、Hive、Trino、Iceberg catalog 侧再做重命名的成本。

### 3.7 MQ 消费端的表名契约

Kafka / Pulsar 场景里，topic、partition 和 payload table name 是不同契约。

当前 table route 的价值在于：

1. topic 仍可按现有 dispatch 规则基于 source 计算。
2. payload 中的 schema/table name 可以按 target 输出。
3. 消费端可以不感知上游真实物理表名。

示例：

```toml
[[sink.dispatchers]]
matcher = ["sales.orders"]
topic = "order-events"
target-schema = "public"
target-table = "orders"
```

适用场景：

1. 消费者以 payload table name 做路由或落库。
2. topic 名是事件域契约，table name 是目标库契约，两者不应绑定。
3. 上游库名不适合暴露给 MQ 消费方。

### 3.8 合规、审计和权限隔离

有些组织要求复制数据进入专门的审计 schema、合规 schema 或只读 schema：

```text
finance.payment -> audit_finance.payment
hr.employee     -> restricted_hr.employee
```

table route 可以表达命名隔离，但不负责数据脱敏和权限控制。

使用建议：

1. route 只改变对象名，不改变敏感字段内容。
2. 脱敏、列裁剪、权限控制必须由其他机制负责。
3. 文档中不能把 table route 描述成安全或合规功能本身。

### 3.9 归档场景专项分析

归档是 table route 的重要场景，但“归档”在用户语境里可能表示几种不同需求，需要拆开判断。

#### 3.9.1 镜像归档

目标：把线上库的数据持续复制到归档 schema，target 表仍然和 source 表一对一。

示例：

```text
sales.orders       -> archive_sales.orders
sales.order_items  -> archive_sales.order_items
finance.payments   -> archive_finance.payments
```

配置：

```toml
[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "archive_sales"
target-table = "{table}"

[[sink.dispatchers]]
matcher = ["finance.*"]
target-schema = "archive_finance"
target-table = "{table}"
```

这是当前 table route 最适合的归档用法。它不改变表结构，也不把多张表合并，只是把下游对象放到归档命名空间。

需要注意：

1. TiCDC 做的是持续复制，不是 cold archive。source 里的 update/delete 会继续同步到 archive target。
2. 如果用户希望 archive 保留历史版本，不能只靠 table route，需要额外的历史表、审计日志或下游加工。
3. 如果 source 和 target 在同一个数据库实例，要确保 archive schema 不在该 changefeed 的复制范围内，避免回环复制。

#### 3.9.2 多业务统一 archive schema

目标：多个业务 schema 归档到一个 archive schema，但 target table 名仍保持唯一。

示例：

```text
sales.orders      -> archive.sales_orders
crm.orders        -> archive.crm_orders
finance.orders    -> archive.finance_orders
```

配置：

```toml
[[sink.dispatchers]]
matcher = ["sales.*", "crm.*", "finance.*"]
target-schema = "archive"
target-table = "{schema}_{table}"
```

这是安全写法，因为 target table 保留了 source schema 信息。

危险写法：

```toml
[[sink.dispatchers]]
matcher = ["sales.*", "crm.*", "finance.*"]
target-schema = "archive"
target-table = "{table}"
```

这个配置会把 `sales.orders`、`crm.orders`、`finance.orders` 都映射到 `archive.orders`。如果没有 conflict 检测，它看起来可以“合表”，但实际会产生主键、DDL、delete/update 语义和来源识别问题。

#### 3.9.3 按时间归档

用户可能希望：

```text
orders rows in 2025 -> archive.orders_2025
orders rows in 2026 -> archive.orders_2026
```

当前 table route 不适合这个需求。原因是 matcher 只看 source schema/table name，不看行内容、时间列或 commitTs。它不能按数据时间把同一张 source table 拆到多个 target tables。

如果 source 端已经按时间拆表，则可以做名字归档：

```text
sales.orders_2025 -> archive.sales_orders_2025
sales.orders_2026 -> archive.sales_orders_2026
```

配置：

```toml
[[sink.dispatchers]]
matcher = ["sales.orders_*"]
target-schema = "archive"
target-table = "{schema}_{table}"
```

#### 3.9.4 历史流水归档

用户也可能希望把每次 update/delete 都追加成历史流水：

```text
orders update id=1 -> archive.orders_history append one version
orders delete id=1 -> archive.orders_history append one tombstone
```

这不是 table route。当前 MySQL / TiDB sink 的表语义仍是目标表上的 insert/update/delete 同步，不会自动把更新转成 append-only history rows，也不会自动增加 `source_schema`、`source_table`、`op_type`、`commit_ts` 等列。

如果要支持历史流水归档，需要单独设计：

1. before image 或完整变更事件模型。
2. append-only sink 语义。
3. 历史表 schema。
4. 主键和版本列。
5. 查询和清理策略。

#### 3.9.5 归档场景验收问题

归档方案评审时应额外问：

1. archive 是持续镜像，还是保留历史版本？
2. source 的 update/delete 是否应该同步到 archive？
3. target 是否和 source 在同一个集群或同一个实例？
4. archive schema 是否被排除在 changefeed 的 capture 范围外？
5. 多个 source schema 写入同一个 archive schema 时，target-table 是否保留 source schema 信息？
6. 下游是否需要 immutable storage、生命周期管理或压缩？这些不是 table route 能力。

### 3.10 上游 schema fan-out 到多个下游 schema

用户可能希望把同一个上游 schema 下的不同表写入不同下游 schema：

```text
source.table1 -> db1.table1
source.table2 -> db2.table2
```

配置示例：

```toml
[[sink.dispatchers]]
matcher = ["source.table1"]
target-schema = "db1"
target-table = "table1"

[[sink.dispatchers]]
matcher = ["source.table2"]
target-schema = "db2"
target-table = "table2"
```

这不是 route conflict。两张 source table 的 target table 不同，DML 和表级 DDL 都有明确目标。

能够正确处理的部分：

```sql
CREATE TABLE source.table1 (...);
ALTER TABLE source.table2 ADD COLUMN c INT;
DROP TABLE source.table1;
RENAME TABLE source.table1 TO source.table1_new;
```

这些 DDL 都携带 table name，可以分别改写为：

```sql
CREATE TABLE db1.table1 (...);
ALTER TABLE db2.table2 ADD COLUMN c INT;
DROP TABLE db1.table1;
RENAME TABLE db1.table1 TO db1.table1_new;
```

问题集中在 DB 级 DDL，也就是只有 schema、没有 table 的 DDL：

```sql
CREATE DATABASE source;
ALTER DATABASE source CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
DROP DATABASE source;
```

这些语句没有 table 信息，无法从 `source` 唯一推导出 `db1` 还是 `db2`。

#### 3.10.1 当前代码现状

当前 router 对 schema-only DDL 的处理是安全失败，而不是按 first-match 随机选择一个 target schema。

实现要点：

1. DDL query rewrite 会把 `CREATE DATABASE`、`ALTER DATABASE`、`DROP DATABASE` 提取成 `{schema: source, table: ""}`。
2. `route(source, "")` 会进入 schema-only 匹配逻辑。
3. schema-only 匹配会扫描所有 `MatchSchema(source)` 的 route rules。
4. 如果所有命中的 rule 展开出同一个 target schema，则允许改写。
5. 如果命中的 rule 展开出多个 target schema，则返回 `ErrTableRoutingFailed`，错误语义是 ambiguous schema routing。

因此，对下面配置：

```toml
[[sink.dispatchers]]
matcher = ["source.table1"]
target-schema = "db1"
target-table = "table1"

[[sink.dispatchers]]
matcher = ["source.table2"]
target-schema = "db2"
target-table = "table2"
```

`CREATE DATABASE source`、`ALTER DATABASE source ...`、`DROP DATABASE source` 都不能被改写成唯一目标。当前合理行为是 fail changefeed，而不是静默生成错误 DDL。

#### 3.10.2 为什么不能简单 fan-out DB 级 DDL

看起来可以把一条 DB 级 DDL 拆成多条：

```sql
CREATE DATABASE db1;
CREATE DATABASE db2;
```

但这个策略并不总是正确。

`CREATE DATABASE` 的问题：

1. 如果 target schema 来自 literal 规则，`db1` / `db2` 是可枚举的。
2. 如果 target schema 表达式依赖 `{table}`，schema-only DDL 没有 table name，无法知道未来会有哪些 target schema。
3. 如果 target schema 已经由其他 changefeed 或其他 source 使用，重复创建可能只是幂等成功，也可能暴露权限和 owner 语义问题。

`ALTER DATABASE` 的问题：

1. `ALTER DATABASE source ...` fan-out 到 `db1` / `db2` 在 dedicated namespace 下看似合理。
2. 但如果 `db1` 还承载其他 source 的 routed tables，修改 charset/collation 会影响不属于 `source` 的对象命名空间。
3. `ALTER DATABASE ...` 没有显式 database name 的语法不能安全 rewrite，因为下游执行依赖 session 当前 database。

`DROP DATABASE` 的问题最大：

```text
source.table1 -> db1.table1
other.tableX  -> db1.tableX
```

如果 `DROP DATABASE source` 被改写成：

```sql
DROP DATABASE db1;
DROP DATABASE db2;
```

它会删除 `other.tableX`。这会把 table route 从“对象名改写”变成“下游 namespace 生命周期管理”，风险不可接受。

更安全的语义应该是：source schema 被 drop 时，只删除属于该 source schema 的 routed target tables，而不是直接 drop target schemas。也就是说，在 fan-out 场景下：

```sql
DROP DATABASE source;
```

更接近：

```sql
DROP TABLE db1.table1;
DROP TABLE db2.table2;
```

是否清理空的 `db1` / `db2`，应交给运维或额外的 dedicated namespace 策略，而不是默认行为。

#### 3.10.3 短期建议

当前版本应明确声明：

1. 同一个 source schema fan-out 到多个 target schema 时，DML 和表级 DDL 可以支持。
2. DB 级 DDL 不支持自动 route。
3. DB 级 DDL 命中多个 target schema 时必须失败，不允许 first-match。
4. 用户需要提前创建 target schema，并通过 TiCDC event filter 过滤 DB 级 DDL，或在运维流程里手动处理 DB 级 DDL。

可接受用法：

```text
pre-create db1
pre-create db2
run changefeed for source.table1 and source.table2
filter source schema-level DDL
only rely on table-level DDL replication
```

推荐配置：

```toml
[filter]
[[filter.event-filters]]
matcher = ["source.*"]
ignore-event = [
  "create schema",
  "drop schema",
  "modify schema charset and collate",
]
```

说明：

1. `create schema` 的别名是 `create database`。
2. `drop schema` 的别名是 `drop database`。
3. `modify schema charset and collate` 对应 `ALTER DATABASE ... CHARACTER SET/COLLATE`。
4. `matcher` 会匹配数据库名。使用 `matcher = ["source.*"]` 可以让过滤规则作用于 `source` 的 DB 级 DDL。
5. 不建议用 `ignore-sql = ["^drop"]` 这种宽泛正则，因为它会同时过滤 `DROP TABLE`，破坏表级 DDL 同步。

如果用户必须自动同步 DB 级 DDL，短期只能调整为单 source schema 到单 target schema：

```toml
[[sink.dispatchers]]
matcher = ["source.*"]
target-schema = "target_source"
target-table = "{table}"
```

或者拆成多个 changefeed，并保证每个 target schema 是该 changefeed 独占的 dedicated namespace。即便如此，`DROP DATABASE` 仍需要谨慎，因为它会删除整个 target schema。

#### 3.10.4 长期解决方向

如果要正式支持 schema fan-out 下的 DB 级 DDL，需要独立设计 schema-level route plan，不能复用当前 table-level route 的 first-match 模型。

建议设计方向：

1. 为 maintainer 引入 per-changefeed schema ownership 视图：
   - `source schema -> target schemas`
   - `target schema -> owned target tables`
   - `target schema -> 是否 dedicated`

2. `CREATE DATABASE source`：
   - 如果 target schema set 是有限且可枚举的，生成 `CREATE DATABASE IF NOT EXISTS target_schema`。
   - 如果 target schema 依赖 `{table}` 或未来对象集合，不能枚举，返回错误并要求用户预创建。

3. `ALTER DATABASE source ...`：
   - 只允许显式带 source schema 的语句。
   - 对有限 target schema set fan-out。
   - 如果 target schema 非 dedicated，应默认拒绝，避免影响其他 source。

4. `DROP DATABASE source`：
   - 默认不生成 `DROP DATABASE target_schema`。
   - 根据 registry 中属于 `source` 的 source tables，生成 target table 级 drop。
   - 只有在 registry 证明 target schema 完全由该 source schema 独占，且产品显式允许 dedicated schema drop 时，才允许 drop target schema。

5. DDL 事件模型需要支持一条 upstream DDL 生成多条 downstream DDL：
   - MySQL / TiDB sink 需要按顺序执行多条目标 DDL。
   - Kafka / Pulsar / Storage 需要定义是一条 DDL message 包含多条语句，还是 fan-out 成多条 DDL events。
   - `DDLEvent.GetEvents()`、redo、checkpoint、错误重试都要能处理 fan-out 后的多目标 DDL。

6. conflict registry 需要参与：
   - source schema drop 时释放该 schema 下所有 source table 的 target ownership。
   - 生成 drop table fan-out 前，必须基于 registry 确认这些 target table 确实由该 source schema 拥有。

这个长期方案本质上是 schema-level lifecycle routing，不只是 table route 的一个小补丁。

## 4. 不推荐或不支持的场景

### 4.1 分库分表合并

用户可能希望：

```text
db_001.orders -> ods.orders
db_002.orders -> ods.orders
db_003.orders -> ods.orders
```

这是市场上常见需求，Flink CDC 和 Debezium topic routing 都覆盖类似场景。理论上，如果 TiCDC 不做 route conflict 检测，用户确实可以用 table route 把多张 source table 指向同一个 target table。但这只是“写到了同一个名字”，不等于正确支持了合库合表。

主要问题如下。

#### 4.1.1 主键冲突

例子：

```text
db_001.orders(id=1, amount=10) -> ods.orders
db_002.orders(id=1, amount=20) -> ods.orders
```

如果 target 表主键仍是 `id`，两条记录会写到同一个 key。后到的 insert/update 可能覆盖先到的数据，delete 也可能删除另一个 source 的记录。

要正确合表，通常需要：

1. 全局唯一主键。
2. 或者把 source 标识写入 target 主键，例如 `(source_schema, source_table, id)`。
3. 或者由上游保证所有分片 key 空间不重叠。

当前 table route 不能自动增加 source 标识列，也不能改写主键。

#### 4.1.2 Update / delete 无法区分来源

例子：

```text
db_001.orders id=1 -> ods.orders id=1
db_002.orders id=1 -> ods.orders id=1
```

当 `db_002.orders` 删除 `id=1` 时，下游 SQL 只能表达“删除 `ods.orders` 里 id=1 的行”。如果 target 里没有 source 标识，它无法知道应该删除来自 `db_002` 的那一行，而不是来自 `db_001` 的那一行。

这不是命名问题，是 DML 语义问题。

#### 4.1.3 DDL 合并语义不明确

多张 source 表写同一 target 时，DDL 会出现冲突。

例子：

```sql
-- db_001.orders
ALTER TABLE orders ADD COLUMN coupon_id BIGINT;

-- db_002.orders
ALTER TABLE orders ADD COLUMN channel VARCHAR(32);
```

两个 DDL 都会被 route 到：

```sql
ALTER TABLE ods.orders ...
```

问题：

1. 如果两个分片 DDL 不同时发生，下游 target 何时变更？
2. 如果某个分片新增列，另一个分片还没有该列，DML 编码如何对齐？
3. 如果两个分片对同一列使用不同类型，谁是准？
4. 如果一个分片 drop column，是否允许影响所有分片的 target？

正确合表需要 schema compatibility 和 schema evolution 策略，而不是简单改 target 名。

#### 4.1.4 事务顺序和一致性边界改变

TiCDC 当前的复制语义围绕 source table、dispatcher、DDL barrier 和 downstream 写入边界构建。多 source 合流后，target table 的写入来自多个 source table。

需要明确：

1. 不同 source table 之间是否需要全局顺序？
2. 跨表事务写入同一 target 时如何保持原子性？
3. 一个 source DDL 阻塞时，是否阻塞整个 target logical table？
4. 部分 source 出错时，target 是否还能继续接收其他 source？

这些问题都超出了 table route 的命名能力。

#### 4.1.5 Schema、索引和约束不一定兼容

即使表名相同，不同 source 表也可能存在差异：

```text
db_001.orders(id BIGINT, amount DECIMAL(10,2), status VARCHAR(16))
db_002.orders(id BIGINT, amount DECIMAL(12,2), status INT)
```

如果它们都写 `ods.orders`，需要决定 target column 类型、nullable、默认值、索引、唯一约束和 generated column 规则。当前 table route 不做这些判断。

#### 4.1.6 来源追踪缺失

合库合表通常要求保留来源：

```text
source_schema
source_table
source_cluster
shard_id
commit_ts
```

否则排查数据、回放变更、做 reconciliation、处理重复主键都会很困难。

当前 table route 只改 target name，不会把来源写入 row data。

#### 4.1.7 Backfill 和历史数据迁移复杂

如果用户在已有 target 上开启多 source 合表，需要回答：

1. target 里已有数据来自哪些 source？
2. 新接入 source 是否需要全量 backfill？
3. backfill 与增量 CDC 如何去重？
4. 同一主键历史冲突如何处理？
5. route 规则回滚后 target 数据如何拆回 source？

这些都不是 conflict detection 关闭后能自然解决的问题。

#### 4.1.8 操作和观测复杂度上升

合表失败时，错误通常表现为 target 写入失败、主键冲突、列不存在、类型不兼容或数据被覆盖。只看 target table 很难反推是哪个 source table 的哪条 route 造成的。

需要额外能力：

1. source -> target owner registry。
2. source 标识日志和指标。
3. per-source 延迟和错误统计。
4. target logical table 级别的 DDL 状态。
5. reconciliation 工具。

因此，如果未来要支持合库合表，应作为独立的 logical table merge 功能设计，而不是移除 route conflict 检测。

### 4.2 字段级转换和 schema conversion

以下需求不属于 table route：

1. 列重命名。
2. 列裁剪。
3. 类型转换。
4. 计算列。
5. 数据脱敏。
6. JSON flatten。
7. source schema 到 target schema 的结构转换。

AWS DMS、Striim 这类产品会把 object mapping 和 column mapping 放在更大的 transformation 能力里。TiCDC 当前 table route 只做对象名改写，不应承诺字段级转换。

### 4.3 基于行内容动态落表

例如：

```text
orders where region = 'US' -> orders_us
orders where region = 'EU' -> orders_eu
```

这需要内容路由和事务语义设计，不是 table route。当前 matcher 只匹配 source schema/table name，不读取行内容。

### 4.4 下游自动建模

table route 不保证：

1. target schema 已存在。
2. target table 可以自动创建成功。
3. target identifier 长度、大小写和保留字一定合法。
4. target DDL 与下游方言完全兼容。

这些仍由 sink、DDL rewrite 能力和下游数据库共同决定。

### 4.5 跨 changefeed 冲突治理

单个 changefeed 内可以做 route conflict 检测。但如果多个 changefeed 写同一个下游实例，TiCDC 不能仅靠单 changefeed registry 发现全局 target 冲突。

这种场景需要外部治理：

1. 命名规范。
2. changefeed 审核。
3. 下游权限隔离。
4. 运维侧目标对象 inventory。

## 5. 场景适配矩阵

| 场景 | 是否适合当前 table route | 关键条件 |
| --- | --- | --- |
| schema 改名 | 适合 | 一对一映射，target 不冲突 |
| table 加前缀/后缀 | 适合 | 表名长度和下游 identifier 合法 |
| 多环境写同一 downstream | 适合 | 每个 changefeed 使用独立 target namespace |
| 多租户 schema 改写为唯一表名 | 适合 | target-table 包含 `{schema}` 或其他唯一片段 |
| 影子同步 / 灰度同步 | 适合 | target 与 source 明确隔离 |
| 镜像归档 | 适合 | 持续复制，一对一写入 archive namespace |
| 多业务统一 archive schema | 适合 | target-table 必须保留 source schema 信息 |
| 上游 schema fan-out 到多个 target schema | 有条件 | DML 和表级 DDL 可用，DB 级 DDL 不支持自动 route |
| MQ payload 表名改写 | 适合 | topic/partition 仍按 source dispatch |
| 数据湖路径规范化 | 适合 | storage sink 消费 routed target name |
| 分库分表合并 | 不适合 | 需要 logical table merge 设计 |
| 历史流水归档 | 不适合 | 需要 append-only history 模型 |
| 列级转换 | 不适合 | 需要 transformation / schema conversion |
| 基于行内容路由 | 不适合 | 需要 content-based routing |
| 多 changefeed 写同一 target | 有风险 | 需要外部治理，单 changefeed 检测不够 |

## 6. 配置设计建议

### 6.1 优先使用可证明唯一的 target 模板

当 matcher 可能匹配多张同名表时，target 必须保留足够来源信息。

推荐：

```toml
[[sink.dispatchers]]
matcher = ["tenant_*.*"]
target-schema = "tenant_mirror"
target-table = "{schema}_{table}"
```

不推荐：

```toml
[[sink.dispatchers]]
matcher = ["tenant_*.*"]
target-schema = "tenant_mirror"
target-table = "{table}"
```

### 6.2 具体规则放在兜底规则前

多条 route rule 同时命中时按配置顺序取第一条。用户应把更具体的规则放在前面。

```toml
[[sink.dispatchers]]
matcher = ["sales.orders"]
target-schema = "special_sales"
target-table = "orders"

[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "ods_sales"
target-table = "{table}"
```

### 6.3 不要用 route 隐藏 source ownership

target 名可以隐藏上游物理命名，但运维上仍需要知道 source -> target 映射。

建议：

1. 在 changefeed 配置评审里记录 route 规则。
2. 在错误和日志里打印 source、target 和命中的 rule。
3. 文档示例中避免只给 target，不解释 source 约束。

### 6.4 route 规则变更应按发布处理

route 规则不是普通格式化选项。变更 route 可能改变下游写入对象。

建议操作流程：

1. 先在测试环境验证 DML、DDL、rename、drop、truncate。
2. 对当前对象集合执行静态 conflict 检查。
3. 评估历史数据是否需要迁移或回填。
4. 在低峰期更新 changefeed。
5. 准备回滚配置和下游对象清理方案。

## 7. 对产品边界的建议

### 7.1 文档应明确“一对一”

市场上很多用户会自然联想到分库分表合并。TiCDC 文档必须直接说明：

1. 当前 table route 是 one source table to one target table。
2. 多 source table to one target table 会被判定为 conflict。
3. 需要合并时，应等待独立 logical table merge 能力，或由下游计算层处理。

### 7.2 conflict detection 是功能完整性的一部分

table route 一旦支持 wildcard 和占位符，就必然存在 target collision 风险。Confluent 文档里 table name truncation 导致 collision 的提示说明这不是理论问题。

TiCDC 需要两层检测：

1. create / update changefeed 时，对当前对象集合做静态检测。
2. 运行时，对新表、rename、recover 等 source name 变化做动态检测。

没有 conflict detection，用户会在下游得到静默混写，这是数据正确性问题。

### 7.3 不应把 table route 描述为 transformation

外部产品常把 rename、column mapping、data masking 放在 transformation 体系里。但 TiCDC 当前功能只改对象名。

对外措辞建议：

1. 使用 `route`、`target schema/table`、`name rewrite`。
2. 避免使用 `transform data`、`schema conversion`、`merge tables`。
3. FAQ 中列出不支持的 transformation 类需求。

### 7.4 sink 支持矩阵必须清楚

不同 sink 对 schema/table name 的暴露位置不同：

1. MySQL / TiDB：DML SQL、DDL SQL、结构化 schema/table。
2. Kafka / Pulsar：payload schema/table、DDL message、topic/partition dispatch。
3. Storage：文件路径、schema 文件、metadata。
4. Redo：持久化和 replay 后的 target name。

文档需要说明哪些 sink 已支持，哪些 sink 只接受配置但不生效，避免用户误以为所有 sink 都有一致行为。

## 8. 验收问题清单

评审一个 table route 使用方案时，至少问清楚：

1. 每张 source table 是否有唯一 target table？
2. wildcard rule 是否可能匹配未来新增同名表？
3. target-schema / target-table 是否会在下游产生大小写、长度或保留字问题？
4. 用户是否期待多 source 合并？如果是，当前功能不满足。
5. 用户是否期待列级转换或脱敏？如果是，当前功能不满足。
6. 下游已有对象是否会被覆盖？
7. route 规则更新后，历史数据如何处理？
8. rename、truncate、drop、recover 是否在用户测试计划中？
9. 多 changefeed 是否写同一个 downstream namespace？
10. 同一个 source schema 是否 fan-out 到多个 target schema？如果是，DB 级 DDL 如何处理？
11. 如果采用 schema fan-out，是否已经配置 event filter 过滤 `create schema`、`drop schema`、`modify schema charset and collate`？
12. 出现 conflict 时，用户是否接受 changefeed fail 而不是自动跳过？

## 9. 参考资料

- [Oracle GoldenGate `TABLE` / `MAP`](https://docs.oracle.com/en/database/goldengate/core/26/reference/table-map.html)
- [AWS DMS transformation rules and actions](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Transformations.html)
- [Apache Flink CDC Route](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/core-concept/route/)
- [Debezium Topic Routing SMT](https://debezium.io/documentation/reference/3.4/transformations/topic-routing.html)
- [Confluent JDBC Sink Connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)
- [Striim CDC table mapping validation](https://www.striim.com/docs/en/about-change-data-capture--cdc-.html)
- [Fivetran database connector source-to-destination mapping](https://fivetran.com/docs/connectors/databases)
- [TiCDC Filter 配置](https://docs.pingcap.com/zh/tidb/stable/ticdc-filter/#event-filter-%E4%BA%8B%E4%BB%B6%E8%BF%87%E6%BB%A4%E5%99%A8-%E4%BB%8E-v620-%E7%89%88%E6%9C%AC%E5%BC%80%E5%A7%8B%E5%BC%95%E5%85%A5)
