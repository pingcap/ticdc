# 合库与分库场景下的库级 DDL：目标行为与实现方案

Last updated: 2026-09-16
Status: 方案待评审
Scope: TiCDC 新架构表路由（`sink.dispatchers` 的 `target-schema` / `target-table`）在合库（多个源库 → 一个目标库）与分库（一个源库 → 多个目标库）场景下，`CREATE` / `ALTER` / `DROP DATABASE` 的目标行为与实现方案
Related documents:

- [TiCDC 表路由](../../documents/docs-cn/ticdc/ticdc-table-routing.md)
- [TiDB Data Migration 表路由](../../documents/docs-cn/dm/dm-table-routing.md)
- [TiCDC Changefeed 配置参数](../../documents/docs-cn/ticdc/ticdc-changefeed-config.md)（`case-sensitive`）

## Background

处理库级 DDL 时，changefeed 不能中断运行，并且要按用户配置期望的行为执行。当前实现不满足这两点：合库时 `DROP DATABASE` 会删除共享目标库（数据被误删），分库时库级 DDL 直接报路由歧义、中断同步，而事件过滤无法绕过路由计算。用户文档已经提示了这些风险，但没有给出可行路径。

本文把目标、现状、成因、参考实现和方案放在一起，便于评审。尚未决定的事项集中在「待决问题」。

## 目标与约束

目标：

1. 库级 DDL 在任何形态下都不因**路由本身**中断 changefeed：不产生歧义错误，也不产生会破坏其他源库数据的改写。
2. 执行结果符合配置表达的意图：只影响本源库在本 changefeed 中的对象；功能上属于一对一映射的场景（如独占目标库）保持现有语义。
3. 配置无法表达意图时，在创建/更新 changefeed 阶段就告警或拒绝，而不是等运行期失败。

非目标：

- 仍然不支持多张源表合并到同一目标表（用户文档已声明）。
- 不改变表级 DDL/DML 的路由、冲突检测与进度语义。
- 不吞掉下游的真实错误（权限、网络、SQL 非法、连接失败）：这类错误仍按现有 DDL 语义重试或报错。

约束：

- MySQL sink 目前按 DDL 事件执行单条语句（`pkg/sink/mysql/mysql_writer_ddl.go:36` 的 `execDDL` 直接执行 `event.Query`），展开需要按事件/顺序执行。
- 所有权信息来自运行期路由登记（`Admin.activeRoutes` / `TargetTableRegistry`）；bootstrap 之前为空，重启后需要重建。
- 只有表进入路由登记（表有 dispatcher）；视图等非表对象不在登记内。

## 实测行为（现状）

以下结果来自 `Router.ApplyToDDLEvent` 的直接调用（分支 `75cd5738a`），可复现。

### 合库

配置：

```toml
[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "archive"
target-table = "{schema}_{table}"

[[sink.dispatchers]]
matcher = ["crm.*"]
target-schema = "archive"
target-table = "{schema}_{table}"
```

| 上游 DDL | 改写结果 | 影响 |
| :--- | :--- | :--- |
| `DROP DATABASE sales` | `DROP DATABASE \`archive\`` | 删除共享目标库，`crm` 的目标表与数据一并被删 |
| `ALTER DATABASE sales CHARACTER SET utf8mb4` | `ALTER DATABASE \`archive\` CHARACTER SET = utf8mb4` | 共享目标库的 charset/collation 被任一源库改写，最终结果取决于 DDL 顺序 |
| `CREATE DATABASE crm` | `CREATE DATABASE \`archive\`` | 目标库已存在时报错，TiCDC 不会补 `IF NOT EXISTS` |
| `CREATE DATABASE IF NOT EXISTS crm` | `CREATE DATABASE IF NOT EXISTS \`archive\`` | 幂等，不报错 |

### 分库

配置（`sales` 的表拆到两个目标库）：

```toml
[[sink.dispatchers]]
matcher = ["sales.orders"]
target-schema = "orders_db"

[[sink.dispatchers]]
matcher = ["sales.customers"]
target-schema = "customers_db"
```

| 上游 DDL | 结果 | 影响 |
| :--- | :--- | :--- |
| `DROP DATABASE sales` | `ErrTableRoutingFailed: ambiguous schema routing for schema sales: target schema customers_db conflicts with orders_db` | DDL 派发失败、changefeed 报错；无法表达"删除 N 个目标库" |
| `ALTER DATABASE sales CHARACTER SET utf8mb4` | 同上 | 同上 |
| `CREATE DATABASE crm`（无规则命中） | `CREATE DATABASE crm`（保持源库名） | 分库需要用户预先创建目标库 |

库级 DDL 的判定与"当前还剩哪些表在同步"无关：即使 `sales` 下只剩一张表，只要命中多条目标库不同的规则就会报歧义（`pkg/routing/router.go:308`）。

### 混合形态（部分表路由、部分保留源库名）

| 上游 DDL | 改写结果 | 影响 |
| :--- | :--- | :--- |
| `DROP DATABASE sales`（仅 `sales.orders` 路由到 `archive`） | `DROP DATABASE \`archive\`` | 目标库被删，而保留原名同步的 `sales.customers` 仍在下游 `sales` 库，两个库状态不一致 |

### 过滤不能绕过路由

`ignore-event = ["drop schema"]` 只会把 DDL 标记为不下发（`NotSync`），路由计算照常执行：`downstreamadapter/eventcollector/dispatcher_stat.go:445` 对所有 DDL 调用 `ApplyToDDLEvent`，不检查 `NotSync`。因此分库场景下过滤后的 `DROP DATABASE` 仍然报歧义、仍然中断同步。

## 成因

1. 库级 DDL 的目标库由"规则基数"决定：`matchRule(schema, "")` 遍历所有 `MatchSchema(schema)` 命中的规则，要求它们的 `target-schema` 展开结果一致，否则报歧义（`pkg/routing/router.go:308-330`），改写本身也只支持单目标库（`pkg/routing/ddl_query_rewriter.go:591-605`）。它既不参考路由登记里该源库实际拥有的目标库集合，也不判断目标库是否被多个源库共享。
2. 缺少库级 DDL 策略：合库时"改写即删共享库"，分库时"只能报错"，没有 DM 分片模式那种"不下发 + 释放登记 + 继续推进"的选项。
3. 可复用的机制已经存在：库级 DDL 的登记释放由 `TableNameChange.DropDatabaseName`（`logservice/schemastore/persist_storage_ddl_handlers.go:2142`）驱动，最终变成 `RELEASE_SCHEMA`（`downstreamadapter/dispatcher/basic_dispatcher.go:944-994`），与是否 `NotSync` 无关。缺的是把库级 DDL 引导到"按所有权执行"的路径。

## DM 的处理方式

- 库级规则：DM 用**不带 `table-pattern` 的规则**指定 `CREATE` / `DROP SCHEMA` 的目标库；没有命中规则时保留源库名（`../tiflow/dm/syncer/syncer.go:3322` 的 `route()` 在 `targetSchema == ""` 时原样返回）。DM 不允许同一张表命中多条路由规则，所以不存在库级 DDL 歧义，也无法表达"一个源库拆到多个目标库"。
- 分片模式（合表/合库的安全网）：
  - 悲观模式：`DROP DATABASE` 不下发，改为退出分片组并清理表 checkpoint（`../tiflow/dm/syncer/ddl.go:478` 的 `preFilter` → `:1356` 的 `dropSchemaInSharding`）；`DROP TABLE` 同样跳过并退出分片组；分片组内的 `TRUNCATE` 跳过。
  - 乐观模式：`DROP DATABASE` 置 `skipOp` 并调用 `osgk.RemoveSchema`（`../tiflow/dm/syncer/ddl.go:829`、`../tiflow/dm/syncer/opt_sharding_group.go:227`）；`CREATE DATABASE` / `ALTER DATABASE` 下发但不参与分片协调（`../tiflow/dm/syncer/ddl.go:825`）。
  - schema 级规则会建立 `IsSchemaOnly` 分片组（`../tiflow/dm/syncer/sharding_group.go:105`）。
- 普通模式：按库级规则改写下发，同时维护 schema checkpoint（`../tiflow/dm/syncer/syncer.go:2886-2895`）。这与 TiCDC 合库现状的风险相同，但 DM 的分片模式默认覆盖合表场景。

结论：DM 的"不下发 + 释放内部状态 + 继续推进"满足"不中断"，但它默认放弃了下游对象清理；TiCDC 的目标更高：既要不中断，也要让下游与配置意图一致，因此需要按所有权执行，而不是一律跳过。

## 目标行为：所有权范围内的忠实执行

决策函数（输入：被 `CREATE` / `ALTER` / `DROP` 的源库 `S`）：

1. 从路由登记取 `S` 当前拥有的表及其目标库集合 `T(S)`。
2. `T(S)` 为空（没有命中规则的同步表）→ 保持源库名，与现状一致。
3. 对每个 `T ∈ T(S)`：
   - `T` 只被 `S` 拥有 → 直接路由成 `DROP DATABASE IF EXISTS T`（1:1 与独占场景行为不变）。
   - `T` 还被其他源库拥有（合库）→ 只删除 `S` 在 `T` 中的对象，逐表展开为 `DROP TABLE IF EXISTS T.t`。
4. 全部语句成功后再释放 `S` 的登记；失败按同一批语句重试，快照随 barrier 状态携带，不在重试时重新计算。

同类推广：

- `CREATE DATABASE S` → 对每个 `T` 发 `CREATE DATABASE IF NOT EXISTS T`；合库时第二个源库不再因"目标库已存在"失败。
- `ALTER DATABASE S` → 对每个 `T` 发 `ALTER DATABASE T ...`；`T` 被多源共享时记录告警（共享库的默认属性会被本源库改写），可用策略跳过。

行为矩阵：

| 形态（`T(S)`） | 目标库所有权 | 默认行为 | `skip` | `error` |
| :--- | :--- | :--- | :--- | :--- |
| 空集 | — | 保持源库名（现状） | 不下发，释放登记 | 报错 |
| 单目标库 | 仅 `S` 拥有 | `DROP DATABASE IF EXISTS T`（现状） | 不下发，释放登记 | 报错 |
| 单目标库 | 多源共享（合库） | 只删 `S` 的对象（`DROP TABLE IF EXISTS` 展开） | 不下发，释放登记 | 报错 |
| 多目标库（分库） | 各自判定 | 独占库 `DROP DATABASE`，共享库只删 `S` 的对象 | 不下发，释放登记 | 报错 |

### 可选策略与显式规则

- changefeed 级策略，默认即上面的行为：

  ```toml
  [sink]
  # ownership：按所有权执行（默认）；skip：只释放登记、不下发；error：严格报错
  database-ddl = "ownership"
  ```

- 库级规则（后续，语义对齐 DM）：为需要显式指定库级 DDL 落点的用户增加只作用于库级 DDL 的规则；未命中时回落到 `database-ddl` 策略。

  ```toml
  [[sink.dispatchers]]
  schema-level = true
  matcher = ["sales"]
  target-schema = "sales_meta"
  ```

### 执行与失败语义

- 展开出的语句与同一个 DDL barrier/commit-ts 绑定，按顺序应用；barrier 在全部语句成功后推进，与普通 DDL 的进度语义一致。
- 对象快照在收到 DDL 时生成并随 barrier 状态携带，避免"先释放登记、后重试"丢对象。
- 语句使用 `IF EXISTS` / `IF NOT EXISTS` 保证重试幂等。
- 只有下游返回的真实错误才中断；路由歧义在默认策略下不再出现。
- 展开条数受该源库在同步中的表数约束；记录展开条数与被跳过 DDL 的日志、状态和 metric。

## 配置校验

创建/更新 changefeed 时（`api/v2` 的 `verifyRouteConflict` 与 `routing.NewAdmin` 共用规则扫描）计算"源库 → 目标库集合"与共享度，用于：

- 分库（一个源库映射到多个目标库）：提示库级 DDL 会展开为多条语句；
- 合库（多个源库映射到同一目标库）：提示 `DROP DATABASE` 只删除本源库对象，不会删除共享库；
- `case-sensitive = false` 下目标库名归一化后相同，同样按合库处理（表级冲突已由 `pkg/routing/registry.go` 覆盖）。

## 当前可用的兜底操作

在方案落地前，用户仍需要以下操作避免中断或误删：

合库：

- 执行源库删除前配置 `ignore-event = ["drop schema"]`：DDL 不下发、路由登记仍释放，但下游目标表保留，需要手工清理。
- 目标表名包含 `{schema}`；不要用 `ALTER DATABASE` 调整共享目标库的 charset/collation。

分库：

- 预先创建全部目标库，避免在同步期间执行库级 DDL。
- 过滤对库级 DDL 无效。若必须删库，先把该库移出 `filter.rules`（库级 DDL 会被直接丢弃、不参与路由）或暂停 changefeed，再执行删除；代价是该库不再同步、登记不释放、下游需要手工清理。

## 验证

- 单测：决策函数矩阵（空集/独占/共享/混合 × `CREATE`/`ALTER`/`DROP` × 默认/`skip`/`error`）；展开语句与幂等；重试不丢对象；`Admin` 重启后重建所有权；`case-sensitive` 下的目标库身份。
- 集成：合库 `DROP DATABASE`（共享目标库只删本源库表，另一源库数据保留）、分库 `DROP DATABASE`（两个独占目标库被删除且无路由错误）、合库 `CREATE DATABASE`（幂等）、共享目标库 `ALTER DATABASE` 的告警、非表对象（视图）的清理行为。
- 回归：1:1 场景行为与现状一致（`DROP DATABASE` 仍删除独占目标库）；现有 `table_route` 用例（EXCHANGE PARTITION、CTE、关联视图等）保持通过。

## 兼容性与风险

- 行为变化只发生在"目标库被多源共享"和"一个源库映射到多个目标库"时；1:1 与独占场景不变。
- 独占判定只看本 changefeed 的登记：目标库里的手工对象或其他 changefeed 的对象仍可能被 `DROP DATABASE` 影响，与现状一致，需要文档强调。
- 视图等非表对象不在登记内，MVP 可能留下孤儿对象；需要扩展登记或明确文档说明。
- 合库且表很多（上千张）时展开语句数量可观，需要 metric 与日志；极端情况下建议使用 `skip`。

## 待决问题

- 独占目标库是否也改为"只删本 changefeed 的对象"（更安全）还是保持 `DROP DATABASE`（更忠实于 1:1 语义）？
- 是否需要把视图等非表对象纳入登记，以完整清理？
- 默认策略是否就取 `ownership`（无配置即安全且不中断）？`error` 是否只服务于合规场景？
- 库级规则的字段形态（`schema-level = true` / 独立 matcher / 只填 `target-schema`）如何选择？
