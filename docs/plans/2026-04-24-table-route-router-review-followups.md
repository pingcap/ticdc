# Table Route Router Review 待处理项

日期：2026-04-24

范围：当前分支 `table-route-pr3-routing-core`，重点是 DDL routing、eventcollector 错误处理，以及基于 TiCDC DDL 白名单的覆盖策略。

## Review 发现的问题

### 1. DDL routing 失败后可能在本地被跳过

严重级别：高

当前流程：

- `dispatcherStat.verifyEventSequence` 会在 DDL routing 前递增 `lastEventSeq`。
- `dispatcherStat.handleSingleDataEvents` 随后调用 `applyRoutingToDDLEvent`。
- 如果 routing 失败，当前逻辑会记录日志，并在目标对象实现 `dispatcher.Dispatcher` 时调用 `HandleError(err)`，然后返回 `false`。
- 失败的 DDL 不会被转发，但本地 sequence 状态已经推进。
- `DispatcherManager.collectErrors` 会异步把错误汇报给 maintainer。

风险：

这可能表现为“坏 DDL 在本地被跳过，然后后续事件继续被接受”。错误虽然会上报给 maintainer，但在 maintainer 停止或重建 changefeed 前，本地状态已经不是严格 fail-close。

建议方向：

DDL routing 失败应该走 fail-close。可选方向是从明确 checkpoint reset dispatcher，或者进入明确的 fail-stop 状态，避免后续事件建立在一个被跳过的 DDL 之上继续推进。

测试缺口：

现有测试只断言错误会被上报。还需要断言失败 DDL 之后，后续事件不会继续推进。

### 2. DDL query rewrite 的默认 schema 可能和结构化字段不一致

严重级别：高

当前流程：

- `downstreamadapter/routing/router.go` 里的 `rewriteDDLQueryWithRouting` 只在 `ddl.TableInfo != nil` 时用 `ddl.TableInfo.GetSchemaName()` 作为 parser 的 default schema。
- `ApplyToDDLEvent` 会单独 route `SchemaName`、`TableName`、`ExtraSchemaName`、`ExtraTableName`、`TableInfo`、`MultipleTableInfos` 和 `BlockedTableNames`。
- 对 schema DDL、没有 `TableInfo` 的 DDL，或者 SQL 里没有显式库名的 DDL，结构化字段可能已经表示需要 routing，但 query rewrite 仍可能拿不到正确 default schema。
- `ddl_table_utils_test.go` 已经记录了一个边界：`ALTER DATABASE ...` 没有显式库名时，当前 AST restore 不能补出数据库名。

风险：

可能出现“DDLEvent 的 target 字段已经 route，但 DDL query 仍然是源库名或无库名”的不一致。MySQL/TiDB sink 最终执行的是 query，这会导致 DDL 落到错误 schema/table，或者 metadata 和实际执行结果不一致。

建议方向：

- 默认 schema 应优先从 DDL 结构化字段推导：`TableInfo` 存在时用 `TableInfo.GetSchemaName()`；否则用 `ddl.SchemaName`；必要时也要考虑 `BlockedTableNames` / `MultipleTableInfos`。
- 对 `ALTER DATABASE` 这类 AST restore 无法补出库名的语句，如果 routing 需要改变 schema，但 query 里无法稳定表达目标 schema，应返回 `ErrTableRoutingFailed`，不要静默放行。
- 补测试覆盖：无显式库名但有 `SchemaName` 的表级 DDL、没有 `TableInfo` 但有 `BlockedTableNames` 的 DDL、`ALTER DATABASE` 无显式库名且命中 schema routing 的 DDL。

### 3. `rewriteDDLQueryWithoutParser` 是脆弱的 SQL 字符串 fallback

严重级别：中

它是什么意思：

`rewriteDDLQueryWithoutParser` 是一个“不经过 TiDB parser，直接处理原始 SQL 字符串”的 fallback。它只在 parser 无法解析 DDL query 时被尝试，目前只处理 `cdcfilter.ActionCreateHybridIndex`。

为什么引入：

`ActionCreateHybridIndex` 被 TiCDC 放进了自己的 DDL 白名单，schemastore 也会把以 `CREATE HYBRID INDEX` 开头的原始 SQL 映射成这个自定义 action。当前 TiDB parser 不认识这类语法，所以正常的 AST rewrite 路径会 parse 失败。这个 fallback 是为了在 parser 不支持的情况下，仍然把 `CREATE HYBRID INDEX ... ON table ...` 里的 table name 改成 routed target name。

风险：

它不是完整 SQL parser，只识别很窄的字符串形态：trim 后必须以 `CREATE HYBRID INDEX ` 开头，并且依赖字面量 ` ON ` 分隔。原始 DDL 里如果有换行、tab、多空格、comment、复杂反引号等，都可能无法正确处理。

当前决策：

V1 table routing 暂不支持这类 parser 外 DDL action。代码已禁用这个 fallback，并在测试和文档里明确 `ActionCreateHybridIndex` 当前不支持 table routing。

### 4. 多语句 DDL 应该保持 query 数量和 `MultipleTableInfos` 数量一致

严重级别：中

当前流程：

- `DDLEvent.GetEvents` 在 split query 数量和 `MultipleTableInfos` 数量不一致时会 panic。
- schemastore 构造 `ActionCreateTables` / `ActionRenameTables` 时也把这个 mismatch 视为致命不变量错误。
- router 当前多语句路径只有在数量一致时才按每条 query 的 `TableInfo` 填默认 schema；数量不一致时会退回到共用默认 schema 继续处理。

风险：

这会掩盖上游事件构造不变量错误，并可能用错误 default schema 改写部分 SQL。

建议方向：

只要多语句 DDL 携带 `MultipleTableInfos` 并且 query 被 split 成多条，就应该要求数量一致；不一致时返回 `ErrTableRoutingFailed`。

测试缺口：

需要补 `ActionCreateTables` / `ActionRenameTables` 的 mismatch 用例，断言 router fail-close，而不是使用共享 default schema 继续改写。

### 5. 白名单覆盖测试仍是手工同步

严重级别：中低

当前流程：

`router_supported_ddl_test.go` 维护了一份 action type 到 rewrite case 的手工 map，并用 `require.Len(t, cases, 39)` 卡数量。`pkg/filter/ddl.go` 里的 `ddlWhiteListMap` 当前有 41 个 action，其中 `ActionAddFullTextIndex` 和 `ActionCreateHybridIndex` 已明确不属于 V1 table routing 支持范围，所以 parser-backed 支持用例是 39 个。

风险：

这个测试没有真正绑定 `pkg/filter/ddl.go` 里的 `ddlWhiteListMap`。如果白名单发生“删一个、加一个”的同数量变化，测试仍可能通过，但实际漏掉新的支持类型。

建议方向：

需要让测试直接对齐白名单 source of truth。可以在 `pkg/filter` 暴露一个测试可用的 supported action 列表，或者用更小的 helper 返回当前白名单 action 集合。routing 测试应校验：

- `ddlWhiteListMap - {ActionAddFullTextIndex, ActionCreateHybridIndex}` 都有 parser-backed routing 用例。
- `{ActionAddFullTextIndex, ActionCreateHybridIndex}` 有明确的不支持语义和错误测试。

### 6. 不支持的 custom DDL 测试还缺少结构化字段边界

严重级别：中低

当前流程：

`ActionAddFullTextIndex` 和 `ActionCreateHybridIndex` 命中 table routing 时会返回 `ErrTableRoutingFailed`，不命中 routing 时原样放行。现有测试主要覆盖 `SchemaName` / `TableName` / `TableInfo` 同时存在的正常形态。

风险：

`ddlEventRequiresRouting` 明确会检查 `TableInfo`、`MultipleTableInfos` 和 `BlockedTableNames`。如果测试只覆盖 canonical 字段，可能漏掉“只有 `MultipleTableInfos` 命中”或“只有 `BlockedTableNames` 命中”这类边界。实际事件构造如果缺少 canonical schema/table，但带有 blocked table names，仍然必须 fail-close。

建议方向：

补充不支持 custom DDL 的边界测试：

- 只有 `TableInfo` 命中 routing。
- 只有 `MultipleTableInfos` 命中 routing。
- 只有 `BlockedTableNames` 命中 routing。
- 所有结构化字段都不命中 routing 时保持原样放行。

### 7. `DispatcherService` 被 table routing 扩宽了接口边界

严重级别：低

当前流程：

`DispatcherService` 新增了 `GetRouter() routing.Router`，eventcollector 通过它拿 router 并应用到 table info / DDL event。

风险：

这把 routing 细节暴露到了一个较通用的 dispatcher service 接口里。功能上可行，但接口边界变宽了。

建议方向：

如果 dispatcher manager owning router 是确定的设计，可以保留。如果希望边界更窄，可以考虑让 eventcollector 直接持有 routing 配置，或者抽一个更小的 routing provider 接口。

### 8. `tableInfoCache` 缺少 CDC runtime 必要 case

严重级别：低

背景：

`router.ApplyToDDLEvent` 曾经引入一个单次调用范围内的 `map[*common.TableInfo]*common.TableInfo`，用于当 `ddl.TableInfo` 和 `ddl.MultipleTableInfos[i]` 指向同一个 `*common.TableInfo` 时复用同一个 routed clone。

基于 CDC runtime 的真实 DDL 构造路径检查：

- `CREATE TABLES`：`logservice/schemastore` 会把每个新表分别 append 到 `MultipleTableInfos`，但不会把同一个 `*common.TableInfo` 同时放到 `TableInfo` 和 `MultipleTableInfos[i]`。
- `RENAME TABLES`：`TableInfo` 和 `MultipleTableInfos` 都是按匹配 table 分别 `WrapTableInfo` 构造，不依赖同一个指针 identity。
- `EXCHANGE PARTITION`：`TableInfo` 来自 `buildDDLEventCommon` 或后续 `NewTableInfo`，`MultipleTableInfos[0]` 又单独 `WrapTableInfo`，也不是同一个指针。

示例 route 配置：

```text
source_db.* -> target_db/{table}
```

示例 DDL：

```sql
CREATE TABLE source_db.t1 (id INT PRIMARY KEY); CREATE TABLE source_db.t2 (id INT PRIMARY KEY);
RENAME TABLE source_db.t1 TO source_db.t1_new, source_db.t2 TO source_db.t2_new;
ALTER TABLE source_db.pt EXCHANGE PARTITION p0 WITH TABLE source_db.t1;
```

判断：

目前没有找到 CDC runtime 在上述 DDL 和 route 配置下必须依赖 `tableInfoCache` 的 case。这个 cache 更像是为了保持某些人工构造 event 的 pointer identity，而不是 runtime correctness 所需。

当前决策：

先删除 `tableInfoCache`，让 `TableInfo` 和 `MultipleTableInfos` 分别按 copy-on-write route。后续如果发现真实 runtime case 需要保留同一个 routed clone，必须补一个明确 DDL case 和测试，再讨论是否恢复类似 cache。

### 9. `CREATE TABLE ... LIKE ...` 可能污染 referenced table dispatcher 的 `tableInfo`

严重级别：待评估

问题说明：

`CREATE TABLE new LIKE ref` 的 DDL event 可能因为需要 block referenced table，被追加到 `ref` 的 DDL history。这个 DDL event 的 `TableInfo` 描述的是新表 `new`，不是 referenced table `ref`。如果 `ref` 对应的 dispatcher 处理这个 DDL 时直接用 `ddl.TableInfo` 更新本地 `dispatcherStat.tableInfo`，后续 `ref` 的 DML 就可能使用 `new` 的 tableInfo assemble row。

这里的“update”不是指执行下游 DDL，也不是指修改上游 schema storage，而是指 eventcollector 在处理 DDL event 时更新 dispatcher 本地缓存：

```go
d.tableInfoVersion.Store(ddl.FinishedTs)
d.tableInfo.Store(ddl.TableInfo)
```

当前决策：

这个问题和 table route 无直接关系，暂不在当前 table route router PR 处理。当前 PR 保持 eventcollector 的直接更新语义，不在 table route 改动里引入 `CREATE TABLE LIKE` 专项保护。

后续需要确认：

- `CREATE TABLE ... LIKE ...` 投递到 referenced table dispatcher 的真实 runtime 路径。
- referenced table dispatcher 后续是否还会收到 DML，以及错误 `tableInfo` 是否会影响 DML assemble。
- 应该在 schemastore 构造阶段避免给 referenced table history 携带 new table `TableInfo`，还是在 eventcollector 阶段按 dispatcher tableID 跳过更新。
- 补专项测试，而不是放在 table route PR 中。

### 10. schema-only DDL 在 fan-out table routing 下存在歧义

严重级别：中

问题说明：

schema-only DDL 只有 schema，没有 table。当前 CDC DDL 白名单里，schema 级 DDL 主要是：

- `ActionCreateSchema`

```sql
CREATE DATABASE source_db;
CREATE SCHEMA source_db;
```

- `ActionDropSchema`

```sql
DROP DATABASE source_db;
DROP SCHEMA source_db;
```

- `ActionModifySchemaCharsetAndCollate`

```sql
ALTER DATABASE source_db CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER SCHEMA source_db CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
```

如果用户配置把同一个 source schema 下的不同表 route 到不同 downstream schema：

```toml
[[sink.dispatchers]]
matcher = ["source_db.orders"]
target-schema = "orders_db"
target-table = "orders"

[[sink.dispatchers]]
matcher = ["source_db.users"]
target-schema = "users_db"
target-table = "users"
```

那么 `CREATE DATABASE source_db` 没有 table 信息，无法判断应该改写成 `CREATE DATABASE orders_db` 还是 `CREATE DATABASE users_db`。如果继续沿用 first-match，结果会依赖配置顺序，语义不稳定。

这不影响表级 DDL，例如：

```sql
CREATE TABLE source_db.t (...);
DROP TABLE source_db.t;
ALTER TABLE source_db.t ADD COLUMN c INT;
RENAME TABLE source_db.t TO source_db.t2;
CREATE VIEW source_db.v AS SELECT ...;
DROP VIEW source_db.v;
```

这些 DDL 都有明确 table name，仍然可以按表级 routing rule 处理。

当前决策：

表级 DDL/DML 继续保持 first-match。schema-only DDL 会检查所有 `MatchSchema(source_db)` 命中的 routing rule；如果这些 rule 计算出的 target schema 不一致，则返回 `ErrTableRoutingFailed`，拒绝改写。

用户文档 TODO：

需要在 table routing 用户文档中说明：同一个 source schema fan-out 到多个 downstream schema 时，不支持自动改写 schema-only DDL。用户如果希望 schema-only DDL 被改写，需要保证命中的规则对该 source schema 只有一个明确 target schema，例如：

```toml
[[sink.dispatchers]]
matcher = ["source_db.*"]
target-schema = "target_db"
target-table = "{table}"
```

如果用户确实需要把同一个 source schema 下的不同表 fan-out 到多个 downstream schema，则 schema-only DDL 没有唯一目标，不能自动复制。文档需要说明可选处理方式：

- 用户提前在 downstream 创建对应 schema。
- 用户避免复制这类 schema-only DDL。
- 用户调整 routing 规则，保证 schema-only DDL 命中的规则只有一个明确 target schema。

### 11. DDL query rewrite 的 `defaultSchema` 语义需要收窄

严重级别：中

问题说明：

`rewriteParserBackedDDLQuery` 当前会先把 `ddl.Query` 拆成多条 query，然后逐条调用：

```go
rewriteSingleDDLQuery(query, ddl.GetSchemaName())
```

`defaultSchema` 的作用是给 SQL 里没有显式 schema 的 table name 补默认库名。当前 CDC runtime 下，DDL query 应该已经是带 schema 的规范化 SQL，因此 table DDL 不应该依赖这个 fallback。这个参数会让读代码的人误以为 runtime query 可能缺 schema，增加理解成本。

真实需要关注的是 multi-query 的结构化字段和 query 数量是否严格对齐。`rewriteParserBackedDDLQuery` 会先 split query，然后逐条 rewrite，但当前没有在 rewrite 阶段显式校验 split query 数量和 `MultipleTableInfos` 数量一致。

当前测试覆盖情况：

- 已覆盖跨 schema 的 single `RENAME TABLE db1.t1 TO db2.t2`。
- 已覆盖一条 `ActionCreateTables` 中创建多个表，但这些表都使用同一个 source schema。
- 已覆盖测试用例 `CREATE TABLE other_db.t1; CREATE TABLE source_db.t2` 这种显式 schema 的 multi-query 场景，位置是 `downstreamadapter/routing/router_apply_test.go`。
- 未覆盖 split query 数量和 `MultipleTableInfos` 数量不一致时 fail-close 的行为。

建议方向：

如果 runtime query 保证全限定 schema，`defaultSchema` 应该只作为 defensive fallback，而不是核心语义。对于 `ActionCreateTables` / `ActionRenameTables` 这类有 `MultipleTableInfos` 的事件，应该强制校验 split query 数量和 `MultipleTableInfos` 数量一致；不一致时返回 `ErrTableRoutingFailed`。

### 12. 无显式库名的 `ALTER DATABASE` 不能安全 route

严重级别：中

问题说明：

`ALTER DATABASE CHARACTER SET ...` 这类 SQL 没有显式 database name。`fetchDDLTables` 可以用 `defaultSchema` 推导出 source schema，但 TiDB AST restore 无法把原 SQL 中不存在的 database name 补出来。现有测试也已经记录了这个限制。

风险：

当 `defaultSchema` 命中 table routing 并且目标 schema 发生变化时，metadata 可能认为 schema 已 route，但 query 仍然没有显式目标 schema，最终执行依赖下游 session 当前 database，存在不确定性。

建议方向：

如果 `ALTER DATABASE` 的 AST 中没有显式 database name，且 routing 会改变 schema，应返回 `ErrTableRoutingFailed`，不要静默放行或生成不完整 query。

### 13. AST table name visitor 可能误处理 `CREATE VIEW` 中的 CTE 名称

严重级别：中

问题说明：

`tableNameExtractor` 和 `tableRenameVisitor` 当前会无差别处理 AST 中所有 `*ast.TableName`。这对普通 table DDL 是直接的，但对 `CREATE VIEW ... AS WITH cte AS (...) SELECT * FROM cte` 这类 SQL，CTE 引用名不是下游物理表名。如果 visitor 把 CTE 引用也当作真实表名 route，可能生成错误 SQL。

建议方向：

补 `CREATE VIEW` + CTE 的专项测试。若确认命中，应在 view query rewrite 时识别并跳过 CTE 名称，或者对 view body 使用更精确的 AST 处理逻辑。

## 已确认问题

### `ActionAddFullTextIndex` 和 `ActionCreateHybridIndex` 暂不支持 table routing

当前代码事实：

- 这两个不是 TiDB `model.ActionType` 原生值。
- TiCDC 在 `pkg/filter/ddl.go` 里本地定义了它们。
- TiCDC 的 `ddlWhiteListMap` 已经包含这两个 action。
- schemastore 会根据原始 `job.Query` 检测它们，并改写成 TiCDC 自定义 action。
- schemastore 对这两类 action 保留原始 query，因为注释里明确提到当前 parser 不能识别它们。

判断：

从 TiDB parser / TiDB model 角度看，它们还不是原生支持的 DDL action。从 TiCDC 白名单角度看，它们已经是 TiCDC 会识别、会进入 pipeline 的支持类型。

当前决策：

V1 table routing 暂不支持这两个 TiCDC 本地 action。这个边界已经记录到 `docs/plans/table_route/table-route-requirements.md` 和 `docs/plans/table_route/table-route-design.md`。

后续影响：

测试不能再宣称完整覆盖 TiCDC DDL 白名单，除非显式把这两个 action 从 V1 table routing 支持范围里排除。实现上也不应该为它们引入脆弱的字符串级 SQL rewrite。

### `ActionAddFullTextIndex` 为什么现在没有专门走 fallback？

当前 routing 测试里的用例是：

```sql
ALTER TABLE `source_db`.`t1` ADD FULLTEXT INDEX `ft_idx`(`c1`)
```

这个 SQL 形态可以走正常 AST rewrite 路径，所以不需要 `rewriteDDLQueryWithoutParser`：

- TiDB parser 解析成 `ALTER TABLE`
- extractor 从 AST 里拿到 table name
- router 改写 table name
- `Restore` 输出 routed DDL

但这里有一个风险：schemastore 注释里的真实示例是：

```sql
ALTER TABLE t2 ADD FULLTEXT INDEX (b) WITH PARSER standard;
```

如果当前 parser 对 `WITH PARSER standard` 这种完整语法不支持，那么现在的测试覆盖就偏弱；它只证明了简化 fulltext DDL 可以 rewrite，不能证明 schemastore 识别出来的真实 fulltext DDL 都能 rewrite。

当前决策：

V1 table routing 暂不支持 `ActionAddFullTextIndex`。后续如果要支持，需要先确认完整 `WITH PARSER standard` 语法能否稳定走 AST rewrite；不能稳定解析时，不应直接用脆弱的字符串 fallback 补齐。

### `ActionCreateHybridIndex` 为什么需要特殊处理？

`CREATE HYBRID INDEX ... ON ...` 当前不是 TiDB parser 能解析的语法，schemastore 也刻意保留原始 query。没有 fallback 时，router 无法从 AST 中提取并改写 table name，会返回 `ErrTableRoutingFailed`。

所以 `rewriteDDLQueryWithoutParser` 的唯一目的就是让这个 TiCDC 白名单里的自定义 action 也能被 table routing 改写。

当前决策：

V1 table routing 暂不支持 `ActionCreateHybridIndex`。代码已禁用这个 fallback，避免为了一个 parser 外语法维护不完整的 SQL 字符串改写。

## 建议处理顺序

1. 修 DDL routing 失败后的 fail-close 行为，避免失败 DDL 被本地跳过。
2. 修 DDL query rewrite 默认 schema 和结构化 route 字段不一致的问题。
3. 收紧多语句 DDL 的 query 数量和 `MultipleTableInfos` 数量一致性。
4. 让白名单覆盖测试对齐 `ddlWhiteListMap` 的 source of truth，并显式排除两个 V1 不支持的 custom action。
5. 补齐不支持 custom DDL 的结构化字段边界测试。
6. 再评估 `GetRouter()` 是否应该放在 `DispatcherService` 上。
