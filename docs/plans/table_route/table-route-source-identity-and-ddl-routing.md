# Table Route Source Identity 与 DDL Routing 方案

- 状态：草案
- 最后更新：2026-05-20
- 基线：当前 table route static conflict detection 分支
- 相关文档：
  - `docs/plans/table_route/table-route-conflict-detection-design.md`
  - `docs/plans/table_route/table-route-conflict-registry-review-guide.md`
  - `docs/plans/table_route/table-route-design.md`

## 1. 结论

`TargetTableRegistry` 的 source identity 应使用 source schema/table name：

```go
type sourceKey struct {
    Schema string
    Table  string
}
```

不需要在 `sourceKey` 中记录 logical table ID。

table route conflict detection 的目标是防止同一个 changefeed 中多个上游 source table 同时占用同一个 downstream target table。这里的 source table 语义应由用户可见的 `(sourceSchema, sourceTable)` 定义，而不是 TiDB 内部 table object ID。

因此：

- 两个不同 source name route 到同一个 target，应报 conflict。
- 同一个 source name route 到同一个 target，应视为同一个 source，幂等通过。
- `TRUNCATE TABLE` 不改变 source schema/table name，不应触发 registry owner release + add。
- `RENAME TABLE` 改变 source schema/table name，应作为 source transition 处理。
- table ID 只属于 schema / scheduling / dispatcher 生命周期，不属于 route conflict identity。

## 2. 背景

当前 static conflict detection 需要判断：

```text
source A -> target X
source B -> target X
```

如果 `source A != source B`，则同一个 target 被两个 source 占用，必须报错。

这个判断可以直接基于 source schema/table name 完成：

```text
sourceKey = (sourceSchema, sourceTable)
targetKey = (targetSchema, targetTable)
```

不需要引入 logical table ID。

## 3. Registry 不变量

### 3.1 Safety

在同一个 changefeed 中：

1. 同一个 `(targetSchema, targetTable)` 最多只能被一个 `(sourceSchema, sourceTable)` 占用。
2. 如果 incoming binding 的 target 已经存在，且 existing source name 与 incoming source name 不同，必须报 conflict。
3. 如果 existing source name 与 incoming source name 相同，应视为幂等操作。
4. DDL 导致 source name 改变时，必须用原子 transition 表达 owner 变化。
5. DDL 不改变 source name 时，不应改变 registry ownership。

### 3.2 Source Identity

source identity 是：

```text
(sourceSchema, sourceTable)
```

不是：

```text
table ID
physical table ID
logical table ID
dispatcher ID
table span ID
```

原因是 table route 规则本身基于名字匹配，route conflict 也是用户可见名字空间中的 target 占用冲突。

## 4. 具体场景分析

### 4.1 两个不同 source route 到同一 target

上游有两张表：

```text
shop_a.orders
shop_b.orders
```

route 规则将它们都映射到：

```text
dw.orders
```

registry 中的 binding 是：

```text
source shop_a.orders -> target dw.orders
source shop_b.orders -> target dw.orders
```

判断逻辑：

```text
target 相同：dw.orders == dw.orders
source 不同：shop_a.orders != shop_b.orders
```

因此必须报 conflict。

这个场景不需要 table ID。source schema/table name 已经足够准确。

### 4.2 同一个 source 重复注册

同一张表因为 retry、重复事件、registry rebuild 或幂等 add 被重复注册：

```text
source shop.orders -> target dw.orders
source shop.orders -> target dw.orders
```

判断逻辑：

```text
target 相同
source 相同
```

因此应返回成功，不报 conflict。

这里也不需要 table ID。source name 相同就是 registry 语义下的同一个 source。

### 4.3 Truncate

上游表：

```text
db.orders
```

执行：

```sql
TRUNCATE TABLE db.orders;
```

TiDB 内部可能生成新的 table ID，但用户可见 source name 没变：

```text
before: db.orders -> dw.orders
after:  db.orders -> dw.orders
```

在 table route conflict detection 中，这不是新的 source 占用 target，而是同一个 source 的 DDL。

registry 处理应是：

```text
source db.orders already owns target dw.orders
truncate does not change source name
registry ownership unchanged
```

不应执行：

```text
remove db.orders(old table ID)
add db.orders(new table ID)
```

也不应因为 table ID 变化报 conflict。

### 4.4 Drop 后 Create 同名表

如果执行：

```sql
DROP TABLE db.orders;
CREATE TABLE db.orders (...);
```

从 table route conflict identity 看，source name 仍是：

```text
db.orders
```

这类 DDL 的 registry 行为应由表是否仍在复制范围决定：

- drop 使 `db.orders` 离开复制范围时，释放 `db.orders -> target`。
- create 使 `db.orders` 进入复制范围时，注册 `db.orders -> target`。
- 如果 changefeed 的 schema 状态中同一时刻只有一个 `db.orders`，registry 不需要关心新旧 table ID。

route conflict detection 不需要区分旧 table object 和新 table object。

### 4.5 Rename

执行：

```sql
RENAME TABLE db.old_orders TO db.orders;
```

source name 发生变化：

```text
before: db.old_orders
after:  db.orders
```

即使 TiDB 内部 table ID 不变，在 registry conflict detection 语义下，这也是 source owner transition。

处理应是原子的：

```text
remove source db.old_orders
add source db.orders
```

在 add `db.orders` 时必须检查 target 是否已经被其他 source 占用。

例子：

```text
existing: source db.active_orders -> target dw.orders
rename:   source db.old_orders -> source db.orders -> target dw.orders
```

如果 `db.orders` route 后也占用 `dw.orders`，且 existing owner 不是本次被移除的 `db.old_orders`，必须报 conflict。

### 4.6 Multi Rename

执行：

```sql
RENAME TABLE
  db.a TO db.b,
  db.c TO db.d;
```

registry transition 应按 source name 集合处理：

```text
remove: db.a, db.c
add:    db.b, db.d
```

检查必须是 all-or-nothing：

1. 在临时 registry 上先移除 old names。
2. 再添加 new names。
3. 任一 new source 的 target 与剩余 owner 冲突，则整个 transition 失败。
4. 原 registry 不应被部分更新。

### 4.7 Partition / Table Span

partition 和 table span 属于调度 / 复制执行层。

对 route conflict detection 来说，source identity 仍然是 parent table 的 source schema/table name。

不应把 physical partition ID、span ID 或 dispatcher ID 作为 registry source identity。

如果多个 dispatcher / span 属于同一个 source table：

```text
source db.orders -> target dw.orders
source db.orders -> target dw.orders
```

应视为同一 source 的重复注册或同一 owner 的多个执行单元，不是 conflict。

## 5. DDL Routing 中的名字处理

DDL routing 仍然需要处理多类名字：

- `ddl.SchemaName` / `ddl.TableName`
- `ddl.ExtraSchemaName` / `ddl.ExtraTableName`
- `ddl.TableInfo`
- `ddl.MultipleTableInfos`
- `ddl.BlockedTableNames`
- DDL query AST 中提取出的 table references

这些名字用于：

- route 计算。
- DDL query rewrite。
- routed metadata 填充。
- async DDL blocked table status 查询。

但它们不需要携带 table ID。

## 6. `BlockedTableNames` 处理

`BlockedTableNames []SchemaTableName` 应继续保持纯名字语义。

它的职责是描述哪些 source table names 需要用于 DDL blocking / status 查询。

route 处理只需要：

```text
source blocked name -> routed blocked name
```

不需要解析 table ID。

如果未来 runtime registry 要从 DDL transition 中更新 owner，应从 DDL 的 source name transition 中推导：

- rename：old name -> new name
- drop：source name removed
- create：source name added
- truncate：source name unchanged

不要从 `BlockedTableNames` 里推导 table identity。

## 7. `ExtraSchemaName` / `ExtraTableName` 处理

`ExtraSchemaName` / `ExtraTableName` 也是名字字段。

route 处理只需要按名字 route：

```text
source extra name -> routed extra name
```

不需要为它寻找 table ID。

但 runtime transition 需要理解 DDL 类型：

- rename：extra name 是 old source name。
- exchange partition：extra name 是另一个 source name。

因此 runtime registry 不能只看到 extra name 就假设它和 primary name 是同一个 source。
它应按 DDL 类型构造 source name transition。

## 8. 推荐数据结构

### 8.1 Source key

```go
type sourceKey struct {
    Schema string
    Table  string
}
```

### 8.2 Target key

```go
type targetKey struct {
    Schema string
    Table  string
}
```

### 8.3 Route binding

```go
type routeBinding struct {
    Source sourceKey
    Target targetKey
}
```

### 8.4 Registry

```go
type TargetTableRegistry struct {
    memo map[targetKey]routeBinding
}
```

判断逻辑：

```text
if target not occupied:
    add binding
else if existing source == incoming source:
    no-op
else:
    conflict
```

## 9. 静态 Conflict 检查

静态检查只需要当前会进入复制范围的 source table names。

流程：

1. 从 `schemastore.VerifyTables` 的结果中取得所有会同步的 table names。
2. 对每个 source schema/table 计算 target schema/table。
3. 向 registry 添加 `(source, target)`。
4. 如果同一 target 已由不同 source 占用，返回 `ErrTableRouteConflict`。

静态检查不需要 table ID。

## 10. Runtime Conflict 检查

runtime registry 仍然应由 maintainer 持有，因为 TiCDC 是分布式系统，同一个 changefeed 的表可能分布在不同节点。

runtime registry 的输入应是 source name transition，而不是 table ID transition。

典型 DDL：

- create：add source name。
- drop：remove source name。
- rename：remove old source name，add new source name。
- truncate：source name 不变，registry no-op。
- recover：如果 source name 进入复制范围，add source name。
- exchange partition：按涉及的 source names 和实际 route target 做检查，不把 physical partition ID 当作 source。

transition 必须 all-or-nothing。

## 11. 测试建议

### 11.1 静态检查

- `shop_a.orders` 和 `shop_b.orders` route 到同一 target，应报 conflict。
- 同一个 source 重复出现，应幂等通过。
- 未命中 route 的 source 也应占用原名 target。
- route 到已有未命中 source 的原名 target，应报 conflict。

### 11.2 DDL routing

- `BlockedTableNames` 被按名字 route，不需要 table ID。
- `ExtraSchemaName` / `ExtraTableName` 被按名字 route，不需要 table ID。
- query-only references 可被 rewrite，但不参与 source identity 判断。

### 11.3 Runtime transition

- rename old -> new：remove old + add new all-or-nothing。
- truncate：registry owner 不变化。
- drop + create 同名：按 source name 离开 / 进入复制范围处理。
- multi rename：多个 remove/add 作为一个原子 transition。

## 12. Review Checklist

- `sourceKey` 是否只包含 source schema/table name。
- conflict 判断是否只比较 target key 和 source key。
- 是否还有 table ID 参与 registry source identity 判断。
- truncate 是否被实现为 registry no-op / 幂等确认。
- rename 是否被实现为 source name transition。
- `BlockedTableNames` 是否保持纯名字语义。
- `ExtraSchemaName` / `ExtraTableName` 是否只作为名字 route。
- runtime registry 是否仍由 maintainer 持有。

