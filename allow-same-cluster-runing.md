# allow-same-cluster 运行时安全性分析

Last updated: 2026-09-30
Status: 分析草案
Scope: `allow-same-cluster` 在数据库级隔离配置下的运行时安全性
Audience: TiCDC 维护者和代码审查者
Related documents:

- [allow-same-cluster 的数据库隔离准则](docs/design/2026-09-18-allow-same-cluster-gate-design.md)
- [静态校验实现](pkg/check/same_cluster.go)
- [DDL 路由实现](pkg/routing/ddl_query_rewriter.go)
- [DDL 事件过滤实现](logservice/schemastore/persist_storage_ddl_handlers.go)

## Background

`allow-same-cluster` 允许 changefeed 向同一 TiDB 逻辑集群写入。当前实现在 create、update 和 resume 阶段分析 filter 与 table route 配置，通过数据库级隔离防止当前 changefeed 再次捕获自己写入的数据。

静态配置无法完整表达单个运行时 DDL 的对象集、下游对象的所有权以及 DDL 执行结果。因此，changefeed 通过静态校验后仍然可能修改非预期对象、产生跨库依赖，或者在重试中造成下游状态分叉。

## 已确认的静态前提

开启 `allow-same-cluster` 的配置必须满足以下条件：

- 启用 table routing。
- 每条 filter 规则都由带路由目标的 dispatch rule 完整覆盖。
- 每个路由后的目标 schema 都位于 filter 可捕获的源 schema 集合之外。
- schema 隔离按 TiDB 标识符语义判断，包含大小写等价性。
- 校验覆盖 filter 模式未来可匹配的数据库和表，不依赖当前 catalog 中已存在的对象。

普通单表 DML 只携带一个具体表。在上述前提下，router 可以将该表稳定映射到源 schema 集合之外。运行时的主要缺口集中在 DDL 及其下游执行边界。

## 问题一：多对象 DDL 可修改 filter 范围外的对象

### 问题说明

一个 DDL 可以同时修改多个表或 schema。DDL 事件只要有一个主要对象或 extra 对象通过 filter，就可能继续下发。Router 会独立处理 SQL AST 中的每个表名；没有匹配 dispatch rule 的对象保留原名。

### 具体例子

以下配置可通过静态校验：

```toml
allow-same-cluster = true

[filter]
rules = ["src.pt"]

[sink]
[[sink.dispatchers]]
matcher = ["src.pt"]
target-schema = "dst"
```

源端执行：

```sql
ALTER TABLE src.pt
EXCHANGE PARTITION p0 WITH TABLE aux.t;
```

路由后的语句可能为：

```sql
ALTER TABLE dst.pt
EXCHANGE PARTITION p0 WITH TABLE aux.t;
```

源集群已经对 `aux.t` 执行过一次交换。Sink 在同一集群重放该 DDL 时会再次修改 `aux.t`，并在 `dst.pt` 分区与 filter 范围外的表之间移动数据。

同类条件还可出现在：

- 跨库 `RENAME TABLE`。
- 包含多个 rename pair 的 `RENAME TABLES`。
- `FLASHBACK DATABASE old_name TO new_name`。
- 未来新增的多对象 DDL。

Rename 目标位于 filter 范围外时，重放语句通常会因目标已存在而失败。延迟复制、并发删除或后续 rename 可以改变这个结果，因此目标已存在不能作为安全保证。

### 影响

- filter 范围外的对象可能被修改、交换、移动或恢复。
- 目标库状态可能与源库状态分叉。
- 部分路由后的 DDL 一旦开始执行，很难通过重试恢复原始状态。

### 解决方法

`ddlRoutePlan` 需要输出 DDL 将修改的完整对象集，包括主对象、extra 对象、rename 端点、exchange 双方和 `MultipleTableInfos`。运行时检查对每个具体对象应用 route，然后根据最终目标 schema 判断安全性。

对象名发生变化不足以证明安全。额外规则可能将 `aux.t` 改名后再放入 filter 可捕获的 schema。检查必须使用最终名称和 TiDB 的标识符大小写语义。

任一可修改对象无法解析、没有安全路由，或者最终目标落入源 schema 集合时，TiCDC 应在写入 sink 前拒绝整个 DDL。安全失败不得保留或执行部分子语句。

## 问题二：目标对象可能属于其他使用者

### 问题说明

当前静态校验证明目标 schema 不会被当前 changefeed 捕获。该结论无法证明以下属性：

- 目标 schema 为当前 changefeed 专用。
- 目标表由当前 changefeed 创建和管理。
- 业务应用不会直接写入目标对象。
- 外部 DDL 不会在安全检查和实际执行之间改变 catalog。

`TargetTableRegistry` 可以防止当前 changefeed 内的两个源表同时占用一个目标表。它不跟踪下游 catalog 中已有对象的所有者，也不能阻止业务侧并发写入。

### 具体例子

以下配置将 `src.orders` 复制到 `dst.orders`：

```toml
allow-same-cluster = true

[filter]
rules = ["src.orders"]

[sink]
[[sink.dispatchers]]
matcher = ["src.orders"]
target-schema = "dst"
```

启动 changefeed 前，同一集群中已经存在业务系统管理的 `dst.orders`，且该表包含 `id = 100` 的订单。源端执行：

```sql
UPDATE src.orders
SET status = 'cancelled'
WHERE id = 100;
```

Sink 会更新 `dst.orders` 中同一主键的数据。如果源端后续执行 `DROP TABLE src.orders`，路由后的 `DROP TABLE dst.orders` 会删除整个业务表。这两个操作都满足数据库级隔离条件，但目标对象的真实所有者没有得到保护。

### 影响

- DML 可能更新已有的业务数据。
- `DROP`/`TRUNCATE`/`RENAME` 等 DDL 可能破坏其他使用者管理的对象。
- 外部 schema 变更可能导致持续的 DML 失败或列映射错误。
- 检查通过后的 catalog 变化可能引入 TOCTOU 竞态。

### 解决方法

TiCDC 需要为同集群目标建立可持久化的所有权契约。所有权记录至少应包含 changefeed ID、源表、目标表、配置版本或 epoch 以及占用状态。

在首次 DML 或 DDL 写入前，changefeed 必须成功申明目标对象。已存在且未登记的对象应默认拒绝写入。如果用户希望接管已有对象，需要执行显式、可审计的授权操作。

Rename、truncate 和 drop/recreate 等 DDL 需要同步更新所有权记录。重启和 failover 后，TiCDC 应从持久化状态恢复相同的占用关系。检测到所有权冲突或未授权 catalog 变化时，changefeed 应停止后续写入。

如果首个版本无法实现持久化所有权，运维契约应要求目标 schema 专用，并在 create、resume 和 sink 重连后校验目标 catalog。

## 问题三：DDL 重试可重复产生非幂等副作用

### 问题说明

MySQL sink 会重试 DDL。连接中断、超时或 TiDB 异步 DDL 可以产生模糊结果：TiCDC 收到错误时，下游 DDL 可能已经提交。

`EXCHANGE PARTITION` 明确属于非幂等 DDL。当 `EnableDDLTs` 开启时，writer 会通过 DDL TS 记录判断事件是否执行过。该机制不能替代安全检查：第一次执行已经可能修改不安全对象，且所有可重试 DDL 都需要明确的恢复语义。

### 具体例子

以下配置将 `src` 库中的表统一路由到 `dst`：

```toml
allow-same-cluster = true

[filter]
rules = ["src.*"]

[sink]
[[sink.dispatchers]]
matcher = ["src.*"]
target-schema = "dst"
```

源端执行：

```sql
ALTER TABLE src.pt
EXCHANGE PARTITION p0 WITH TABLE src.t;
```

路由后的语句为：

```sql
ALTER TABLE dst.pt
EXCHANGE PARTITION p0 WITH TABLE dst.t;
```

第一次下游执行已经提交，但 TiCDC 在收到成功响应前遇到连接中断。Writer 如果直接重试，第二次执行会再次交换 `dst.pt` 分区和 `dst.t` 的数据，使下游状态偏离源端一次执行后的状态。

这个例子中的两个表都已路由到隔离目标，数据库级隔离检查会通过。风险来自下游执行结果不确定和非幂等重试。

### 影响

- 重复交换可能将数据再次移动或恢复到旧位置。
- 重复 rename、flashback 或多语句 DDL 可能在不同对象集上得到不同结果。
- 进程崩溃或 failover 后，仅依赖内存中的检查结果会丧失判定状态。
- 已执行部分语句的 DDL 无法通过单纯重试达到原子恢复。

### 解决方法

每个 DDL 需要稳定的事件身份，至少包含 changefeed ID、commitTs 和 DDL 类型。执行状态需要持久化，并区分“未执行”、“执行中且结果未知”、“已成功”和“已失败”。

`allow-same-cluster` 应对非幂等 DDL 强制启用 DDL TS 或具有等价持久性的去重机制。遇到超时、无效连接或进程重启时，writer 需要先查询持久化执行记录和 TiDB DDL 历史。只有能够证明事件未执行时才可重试。无法确认结果时，changefeed 应停止并报告结果不确定。

整个 DDL 及其全部子语句的安全检查必须在首条 SQL 发送到 sink 前完成。安全检查失败属于不可重试错误，不应进入通用 DDL 重试循环。

## 问题四：引用型 DDL 可建立跨隔离边界的依赖

### 问题说明

DDL AST 中的表名具有不同角色。一些表是 DDL 的修改目标，一些表只作为模板、查询数据源或外键引用。通用 AST 遍历可以找到表名，但当前 route plan 没有将对象标注为“修改”或“引用”。

### 具体例子

以下配置将 `src.*` 路由到 `dst.*`：

```toml
allow-same-cluster = true

[filter]
rules = ["src.*"]

[sink]
[[sink.dispatchers]]
matcher = ["src.*"]
target-schema = "dst"
```

源端执行：

```sql
CREATE VIEW src.recent_orders AS
SELECT id, customer_id
FROM aux.orders;
```

视图名会被路由为 `dst.recent_orders`。`aux.orders` 不匹配 `src.*`，因此会保留原名。最终的目标 view 每次查询都直接读取同集群中 `aux.orders` 的实时数据。

相同的依赖还可以由以下 DDL 引入：

```sql
CREATE TABLE src.orders_copy LIKE aux.orders;

ALTER TABLE src.order_items
ADD CONSTRAINT fk_order
FOREIGN KEY (order_id) REFERENCES aux.orders(id);
```

### 影响

- `CREATE TABLE ... LIKE` 的结果取决于 sink 执行时的实时模板，可能与源 DDL 执行时的 schema 不同。
- 目标 view 可能直接读取源对象，使目标数据不再是独立副本。
- 外键可能使目标 DML 受源对象状态限制，并引入级联行为或 DDL 失败。
- 依赖对象被 rename、drop 或修改后，复制结果可能失效或分叉。

### 解决方法

`ddlRoutePlan` 需要为每个对象记录影响角色，至少区分修改目标、rename 源、rename 目标、exchange 对象、模板表、view 数据源和外键引用。

修改目标必须通过数据库级隔离和所有权检查。引用对象应经过路由后指向当前 changefeed 拥有的目标对象。

首个实现应采用保守策略：任一引用保留源集群原名、指向未授权对象或无法解析时，TiCDC 在写入 sink 前拒绝该 DDL。后续如果支持显式跨库引用，需要单独定义授权配置和生命周期语义。

## 统一运行时不变量

对每个最终会写入 sink 的 DDL，运行时检查应同时保证：

1. 检查覆盖 SQL AST、`DDLEvent` 主对象、extra 对象、`MultipleTableInfos` 和其他会影响 sink 执行的 metadata。
2. 每个可修改对象的最终 schema 都位于源 schema 集合之外。
3. 每个可修改对象都属于当前 changefeed 或有明确的接管授权。
4. 每个引用对象都指向稳定、已授权的目标对象。
5. 对整个 DDL 完成检查后才允许执行首条 SQL。
6. 未知 DDL 类型、未知对象角色和 metadata/query 不一致都导致事件被拒绝。
7. 判定在重试、重启和 failover 后保持一致。

## 建议的检查位置与错误行为

DDL 在 `downstreamadapter/eventcollector/dispatcher_stat.go` 中先调用 `Router.ApplyToDDLEvent`，随后交给 dispatcher 和 sink。运行时安全检查可以放在路由计划已生成、事件尚未转发的边界。

建议扩展 `ddlRoutePlan`，使它输出具体的源对象、目标对象和对象角色。这样可以在 AST 变更前保留源到目标的关系，并避免通过重新解析改写后 SQL 推测安全性。

安全检查失败时：

- 不向 sink 发送 DDL 或其任何子语句。
- 通过 changefeed 错误路径停止复制进度。
- 返回稳定、不可重试的错误。
- 错误包含 changefeed ID、DDL 类型、commitTs、对象角色、源名称和最终目标名称。
- 日志和指标区分隔离失败、所有权冲突、未支持 DDL 和结果不确定。

## 验证重点

单元测试至少应覆盖：

- `EXCHANGE PARTITION` 一侧命中 filter、另一侧保留原名时被拒绝。
- `EXCHANGE PARTITION` 两侧都路由到安全且已授权的目标时通过。
- 额外对象名发生变化，但最终目标仍位于源 schema 集合内时被拒绝。
- 跨库单表 rename、多表 rename 和 flashback database 的每个端点都被检查。
- 目标表已存在、所有权不符或在恢复时缺少所有权记录的情况。
- 非幂等 DDL 首次成功后连接中断，恢复时不重复执行。
- view、foreign key 和 `CREATE TABLE ... LIKE` 的引用对象分类与授权检查。
- 解析失败、未知 DDL 类型以及 query/metadata 不一致时不会写入 sink。

同集群集成测试应确认：

- 被拒绝 DDL 涉及的 filter 外表保持不变。
- 安全的多对象 DDL 可以正常推进 checkpoint。
- changefeed 在安全错误后停止，且重启不会绕过检查。
- 注入 sink 超时、连接中断和进程重启后，非幂等 DDL 不会重复产生副作用。
