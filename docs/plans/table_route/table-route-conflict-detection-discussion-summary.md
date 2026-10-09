# Table Route Conflict Detection 讨论纪要

- 状态：讨论纪要
- 最后更新：2026-05-28
- 范围：记录 2026-05 下旬围绕 table route runtime conflict detection、target table registry、DDL/block/barrier 语义的讨论过程和阶段性结论。

## 1. 背景

table route 支持把 source table name 映射到 target table name。TiCDC 不支持同一个 changefeed 内多张 source table 同时写入同一个 target table，因此需要检测 route conflict：

```text
(sourceSchemaA, sourceTableA) -> (targetSchema, targetTable)
(sourceSchemaB, sourceTableB) -> (targetSchema, targetTable)
source A != source B
```

静态 conflict 可以在 changefeed 创建或更新时检查；运行时 conflict 来自复制过程中 table info 集合变化，例如新增表、删除表、rename table、recover table。

讨论的核心问题是：如何在 maintainer 侧维护 target owner registry，并在不侵入现有 DDL block/barrier 语义的前提下，在真正有冲突风险的 DDL 写入下游前完成检查。

## 2. 术语澄清

### 2.1 Source identity

registry 的 source identity 只使用 source schema name 和 source table name：

```text
(sourceSchema, sourceTable)
```

table ID 不参与 source identity。`TRUNCATE TABLE`、分区 DDL 可能改变 physical table ID，但只要 source schema/table name 不变，就不是新的 source owner。

### 2.2 Target owner

target owner 指 registry 内某个 target table 当前绑定的唯一 source name：

```text
(targetSchema, targetTable) -> (sourceSchema, sourceTable)
```

### 2.3 Target table registry

registry 是 maintainer 内存态、per-changefeed 的派生状态。它维护两个索引：

```go
target2Source map[TableKey]TableKey
source2Target map[TableKey]TableKey
```

`target2Source` 用于判断 target 是否已被其他 source 占用；`source2Target` 是必须索引，用于 O(1) 删除 source owner，避免百万级表场景下 drop/rename 变成 O(N) 扫描。

## 3. 讨论过程

### 3.1 第一阶段：registry 的输入不应该是 SQL 字符串

最初讨论了 maintainer 如何拿到 source schema/table name。结论是：

- DDL 事件的源头在 schema store。
- maintainer 不应该解析 DDL SQL。
- dispatcher status 中的 table ID / schema ID 只能作为 lifecycle 信号，不能作为 source identity 权威来源。
- source schema/table name 应从 schema store 读取 table info 获得。

当前 schema store 已有相关能力：

- `GetAllPhysicalTables(keyspaceMeta, snapTs, filter)`：用于 maintainer 启动/failover 时重建 registry。
- `GetTableInfo(keyspaceMeta, tableID, ts)`：用于 runtime DDL transition 中按 table ID 和 commitTs 读取 source table info。
- `FetchTableTriggerDDLEvents(...)`：table trigger dispatcher 的 DDL event 来源。

### 3.2 第二阶段：哪些 DDL 会改变 registry owner

真正改变 registry owner 语义的 DDL 是：

| DDL | registry owner 变化 |
|---|---|
| `CREATE TABLE` / `CREATE TABLES` / `RECOVER TABLE` | 新增 source owner |
| `DROP TABLE` | 删除 source owner |
| `DROP DATABASE` | 删除该 database 下所有 source owner |
| `RENAME TABLE` / `RENAME TABLES` | 删除旧 source owner，新增新 source owner |
| rename 后表被 filter 掉 | 删除旧 source owner |

因此，route conflict 的核心触发点是“新增和删除 target owner”。

### 3.3 第三阶段：master 上已有的 block/barrier 行为

我们对比了 `upstream/master` 的行为。结论是：许多 lifecycle DDL 在 master 上已经会汇报 maintainer 或进入 barrier。

| DDL | master block 行为 | 是否已被 maintainer 观察到 |
|---|---|---|
| `CREATE TABLE` / `CREATE TABLES` / `RECOVER TABLE` | 通常 non-block | 是，写下游后通过 `Stage=NONE` 汇报 schedule 状态 |
| `CREATE TABLE ... LIKE ...` | 可能 block | 是 |
| `DROP TABLE` | block | 是 |
| `DROP DATABASE` | block | 是 |
| `RENAME TABLE` / `RENAME TABLES` | 通常 block | 是 |
| `TRUNCATE TABLE` | block | 是 |
| 分区 lifecycle DDL | 通常 block | 是 |
| 普通完整表单表 DDL | 通常 non-block | 通常否 |

这个结论很重要：当前分支不应该为了“让 maintainer 看见 DDL”而把大量 DDL 强制 block。master 已经能让 maintainer 看到 add/drop/schedule 类 DDL；

唯一需要改变 block 行为的理由，是要把 conflict check 前移到下游 DDL 写入之前。 这是错误的。


origin.A -> target.A
origin.B -> target.A

避免限制 dispatcher 被调度出来。

两张表，被映射到同一个 target table；实际上是，两个独立的 dispatcher，写同一张表。因此，只要避免第二个 dispatcher 被 schedule 就可以，这个可以再 maintainer 侧调度表之前给拦住就行。

### 3.4 第四阶段：table trigger dispatcher 的职责

table trigger dispatcher 是 DDL span dispatcher，span 是 `common.KeyspaceDDLSpan`。它的事件来自 schema store 的 table-trigger DDL history：

```text
schema store tableTriggerDDLHistory
  -> FetchTableTriggerDDLEvents
  -> event service
  -> table trigger dispatcher
  -> DealWithBlockEvent
```

由 table trigger dispatcher 负责的主要 DDL 类型包括：

| 类型 | DDL action |
|---|---|
| database 级 | `CREATE DATABASE`, `DROP DATABASE`, modify database charset/collate |
| table 生命周期 | `CREATE TABLE`, `DROP TABLE`, `RECOVER TABLE`, `TRUNCATE TABLE` |
| table rename | `RENAME TABLE`, `RENAME TABLES` |
| multi-create | `CREATE TABLES` |
| view | `CREATE VIEW`, `DROP VIEW` |
| partition 生命周期 | add/drop/truncate/reorganize partition |
| partition 形态切换 | alter table partitioning, remove partitioning |
| partition/table 交换 | exchange table partition |

普通单表结构变更，例如 add/drop column、add/drop index、modify column、rename index、multi-schema change、TTL 变更等，主要进入自身 table dispatcher 的 table DDL history。

### 3.5 第五阶段：改变 registry owner 的 DDL 总是 table trigger dispatcher 写

进一步分析 writer 选择后，结论是：改变 registry owner 语义的 DDL，当前路径中最终 writer 都应是 table trigger dispatcher。

| DDL | registry owner 变化 | DDL writer |
|---|---|---|
| `CREATE TABLE` / `CREATE TABLES` / `RECOVER TABLE` | add source owner | table trigger dispatcher |
| `DROP TABLE` | remove source owner | table trigger dispatcher |
| `DROP DATABASE` | remove database 下所有 source owner | table trigger dispatcher |
| `RENAME TABLE` / `RENAME TABLES` | remove old source owner + add new source owner | table trigger dispatcher |

原因：

- 新增类 DDL 的新 table dispatcher 还不存在，只能由 table trigger dispatcher 写。
- DB/All 级 DDL writer 固定选择 table trigger dispatcher。
- drop/rename 虽然相关 table dispatcher 也会参与 barrier，但 table trigger dispatcher 是 DDL span dispatcher，writer 选择会优先使用它。

这使 route detector 的设计可以大幅收敛：不需要把所有 dispatcher 都看成可能触发 conflict precheck 的执行者。

## 4. 阶段性结论

### 4.1 正确的 conflict 场景

runtime conflict 发生时，一定是某个 DDL 尝试新增 target owner：

```text
existing source A -> target T
incoming source B -> target T
A != B
```

典型 DDL：

- `CREATE TABLE`
- `CREATE TABLES`
- `RECOVER TABLE`
- `RENAME TABLE`
- `RENAME TABLES`

这些 DDL 总是由 table trigger dispatcher 执行。因此正确路径应该是：

1. table trigger dispatcher 在写下游 DDL 前汇报 maintainer。
2. maintainer 通过 schema store 读取本次 DDL 涉及的新 source table info。
3. maintainer 使用 router 计算 target。
4. detector 在当前 registry 上执行 precheck。
5. 如果 target 已被其他 source owner 占用，maintainer 立即 fail changefeed，不发送 `Action_Write`。
6. 如果不冲突，maintainer 发送 `Action_Write`。
7. table trigger dispatcher 写下游 DDL。
8. writer 汇报 `DONE` 后，maintainer commit registry transition。

### 4.2 Precheck 与 apply 的边界

precheck 必须做真实冲突检查，但不能修改 registry。

apply 只在 writer `DONE` 后执行，负责提交 registry transition 和更新 detector 内部 tableID 映射。

这个边界避免两类错误：

- precheck 提前修改 registry，但 DDL 最终没有写入下游，导致 registry 与真实复制状态分叉。
- DDL 已经写到下游后才发现 conflict，导致 fail 太晚。

### 4.3 Barrier 的职责边界

barrier 不应该理解 route conflict 的业务语义。它只应该提供两个生命周期挂点：

1. 在发送 `Action_Write` 前调用 route detector precheck。
2. 在 writer `DONE` 后调用 route detector apply。

不应该为了 route detector 改写 barrier 的核心状态机、range checker、writer/pass 选择或 resend 协议。

### 4.4 Dispatcher gate 的收敛条件

不应该用宽泛的 `routeAffectingDDL` 让所有 table-info 变化 DDL 都额外 block。

额外 block 应只发生在以下条件同时满足时：

```text
IsTableTriggerDispatcher()
&& router.HasTableRoute()
&& DDLEvent.TableNameChange != nil
&& len(DDLEvent.TableNameChange.AddName) > 0
```

也就是只拦截会新增 target owner 的 DDL。drop、truncate、partition DDL 不应该因为 route detector 被额外 block；它们应保持 master 原有 block 语义。

## 5. 对当前分支的评估

当前分支已经具备一些基础能力：

- 引入了 maintainer-owned `routeConflictDetector`。
- 启动时可从 schema store 初始化 registry。
- registry 内部维护 `target2Source` 和 `source2Target`。
- barrier 已有 precheck/apply 生命周期挂点。
- dispatcher 侧已有 route DDL gate。

但当前实现还有关键问题：

1. `routeAffectingDDL` 判断过宽。
   - 当前基于 `NeedDroppedTables`、`NeedAddedTables`、`UpdatedSchemas`、`TableNameChange`。
   - 这会把 truncate、partition、schemaID update 等非新增 owner DDL 也纳入额外 block。

2. `precheck` 没有真实执行 registry conflict 检查。
   - 当前只构造 pending transition 并保证顺序。
   - 真正的 `registry.ApplyTransition` 在 `apply` 阶段，也就是 writer `DONE` 后才执行。
   - 这意味着当前实现可能增加了 block 成本，但没有完全获得“写下游前 conflict precheck”的收益。

3. barrier 层有被 route detector 牵引变复杂的风险。
   - route detector 应该保持在生命周期挂点内，不应该改变 barrier 的通用语义。

## 6. 建议实现方案

### 6.1 Dispatcher 侧

把新增 block 条件改为只针对新增 target owner 的 table trigger DDL：

```go
func (d *BasicDispatcher) routeConflictPrecheckRequired(event *commonEvent.DDLEvent) bool {
    return d.IsTableTriggerDispatcher() &&
        d.sharedInfo.GetRouter().HasTableRoute() &&
        event.TableNameChange != nil &&
        len(event.TableNameChange.AddName) > 0
}
```

该函数只影响 `shouldBlock` 的额外 route gate。原有 master `BlockedTables` 判断保持不变。

### 6.2 Maintainer detector 侧

`precheck` 应执行真实校验：

1. 构造或复用 pending transition。
2. 如果当前 event 不是 pending queue head，返回 `ready=false`，不 ACK dispatcher。
3. 如果是 head，对当前 registry 做只读校验。
4. 冲突时返回 `ErrTableRouteConflict`，通过 maintainer error reporter fail changefeed。
5. 校验通过后标记 prechecked，允许 barrier 发送 `Action_Write`。

`apply` 仍在 writer `DONE` 后调用：

1. 确认 event 是 pending queue head。
2. 调用 `registry.ApplyTransition`。
3. 更新 detector 内部 `tables`。
4. 弹出 pending event。

### 6.3 Registry 侧

registry 需要支持“校验 transition 但不 mutation”的能力。实现可以是一个内部方法，供 `PrecheckTransition` 和 `ApplyTransition` 共用验证逻辑。

要求：

- 不为了错误返回写简单 wrapper。
- runtime 错误使用 `GenWithStack...`。
- 不引入可选参数。
- 不为了日志或错误展示引入复杂业务状态。
- 不复制整个 registry；百万级表场景下不能引入 O(N) precheck。

### 6.4 Barrier 侧

barrier 只保留两个调用点：

- `Action_Write` 前：`routeDetector.precheck(...)`。
- writer `DONE` 后：`routeDetector.apply(...)`。

如果某个 DDL 不需要 route precheck，detector 返回 nil，不影响原有 barrier 行为。

## 7. 对 master block 行为的影响

目标是最大限度保持 master 行为，只对必须前置 conflict check 的 DDL 做有意改变。

| 场景 | master 行为 | 建议实现后 |
|---|---|---|
| 未开启 table route | 保持 master | 不变 |
| 普通单表 DDL | 完整表通常 non-block | 不变 |
| `DROP TABLE` / `DROP DATABASE` | master 已 block | 不额外改变，只在 DONE 后更新 registry |
| `TRUNCATE TABLE` | master 已 block | 不因 route detector 扩大 block |
| partition DDL | master 已有 block/schedule 语义 | 不因 route detector 扩大 block |
| `RENAME TABLE(S)` | master 通常已 block | block 行为基本不变，增加 write 前 precheck |
| `CREATE TABLE(S)` / `RECOVER TABLE` | master 通常 non-block，写后汇报 schedule | table route 开启时变为 block，用于写下游前 precheck |

唯一有意改变 master block 行为的是：table route 开启时，会新增 source owner 的 table trigger DDL。这个改变是功能所需，否则 conflict 只能在下游 DDL 已经写入后才发现。

## 8. Maintainer failover 结论

maintainer failover 本质是旧 maintainer 下线、新 maintainer 上线。新 maintainer 不应继承旧 maintainer 的 registry 内存。

正确恢复方式：

1. 新 maintainer 完成 bootstrap。
2. 从 schema store 按 startTs 加载当前复制范围内所有 tables。
3. 使用 table info + router 重建 registry。
4. dispatcher 后续重发的 WAITING/DONE/status 继续驱动 barrier 和 route detector。

如果需要在 failover 后对正在进行的 DDL 做 detector 变更，必须建立在“已经收集完所有正在复制表”的前提下；否则 registry snapshot 可能不完整。

## 9. 仍需实现或修正的事项

1. 收窄 dispatcher route gate，只拦截 `TableNameChange.AddName` 的 table trigger DDL。
2. 让 detector precheck 在写下游前执行真实 registry conflict 校验。
3. 保持 registry mutation 只在 writer `DONE` 后提交。
4. 清理 barrier 中与 route detector 无关的语义改动，避免侵入 range checker / writer 选择。
5. 更新单测覆盖：
   - create/recover/rename conflict 在 write 前 fail changefeed。
   - drop 不触发 conflict，只释放 owner。
   - truncate/partition DDL 不因 route detector 额外 block。
   - duplicate status / DONE / resend 下 precheck/apply 幂等。
   - maintainer bootstrap 重建 registry。

## 10. 最终共识

route detector 的正确边界是：

```text
schema store 提供 table info
router 计算 source -> target
registry 判断 target owner 是否冲突
maintainer 在 DDL 生命周期挂点调用 detector
barrier 不承载 route 业务语义
```

runtime conflict detection 不应成为一个新的 DDL 调度协议，也不应泛化改变所有 table-info DDL 的 block 行为。它只需要在新增 target owner 的 table trigger DDL 写下游前完成 precheck，并在 DDL DONE 后按顺序提交 registry transition。
