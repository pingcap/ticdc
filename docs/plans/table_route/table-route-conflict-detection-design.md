# Table Route Runtime Conflict Detection 设计文档

- 状态：草案
- 最后更新：2026-05-30
- 范围：本文只讨论运行时 table route conflict detection，不讨论静态配置检查。

## 1. 背景与目标

`route conflict` 指同一个 changefeed 内，不同 source table 经 table route 计算后得到同一个 target table：

```text
(sourceSchemaA, sourceTableA) -> (targetSchema, targetTable)
(sourceSchemaB, sourceTableB) -> (targetSchema, targetTable)
source A != source B
```

TiCDC 不支持把多张上游表合并到同一张下游表。运行时 conflict detection 的核心目的，是避免两个 source tables 对应的 dispatcher 同时向同一张 target table 写入。

因此，runtime detector 需要保证：

1. maintainer 侧维护 changefeed 级 target table owner registry。
2. registry 的 source identity 只使用 source schema name 和 source table name。
3. 优先在 table trigger dispatcher 写下游 DDL 前完成 target owner 检查。若能在写前确认冲突，changefeed 进入 failed 状态，不能继续写该冲突 DDL。
4. 无论 pre-write 检查是否可用，在 maintainer 调度新 source table dispatcher，或放行已有 dispatcher 以新的 source owner 继续写入之前，都必须完成 target owner 检查。
5. 如果新增 owner 会占用已存在的 target owner，changefeed 进入 failed 状态，maintainer 不创建或放行该 source table 对应的 dispatcher。
6. maintainer 启动和 failover 后都能从 schema store snapshot 重建 registry；未持久化的 pending reservation 不能成为正确性前提。

非目标：

1. 不改变 table route 配置语义和 rule 匹配语义。
2. 不支持多 source table 合并到同一 target table。
3. 不扩展 DDL SQL rewrite 能力。
4. 不新增持久化 registry 元数据。pre-write reservation 是 maintainer 内存态，只能作为优化和提前阻断手段。
5. 不保证所有冲突 DDL 都一定能在写下游前被发现。failover、旧版本 dispatcher、non-block DDL 或缺少 pre-write request 时，系统必须回退到 schedule/pass admission，至少保证“不调度第二个会写冲突 target 的 source dispatcher”。
6. 不要求所有无关 DDL 串行执行；只要求会访问同一 source owner 或同一 target owner 的相关 DDL 按 commitTs 顺序提交 route owner transition。

## 2. 端到端入口

### 2.1 Maintainer 启动时如何初始化 detector

maintainer 启动和 maintainer failover 使用同一套初始化流程。failover 本质是旧 maintainer 下线、新 maintainer 重新 bootstrap；新 maintainer 不继承旧 maintainer 的内存 registry。

初始化步骤：

1. maintainer 完成 bootstrap，确认已经收集当前 changefeed 正在复制的 table 集合。
2. maintainer 从 changefeed config 构建 router。
3. 如果所有 dispatch rule 的 target-schema 和 target-table 都为空（即没有配置任何路由），则不创建 runtime conflict detector。注意：判断依据是**配置中是否存在非空 target 表达式**（`{schema}`、`{table}` 等占位符视为非空），而非运行时路由结果。因为即使当前所有表的 route 结果都是 identity（source == target），后续的表或 schema 变更也可能产生实际路由，detector 必须始终保持活跃。
4. 如果存在非空 target 表达式，maintainer 创建 `routeConflictDetector` 和空的 `TargetTableRegistry`。
5. maintainer 使用 schema store snapshot 加载当前复制范围内的 tables。
6. 对每个 table：
   - 通过 `schemaStore.GetTableInfo(keyspaceMeta, tableID, startTs)` 读取 source schema/table name。
   - 使用 router 计算 target schema/table name。
   - 构造 `RouteBinding{Source, Target}`。
   - 写入 registry。
   - 同时维护 detector 内部的 tableID -> current binding 映射，供后续 drop/rename 构造 remove transition 使用。
7. 如果初始化过程中发现两个 current source tables route 到同一个 target，maintainer 直接 fail changefeed。
8. 初始化成功后，registry 代表“当前已经允许写入的 source owners”。

这个初始化过程是 O(N)，N 是当前复制范围内的 source tables。它不读取 DDL SQL，不依赖 dispatcher status，也不需要旧 maintainer 的内存状态。

### 2.2 Runtime 通过什么事件触发 detector

runtime detector 不是由 maintainer 直接消费完整 DDL event 触发。DDL event 的源头在 schema store，既有执行路径是：

```text
schema store tableTriggerDDLHistory
  -> event service FetchTableTriggerDDLEvents
  -> table trigger dispatcher
  -> dispatcher 写 DDL 或进入 block / pre-write route admission
  -> dispatcher 向 maintainer 汇报 block/schedule status
  -> maintainer 在 pre-write 或 schedule/pass admission gate 调用 conflict detector
```

也就是说，maintainer 侧 detector 的直接触发事件仍然是 dispatcher 上报的 lifecycle 信号，不是 DDL SQL，也不是 schema store 原始 DDL event。区别是 runtime 有两道 gate：

1. `pre-write admission gate`：table trigger dispatcher 在写下游 DDL 前请求 maintainer 检查并 reserve owner transition。检查失败时，不发送 ACK/Action_Write，不写冲突 DDL。
2. `schedule/pass admission gate`：maintainer 在创建、恢复或放行 table dispatcher 前提交或回退执行 owner transition。即使 pre-write gate 缺失或 failover 丢失 reservation，这道 gate 也必须存在。

触发路径按 DDL 类型分三类：

| DDL 类型 | maintainer 可见事件 | detector 触发点 | detector 动作 |
|---|---|---|---|
| `CREATE TABLE` / `CREATE TABLES` / `RECOVER TABLE` | table trigger dispatcher 可在写前发送轻量 route admission request；旧路径是在写完 DDL 后发送 `IsBlocked=false, Stage=NONE`，携带 `NeedAddedTables` | 优先在写前 reserve；fallback 在创建新 table dispatcher 前 | 读取新增 table info，计算 target，执行 add admission。写前冲突则 fail changefeed 且不写 DDL；fallback 冲突则 fail changefeed，不创建新 dispatcher |
| `DROP TABLE` / `DROP DATABASE` | blocking DDL 的 WAITING/DONE，携带 `NeedDroppedTables` 或 DB 级 drop 信息 | writer DONE 后、提交 remove schedule 前 | 从 detector 当前 tableID 映射找到旧 source owner，执行 remove transition。remove 不能在写前释放 target |
| `RENAME TABLE` / `RENAME TABLES` | blocking DDL 的 WAITING/DONE，可能携带 `UpdatedSchemas` / drop 信息，并由 table trigger dispatcher 参与 barrier | 优先在 Action_Write 前 reserve；DONE 后 commit；fallback 在 pass/schedule 前 | 构造 remove old owner + add new owner；冲突则 fail changefeed，不写冲突 DDL或不放行 renamed dispatcher |

因此，conflict 的发现点不是“DDL 语句被解析时”。理想路径是在 table trigger dispatcher 写下游前，由 maintainer 根据 schema store 的 source owner transition 做 admission；保底路径是在 maintainer 准备让某个 source owner 对应的 dispatcher 进入可写状态时做 admission。这样既能尽量避免写冲突 DDL，又能在无法写前判断时避免调度出第二个会写同一 target table 的 dispatcher。

对于 `TRUNCATE TABLE`、分区 DDL、普通 alter table，这些事件即使会出现在 block/schedule path 中，也不会改变 source owner。detector 应返回 no-op，不能因为 table ID add/drop 就修改 registry。

## 3. 术语

### 3.1 Source identity

source identity 只由 source schema name 和 source table name 组成：

```text
(sourceSchema, sourceTable)
```

table ID 不参与 source identity。`TRUNCATE TABLE`、分区 DDL 等可能改变 physical table ID，但只要 source schema/table name 不变，registry 中仍然是同一个 source。

### 3.2 Target owner

`target owner` 指 registry 中某个 target table 当前绑定的唯一 source name：

```text
(targetSchema, targetTable) -> (sourceSchema, sourceTable)
```

它不是 maintainer owner、dispatcher owner，也不是下游物理表 owner。它只表示在同一个 changefeed 内，哪个 source schema/table name 当前被允许写入该 route 后 target table。

dispatcher ID、table ID、所在节点、DDL writer dispatcher 都不参与 owner 判断。

### 3.3 Schedule admission gate

本文把 maintainer 创建、恢复或放行某个 source table dispatcher 写入的时刻称为 `schedule admission gate`。

runtime conflict detection 的检查点在这个 gate 之前：

```text
DDL lifecycle signal
  -> maintainer 构造 owner transition
  -> registry 检查并提交 transition
  -> 成功后才 schedule / resume dispatcher
```

这不是新的 heartbeat action，也不是新的 barrier 协议。它只是 maintainer 在现有 schedule / barrier 生命周期中调用 detector 的位置。

### 3.4 Pre-write admission gate

本文把 table trigger dispatcher 写下游 DDL 之前，向 maintainer 请求 route owner 检查的时刻称为 `pre-write admission gate`。

pre-write gate 的目标是提前发现 owner-add 或 owner-replace 冲突，避免把明显冲突的 DDL 写到下游。它有两个重要约束：

1. pre-write gate 只能 reserve 即将新增的 target owner，不能直接修改 applied registry。
2. pre-write gate 不能提前释放 remove-only transition 的 target owner。drop/drop database 即使已经进入 barrier，也必须等 writer DONE 后才能释放 target。

### 3.5 Applied registry 与 route reservation

runtime detector 维护两类内存状态：

1. `applied registry`：已经允许写入的 source owner 集合，是 schedule/pass gate 的安全基线。
2. `route reservation`：pre-write admission 已接受但尚未 DONE/commit 的 owner transition。

reservation 只保留到对应 DDL lifecycle 完成或 maintainer failover。它的作用是让后续相关 DDL 看到“已有 pending add 会占用这个 target”，从而避免两个冲突 DDL 同时通过写前检查。reservation 丢失后，schedule/pass admission 必须能基于 applied registry 和 schema snapshot 重新判断。

## 4. 当前代码事实

### 4.1 Schema store 是 source name 权威来源

maintainer 运行时 block/status 路径接收的是 dispatcher 上报的 `BlockStatusRequest`，不是完整 `commonEvent.DDLEvent`。status 中的 table 信息主要是 table ID / schema ID。

source schema/table name 不能从 status 里推断，应该从 schema store 的 table info 获取。当前 schema store 已提供这些相关能力：

1. `GetAllPhysicalTables(keyspaceMeta, snapTs, filter)`：返回 snapshot 下所有复制范围内的 physical tables，可用于 startup / failover rebuild。
2. `GetTableInfo(keyspaceMeta, tableID, ts)`：按 table ID 和 ts 查询 table info，并从 `TableInfo` 读取 schema/table name。
3. `FetchTableTriggerDDLEvents(...)`：table trigger dispatcher 的 DDL event 来源。

dispatcher status 只能作为 maintainer lifecycle 信号，不能作为 registry source identity 的权威来源。

### 4.2 Master 已经能让 maintainer 观察 lifecycle DDL

在 `upstream/master` 中，很多 table lifecycle DDL 已经会通过 block status 或 schedule status 到达 maintainer：

| DDL | master block 行为 | maintainer 是否可观察 |
|---|---|---|
| `CREATE TABLE` / `CREATE TABLES` / `RECOVER TABLE` | 通常 non-block | 是，写下游后通过 `Stage=NONE` 汇报 schedule 状态 |
| `CREATE TABLE ... LIKE ...` | 可能 block | 是 |
| `DROP TABLE` | block | 是 |
| `DROP DATABASE` | block | 是 |
| `RENAME TABLE` / `RENAME TABLES` | 通常 block | 是 |
| `TRUNCATE TABLE` | block | 是 |
| 分区 lifecycle DDL | 通常 block | 是 |
| 普通完整表单表 DDL | 通常 non-block | 通常否 |

因此，conflict detector 不需要为了“让 maintainer 看见 DDL”而扩大 block 范围。对 create/recover 这类新增表 DDL，master 的 non-block schedule status 已经足够让 maintainer 在创建新 dispatcher 前执行 registry 检查。

### 4.3 改变 owner 的 DDL 由 table trigger dispatcher 驱动

改变 registry owner 语义的 DDL 都来自 table trigger dispatcher 的 DDL span 路径：

```text
schema store tableTriggerDDLHistory
  -> FetchTableTriggerDDLEvents
  -> event service
  -> table trigger dispatcher
  -> maintainer block/schedule status
```

典型 owner-changing DDL：

| DDL | owner 变化 | DDL writer |
|---|---|---|
| `CREATE TABLE` / `CREATE TABLES` / `RECOVER TABLE` | add source owner | table trigger dispatcher |
| `DROP TABLE` | remove source owner | table trigger dispatcher |
| `DROP DATABASE` | remove database 下所有 source owners | table trigger dispatcher |
| `RENAME TABLE` / `RENAME TABLES` | remove old source owner + add new source owner | table trigger dispatcher |

这使 detector 的实现可以收敛在 table trigger dispatcher 触发的 lifecycle / schedule 信号上，不需要让所有 table dispatcher 都理解 route conflict。

## 5. 正确性要求

### 5.1 Safety

必须始终成立：

1. 同一个 changefeed 内，一个 target table 最多被一个 source name 占用。
2. 如果某个 source owner transition 会让不同 source names 同时占用同一 target，maintainer 必须 fail changefeed。
3. conflict 后，maintainer 不能创建新的冲突 dispatcher，也不能放行已有 dispatcher 以冲突 owner 继续写入。
4. registry mutation 只在 maintainer 事件循环内执行。
5. rename / multi-rename 必须按一个原子 transition 处理，不能暴露中间状态。
6. `TRUNCATE TABLE` 和只改变 physical table ID 的 DDL 不能释放或重新注册 target owner。
7. pre-write admission 不能修改 applied registry；它只能创建 pending reservation。
8. pending add reservation 必须阻止后续不同 source 对同一 target 的 pre-write 或 schedule/pass admission。
9. pending remove 不能让后续 transition 提前看到 target 已释放；target 只有在 writer DONE 后 commit remove 时才释放。
10. 如果 pre-write admission 返回 conflict，table trigger dispatcher 不能继续 ACK/Action_Write，也不能写该冲突 DDL。
11. 如果 pre-write reservation 不存在、丢失或重复上报，schedule/pass admission 仍必须重新检查并阻止冲突 dispatcher。

### 5.2 Liveness

在没有 conflict、schema store 可用、maintainer 正常推进生命周期的情况下：

1. 不改变 source name 集合的 DDL 不应被 registry 阻塞。
2. drop 后应释放 target，使后续合法 source 可以占用该 target。
3. rename 后应释放旧 source 的 target，并占用新 source 的 target。
4. create/recover/create tables 应在 registry admission 成功后继续调度新 dispatcher。
5. maintainer failover 后不需要旧 maintainer 的内存状态即可恢复。
6. 无关 DDL 的 owner transition 若 source set 和 target set 不相交，可以并行通过 pre-write admission；相关 DDL 必须按 commitTs 顺序 commit。
7. pre-write reservation 对应的 DDL DONE 后必须被 commit；如果 DONE 先于 reservation 到达或 reservation 因 failover 丢失，fallback admission 必须幂等推进。

## 6. Registry 数据结构

registry 是 maintainer 内的 per-changefeed 派生状态，不持久化。

```go
type TableKey struct {
    Schema string
    Table  string
}

type RouteBinding struct {
    Source TableKey
    Target TableKey
}

type TargetTableRegistry struct {
    target2Source map[TableKey]TableKey
    source2Target map[TableKey]TableKey
}
```

`source2Target` 是必须索引，不是可选优化。没有它，`Remove(source)` 需要扫描全部 owners；百万级表场景下这会把单表 drop、rename 和生命周期提交放大成 O(N) 操作。

核心操作：

1. `Add(binding)`：
   - source 已经占用同一个 target：幂等成功。
   - target 未被占用：写入 `target2Source` 和 `source2Target`。
   - target 已被不同 source 占用：返回 route conflict。
   - source 已占用不同 target：表示调用方 transition 不完整或 registry 状态不一致，返回内部错误。

2. `Remove(source)`：
   - 通过 `source2Target` 找到 target。
   - 删除 `source2Target[source]` 和 `target2Source[target]`。
   - source 不存在时幂等成功。

3. `ApplyTransition(removes, adds, mutate)`：
   - 在当前 registry 上先做完整校验，再做 mutation。
   - 校验失败时不修改任何 map。
   - `mutate=false` 时只做校验，不修改 registry，用于 pre-write gate。
   - `mutate=true` 且校验成功后，先 remove，再 add。
   - 这是单个 registry 上的事务式操作，不需要复制一份 registry，也不存在两个 registry。

`ApplyTransition` 的校验逻辑不需要 registry copy。它只需要在校验阶段把即将 remove 的 sources 视为已释放：

```text
removeSet = set(removes)

for each add:
    if add.source already owns a different target and add.source not in removeSet:
        error
    if add.target is owned by another source not in removeSet:
        conflict
    if another add in the same transition uses the same target from a different source:
        conflict

if all checks pass:
    remove all removes
    add all adds
```

这样可以保证 rename / multi-rename 的 all-or-nothing 语义，同时避免复制百万级 registry。

### 6.1 Route admission 与 reservation

`TargetTableRegistry` 只表示已经生效的 owner。pre-write admission 需要在 detector 外层维护 reservation：

```go
type RouteAdmissionKey struct {
    CommitTs uint64
    DispatcherID common.DispatcherID
}

type RouteReservation struct {
    Transition RouteOwnerTransition
    AddTargets map[TableKey]TableKey // target -> incoming source
}
```

实现上可以先把 `RouteAdmissionKey` 收敛为现有 barrier event / dispatcher status 中能稳定标识同一 DDL lifecycle 的 key；关键要求是同一 DDL 重试时命中同一 reservation，不同 DDL 不能互相覆盖。

detector 对外暴露三类语义，而不是把 registry map 暴露给 maintainer：

1. `PrewriteAdmit(key, transition)`：
   - 在 applied registry 加 pending reservations 的有效状态上校验 transition。
   - 只 reserve add targets；transition 自己的 removes 只在本 transition 校验中视为释放。
   - pending removes 不会释放 target 给其他 transition。
   - 同一 key 重复调用且 transition 相同，幂等成功。

2. `Commit(key, transition)`：
   - 如果存在 reservation，校验 transition 与 reservation 一致，然后对 applied registry 执行 `ApplyTransition` 并删除 reservation。
   - 如果 reservation 不存在，走 `ScheduleAdmit` fallback。

3. `ScheduleAdmit(transition)`：
   - 用于没有 pre-write reservation 的旧路径、failover 后重放、或 non-block schedule status。
   - 在 applied registry 加 pending add reservations 的有效状态上校验，然后直接提交 applied registry。

这三个入口把 maintainer 和 barrier 的耦合降到 lifecycle gate 层面：barrier 只负责告诉 detector “这个 DDL 现在处于 pre-write / done / schedule gate”，detector 自己维护 registry 与 reservation 语义。

## 7. 启动与 Failover

maintainer 启动和 maintainer failover 使用同一套 registry 构建流程。failover 本质上是旧 maintainer 下线、新 maintainer 上线；新 maintainer 不依赖旧 maintainer 的内存 registry。

构建流程：

1. maintainer 完成 `determineStartTs`、加载 replicated source tables、构建 `taskInfo`，确认 bootstrap snapshot 下的 table 集合稳定。
2. 使用 schema store snapshot 和 changefeed config 获取当前 replicated source tables。
3. 对每个 source table 调用 router 计算 target。
4. 调用 `Add(binding)` 构建 applied registry，同时写入 tableID -> current binding 辅助映射。
5. 如果构建过程中发现 conflict，直接 fail changefeed。
6. 在创建 `Barrier` 或开始处理 dispatcher status 之前，把 detector 注入 maintainer/barrier。不能先创建 barrier 再初始化 detector，否则 barrier 捕获到 nil detector 后 runtime admission 实际不会生效。
7. 构建成功后，registry 成为后续 runtime owner transition 的基线。

为什么 detector 必须在 bootstrap 完成后初始化：

1. bootstrap 前，maintainer 还不知道当前 changefeed 应复制的完整 table 集合，registry 无法表达“当前已经允许写入的 owners”。
2. bootstrap snapshot 是 failover/restart 的唯一权威基线。若在 table 集合不完整时初始化，会把合法 existing owner 漏掉，后续冲突 create/rename 可能被错误放行。
3. detector 又必须在 barrier/status 处理开始前可用，否则 DDL lifecycle 事件可能绕过 admission。

因此正确顺序是：

```text
determine startTs
  -> load current replicated tables
  -> build taskInfo
  -> build router + route conflict detector from snapshot
  -> create barrier / initialize components with detector
  -> start handling dispatcher status
```

构建必须尽可能快：

1. 时间复杂度是 O(N)，N 是当前复制范围内的 source tables。
2. 创建 map 时应按表数量预估 capacity，减少 rehash。
3. 不为了日志、错误展示或稳定输出对全量表排序。
4. 不逐表打印日志。
5. 错误信息只包含冲突 target、existing source、incoming source。

内存要求：

1. registry 只保存判断冲突所需字段：source key 和 target key。
2. 不保存 table info、DDL event、dispatcher ID、schema ID、table ID、matcher 列表等大对象。
3. `target2Source` 和 `source2Target` 是必要的两份索引；新增第三类索引前必须证明它能解决实际瓶颈。
4. route reservations 是 failover 可丢弃状态；新 maintainer 不继承旧 reservation，而是从 snapshot 重建 applied registry，并通过 schedule/pass fallback 处理重放的 WAITING/DONE/NONE status。

## 8. DDL 对 Registry 的影响

registry 的判断单位是 source owner transition，不是 DDL 类型本身。下面矩阵描述当前应支持的语义：

| DDL / table info 变化 | maintainer 可见信号 | owner transition / admission 行为 |
|---|---|---|
| `CREATE TABLE` | table trigger dispatcher 可在写前发 route admission；旧路径在写完 DDL 后通过 `NeedAddedTables` 调度新 dispatcher | source name 新增。优先 pre-write reserve add target；DONE/NONE 后 commit。若无 reservation，创建 dispatcher 前 fallback `ScheduleAdmit` |
| `CREATE TABLES` | 同一 DDL 可能产生多个 `NeedAddedTables` | 多个 source names 新增。合并成一个 transition，任意 add 冲突则整个 transition 失败 |
| `RECOVER TABLE` | 当前 schema store 复用 new table DDL 路径 | source name 新增。按 create table 处理 |
| `DROP TABLE` | blocking DDL 的 WAITING/DONE，携带 `NeedDroppedTables` | source name 删除。不能 pre-release；writer DONE 后构造 `removes` 并按 DDL 顺序释放 target owner |
| `DROP SCHEMA` / `DROP DATABASE` | blocking DDL 的 WAITING/DONE，或 DB 级 drop 信号 | 该 schema 下 replicated source names 删除。writer DONE 后批量构造 `removes` |
| `RENAME TABLE` / `RENAME TABLES`，old/new 都在复制范围内 | blocking DDL 的 WAITING/DONE，跨 DB 还可能有 `UpdatedSchemas` | source name 替换。Action_Write 前用 `ApplyTransition(..., mutate=false)` 做 pre-write check；DONE 后一次 `ApplyTransition(..., mutate=true)` commit；fallback 在 pass/schedule 前执行 |
| `RENAME TABLE` / `RENAME TABLES`，old 在复制范围内、new 被过滤出复制范围 | 通常表现为 drop 侧 lifecycle 变化 | source name 删除。只构造 `removes`，DONE 后释放 |
| `RENAME TABLE` / `RENAME TABLES`，old 被过滤、new 进入复制范围 | 当前代码路径中这类 filter-in rename 会返回 rename table sync error | 当前不应产生 registry add。未来若支持，按 source name 新增处理，并优先 pre-write reserve |
| `TRUNCATE TABLE` | 可能产生 `NeedDroppedTables` / `NeedAddedTables`，因为 table ID 变化 | source schema/table name 不变。不修改 owner registry；只更新 tableID -> binding 辅助映射 |
| 分区 DDL：add/drop/truncate/reorganize partition、partition by、remove partitioning 等 | 可能产生 partition physical table ID add/drop | logical source table name 不变。不修改 owner registry，不把 partition ID 当 source owner；按需更新 tableID 辅助映射 |
| `EXCHANGE PARTITION` | 可能产生 table ID、partition ID 或 schema ID 调度变化 | 只要 logical source schema/table name 集合不变，就不修改 owner registry；按需更新 tableID 辅助映射 |
| 普通 `ALTER TABLE`：列、索引、charset/collation、auto id、TTL 等 table version update | 可能有 DDL block 或 schema version 推进信号 | source schema/table name 不变。不修改 owner registry；按需更新 tableID 辅助映射 |
| `CREATE SCHEMA`、modify schema charset/collation、create/drop view 等 | 可能有 schema 级或 table-trigger-only 信号 | 不产生 replicated source table owner。不修改 registry |

总结成 registry 关心的三类 transition：

1. source name 新增：create table、create tables、recover table，以及未来支持的 filter-in rename。
2. source name 删除：drop table、drop schema/drop database、rename 后离开复制范围。
3. source name 替换：rename table、rename tables，且 old/new source 都在复制范围内。

`NeedAddedTables` / `NeedDroppedTables` 不是 registry mutation 的充分条件。`TRUNCATE TABLE` 和分区 DDL 是反例：它们可能有 table ID add/drop，但 source name 没变。

## 9. Runtime 处理路径

runtime detector 的目标不是把所有 DDL 都改成 blocking，而是在 maintainer 已经能观察到的生命周期上增加两层 admission：能写前拦截就写前拦截，不能写前拦截就必须在 schedule/pass 前兜底。

### 9.1 Best-effort pre-write admission

#### Blocking owner-add / owner-replace DDL

适用于 `RENAME TABLE` / `RENAME TABLES`，以及当前已经进入 blocking path 的 create-like DDL：

1. affected dispatcher 和 table trigger dispatcher 按现有 barrier 语义汇报 WAITING。
2. maintainer 在发送 `Action_Write` 前构造 owner transition。
3. detector 执行 `PrewriteAdmit(key, transition)`：
   - add targets 与 applied registry 或其他 pending add reservation 冲突：fail changefeed，不发送 `Action_Write`。
   - 无冲突：记录 reservation，maintainer 按既有逻辑发送 `Action_Write`。
4. writer DDL DONE 后，maintainer 调用 `Commit(key, transition)`。
5. commit 成功后再 pass affected dispatcher 或提交 schedule 变化。

这条路径可以避免 rename 到已占用 target 时写下游冲突 DDL。

#### Non-blocking create / recover / create tables

create-like DDL 当前通常不进入 barrier，但 table trigger dispatcher 在写 DDL 前已经知道该 DDL 会产生新增 table。为了尽量避免写冲突 DDL，需要新增一个轻量 route admission request：

1. table trigger dispatcher 从 DDL event 中拿到 commitTs 和新增 table IDs，在写下游前向 maintainer 发送 route admission request。
2. maintainer 读取新增 table info，计算 route binding，执行 `PrewriteAdmit(key, adds)`。
3. admission 成功后，maintainer ACK，table trigger dispatcher 才写下游 DDL。
4. admission 失败时，changefeed failed，table trigger dispatcher 不写该 DDL。
5. DDL 写完后，table trigger dispatcher 继续发送既有 `Stage=NONE` schedule status；maintainer 用该 status commit reservation 并创建新 dispatcher。

如果轻量 request 尚未实现、旧 dispatcher 没有发送 request，或 maintainer failover 后丢失 reservation，则必须回退到 `ScheduleAdmit`。

### 9.2 Mandatory schedule/pass fallback

fallback 是正确性必需路径，不能因为有 pre-write admission 就删除：

1. non-blocking create/recover/create tables：dispatcher 写完 DDL 后发送 `BlockStage_NONE` scheduling status，maintainer 在 `AddNewTable` 前执行 `Commit` 或 `ScheduleAdmit`。
2. blocking rename：writer DONE 后，maintainer 在 pass/schedule 前执行 `Commit`；若没有 reservation，执行 `ScheduleAdmit`。
3. blocking drop/drop database：writer DONE 后，maintainer 在 remove schedule 前执行 `ScheduleAdmit(removes)`。

fallback conflict 的处理方式是 fail changefeed，并禁止创建或放行冲突 dispatcher。此时下游 DDL 可能已经写入，但不会出现第二个 source dispatcher 继续写同一 target。

### 9.3 Drop Table / Drop Database

drop 类 DDL 的 remove-only transition 不会制造 route conflict，但释放 target 的时机影响后续 create/rename 是否会被错误放行：

1. WAITING 阶段不能释放 target，也不应创建 pending remove 对外可见状态。
2. writer DONE 后，maintainer 根据 `NeedDroppedTables` 或 schema ID 找到旧 source owners。
3. detector 执行 remove transition，释放 target。
4. 后续 commitTs 更大的 create/rename 才能占用该 target。

这保证了“DROP 后 CREATE 同一 target”按 DDL commitTs 正确串行，同时避免 DROP 尚未写下游时新 owner 提前通过。

### 9.4 Truncate / Partition / Ordinary Alter

这些 DDL 不改变 source owner：

1. `TRUNCATE TABLE` 只改变 table ID，不改变 source schema/table name。
2. 分区 DDL 只改变 physical table 集合。
3. 普通 alter table 只改变 table version。

detector 对 owner registry no-op，不能因为 table ID add/drop 或 schema version 推进而释放或重新注册 target owner。但 detector 的 tableID -> current binding 辅助映射必须跟随 table ID 变化更新，否则后续 drop/rename 可能找不到旧 owner。

## 10. Schema Store 访问边界

schema store 是 source schema/table name 的权威来源。route detector 不解析 DDL SQL，也不把 dispatcher status 里的 table ID 当 source identity。

短期实现可以直接使用现有 API：

1. startup / failover rebuild 使用 `GetAllPhysicalTables(snapshotTs, filter)`。
2. runtime add / rename 使用 `GetTableInfo(tableID, commitTs)` 读取 source name。
3. runtime drop 使用 detector 内部 tableID -> current source binding 辅助映射，避免 drop 后再去 schema store 查已删除 table。

长期可以新增 schema-store-owned helper，把 DDL event、`TableNameChange`、table info snapshot 转换成 owner transition 和 tableID 辅助映射更新：

```go
GetRouteOwnerTransition(
    keyspaceMeta,
    commitTs,
    filter,
) (transition RouteOwnerTransition, tableIDUpdates []RouteTableBinding, error)
```

这个 helper 的职责属于 schema store；route detector 只消费 source owner transition，不理解 raw DDL SQL。`tableIDUpdates` 用于在 truncate、partition、rename、drop 等 DDL 后维护 detector 内部的 tableID -> binding 映射，但不能被误用为 owner registry mutation 的充分条件。

## 11. Conflict 处理

`PrewriteAdmit`、`Commit`、`ScheduleAdmit` 可能返回两类错误：

1. route conflict：incoming source 计算出的 target 已被不同 source 占用。
2. registry state error：同一个 source 已占用不同 target，或 transition 内部自相矛盾。

处理方式：

1. maintainer fail changefeed。
2. 如果发生在 pre-write admission，maintainer 不发送 route admission ACK 或 `Action_Write`，table trigger dispatcher 不写该冲突 DDL。
3. 如果发生在 fallback schedule/pass admission，下游 DDL 可能已经写入；maintainer 仍不能提交对应 target owner 变化。
4. 不创建会写入冲突 target 的新 dispatcher。
5. 不放行已有 dispatcher 以冲突 target owner 继续写入。
6. 日志打印 changefeed、commitTs、target、existing source、incoming source、DDL 类型、transition 类型和 gate 类型（pre-write / commit / fallback）。

错误对象只需要包含 incoming source、existing source 和 target。route rule matcher、rule index 等辅助排查信息不应让业务逻辑变复杂。

## 12. 并发、重复与恢复

registry mutation 只在 maintainer 事件循环内执行，因此不需要跨 goroutine 加锁维护 registry 内部一致性。pre-write reservation 也由同一个事件循环维护，避免 reservation 与 registry commit 乱序。

DDL 顺序要求按 owner 相关性判断：

1. 相关 DDL：transition 的 source set 或 target set 有交集，必须按 commitTs 顺序 admission/commit。
2. 无关 DDL：source set 和 target set 都不相交，可以并行进入 pre-write admission；它们的 commit 互不影响。
3. pending add 对所有后续 transition 可见，因为它会占用 target。
4. pending remove 只在本 transition 内可见，不能释放 target 给其他 transition。
5. 对于无法精确判断 read/write set 的 DDL，按保守相关处理，等待前序相关 transition commit。

transition 的相关性集合可以定义为：

```text
sources = removes.sources + adds.sources
targets = removes.currentTargets + adds.targets
```

其中 `removes.currentTargets` 来自 detector 当前 tableID -> binding 或 source2Target 映射。drop/drop database 即使只有 removes，也必须把被删除 source 当前占用的 targets 放入相关性集合，确保后续 create/rename 不会提前 claim。

控制面是 at-least-once 的。实现上应保证同一个 commitTs / lifecycle gate 重复进入时幂等：

1. 重复 remove 已不存在 source 应成功。
2. 重复 add 同一 source 到同一 target 应成功。
3. 重复 `PrewriteAdmit` 同一 key 和同一 transition 应返回已有 reservation 成功。
4. `Commit` 找不到 reservation 时应走 fallback admission，而不是把缺失 reservation 当作 fatal error。
5. 重复 commit 已经生效的同一 transition 应成功。
6. 如果 maintainer failover，新 maintainer 通过 startup rebuild 从 schema snapshot 得到当前 applied registry，不依赖旧 maintainer 内存 reservation。

## 13. 性能与内存

startup / failover rebuild：

1. O(N) 时间，N 是当前复制范围内 source tables 数。
2. O(N) 内存，主要是 `target2Source` 和 `source2Target` 两个 map。
3. 需要按表数量预分配 map capacity。
4. 禁止全量排序和逐表日志。

runtime transition：

1. create/drop/rename 单表是 O(1) 到 O(changed source count)。
2. create tables / rename tables 是 O(number of changed source names)。
3. drop schema 默认 O(number of current source names)，因为需要找出该 schema 下 owners；这是低频操作。
4. 不为普通 DDL 分配全量 registry copy。

内存控制：

1. `RouteBinding` 只保存 source 和 target。
2. 不在 registry 中保存 table info、DDL SQL、dispatcher metadata、schema store event 或完整 route rule。
3. 高基数字符串只在必要 map key 中保存。

## 14. 可观测性

需要保留以下日志：

1. registry rebuild conflict：changefeed、target、existing source、incoming source。
2. pre-write admission conflict：changefeed、commitTs、transition 类型、target、existing source、incoming source。
3. fallback schedule/pass admission conflict：changefeed、commitTs、transition 类型、target、existing source、incoming source。
4. reservation created / committed / missing fallback：changefeed、commitTs、transition 类型、reservation key。
5. related transition waiting：changefeed、commitTs、blocked by commitTs、source/target set 摘要。
6. tableID -> binding 更新异常：changefeed、commitTs、tableID、operation。
7. table info 获取失败：changefeed、commitTs、schema store error。
8. unexpected registry state error：changefeed、commitTs、source、target、operation。

日志不能按每张表产生百万级输出。本文不新增监控项。

## 15. 测试计划

### 15.1 Registry 单元测试

覆盖：

1. 不同 source route 到同一 target，返回 conflict。
2. 同一 source 重复 add 同一 target，幂等成功。
3. 同一 source add 到不同 target，返回 state error。
4. remove 已存在 source，同时删除两个索引。
5. remove 不存在 source，幂等成功。
6. rename replace 成功时一次性删除旧 source 并加入新 source。
7. rename replace 失败时两个 map 都保持原状。
8. multi-rename 内部两个 new sources 撞同一 target，返回 conflict。
9. truncate / partition no-op transition 不修改 registry。
10. pending add reservation 阻止另一个 source claim 同一 target。
11. pending remove 不释放 target 给其他 transition。
12. prewrite 重复调用同一 key 同一 transition 幂等成功。
13. commit 缺失 reservation 时 fallback admission 成功。

### 15.2 Maintainer 单元测试

覆盖：

1. startup rebuild 发现已有 route conflict，changefeed fail。
2. failover rebuild 使用与 startup 相同流程恢复 current owners。
3. bootstrap 初始化 detector 发生在 barrier 创建或 status 处理前，barrier 不能持有 nil detector。
4. startup rebuild 同时填充 tableID -> binding，drop/rename 能找到旧 owner。
5. pre-write create table admission 撞到已有 target，changefeed fail，table trigger dispatcher 不写冲突 DDL。
6. fallback create table admission 撞到已有 target，changefeed fail，new dispatcher 不创建。
7. create tables 中任意一个新增 source 撞到已有 target，整个 transition 失败，相关 new dispatchers 不创建。
8. recover table 按 source add 处理。
9. drop table 在 DONE 前不释放 target，DONE 后释放 target，后续合法 source 可以占用。
10. drop schema 后批量释放该 schema 下 target owners。
11. pre-write rename table 撞到已有 target，changefeed fail，不发送 `Action_Write`。
12. fallback rename table 撞到已有 target，changefeed fail，renamed source dispatcher 不继续写入。
13. multi-rename 中任意一个 add 冲突，整个 transition 不生效。
14. rename 后离开复制范围时只释放旧 target owner。
15. truncate table 不释放也不重新注册 target，但更新 tableID -> binding。
16. 分区 DDL / exchange partition 只有 table ID 调度变化时不修改 registry。
17. WAITING、ACK、DONE、Action resend 或调度 callback 不直接触发 applied registry mutation。
18. 同一个 schedule admission gate 重复进入时不重复破坏 registry。
19. 相关 transition 按 commitTs 串行，无关 transition 可以并行 admission。
20. failover 丢弃 reservation 后，snapshot rebuild + replayed status 能幂等 fallback。

### 15.3 集成测试

覆盖：

1. changefeed 运行中 create 一张会 route 到已有 target 的表，changefeed fail，新增 source dispatcher 不创建。
2. changefeed 运行中 rename 到已有 target，changefeed fail，renamed source dispatcher 不继续写入。
3. drop 原 owner 后，再 create 合法 owner，changefeed 可继续推进。
4. truncate 后，原 target owner 保持不变。
5. 分区 DDL 后，原 target owner 保持不变。
6. 支持 pre-write route admission 后，create/rename 冲突在写下游前失败；如果测试环境走旧路径，则断言 fallback 不创建/不放行冲突 dispatcher。

集成测试应区分两类断言：pre-write path 要断言冲突 DDL 没有写入下游；fallback path 至少断言不会出现两个 source dispatchers 同时写同一 target。

## 16. 场景分析

### 16.1 DROP TABLE 后 CREATE TABLE 同一 target

**场景**: source.a 映射到 target.a。上游依次执行 `DROP TABLE source.a` 和 `CREATE TABLE source.b`（source.b 也路由到 target.a）。

**分析**: 

两条 DDL 是独立的 SQL 语句，分别对应不同的 commitTs（记 drop 的 commitTs 为 D，create 的 commitTs 为 C，且 D < C）。在 barrier 中，不同 commitTs 的事件按序处理：

| commitTs | 事件 | `buildTransition` 产出 | `ApplyTransition` 效果 |
|----------|------|----------------------|----------------------|
| D | DROP TABLE source.a | `removes={source.a}` | 释放 target.a 的 owner |
| C | CREATE TABLE source.b | `adds={source.b → target.a}` | target.a 空闲，add 成功 |

**结论**: ✅ 安全。barrier 的 commitTs 排序保证了 remove 先于 add 执行，target.a 在 create 时已释放。

**注意**: 如果两条 DDL 由于某些原因具有相同的 commitTs（例如通过 `CREATE TABLE source.b LIKE source.a` 这样需要 block 源表的 DDL 间接触发），barrier 会将它们合并到同一个 `BarrierEvent` 中。此时 `buildTransition` 会将 removes 和 adds 合并成一个 transition，`ApplyTransition` 会原子地验证：source.a 在 removeSet 中，所以 source.b 可以 claim target.a。同理安全。

### 16.2 RENAME TABLE 与 target 冲突 / 替换

#### 场景 2a: multi-rename 替换 source name

**场景**: source.a → target.a，source.b → target.b（均在路由范围内）。上游执行 `RENAME TABLE source.a TO source.a_old, source.b TO source.a`（同一条 DDL，同一 commitTs）。

**分析**:

这是单个 DDL（multi-rename），同一个 `BarrierEvent` 携带所有受影响表的信号：
- `blockTables`: 包含 tableID_a（旧 source.a）和 tableID_b（旧 source.b）
- 没有 `droppedTables`（没有表被 drop）
- 没有 `addedTables`（没有全新表创建）

`buildTransition` 按顺序处理：

1. `blockTables` 遍历 `{tableID_a, tableID_b}`：
   - `tableID_a`（旧 source.a → 新 source.a_old）：`buildBindingForTable` 返回 source.a_old → whatever_target，与现有 binding (source.a → target.a) 不同 → `addRemove(tableID_a, source.a)` + `addBinding(source.a_old → ...)`
   - `tableID_b`（旧 source.b → 新 source.a）：`buildBindingForTable` 返回 source.a → target.a，与现有 binding (source.b → target.b) 不同 → `addRemove(tableID_b, source.b)` + `addBinding(source.a → target.a)`

最终 transition：
- `removes = {source.a, source.b}`
- `adds = {source.a_old → whatever, source.a → target.a}`

`ApplyTransition` 验证：
- `source.a → target.a`: target.a 的旧 owner source.a **在 removeSet 中** → 视为已释放 → 通过
- `source.a_old → whatever`: 新 target，未被占用 → 通过

**结论**: ✅ 安全。`ApplyTransition` 将 removeSet 中的 owner 视为已释放，因此新 source.a（来自 source.b 的 rename）可以安全地 claim target.a。

#### 场景 2b: source.b 不在路由范围内

**场景**: source.a → target.a（在路由范围内），source.b **不在** filter 范围内（没有路由规则匹配它）。上游执行 `RENAME TABLE source.a TO source.a_old, source.b TO source.a`。

**分析**:

source.b 不在 filter 范围内，意味着 source.b 没有对应的 dispatcher。`BarrierEvent` 的 `blockTables` **只包含 tableID_a**（source.a 的旧 tableID），不包含 tableID_b。

`buildTransition` 只处理 tableID_a：
- `tableID_a`: 旧 source.a → 新 source.a_old → `addRemove(tableID_a, source.a)` + `addBinding(source.a_old → ...)`

transition：
- `removes = {source.a}`
- `adds = {source.a_old → ...}`

source.a（新名，来自 source.b）**不在 transition 中**。

registry 最终状态：
- source.a（新名，旧 tableID_b）的路由 binding **未被注册**
- 这意味着如果之后再有表路由到 source.a 对应的 target，**不会被检测出来**

**但此类场景实际不会触发 detector 问题**，因为：

1. 当前 TiCDC 对 filter-in rename（表从范围外 rename 到范围内）返回 `rename table sync error`，**changefeed 会在更高层面失败**，不会到达 detector 的 admission gate。
2. 如果未来支持 filter-in rename，处理路径会经过 `addedTables`（视为新表进入范围），而非 `blockTables`，detector 会按 source add 处理。

**结论**: ✅ 当前安全（被 filter-in rename 的更高层错误拦截）。未来若支持，需走 `addedTables` 路径。

#### 场景 2c: 先 DROP 再 RENAME

**场景**: source.a → target.a。上游先 `DROP TABLE source.a`（commitTs=D），再 `RENAME TABLE source.b TO source.a`（commitTs=R，D < R），source.b 之前不在范围内或路由到 target.b。

**分析**: 这是两个独立的 DDL，不同 commitTs。barrier 按序处理：

| commitTs | 事件 | transition |
|----------|------|------------|
| D | DROP source.a | removes={source.a}，释放 target.a |
| R | RENAME source.b → source.a | 来源取决于 source.b 是否在范围内 |

- 若 source.b **在范围内**：transition = `removes={source.b}, adds={source.a → target.a}`。target.a 已释放 → 安全。
- 若 source.b **不在范围内**：同场景 2b，RENAME 不会产生 registry add。但此时 source.a 已在 DROP 中释放，source.a（新名）未被注册也不会有冲突。

**结论**: ✅ 安全。commitTs 排序保证 DROP 先于 RENAME 执行。

### 16.3 TRUNCATE TABLE 的过渡期风险

**场景**: source.a → target.a。执行 `TRUNCATE TABLE source.a`。

`TRUNCATE TABLE` 虽然不改变 source name，但会改变 physical table ID。dispatcher 上报的状态会携带 `NeedDroppedTables`（旧 table ID）和 `NeedAddedTables`（新 table ID）。

detector 必须识别这是同一个 logical source name 的 table ID 替换，而不是 source owner 删除再新增：

1. owner registry 保持 `source.a -> target.a` 不变。
2. tableID -> binding 辅助映射从旧 table ID 更新到新 table ID。
3. pending remove 不对外释放 target.a，因此同一时间另一个 `CREATE TABLE source.c -> target.a` 不能因为 truncate 的旧 table ID drop 而通过 admission。

**结论**: ✅ 安全。TRUNCATE 只更新辅助映射，不修改 owner registry，不存在 target.a 被短暂释放的窗口。

### 16.4 Pre-write create 冲突

**场景**: source.a -> target.x 已在 registry 中。上游执行 `CREATE TABLE source.b`，source.b 也 route 到 target.x。

**理想路径**:

1. table trigger dispatcher 在写下游前发送 route admission request。
2. maintainer 构造 `adds={source.b -> target.x}`。
3. detector 发现 target.x 已由 source.a 占用，返回 conflict。
4. maintainer fail changefeed，table trigger dispatcher 不写该 DDL。

**fallback 路径**:

1. 如果没有 pre-write request，table trigger dispatcher 可能已经写完下游 DDL。
2. maintainer 在 `Stage=NONE` schedule status 中看到 `NeedAddedTables`。
3. detector 在 `ScheduleAdmit` 中发现 conflict。
4. maintainer fail changefeed，不创建 source.b dispatcher。

**结论**: ✅ pre-write path 可以避免冲突 DDL 写下游；fallback path 仍保证不会创建第二个写 target.x 的 dispatcher。

### 16.5 Drop 未 DONE 与后续 create

**场景**: source.a -> target.x。上游执行 `DROP TABLE source.a` 后，又执行 `CREATE TABLE source.b`，source.b -> target.x。

**分析**:

1. DROP 在 WAITING 阶段不能释放 target.x。
2. 如果 CREATE 的 pre-write admission 在 DROP DONE 前到达，detector 必须看到 target.x 仍由 source.a 占用，不能让 source.b 通过。
3. DROP writer DONE 后，remove transition commit，target.x 才释放。
4. commitTs 更大的 CREATE 才能成功 claim target.x。

**结论**: ✅ 相关 DDL 必须按 commitTs 串行；pending remove 不可见，避免下游 DROP 尚未完成时 CREATE 提前通过。

---

## 17. Review Checklist

review 时重点检查：

1. registry 是否只在 maintainer 侧维护。
2. source identity 是否只使用 source schema/table name。
3. source name 是否来自 schema store table info，而不是 DDL SQL 或 dispatcher status 推断。
4. detector 是否在 bootstrap table snapshot 完成后、barrier/status 处理开始前初始化。
5. pre-write admission 是否只创建 reservation，不直接修改 applied registry。
6. schedule/pass fallback 是否始终存在，reservation 缺失时是否重新检查。
7. conflict 时是否阻止写冲突 DDL，或至少阻止创建/放行冲突 source dispatcher。
8. 是否保持 master 的 DDL block / writer / pass / resend 语义。
9. 是否没有为了 detector 把 create/recover 强制改成 blocking。
10. `source2Target` 是否作为必须索引维护一致性。
11. rename / multi-rename 是否由单次 `ApplyTransition` 原子处理。
12. drop/drop database 是否只在 DONE 后释放 target。
13. truncate、partition DDL、exchange partition 是否没有按 table ID 修改 owner registry，并正确更新 tableID 辅助映射。
14. 相关 DDL 是否按 commitTs 串行，无关 DDL 是否不被全局串行化。
15. startup 和 failover 是否使用同一套 rebuild 流程。
16. rebuild 和 runtime transition 是否避免全量排序、逐表日志和 registry copy。
