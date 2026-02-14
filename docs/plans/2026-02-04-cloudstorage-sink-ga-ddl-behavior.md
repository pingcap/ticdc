# Cloud Storage Sink GA - DDL 行为梳理（拆表开/关）

- Date: 2026-02-04
- Scope: TiCDC dispatcher/maintainer 的 DDL 行为梳理（重点：cloud storage sink 的 DDL 顺序与 barrier 机制）
- Status: Draft
- References:
  - `docs/plans/2026-02-04-cloudstorage-sink-ga-requirements.md`
  - `docs/plans/2026-02-04-cloudstorage-sink-ga-design.md`
  - `docs/plans/2026-02-04-cloudstorage-sink-ga-implementation.md`

## 1. 目的与结论摘要

本文件的目的：
1) **完整梳理 TiCDC 在“未开启拆表 / 开启拆表”两种情况下，各类 DDL 的端到端行为**（dispatcher → maintainer barrier → sink）。
2) 对照本 GA 的 DDL 策略（`PassBlockEvent` + `DrainMarker`），检查是否存在覆盖盲区或冲突，确保策略全面性。

结论摘要（先给结论，细节见后续章节）：
- TiCDC 对 DDL 的核心分流点是 dispatcher 的 `shouldBlock()`：**是否需要走 maintainer barrier（write/pass 协调）**。
- “拆表关闭”下：大多数**单表 DDL**不会进入 barrier；“拆表开启”下：该表上的**单表 DDL**会进入 barrier（因为 `!isCompleteTable`）。
- 本 GA 的 DDL 策略（每个 involved dispatcher 在写入/上报 DDL 前先 `PassBlockEvent`，在 sink 内用 `DrainMarker` 覆盖编码队列→spool→远端）与现有 DDL 行为**不冲突**，并能覆盖：
  - 非 barrier 的单表 DDL（必须覆盖 sink 内在途数据）
  - barrier 的多 dispatcher DDL（必须覆盖跨 dispatcher 的在途数据）
- 需要显式记录的假设/边界：
  - 部分 DDL 的 `BlockedTables` 可能为空（不走 barrier），但是否需要 `PassBlockEvent` 取决于该 DDL 是否会导致 cloud storage sink 产出/更新 schema file。

> 术语说明：本文统一使用“**在途数据**（仍在编码队列/spool/未落远端）”来描述 early-wake 下的顺序风险，避免引入与本 GA 无关的历史方案术语，减少歧义。

---

## 2. 关键术语与配置含义

### 2.1 “拆表功能”在 TiCDC 中的含义（本文口径）

本文所说的“拆表”指：同一张逻辑表被切分为多个同步单元，从而同一张表可能对应**多个 dispatcher**；这些 dispatchers 可能在同一节点或跨节点分布。

在代码层面的可观测特征：
- dispatcher 在创建时记录 `isCompleteTable = common.IsCompleteSpan(...)`：
  - **未拆表：** `isCompleteTable = true`（完整表）
  - **已拆表：** `isCompleteTable = false`（分片表）

### 2.2 `enable-table-across-nodes` 与“拆表”

`enable-table-across-nodes` 是调度层（scheduler）关于“是否允许把拆分后产生的多个 dispatcher 分配到不同节点”的策略开关。

对本文的 DDL 行为分析而言：
- **拆表是否开启**决定“同表是否会有多个 dispatcher”（影响 `shouldBlock` 的判定）。
- **是否跨节点**不改变 DDL 协议本身，只影响 involved dispatchers 是否位于多个节点；因此只会把“多 dispatcher”场景从单节点扩展到跨节点。

> GA 默认按 `enable-table-across-nodes=false` 理解（见 requirements 文档），但本文会同时描述“跨节点”与“同节点”的行为，因为它们对 DDL 顺序要求一致。

### 2.3 TableTriggerEventDispatcher（表触发 dispatcher）

TiCDC 每个 changefeed 有一个特殊 dispatcher 负责 DDLSpan（`common.KeyspaceDDLSpan`），代码中通过 `IsTableTriggerDispatcher()` 判断。

它的作用（只列与 DDL 相关的）：
- 在一些 DDL（尤其是 DB/All 级别）场景，作为 maintainer 构建“影响范围检查器（range checker）”的关键输入，且会有“hold DB/All block events 直到无 pending scheduling tasks”的逻辑。
- 对于“需要 add/drop tables 的 DDL”，它会维护 `pendingACKCount` 与 resend task 逻辑，确保 maintainer 的调度行为与 DDL 观察顺序一致。

---

## 3. DDL 事件模型（DDLEvent 字段）与行为分流点

### 3.1 DDLEvent 的关键字段

TiCDC 的 DDL 在 dispatcher/sink 之间以 `pkg/common/event.DDLEvent` 形式传递。与 DDL 行为强相关的字段有：
- `Type`：TiDB DDL job type（`model.ActionType`），例如 `ActionCreateTable`、`ActionRenameTable` 等。
- `Query`：DDL SQL 文本（仅用于展示/下游执行/写入 schema file）。
- `FinishedTs`：DDL commitTs（本文用 `T` 表示）。
- `BlockedTables`：`InfluencedTables`，决定该 DDL 的影响范围类型：
  - `InfluenceTypeNormal`：影响特定表集合（`TableIDs`）。
  - `InfluenceTypeDB`：影响某个 schema（`SchemaID`）。
  - `InfluenceTypeAll`：影响全局。
- `NeedAddedTables` / `NeedDroppedTables`：影响调度（create/drop table、drop schema 等）。
- `UpdatedSchemas`：rename table(s) 跨 database 时的 schemaID 更新信息。
- `NotSync`：单表 DDL 可能被标记为“不需要同步到下游”，但仍需让 dispatcher 更新元数据（例如过滤掉的 `TRUNCATE TABLE` 仍需让新表继续同步）。

### 3.2 关键分流点：dispatcher 的 `shouldBlock()`

dispatcher 收到 DDL 后，会先判断是否需要进入 maintainer barrier（write/pass 协调）。判定逻辑可概括为：

1) 如果 `ddl.BlockedTables == nil`：认为无需 barrier（直接处理）。
2) 若 `InfluenceTypeDB` 或 `InfluenceTypeAll`：**一定进入 barrier**。
3) 若 `InfluenceTypeNormal`：
   - `len(TableIDs) > 1`：多表 DDL，**进入 barrier**。
   - `len(TableIDs) == 1`：
     - 若 `isCompleteTable == false`（拆表）：**进入 barrier**（同一逻辑表多个 dispatcher 需要协同）。
     - 若 `isCompleteTable == true`（未拆表）：**不进入 barrier**（单 dispatcher 直写/直 pass）。

这就是“拆表开/关”对 DDL 行为的主要差异来源。

---

## 4. 未开启拆表时的 DDL 端到端行为

本节讨论：逻辑表不拆分，每张表只有一个 dispatcher（`isCompleteTable=true`）。

### 4.1 单表 DDL（InfluenceTypeNormal，单表）

典型 SQL（非穷举）：`CREATE TABLE`、`DROP TABLE`、`TRUNCATE TABLE`、`ALTER TABLE ...`、`RENAME TABLE ...`（单表）、大多数 partition DDL（单表）等。

行为：
1) 该表的 dispatcher 收到 `DDLEvent(T)`。
2) `shouldBlock=false`（单表 + complete）。
3) dispatcher 异步执行 `AddBlockEventToSink(event)`：
   - 若 `NotSync=false`：调用 sink `WriteBlockEvent` 写下游（cloud storage sink 写 schema file）。
   - 若 `NotSync=true`：不会写入 sink，而是直接 `PassBlockEventToSink`（仅更新 tableProgress 并触发 postFlush）。
4) 若 DDL 带 `NeedAddedTables/NeedDroppedTables`（例如 create/drop table）：
   - dispatcher 向 maintainer 发送“非阻塞调度信息”（`IsBlocked=false` 的 block status message），用于后续创建/删除 dispatchers、阻断 checkpoint 转发等调度动作。
5) `event.PostFlush()` 触发后，唤醒事件流继续推进。

要点：
- 此类 DDL **不走 maintainer barrier**，不会发生“write/pass 选择”。
- DDL 写入由该表 dispatcher 自己完成。

### 4.2 多表 DDL（InfluenceTypeNormal，多表）

典型 SQL：`CREATE TABLES`、`RENAME TABLES` 等（对应 `DDLEvent.GetEvents()` 会拆成多个单表事件写入）。

行为：
1) 相关 dispatchers 收到 `DDLEvent(T)`（至少会有 table trigger dispatcher 参与）。
2) `shouldBlock=true`（Normal + 多表）。
3) 每个 involved dispatcher 向 maintainer 上报 block status（`IsBlocked=true`），进入 barrier 协调：
   - maintainer 决定某一个 dispatcher 执行 `Write`，其他 dispatcher 执行 `Pass`。
4) 被选中 `Write` 的 dispatcher 执行 `sink.WriteBlockEvent`：
   - cloud storage sink 内部会按 `GetEvents()` 拆分并写出多个 schema 文件（每表一份）。
5) 其他 dispatchers 走 `PassBlockEventToSink`（仅推进自身 tableProgress，触发 postFlush）。

要点：
- 多表 DDL 的写入者通常是 table trigger dispatcher（取决于 maintainer 的选择策略），但协议层面是“任选一个 write，其余 pass”。

### 4.3 DB/All 级 DDL（InfluenceTypeDB / InfluenceTypeAll）

典型 SQL：`CREATE DATABASE`、`DROP DATABASE`、某些全局级 DDL 等。

行为与“多表 DDL”类似：必定进入 barrier。

另外的特殊点：
- table trigger dispatcher 可能会 **hold DB/All block events**：若当前存在未完成的 schedule-related DDL ACK（`pendingACKCount>0`），它会延迟上报 DB/All block event，直到 pending scheduling tasks 完成，以确保 maintainer 构建 range checker 的任务快照正确。

---

## 5. 开启拆表时的 DDL 端到端行为

本节讨论：同一张逻辑表可能对应多个 dispatcher，因此同表多个 dispatcher（`isCompleteTable=false` 对应的 dispatchers 存在）。

### 5.1 单表 DDL（InfluenceTypeNormal，单表）在拆表下的变化

这是拆表开/关最大的差异点：
- 在未拆表时，单表 DDL 不走 barrier；
- 在拆表时，**单表 DDL 也必须走 barrier**（否则无法保证该表的多个 dispatchers 同步到一致的 DDL 边界）。

行为：
1) 该表的多个 split dispatchers 都会在各自事件流中遇到 `DDLEvent(T)`。
2) `shouldBlock=true`（因为 `!isCompleteTable`）。
3) 每个 involved split dispatcher 向 maintainer 上报 block status，进入 barrier 协调：
   - maintainer 选择一个 dispatcher 执行 `Write`（写 DDL），其余执行 `Pass`。
4) 只有被选中 `Write` 的 dispatcher 负责真正向 sink 写 DDL（cloud storage sink 写 schema file）。

关键收益：
- barrier 把“同表多个 dispatchers”在 DDL 点 `T` 对齐，避免部分 dispatchers 继续推进到 `>T` 而另一部分还停留在 `<T`。

### 5.2 拆表下的“可拆分性检查”边界

当某张表已经被拆分（split dispatcher）且启用了 `enableSplittableCheck` 时：
- dispatcher 在处理 DDL 时会检查 DDL 后表是否仍满足“可拆分”的条件（例如“必须有主键且不能有唯一键”）。
- 若 DDL 使表不再满足可拆分条件，会直接报错并终止（避免在错误的拆分前提下继续同步）。

这属于现有系统的约束，本文的 GA DDL 策略不会改变该约束；但它是“开启拆表”场景下 DDL 行为的一部分，需要在评估全面性时明确。

### 5.3 多表/DB/All DDL

与未拆表一致：必定进入 barrier；拆表只会让 involved dispatchers 数量变多（可能跨节点），但协议不变。

---

## 6. 行为矩阵（用“影响范围 + 是否拆表”覆盖所有 DDL）

下表用最少维度覆盖“所有 DDL 语句”的行为（具体 SQL 归类到对应行即可）。

| DDL 影响范围（BlockedTables） | 其他条件 | 未拆表（complete） | 拆表（split） | 是否 barrier | DDL 写入者 |
|---|---|---|---|---|---|
| `nil` | 无 | 直接处理 | 直接处理 | 否 | 收到该事件的 dispatcher（通常是 table trigger） |
| Normal + 1 table | `isCompleteTable=true` | 直接写/直 pass（不协同） | 不适用（该行条件不成立） | 否 | 该表 dispatcher |
| Normal + 1 table | `isCompleteTable=false` | 不适用（该行条件不成立） | 进入 barrier（同表多 dispatcher 协同） | 是 | maintainer 选 1 个 write，其余 pass |
| Normal + N tables | `N>1` | 进入 barrier | 进入 barrier | 是 | maintainer 选 1 个 write，其余 pass |
| DB / All | - | 进入 barrier（table trigger 可能 hold） | 进入 barrier（table trigger 可能 hold） | 是 | maintainer 选 1 个 write，其余 pass |

> 注：`NotSync=true` 的单表 DDL 在“写入者”路径上会走 “pass（不写 sink）”，但是否进入 barrier 仍取决于上表规则（尤其是拆表下的单表 DDL）。

---

## 7. 与本 GA DDL 策略的对照：是否冲突？

本 GA 的 DDL 策略核心是：
- early-wake 后，DML 在 sink 内可能存在大量“在途数据”（编码队列/spool/未落远端），因此不能再依赖旧的隐含顺序。
- 为保证 DDL schema file 写入远端之前 `<T` 的 DML 都已落远端，本 GA 引入：
  - `sink.PassBlockEvent(ddl)`：**每个 involved dispatcher** 在写入/上报 DDL 之前调用并等待返回；
  - sink 内部用 `DrainMarker` 保证 drain 覆盖 encoding→spool→远端；
  - barrier 场景：所有 involved dispatchers 仅在 `PassBlockEvent` 成功后才进入 barrier；随后由 maintainer 选择一个 dispatcher 执行 `WriteBlockEvent` 写 schema file。

下面逐类对照上面的行为矩阵，检查冲突点：

### 7.1 非 barrier 的单表 DDL（未拆表）

现状：直接 `WriteBlockEvent` 写 schema file（不经 barrier）。

GA 策略：在调用 `WriteBlockEvent` 之前必须 `PassBlockEvent`（即使不需要 barrier）。

是否冲突：**不冲突**。这是 GA 必须补齐的正确性前置条件；只是在“直接写”路径上增加一次同步 drain。

### 7.2 barrier 的单表 DDL（拆表场景）

现状：拆表导致 `shouldBlock=true`，进入 barrier，只有一个 dispatcher 写 DDL，其余 pass。

GA 策略：每个 involved dispatcher 在上报 barrier 之前先 `PassBlockEvent`，保证各自 `<T` 的 DML 在途数据都已落远端；然后 barrier 选一个 write 写 schema file。

是否冲突：**不冲突**。GA 策略正是为了解决“拆表/多 dispatcher 场景下，单点 `WriteBlockEvent` 无法覆盖其他 dispatchers 在途数据”的根因。

### 7.3 barrier 的多表 / DB/All DDL

现状：进入 barrier，maintainer 选一个 write，其余 pass；table trigger dispatcher 在 DB/All 场景可能 hold。

GA 策略：同样要求 involved dispatchers 在进入 barrier 前 `PassBlockEvent`。

是否冲突：**不冲突**。
- table trigger dispatcher 的 “hold” 只改变“何时向 maintainer 上报 block status”，不会改变“各 dispatcher 的事件流已被 DDL 阻塞”的事实；
- `PassBlockEvent` 的 drain 发生在各 dispatcher 自身范围内，完成后再进入 barrier，只会把顺序从“隐含依赖旧 wake”变为“显式 drain 后再协同”。

### 7.4 需要额外记录的假设/可能盲点

1) **`BlockedTables=nil` 的 DDL**
- 这类 DDL 不进入 barrier，且影响范围语义由上游决定。
- 对 cloud storage sink 来说，若该 DDL 不会产出/更新 schema file，则无需 `PassBlockEvent`；若会更新 schema file，则应按“非 barrier DDL”策略处理。
- 需要在实现阶段明确：哪些 DDL types 在 cloud storage sink 会写 schema file，以及其对应的 `BlockedTables` 是否可靠设置。

---

## 8. 建议的实现时机映射（供实施文档引用）

为避免“只在 barrier 场景调用 `PassBlockEvent`”导致遗漏，建议实现阶段按如下规则落地：
- 对所有 `DDLEvent`：
  - 若该 DDL 将会 `WriteBlockEvent`（即会写 schema file）：**必须先 `PassBlockEvent`**。
  - barrier 场景：在 `reportBlockedEventToMaintainer()` 之前调用并等待。
  - 非 barrier 场景：在 `WriteBlockEvent()` 之前调用并等待。

> `NotSync=true` 的单表 DDL 是否需要 `PassBlockEvent`：取决于 cloud storage sink 是否会为它产出 schema file。若不产出，可视为 no-op，避免无谓 drain；本文暂不强制。
