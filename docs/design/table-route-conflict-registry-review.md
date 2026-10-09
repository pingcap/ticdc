# Table Route Runtime Conflict Detection Review

> 分支: `table-route-conflict-registry`
> 最新状态: 2026-06-01
> 范围: 当前实现的代码审阅结论，不替代设计文档。

## 结论

当前实现已经从早期的 `routeConflictDetector` 方案收敛为 maintainer 内部的 `routeAdmin` admission 状态机：

- 静态检查仍在 API 层完成，阻止 changefeed 创建/更新时已有的 table route conflict。
- 运行时检查由 maintainer 持有 `routeAdmin`，在 barrier 的 precheck/apply 生命周期里维护 target owner registry。
- `barrier` 只保留生命周期挂点，不理解 table route 业务语义；route 语义集中在 `routeAdmin` 和 `TargetTableRegistry`。
- `source schema/table name` 是 route owner identity；`tableID` 只是 dispatcher/schema-store lookup 和调度生命周期信号。
- `TRUNCATE TABLE`、partition DDL 这类只改变 physical table ID 的事件，不应释放或重新注册 target owner。

整体方向是正确的：冲突判断从 SQL 字符串解析下沉到 schema-store table info 和 registry transition，复杂度主要留在 route 组件内部，barrier 行为保持可控。

## 当前代码入口

| 职责 | 位置 | 说明 |
| --- | --- | --- |
| 静态检查 | `api/v2/changefeed.go:verifyRouteConflict` | changefeed 创建/更新时调用 `routing.ValidateNoStaticRouteConflict`。`ForceReplicate=true` 时会把 ineligible tables 也纳入静态检查。 |
| 路由计算 | `downstreamadapter/routing/router.go` | `Router.Route` 计算 source -> target binding。 |
| Registry | `downstreamadapter/routing/registry.go` | `TargetTableRegistry` 维护 `source2Target` 和 `target2Source`，通过 `ApplyTransition` 原子校验和提交变更。 |
| Runtime admission | `maintainer/route_admin.go` | `routeAdmin` 从 barrier event 构造 route transition，做 precheck/apply，并在冲突时上报 changefeed error。 |
| Barrier 挂点 | `maintainer/barrier.go` | 在 `Resend`、blocked WAITING、writer DONE、non-block schedule path 调用 route precheck/apply。 |
| Event 输入 | `maintainer/barrier_event.go` | 将 `heartbeatpb.State` 转成 `routeAdmissionInfo`。 |

## 核心不变量

1. 同一个 changefeed 内，一个 target table 最多只能由一个 source table owner 占用。
2. source owner 由 source schema/table name 决定，不由 table ID、dispatcher ID 或 writer dispatcher 决定。
3. `TargetTableRegistry.ApplyTransition` 必须先完整校验，再修改 `source2Target` / `target2Source`，避免 rename/multi-rename 暴露中间状态。
4. route-related DDL 必须按 DDL commitTs 顺序推进 admission。无关 DDL 可以并行，但一旦进入 route transition queue，就不能越过更早的相关 DDL。
5. `precheck` 可以只验证不提交，`apply` 才能改变 applied registry。
6. failover/replay 路径必须能在没有本进程 pending transition 的情况下，从 `routeAdmissionInfo` 重建 transition。
7. route-neutral DDL 可能仍然通过 `BlockTables` 到达 route admin，但必须快速变成 no-op，避免扩大 barrier 成本。
8. partition physical table ID 的 add/drop/replay 不能改变 source owner，也不能因为旧 physical table ID 已被释放而报内部错误。

## 当前 DDL 映射

| DDL 类别 | RouteAdmin 行为 | 说明 |
| --- | --- | --- |
| `CREATE TABLE` / `CREATE TABLES` / `RECOVER TABLE` | Admit new source owner | non-block schedule path 会在创建 dispatcher 前执行 admission；冲突时 fail changefeed。 |
| `DROP TABLE` | Release old source owner | 只释放已 admission 的 source；重复 drop / 未知 table ID 是 no-op。 |
| `DROP DATABASE` | Release schema 下已 admission sources | 按 `sourceSchemaID` 找到当前 registry 中属于该 schema 的 source。 |
| `RENAME TABLE` / `RENAME TABLES` | Replace old source owner with new source owner | 用一个 atomic transition 表示 remove + add，覆盖 multi-rename 的 all-or-nothing 语义。 |
| `TRUNCATE TABLE` | source owner 不变 | table ID 一定变化，但 source schema/table name 不变；registry owner 不应释放或重新注册。 |
| partition DDL | source owner 不变 | add/drop/truncate partition 只改变 physical table ID，不改变 table route owner。 |
| column/index/schema-only DDL | route-neutral no-op | 仍可能因 `BlockTables` 进入 route admin；首次比较后进入 `routeNeutralEventCache`，后续 resend/apply 低成本返回。 |

## 关键实现评估

### `TargetTableRegistry`

`TargetTableRegistry` 当前职责清晰：

- `Add` 支持同一 source/target 重复注册的幂等性。
- 不同 source 指向同一 target 时返回 `ErrTableRouteConflict`。
- 同一 source 重定向到不同 target 时返回 internal check failed，要求调用方通过 transition 明确 remove old owner。
- `ApplyTransition(removes, adds, mutate)` 支持 dry-run validation，也支持提交；所有校验通过后才修改两个索引。

这个设计适合 maintainer admission：precheck 用 `mutate=false`，apply 用 `mutate=true`。

### `routeAdmin`

当前 `routeAdmin` 维护两层状态：

- `registry`: source owner 到 target owner 的权威状态。
- `tableSources map[int64]admission`: table ID 到当前 admission 的辅助索引，只用于根据 DDL status 里的 table ID 构造 transition。

`admission.equal` 用于识别重复上报的同一 table ID/source/target。这个幂等判断对 partition DDL 和 failover replay 很关键：旧物理表 ID 可能已经释放，新的物理表 ID 可能已经 admission；重复到达的 status 不应破坏 owner state。

`buildTransition` 当前行为符合设计：

- dropped tables 只 remove 已存在于 `tableSources` 的 table ID。
- added tables 如果 table ID 已存在且 admission 完全相同，直接跳过。
- added tables 如果 table ID 已存在但 admission 改变，先 remove 再 add。
- block tables 中未知 table ID 直接跳过，避免 replay 中旧 physical partition ID 触发 internal error。
- block tables 中已知 table ID 会重新从 schema store 取 table info；source/target 没变则 transition 为空，进入 route-neutral cache。

### Barrier 挂点

Barrier 中 route 相关逻辑保持在四个生命周期点：

1. `Resend`: 对 recovered selected events 先按 event key 排序，再 route precheck/apply，避免 map 遍历顺序影响 admission 顺序。
2. `handleBlockState` blocked WAITING: table trigger dispatcher 报告时先 precheck；全部 dispatcher reported 后再次保证 ready。
3. `handleEventDone`: writer DONE 后 apply route transition，再推进 schedule/pass。
4. non-block schedule path: 对 create/recover 这类写完 DDL 后上报的事件，在创建新 dispatcher 前 precheck/apply。

保留 blocked event 的行为是有意的：precheck 失败或等待更早 DDL 时，一些 dispatcher 可能已经收到 WAITING 阶段 ACK。删除 blocked event 会丢失已覆盖状态；保留 event 可以让未 ACK dispatcher 继续 resend，同时 route admin 负责上报 conflict 或等待更早 transition。

## 测试现状

### 单元测试

`maintainer/route_admin_test.go` 覆盖了关键状态机行为：

- conflict precheck 会上报错误且不污染 `tableSources`。
- apply 会更新 registry 和 `tableSources`。
- recovered apply 可以在没有 pending transition 时重建 transition。
- 新表 admission 不依赖 dispatcher table registration。
- DDL span table ID 会被忽略。
- route-neutral block events 会缓存并快速 no-op。
- drop 会释放 bootstrap binding。
- source admission 不等于 table ID，覆盖 truncate/partition physical table ID replay。

`maintainer/barrier_test.go` 覆盖了 route precheck/apply 的 barrier 生命周期挂点，包括 selected/recovered event 和 blocked event precheck 不 ready/报错时保留 event 的行为。

### 集成测试

`tests/integration_tests/table_route_conflict_detection/run.sh` 当前只保留 3 个 light CI 高价值场景：

| 用例 | 覆盖点 |
| --- | --- |
| `run_static_conflict_case` | changefeed 创建时已有静态冲突。 |
| `run_create_table_conflict_case` | 运行时 create table 引入新 source owner 并冲突。 |
| `run_multi_rename_table_conflict_case` | multi rename 作为 atomic replace transition 引入冲突。 |

删除的 drop/drop database/truncate/out-of-filter release 端到端用例，当前由 RouteAdmin 单元测试覆盖核心 admission 语义。这个取舍是合理的：light integration 保留跨进程关键路径，细粒度 owner state 行为放在单元测试中，避免 CI 体积膨胀。

## 已修正的旧问题

1. `apply` 依赖本进程 pending transition 的问题已修正：没有 pending 时会从 `routeAdmissionInfo` 重建 transition。
2. `Resend` 的 map 顺序问题已通过 event key 排序处理，避免 recovered selected events 乱序推进 route admission。
3. writer DONE 路径 apply 前补了 route precheck，覆盖 recovered WRITING 后收到 DONE 的情况。
4. route-neutral DDL 已有 cache，降低 column/index/schema-only DDL 通过 `BlockTables` 进入 route admin 的影响。
5. partition physical table ID replay 报 internal error 的问题已通过 idempotent admission 和未知 block table ID no-op 修正。
6. light integration 测试已从 8 个场景收敛为 3 个高价值场景。

## 仍需关注

### 1. `ForceReplicate` 与 runtime 初始化范围

静态检查在 `ForceReplicate=true` 时会把 ineligible tables 纳入冲突检查；runtime `newRouteAdmin` 当前只接收 maintainer bootstrap 得到的 `tables`。

如果后续有“ineligible table 变 eligible”的路径，这里需要确认 bootstrap/runtime table set 是否能保持与静态检查一致。当前 review 不能从 `routeAdmin` 自身证明这点，需要结合 maintainer bootstrap table source 和 schema store 语义继续确认。

建议：

- 如果 runtime table set 只包含 eligible replicated tables，则补充设计说明，明确 ineligible owner 不属于 runtime admission。
- 如果 `ForceReplicate=true` 语义要求 ineligible 也参与 runtime owner tracking，则需要把同一 table set 传入 `newRouteAdmin`，并补单元测试。

### 2. `UpdatedSchemas` 对未知 table ID 仍然 fail-fast

`buildTransition` 对 `updatedSchema` 中不存在于 `tableSources` 的 table ID 仍返回 internal check failed。这比 dropped/block tables 更严格。

这个选择可以接受，因为 `UpdatedSchemas` 表示已有 table ID 的 schema owner 变化；如果 route admin 没有该 table，说明 upstream status 与 admission state 不一致。建议在后续 review 中继续确认 exchange partition / filtered rename 等边界是否会产生合法的 unknown updated schema。

### 3. Barrier 注释仍有历史包袱

新增 route 相关注释已经比较清楚，但 barrier 文件中仍有一些历史注释存在拼写、语法和抽象层次问题。这不是当前 PR 的 correctness blocker，不建议在本 PR 继续扩大重写；可以单独做 barrier 注释清理。

## Review 建议

当前 PR 可以继续沿着现有最小实现推进，不建议再引入新的 pre-write protocol 或额外 RouteAdmin/Barrier 抽象。重点应放在：

1. 保持 route 语义集中在 `routeAdmin` / `TargetTableRegistry`，不要让 barrier 理解具体 DDL 类型。
2. 保持 table route owner 以 source name 为准，不让 physical table ID 泄漏成冲突判断依据。
3. 对所有恢复、重复上报、resend 路径保持幂等。
4. 用单元测试覆盖细粒度 DDL admission 语义，用少量 integration 测试覆盖跨进程关键路径。

## 建议验证命令

```bash
GOCACHE=/private/tmp/ticdc-gocache go test --tags=intest ./maintainer -run 'TestRouteAdmin|TestBarrier'
bash -n tests/integration_tests/table_route_conflict_detection/run.sh tests/integration_tests/table_route/run.sh
base=$(git merge-base HEAD upstream/master)
GOCACHE=/private/tmp/ticdc-gocache GOLANGCI_LINT_CACHE=/private/tmp/ticdc-golangci-lint-cache tools/bin/golangci-lint run --timeout 10m0s --new-from-rev=$base ./maintainer
```
