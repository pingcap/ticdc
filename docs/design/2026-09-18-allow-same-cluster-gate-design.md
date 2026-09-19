# allow-same-cluster 的数据库隔离准则

Last updated: 2026-09-19
Scope: 同集群复制的静态准入条件及验证方式
Related documents:

- [校验实现](../../pkg/check/same_cluster.go)
- [单测](../../pkg/check/same_cluster_test.go)
- [真实 filter/router 对照测试](../../pkg/check/same_cluster_runtime_test.go)
- [集成测试](../../tests/integration_tests/same_upstream_downstream/run.sh)

## Background

TiCDC 默认拒绝上下游为同一个集群的 changefeed，避免复制产生的写入再次被捕获。`allow-same-cluster` 允许经过静态校验的跨库复制。

仅要求源表与目标表不相交不足以保护源数据。数据库 DDL 按数据库名过滤和路由，即使目标表没有被 filter 选中，`DROP DATABASE` 仍可能删除其他源表。当前方案要求数据库级隔离，保留数据库 DDL 的正常复制行为。

## 准入条件

令 `D` 为所有正向 filter 规则的 schema 模式表示的集合。这是可能复制的数据库范围，包含未来创建的数据库；校验不依赖当前已有表，也不通过排除系统库来放宽这个集合。

配置必须同时满足：

1. 每条完整的 `schema.table` filter 规则，至少被一条带路由目标的 matcher 完整覆盖，避免未路由表写回自身。
2. 对属于 `D` 的源库，所有匹配其 schema 的路由目标均在 `D` 之外，即 `routeSchema(D) ∩ D = ∅`。检查不受 matcher 的 table 部分限制，因为数据库 DDL 不按表名匹配。
3. 多条路由可能匹配同一个源库时，必须使用完全相同的 `target-schema` 表达式，包括大小写。表路由可以采用不同的目标表名。

数据库级隔离蕴含表级隔离，因此删除表级目标冲突搜索。表模式仅参与路由覆盖性检查。数据库路由需要检查所有 schema 匹配规则，不能采用表路由的 first-match-wins 语义。

## 配置范围的变化

以下配置以前可通过表级隔离判定，现在拒绝：

| 源范围 | 路由 | 拒绝原因 |
| --- | --- | --- |
| `src.t1` | `src.t1 → src.t1_bak` | 目标库仍为源库 |
| `a.t1, b.t2` | `a.t1 → b.t1_copy; b.t2 → c.t2_copy` | 目标库 `b` 也是源库 |
| `*.orders` | `*.orders → {schema}_backup.orders_copy` | 任意目标库均在源库范围内 |

第二个例子中，`DROP DATABASE a` 会被路由成 `DROP DATABASE b`，删除源表 `b.t2`。目标表 `b.t1_copy` 不在 filter 中，无法阻止这个副作用。现在 create/update 在执行复制之前拒绝该配置，错误指出源库和目标库。

`src.* → dst.*` 在 `dst` 不匹配源库范围时仍然允许。`isolation_src*.* → copy_{schema}.{table}` 也允许，未来新建 `isolation_src_future` 的数据库 DDL 会路由到 `copy_isolation_src_future`。

数据库隔离保护 filter 定义的源范围。目标库仍会正常执行复制产生的数据库 DDL，用户需要为目标库安排相应的数据所有权。

## 输入契约与保守限制

支持的 filter/matcher 为 `schema.table`，每段支持字面量、`*`、单个前缀或后缀通配，以及支持范围内的引号名字。仅去掉规则外围空格和制表符，保留引号内空白。未配置 filter 规则按 `*.*` 处理。

目标支持字面量或带一个对应占位符的表达式：`target-schema` 使用 `{schema}`，`target-table` 使用 `{table}`；空目标表示保留原名。虽然目标表不参与隔离推理，仍校验其表达式是否属于支持范围。大小写按 `case-sensitive` 配置处理，先解析占位符再规范化目标字面文本。

否定规则、`?`、字符类、正则和多段通配不在支持范围内，直接报配置错误。以下保守限制也属于明确的准入契约：

- 每条 filter 必须由单条 matcher 完整覆盖，不求多个 matcher 的联合覆盖。
- 表级被遮蔽的规则仍参与 schema 检查，因为它们可能影响数据库 DDL。
- 同一源库上不同的目标表达式，即使恰好产生相同字符串，也不放行。例如 `src → dst` 与 `src → d{schema}` 不相等；`src → dst_src` 与 `src → dst_{schema}` 虽在该源库上相等，也按不同表达式拒绝。

该判定提供支持范围内的充分条件，允许保守拒绝，不承诺接受所有实际安全的配置。

## 实现与性能

`ValidateSameClusterRouting` 在开关生效处统一执行，create、update、resume 使用各自当前配置进行校验。

解析阶段将一条 dispatch rule 的多个 matcher 展开为独立检查项，覆盖性、目标隔离和路由一致性共用这个表示。目标检查按单条路由寻找被捕获的库，一致性检查按两条路由寻找共同源库，避免反复嵌套展开规则。

`findNameWitness` 只搜索 schema 名称：先排除不匹配目标 filter 的固定目标，再生成并验证候选名称。没有 schema/table 的笛卡尔积，也不再生成目标表候选。schema 路由一致性复用同一个搜索函数求源 filter 与两条 matcher 的交集。

[基准测试](../../pkg/check/same_cluster_bench_test.go) 使用 `b.Loop()`，覆盖固定目标、派生目标及多规则。Apple M3、Darwin arm64 环境中，48 字符前缀的固定目标用例在旧实现曾耗时约 18.3 秒、累计分配约 32 GB。数据库级检查删除了该笛卡尔积；基准保留用于后续性能回归验证。

```bash
go test ./pkg/check -run '^$' \
  -bench '^BenchmarkValidateSameClusterRouting$' \
  -benchtime=1x -count=3 -benchmem
```

## 测试与验证边界

单测覆盖同库改表名、跨源库链路、通配源库、表 matcher 不相交但 schema 冲突、目标库表达式不一致、不同目标表共用隔离目标库，以及原有语法、覆盖性、空白和大小写行为。运行时对照测试调用真实 filter/router 验证数据库 DDL 的捕获结果。

集成测试通过配置文件与正常 CLI 操作创建、暂停、更新、恢复和删除 changefeed。危险配置的 create/update 必须失败并返回预期错误；更新被拒绝后，原任务仍应保持暂停状态，恢复后按原配置继续复制。合法配置检查 `normal` 状态、行复制，以及未来数据库的 CREATE/ALTER/DROP DDL。测试还保留其他源库的数据，检查其未受影响。

已执行的验证包括定向 race 单测、现有 TOML 配置的解析和准入结果检查、数据库 DDL 的真实 router 改写检查，以及脚本语法、格式和 diff 检查。集成测试的端到端运行结果尚未验证。
