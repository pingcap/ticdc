# allow-same-cluster 的启用准则

Last updated: 2026-09-18
Status: 已实现，`pkg/config` 单测与 `same_upstream_downstream` 集成测试用例已覆盖
Scope: `allow-same-cluster = true` 时的准入判定应该遵循什么准则
Related documents:

- 判定实现：`pkg/config/same_cluster.go`
- 判定单测：`pkg/config/same_cluster_test.go`
- 集成测试用例：`tests/integration_tests/same_upstream_downstream/`

## Background

TiCDC 默认拒绝"下游就是上游自己所在集群"的 changefeed（`IsSameUpstreamDownstream`），因为这种 changefeed 会把自己 sink 的写入再同步一遍，形成自复制。`allow-same-cluster` 是跳过该检查的开关，因此这个开关必须只在"可以证明不会自捕获"的配置上放行。

## 判定不变式

设 `S` 为 filter 选中的、会被同步的表集合，`route(t)` 为表 `t` 按路由规则得到的目标名。安全等价于：

```
route(S) ∩ S = ∅
```

两条推论构成了判定的全部内容：

- 没有任何路由规则命中的表，目标名就是它自己，必然落在 `S` 内；
- 路由目标如果也被 filter 选中，就同样落在 `S` 内。

## 准则

**只拒绝真实存在的冲突。** 判定必须能给出一个具体的反例（哪张表会被路由到哪张被同步的表）才拒绝；不允许"证不出来就拒绝"式的近似。这条准则的直接后果是：判定要基于模式做推理，而不是选一个稳妥的充分条件。

**推理基于模式，因此覆盖未来的表。** 判定看的是 filter 规则和路由规则本身，而不是创建 changefeed 那一刻表集合里有什么。所以之后通过 `CREATE TABLE`、`RENAME TABLE` 等出现的新表，只要落在 filter 范围内，就已经被判定覆盖，不需要运行时再补检查。

**必须开启 table route。** 没有 target 的路由规则不会产生新名字，无论表集合怎么变都等价于"未路由"，所以完全没配 table route 时直接拒绝。

**单一判定点，不留兜底。** 判定逻辑本身必须正确，不再叠加第二层"表级校验"或"运行时守卫"来兜。凡是这套逻辑无法推理的输入形态，就按"不支持该配置"直接报错——这是明确的输入契约，不是静默近似，也不是保守拒绝的借口。

**报错指向配置。** 拒绝时给出可操作的证据：要么指出哪条 filter 规则没有任何 matcher 覆盖，要么给出见证例子（`表 A 被路由到表 B，而 B 也在同步范围内`）。用户需要改的是配置，不是数据。

**时机放在配置校验。** 判定挂在 `ReplicaConfig.ValidateAndAdjust` 上，create、update、verify-table 都会走到，CLI 本地预校验即可拦住，不进入运行时数据面。

## 判定的输入契约

判定能够精确处理的形式：

- filter 规则与路由 matcher：`schema.table`，每段可以是字面量、`*`、`前缀*`、`*后缀`，也支持引号名字；
- 路由目标表达式：字面量，或"字面量 + 单个 `{schema}`/`{table}` + 字面量"，例如 `dst`、`{schema}_routed`、`{table}_bak`；留空表示保持源名；
- 未设置 `filter.rules` 按 `*.*` 处理（与运行时的 filter 行为一致）。

不支持、直接报错的形式：

- `!` 否定规则（它的语义取决于 table filter 的规则顺序，判定不做顺序推理）；
- `?`、字符类、`/regexp/`、多段通配（如 `a*b*c`）；
- `{table}` 出现在 target-schema，或 `{schema}` 出现在 target-table。

## 已知的保守情形

这两类配置安全，但会被判定拒绝，原因是它们超出了上面"逐条、可判定"的推理范围：

- 规则被前面的规则完全遮蔽（永远不生效），但它的目标落在 filter 范围内；
- 一条通配 filter 规则只能被多条 matcher 联合覆盖，没有任何单条 matcher 完整覆盖它。

## 验证方式

单测 `pkg/config/same_cluster_test.go` 覆盖：覆盖性不足、目标 schema 由源 schema 派生、目标 schema 落在 filter 内、目标表落在 filter 内、目标保持源名、字面目标表在 filter 外的放行、引号名字、大小写不敏感、以及各类不支持形态的报错。

集成测试 `same_upstream_downstream` 覆盖端到端行为：

- 正例：matcher 与 filter 等宽、目标 schema 在 filter 之外 → changefeed 创建成功，并且写入确实落到目标表；
- 反例一：没有配置 table route → 创建被拒；
- 反例二：目标 schema 仍被 filter 同步 → 创建被拒，错误信息包含见证例子。
