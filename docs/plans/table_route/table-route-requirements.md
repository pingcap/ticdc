# Table Route 需求文档

- 状态：待内部评审
- 最后更新：2026-04-16
- 读者：产品、架构、开发、测试 reviewer、文档与支持团队
- 关联文档：`docs/plans/table_route/table-route-prfaq.md`、`docs/plans/table_route/table-route-design.md`
- 说明：本文只定义用户可见的配置契约、行为语义、生效范围、失败语义和验收标准，不展开代码结构和实现细节。

## 1. 功能定义

`table route` 允许用户在 `sink.dispatchers` 中声明下游目标 schema / table 名。

它只解决一件事：把单张上游表稳定地改写为单张下游目标表名。它不负责：

- 改变 `matcher`、`topic`、`partition`、`columns` 等现有 dispatch 字段的计算规则
- 多张上游表合并到一张下游表
- 通用的数据内容变换

当前需求评审和实现优先围绕两家明确目标客户展开：

- A
- W

## 2. 配置示例

### 2.1 仅做 table route

```toml
[sink]
dispatchers = [
  { matcher = ["sales.orders"], target-schema = "archive", target-table = "{schema}_{table}" },
]
```

效果：

- `sales.orders` 输出为 `archive.sales_orders`

### 2.2 同一条 rule 同时承担 dispatch 和 table route

```toml
[[sink.dispatchers]]
matcher = ["sales.orders"]
topic = "order_topic"
target-schema = "archive"
target-table = "{schema}_{table}"
```

效果：

- `topic` 仍为 `order_topic`
- 下游名字变为 `archive.sales_orders`

### 2.3 `matcher` 使用通配符

`matcher` 继续沿用现有 dispatcher matcher 语义，匹配对象始终是上游 `schema.table`，支持 `*` 通配符，也支持用 `!` 写排除规则。

示例一：匹配 `sales` schema 下的所有表

```toml
[[sink.dispatchers]]
matcher = ["sales.*"]
target-schema = "archive"
target-table = "{table}_bak"
```

效果：

- `sales.orders` 输出为 `archive.orders_bak`
- `sales.order_items` 输出为 `archive.order_items_bak`

示例二：匹配任意 schema 下名为 `orders` 的表

```toml
[[sink.dispatchers]]
matcher = ["*.orders"]
target-schema = "archive"
target-table = "{schema}_{table}"
```

效果：

- `sales.orders` 输出为 `archive.sales_orders`
- `hr.orders` 输出为 `archive.hr_orders`

示例三：匹配 `sales` schema 下以 `order_` 开头的表

```toml
[[sink.dispatchers]]
matcher = ["sales.order_*"]
target-schema = "sales_archive"
```

效果：

- `sales.order_log` 输出为 `sales_archive.order_log`
- `sales.order_history` 输出为 `sales_archive.order_history`

示例四：先匹配 `sales` schema 下的所有表，再排除临时表

```toml
[[sink.dispatchers]]
matcher = ["sales.*", "!sales.tmp_*"]
target-schema = "archive"
target-table = "{table}"
```

效果：

- `sales.orders` 输出为 `archive.orders`
- `sales.tmp_orders` 不命中这条 rule

示例五：匹配所有上游表，作为兜底 rule

```toml
[[sink.dispatchers]]
matcher = ["*.*"]
target-schema = "{schema}_mirror"
```

效果：

- `sales.orders` 输出为 `sales_mirror.orders`
- `hr.employees` 输出为 `hr_mirror.employees`

说明：

- `matcher` 列表中的 pattern 按现有 table-filter 语义组合使用，可以同时写正向匹配和 `!` 排除匹配
- `*.*` 这类兜底 wildcard rule 应放在更具体的 rule 之后，因为多条 route rule 同时命中时按配置顺序取首条生效

### 2.4 非法示例

```toml
[[sink.dispatchers]]
matcher = ["sales.orders"]
target-table = "{db}_{table}"
```

效果：

- 创建或更新 changefeed 失败，因为 `{db}` 不是合法占位符

## 3. 配置契约

`table route` 复用 `sink.dispatchers` 作为配置入口。用户可以写成：

- `[sink] dispatchers = [...]`
- `[[sink.dispatchers]]`

两种写法都合法，都会落到同一组 dispatcher rule。

新增字段只有两个：

- `target-schema`：目标 schema 名
- `target-table`：目标 table 名

字段语义：

- `matcher` 继续匹配上游 schema / table，沿用现有 dispatcher matcher 语义，支持 `sales.*`、`*.orders`、`sales.order_*`、`*.*` 这类通配写法，也支持 `!sales.tmp_*` 这类排除写法
- `target-schema` 和 `target-table` 基于上游名展开，不反向影响 `matcher`
- 只有配置了 `target-schema` 或 `target-table` 的 rule 才参与 table route
- 多条 route rule 同时命中时，按 `dispatchers` 中的配置顺序取首条生效

支持的占位符只有：

- `{schema}`
- `{table}`

以上游 `sales.orders` 为例：

- `target-schema = "{schema}_bak"` -> `sales_bak`
- `target-table = "{table}_bak"` -> `orders_bak`
- `target-table = "{schema}_{table}"` -> `sales_orders`

默认行为：

- 未配置 `target-schema`：目标 schema 与上游 schema 相同
- 未配置 `target-table`：目标 table 与上游 table 相同
- 未命中任何 route rule：下游继续使用上游名

## 4. 输出语义

### 4.1 Route 与 Dispatch 的边界

因为 `table route` 复用的是 `sink.dispatchers`，文档必须明确边界：

- `matcher` 继续按上游名匹配
- `topic`、`partition`、`columns` 等已有 dispatch 结果继续按上游名计算
- `table route` 只决定 sink 最终输出成什么 schema / table 名

### 4.2 一致性的定义

对已经进入生效矩阵的 sink，“一致”指的是：

- 同一个 event 在该 sink 内所有对外暴露 schema / table 名的位置，都必须使用同一组 route 结果
- DDL 的结构化字段和 SQL 文本必须指向同一组目标名
- rename DDL 中，旧名字相关位置使用 routed old name，新名字相关位置使用 routed new name

### 4.3 DML

- 命中 table route 时，sink 对外看到的是目标 schema / table 名
- 未命中 table route 时，sink 对外看到的是上游 schema / table 名

### 4.4 DDL

- 命中 table route 时，DDL 的结构化名称字段和 SQL 文本都必须体现目标名
- 未命中 table route 时，DDL 继续使用上游名
- V1 的 DDL route 范围只包含 TiDB parser 能解析并能通过 AST 恢复输出的 DDL
- `ActionAddFullTextIndex` 和 `ActionCreateHybridIndex` 是 TiCDC 基于原始 SQL 识别出的本地 action，暂不纳入 V1 table route DDL 改写范围
- 未配置 table route 时，这两个 DDL 的既有同步行为不因本功能改变

### 4.5 Rename DDL

输入：

```sql
RENAME TABLE sales.temp_table TO sales.renamed_table
```

若 route 规则为：

- `target-schema = "archive"`
- `target-table = "{table}_routed"`

则期望输出为：

- 旧名字：`archive.temp_table_routed`
- 新名字：`archive.renamed_table_routed`
- SQL：`RENAME TABLE archive.temp_table_routed TO archive.renamed_table_routed`

## 5. 校验与失败语义

### 5.1 非法配置

以下情况必须在 create / update changefeed 阶段直接失败：

- 未知占位符
- 非法表达式
- 展开后为空字符串

### 5.2 Route Conflict

`route conflict` 的定义是：两张不同的上游表，经 table route 计算后得到同一个 `(targetSchema, targetTable)`。

处理规则：

- 基于当前对象集合就能判断出的冲突，必须在 create / update changefeed 阶段报错
- 只有依赖运行中对象集合变化才能暴露的冲突，才允许在运行时识别
- 一旦识别出运行时 conflict，changefeed 必须进入失败状态
- 系统不得静默把多张上游表的数据或 DDL 合并到同一个下游目标表

## 6. 生效矩阵

配置契约从一开始对所有 sink 统一暴露；是否真正影响输出，由生效矩阵决定。

| Sink 类型 | 阶段 | 输出要求 |
| --- | --- | --- |
| MySQL / TiDB sink | V1 | DML 写入目标表；DDL 作用于目标对象；下游数据库看到目标 schema / table 名 |
| Redo | V1.1 | redo 持久化和回放都体现目标名 |
| Kafka / Pulsar sink | V2 | payload 中的 schema / table 字段体现目标名；topic / partition 保持基于上游名 |
| Storage sink | V3 | 路径、schema 文件、表定义等与 schema / table 相关的命名体现目标名 |

未生效 sink 的统一契约：

- 配置可以被接收和校验
- 当前版本输出语义保持不变
- 发布说明和用户文档必须明确哪些 sink 已生效、哪些 sink 仅接受配置但尚未生效

## 7. 升级与回退

- 未配置 `target-schema` / `target-table` 的现有 changefeed，升级后行为保持不变
- 对当前版本尚未生效的 sink，用户可以先保存配置；该 sink 后续进入生效矩阵时，升级会改变这部分 changefeed 的输出语义
- 升级到“某 sink 新进入生效矩阵”的版本前，操作者至少要检查：
  1. 当前上游对象集合下的 route 结果和 route conflict
  2. 下游目标对象、权限和命名规范
  3. 依赖 schema / table 字段、路径或 redo 目标的下游消费方
  4. staging 或低风险 pilot changefeed 验证结果
- 回退二进制版本或移除配置，不会自动撤销已经写出的目标名结果；已经落到下游数据库、消息、文件路径或 redo 记录中的目标名，需要按 sink 类型单独清理或迁移

## 8. 非功能要求

- 一致性：对已生效 sink，同一个 event 的所有 schema / table 输出位点必须使用同一组 route 结果
- 兼容性：未配置 table route 的现有行为保持不变；后续阶段扩展更多 sink 时，不得改变既有配置语义
- 可诊断性：配置校验失败应尽量指出具体 rule、字段和非法占位符；route conflict 应尽量指出冲突的上游对象和目标名
- 性能：持续 DML 负载场景下，开启 table route 不应引入明显性能回退

## 9. V1 验收标准

当且仅当以下条件全部满足，V1 才视为满足需求：

1. 用户可以通过 `sink.dispatchers` 中的 `target-schema` / `target-table` 配置下游目标名，`[sink] dispatchers = [...]` 和 `[[sink.dispatchers]]` 两种写法都可用。
2. `matcher`、`topic`、`partition`、`columns` 等现有 dispatch 语义保持不变。
3. MySQL / TiDB sink 上，DML 和 DDL 的对外输出都正确体现目标名。
4. rename DDL 中，旧名字相关位置和新名字相关位置都正确体现各自的 route 结果。
5. 非法占位符、非法表达式、空结果和静态可判定的 route conflict 会在 create / update changefeed 阶段返回明确错误。
6. 未生效 sink 可以接受并保留配置，但当前阶段不会改变既有输出语义。
7. 未配置 table route 的现有用户配置不受影响。
8. 开启 table route 后，持续 DML 负载场景下性能无明显回退。
9. 发布说明和用户文档明确 V1 支持范围，以及后续 sink 生效时的升级影响与回退限制。
