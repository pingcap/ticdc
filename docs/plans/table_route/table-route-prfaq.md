# Table Route PRFAQ 与 Rollout 文档

- 状态：待内部评审
- 最后更新：2026-04-13
- 读者：产品、研发、架构、SA、Support、文档团队
- 关联文档：`docs/plans/table_route/table-route-requirements.md`、`docs/plans/table_route/table-route-design.md`
- 说明：本文补充需求文档和设计文档之外的客户价值、Pilot、成功标准、迁移与 GTM 要求。

## 1. Press Release 草案

TiCDC 推出 `table route`，允许用户在不改变现有 dispatch 结果的前提下，统一指定下游 schema / table 名。

这一能力面向需要做归档、迁移、命名规范统一的一对一路由场景。用户继续使用 `sink.dispatchers` 作为配置入口，通过 `target-schema` 和 `target-table` 声明目标名；对已经进入生效矩阵的 sink，DML 和 DDL 会一致地输出这组目标名。

`table route` 解决的是“下游最终输出成什么名字”，而不是新增一套数据内容变换能力。相比在下游数据库、消息消费侧或存储路径处理脚本中分别改名，TiCDC 提供的是统一配置、统一语义和分阶段启用的交付路径。

## 2. FAQ

### 2.1 这个功能为谁而做

- 当前已明确的目标客户是 A 和 W 两家公司
- 需要把单张上游表一对一映射到不同下游名字的 TiCDC 用户
- 需要保持现有 dispatch 规则不变，但要求下游名字统一改写的用户
- 可以先在 MySQL / TiDB sink 验证，再逐步扩展到 redo、MQ、storage 的用户

### 2.2 这个功能不为谁而做

- 需要多表合并、拆分或通用内容变换的用户
- 需要每种 sink 各自定义完全不同命名语义的用户
- 下游消费方强依赖原始 schema / table 名，且短期内无法迁移的用户

### 2.3 客户今天如何解决

- 在下游数据库手工创建另一套目标对象，并在应用侧适配
- 在 MQ 或 storage 消费侧再做一次名字转换
- 由不同 sink 的局部实现分别处理 DML、DDL、payload、path 和回放

这些做法的问题是：配置入口不统一，语义容易分叉，迁移和运维成本高。

### 2.4 我们的差异化优势

- 统一配置入口：继续复用 `sink.dispatchers`
- 统一语义：已生效 sink 对外看到同一组目标名
- 分阶段交付：配置契约先统一暴露，sink 按阶段生效
- 失败语义明确：非法配置、静态冲突、运行时冲突都有确定行为

### 2.5 为什么现在做

- 归档、迁移、命名规范统一这类场景已经有明确需求
- 当前改名逻辑分散在多个下游环节，长期维护成本高
- 如果继续等到每种 sink 各自补齐后再统一契约，后续收敛成本会更高

### 2.6 使用前提与限制

前提：

- 下游目标对象、权限和命名规范已准备好
- 相关消费方可以接受目标名语义
- 操作者可以在 staging 或 pilot changefeed 上先验证

限制：

- 只支持单表到单表的一对一路由
- 不提供通用数据内容变换
- 生效范围由 sink 生效矩阵决定，不是所有 sink 同步生效

### 2.7 Adoption Cost

- 需要审计 route 结果是否会与现有对象冲突
- 需要确认下游对象创建、权限和运维脚本是否适配目标名
- MQ、storage、redo 相关消费方可能要同步调整字段解析、路径处理或回放目标
- 某 sink 从“接受配置但不生效”进入“真正生效”时，升级会改变输出语义

### 2.8 迁移与回退要求

- 未配置 `target-schema` / `target-table` 的现有 changefeed，升级后行为保持不变
- 对于后续版本才生效的 sink，已有配置在新版本中会真正改变输出语义
- 升级前必须完成 route 结果、冲突、目标对象和消费方适配检查
- 回退二进制版本或删除配置，不会自动撤销已经写出的目标名结果；需要按 sink 类型协调清理或迁移

### 2.9 Pilot 要求

进入正式开发和上线评审前，至少满足：

- 当前优先对接的目标客户 / pilot customer 是 A 和 W 两家公司
- 绑定至少 1 个具名 pilot customer
- pilot 场景是真实的一对一 schema / table rename / archive 需求
- pilot 可以提供 staging 验证环境和生产落地窗口
- pilot 接受验证 DML / DDL 语义、冲突处理和回退限制

当前已明确的具名目标客户是 A 和 W；owner、时间窗口和各自对应场景需要在评审前补齐。

### 2.10 成功标准

开发前要先明确 launch gate：

- A 或 W 至少一家具备可执行的 staging 验证计划
- 至少 1 个具名 pilot customer
- 至少 1 次 staging 验证通过
- 文档、FAQ、Demo、Support 说明 ready

判断 V1 是否值得继续扩展到更多 sink，至少看：

- 至少 1 个生产 changefeed 成功启用 `table route`
- pilot 给出正向反馈，并继续使用
- 没有 P1 级别的语义错误、冲突处理错误或无法回退的问题
- 明确存在 redo / MQ / storage 的后续客户需求

### 2.11 GTM / Support Ready 清单

- 用户文档：配置方法、限制、生效矩阵、升级与回退
- FAQ：适用与不适用场景、常见错误、冲突处理
- Demo：MySQL / TiDB sink 的 DML / DDL rename 场景
- Support 说明：如何定位非法占位符、route conflict、sink 未生效问题
- SA / Sales 说明：这个能力解决什么问题、不解决什么问题、当前支持哪些 sink

### 2.12 Continue / Exit 机制

继续投入：

- pilot 在真实生产场景中验证了明确价值
- V1 输出语义稳定，且有下一个 sink 的明确客户需求

收缩范围：

- MySQL / TiDB sink 有价值，但其他 sink 暂无清晰需求或 adoption cost 过高

停止扩面：

- 无法绑定具名 pilot customer
- pilot 长期不进入生产
- adoption cost 明显高于客户获得的价值

## 3. Rollout 原则

- 先以 MySQL / TiDB sink 建立统一配置契约和语义
- 后续 sink 逐个进入生效矩阵，不做“一次性全开”
- 每个新增生效 sink 都需要单独做 pilot、FAQ、升级说明和回退说明
- 发布节奏按真实客户需求和 adoption 结果决定，不按技术实现完成度单独推进
