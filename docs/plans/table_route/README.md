# Table Route 文档索引

- 状态：实现进度已更新，待补齐剩余项
- 最后更新：2026-05-21

建议按下面顺序阅读：

1. [PRFAQ / Rollout 文档](./table-route-prfaq.md)
   说明：客户价值、Pilot、成功标准、迁移与 GTM 要求。
2. [需求文档](./table-route-requirements.md)
   说明：配置契约、输出语义、生效矩阵、失败语义和验收标准。
3. [适用场景分析](./table-route-use-cases.md)
   说明：table route 的生产适用场景、市场对照、误用边界和配置建议。
4. [设计文档](./table-route-design.md)
   说明：数据模型、route 计算边界、冲突检测和各 sink 改动点。
5. [实施计划](./table-route-implementation-plan.md)
   说明：执行顺序、分阶段接入、当前完成进度和剩余工作。
6. [Conflict Detection 设计文档](./table-route-conflict-detection-design.md)
   说明：maintainer 侧运行时 `TargetTableRegistry`、DDL transition、failover 重建和失败语义。
7. [Conflict Registry Review Guide](./table-route-conflict-registry-review-guide.md)
   说明：`table-route-conflict-registry` 分支的代码 review 阅读顺序、关键不变量、checklist 和验证命令。
8. [Conflict Detection 讨论纪要](./table-route-conflict-detection-discussion-summary.md)
   说明：记录 target owner、registry owner DDL、table trigger dispatcher、block/barrier 语义和最终实现共识。
9. [Source Identity 与 DDL Routing 方案](./table-route-source-identity-and-ddl-routing.md)
   说明：registry source identity 基于 source schema/table name，DDL routing 保持纯名字处理。

补充说明：

- PRFAQ、需求文档和设计文档是内部评审主材料。
- 实施计划服务执行和拆解，不替代需求或设计。
- 当前分支的完成进度、已合入 PR 和剩余工作以实施计划为准。
- 分支级 PR review 记录不是本功能需求和设计的事实来源。
- 本目录统一使用 `table route` 这一术语。
