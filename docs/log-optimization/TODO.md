# Log Optimization TODO

## 当前判断

- 飞书根页面 `TiCDC Log Governance` 已存在，适合继续作为总览/入口页使用。
- 本地 `docs/log-optimization` 已经是“主文档 + 子文档”的结构，不应整体粗暴覆盖到单个飞书长文档里。
- 当前 [README.md](/Users/edison/go/ticdc/docs/log-optimization/README.md) 仍然偏长，和 `principles.md`、各场景文档有重复，暂不适合原样同步到飞书根页。

## 待办

- 收缩 [README.md](/Users/edison/go/ticdc/docs/log-optimization/README.md)，只保留“评审入口 / 文档导航 / 需要拍板的少量结论”。
- 从 README 中删除已经在子文档展开的冗余内容，尤其是过细的原则复述、场景细节和重复路线图。
- 明确 README 与子文档的边界：
  - README 负责总览、优先级、导航和少量关键决策。
  - `principles.md` 负责治理原则与日志契约。
  - `dispatcher.md`、`changefeed.md`、`runtime.md`、`references.md` 负责场景与材料。
- 备份当前飞书根页面内容，避免后续同步时误覆盖已有内容。
- 在飞书知识库根页面 `TiCDC Log Governance` 下创建子页面：
  - `principles.md`
  - `dispatcher.md`
  - `changefeed.md`
  - `runtime.md`
  - `references.md`
- 记录每个新建飞书子页面的 URL / token，作为后续导航和增量同步入口。
- 回填飞书根页面的“文档导航”，把子页面链接补齐。
- 在 README 收缩完成后，再决定飞书根页面是否做局部替换同步；默认不要直接 overwrite。

## 执行顺序

1. 先瘦身 README。
2. 再创建飞书子页面并补导航。
3. 最后评估是否需要同步 README 到飞书根页。
