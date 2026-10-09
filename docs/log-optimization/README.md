# TiCDC Log Governance

- Author(s): Codex
- Tracking Issue(s): `pingcap/ticdc#4691`
- Status: Proposal

## 文档导航

- [治理原则与日志契约](principles.md)
- [Dispatcher 场景：海量 dispatcher 生命周期日志](dispatcher.md)
- [Changefeed 场景：海量 changefeed 生命周期日志](changefeed.md)
- [运行时与基础设施场景](runtime.md)
- [现状、热点与参考链接](references.md)

## 背景与范围

本文为 TiCDC 建立一套可以长期执行的日志治理方案，聚焦当前 `ticdc` 仓库，目标不是一次性删噪音，而是建立能持续约束开发、代码评审、排障和观测平台接入的日志契约。

## 治理目标

TiCDC 的日志治理目标是提升默认日志质量和排障效率，减少日志数量与日志洪流，并让日志同时对人、AI agent 和 Loki 一类平台友好。

## 核心决策

- 日志是运维接口，不是控制流注释。
- 默认 `INFO/WARN` 只保留高价值事件，不保留正常路径生命周期旁白。
- 计数、规模、频率和趋势优先进入 metrics；日志负责 metrics 无法承载的诊断事实。
- 海量对象场景默认不能逐对象逐次打印，而应优先使用窗口摘要、代表样本、重复折叠、workflow memo 和 stalled 检测。
- 默认 `INFO/WARN` 禁止大对象输出，只保留必要字段、摘要字段和代表样本。
- TiCDC 负责稳定的 `message + 字段契约`；平台侧再决定如何提取索引、聚合和告警。
- `DEBUG` 不是垃圾桶，仍然要控制频率、对象大小和语义边界。

完整原则、场景设计和材料索引见上面的子文档。
