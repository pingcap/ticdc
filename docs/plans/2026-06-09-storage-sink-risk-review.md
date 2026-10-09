# Storage Sink 风险记录

日期：2026-06-09

## 已修复

1. `FlushConcurrency > 1` 时 multipart writer `Close` 错误不能只记录日志。`Close` 是完成上传的提交边界，失败后不能继续写 index 或执行 `PostFlush`。
2. data file 已写出但 index file 落后或缺失时，恢复不能反复生成已存在的 data file 名。新代码从 index 推导出的候选序号开始探测，遇到已存在的 data file 就继续尝试下一个序号，避免覆盖。

## 待处理风险

1. `msgCh` 是 unbounded queue，spool quota 只覆盖已进入 spool 的数据，慢下游时仍可能在编码前积压。
2. `FlushDMLBeforeBlock` 使用构造时 context 等待 marker，内部 writer 失败时可能不随 errgroup context 立即退出。

## 验证重点

- multipart close 失败不能写 index，不能触发 flush callback。
- stale index 或缺失 index 时，已有 data file 不被覆盖，下一次写入继续使用后续未占用序号。
