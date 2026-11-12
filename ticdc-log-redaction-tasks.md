# TiCDC 日志敏感信息泄漏修复清单

以下待办针对 [PR #6851](https://github.com/pingcap/ticdc/pull/6851) 当前分支的剩余问题。高优先级包括凭据泄漏、常规 INFO 日志中的业务键泄漏，以及完整列值泄漏；中优先级包括 schema 元数据、异常协议事件和受限类型的异常值。

证据标注区分运行观察、字段序列化观察和静态代码推断。凭据在 OFF、ON、MARKER 三种模式下均应隐藏；业务键和值遵循现有脱敏模式。修复应优先删除缺乏诊断价值的字段，保留 ID、类型、状态和计数；实际 CDC 数据、网络请求、错误分类及恢复行为保持正确。

## 高优先级

- [ ] 阻止 Pulsar 第三方日志转发未经筛选的认证错误文本。
  - 触发与风险：Pulsar OAuth 的 `error_description` 经 `WithError` 直接写入日志。认证服务回显凭据时，该路径不受 TiCDC 脱敏模式保护。
  - 代码证据：
    - [Pulsar 将错误直接交给 zap.Error](pkg/sink/pulsar/logger.go#L46)。
    - [生产客户端安装 Pulsar 日志适配器](pkg/sink/pulsar/factory.go#L42)。
  - 证据级别：已运行观察。调用实际适配器并传入 SDK 的 `TokenError`，确认 ON 模式仍输出合成敏感文本；未连接真实 broker 或认证服务。
  - 修复建议：需要保留的日志仅使用明确安全的错误码和连接元数据，并覆盖字段日志及格式化消息，避免未经筛选的服务端文案进入日志。
  - 验收：三种模式下均不输出合成密码、token 或 secret；客户端认证、发送错误处理和指标行为保持正确。

- [ ] 删除或脱敏 Debezium 编码失败日志中的实际列值。
  - 触发与风险：列编码失败后，代码通过 `datum.String()` 或十六进制转换生成 `valueStr`，截断后直接写入 ERROR 日志。截断及十六进制编码均不能隐藏原始业务值。
  - 代码证据：[构造并输出未脱敏的 valueStr](pkg/sink/codec/debezium/codec.go#L136)。
  - 证据级别：静态代码推断。当前代码仍直接输出 `valueStr`，尚未运行该编码失败分支的复现。
  - 修复建议：优先删除 `value` 字段，保留 schema、table、column、commitTs；确需保留时复用现有脱敏工具，并检查附带错误是否重复携带同一值。
  - 验收：用字符串和二进制合成值触发编码失败，ON 日志不能恢复原值；编码输入、输出及返回错误语义保持正确。

## 中优先级

- [ ] 删除非法 dispatcher action 日志中的完整请求对象。
  - 触发与风险：EventService 收到未知 action 时，WARN 日志打印整个 `DispatcherInfo`。实际对象为包装 protobuf 的 `messaging.DispatcherRequest`，其诊断字符串包含原始 `TableSpan.StartKey` 和 `EndKey`。
  - 代码证据：
    - [未知 action 分支展开 info](pkg/eventservice/event_service.go#L180)。
    - [DispatcherRequest 内嵌原始 protobuf](pkg/messaging/message.go#L223)。
    - [TableSpan.String 直接生成 protobuf 文本](heartbeatpb/heartbeat.pb.go#L325)。
  - 证据级别：已观察字段序列化。使用实际请求类型、合成 span 键和未知 action 值，按实际日志表达式记录，ON 模式仍出现明文键；未通过生产消息队列投递该请求。
  - 修复建议：删除 `info` 字段，仅保留 dispatcher ID、action 类型及必要节点标识；检查相邻的非法消息类型日志是否同样展开请求。
  - 验收：含未知 action 的合成请求不能使 ON 日志输出原始键或其可恢复编码，正常消息处理保持正确。

- [ ] 删除 Glue 注册日志中的完整 schema。
  - 触发与风险：Avro schema 包含列默认值及 ENUM/SET 选项。Glue 注册日志在常规 INFO 级别输出完整 schema，绕过值脱敏函数。
  - 代码证据：[Glue 将 schemaDefinition 拼入日志消息](pkg/sink/codec/schemamanager/glue_schema_registry.go#L184)。
  - 证据级别：静态代码推断。尚未运行 Glue 注册路径的复现；master 和两个参考 PR 也未删除该日志。
  - 修复建议：删除包含完整 schema 的日志，保留 subject、schema ID、版本等诊断信息。
  - 验收：Glue 注册路径的 ON 日志不能输出合成默认值及选项；实际注册的 schema 保持完整。

- [ ] 阻止 Avro ENUM/SET 转换错误文本泄漏实际列值和选项。
  - 触发与风险：`ParseEnumValue` 和 `ParseSetValue` 返回的错误文本携带实际数值，SET 错误还包含选项。错误逐层返回后通过 `zap.Error` 写入日志；删除直接记录值的字段仍不能阻止此路径。
  - 代码证据：
    - [ENUM 和 SET 的转换错误直接包装返回](pkg/sink/codec/avro/arvo.go#L763)。
    - [转换失败时记录原始错误](pkg/sink/codec/avro/arvo.go#L171)。
    - [编码器再次记录原始错误](pkg/sink/codec/avro/encoder.go#L71)。
  - 证据级别：已运行观察。实际编码器处理两个合法选项及非法 SET 位值 `4` 时，ON 日志仍通过错误文本输出 `invalid number 4 for Set` 及选项；ENUM 分支为相邻的同类静态代码证据。
  - 修复建议：在日志边界隐藏错误中携带的值，保留列名、MySQL 类型和必要错误类别；检查同一错误的上层日志，避免重复泄漏。
  - 验收：ENUM/SET 非法值路径的 ON 日志不能恢复当前列值；返回错误仍能供现有错误处理逻辑识别。

- [ ] 移除 RawKVEntry 按值记录的整包日志，避免绕过 String 脱敏。
  - 触发与风险：解析失败时，FATAL 日志按值记录 `RawKVEntry`，反射编码输出 Key、Value、OldValue 的可恢复字节表示。`String()` 使用指针接收者，按值记录不会调用其脱敏逻辑。
  - 代码证据：
    - [按值记录完整 entry](logservice/schemastore/ddl_job_fetcher.go#L154)。
    - [RawKVEntry.String 使用指针接收者](pkg/common/kv_entry.go#L135)。
  - 证据级别：已观察字段序列化。实际类型的合成键值通过 `zap.Any("entry", entry)` 按值记录时，ON 模式仍输出可恢复的 base64；尚未运行 DDL job 解析失败及 FATAL 分支，需进一步确认该路径实际处理数据的敏感内容。
  - 修复建议：删除完整 `entry` 字段，保留 regionID、时间戳及必要错误类别，保持原始 KV 数据及序列化内容。
  - 验收：该失败路径的 ON 日志不能输出合成键值或其可恢复编码，错误处理行为保持正确。
