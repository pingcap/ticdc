# Table Route PR Review

基线：

- `upstream/master`: `8a067c41d`
- 当前分支 `HEAD`: `4c57fc9c8`
- 评审目标：确认当前分支是否已经按 source-preserving 模型闭合 table route 语义，并且没有混入无关改动

结论摘要：

- [x] 当前主线语义已经收敛为：`Schema/Table` 保持 source name，sink 输出位点显式读取 `TargetSchema/TargetTable`
- [x] 当前没有再看到新的阻塞性 correctness 问题
- [x] `eventrouter` 的 table route 回归测试已经按 source-preserving 模型更新，targeted run 已绿
- [x] codec / cloudstorage 新增测试已经回到各自 package 的职责边界，不再跨 package 直接测试 `routing`
- [ ] 仍建议按下面顺序 review；前 1 到 6 项是 correctness 主路径
- [ ] 集成执行结果仍需在完整外部环境确认，不能只靠本地 targeted tests 替代

## 按阅读优先级排列的 Review 清单

### [ ] 1. 配置契约与配置裁剪

入口：

- `/Users/edison/go/ticdc/pkg/config/sink.go:413`
- `/Users/edison/go/ticdc/pkg/config/sink.go:421`
- `/Users/edison/go/ticdc/pkg/config/sink.go:829`
- `/Users/edison/go/ticdc/pkg/config/sink.go:885`
- `/Users/edison/go/ticdc/pkg/config/sink.go:1153`
- `/Users/edison/go/ticdc/pkg/config/changefeed.go:498`
- `/Users/edison/go/ticdc/pkg/config/sink_test.go:298`
- `/Users/edison/go/ticdc/pkg/config/sink_test.go:357`

确认点：

- `target-schema` / `target-table` 是否已经是唯一清晰的 table route 外部配置
- `validateTableRoute()` 是否只做 table route 自身的表达式校验
- `rmMQOnlyFields()` 是否只清理 MQ 专属字段，而不会把 table route 规则整体误删

### [ ] 2. Source-preserving 的名字模型

入口：

- `/Users/edison/go/ticdc/pkg/common/table_name.go:23`
- `/Users/edison/go/ticdc/pkg/common/table_name.go:36`
- `/Users/edison/go/ticdc/pkg/common/table_name.go:41`
- `/Users/edison/go/ticdc/pkg/common/table_name.go:46`
- `/Users/edison/go/ticdc/pkg/common/table_name.go:84`
- `/Users/edison/go/ticdc/pkg/common/table_name.go:89`
- `/Users/edison/go/ticdc/pkg/common/table_info.go:158`
- `/Users/edison/go/ticdc/pkg/common/table_info.go:396`
- `/Users/edison/go/ticdc/pkg/common/table_info.go:426`
- `/Users/edison/go/ticdc/pkg/common/table_info.go:437`
- `/Users/edison/go/ticdc/pkg/common/table_info_test.go:27`

确认点：

- `Schema/Table`、`String()`、`QuoteString()` 是否都稳定保持 source 语义
- `TargetSchema/TargetTable` 是否只承担 routed target name 的职责
- `CloneWithRouting()` 是否只设置 target，而不会污染 source 字段

### [ ] 3. DDL 事件的 source + target 双轨模型

入口：

- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:36`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:202`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:230`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:244`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:291`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:313`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:374`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event.go:567`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event_test.go:621`
- `/Users/edison/go/ticdc/pkg/common/event/ddl_event_test.go:649`
- `/Users/edison/go/ticdc/pkg/common/event/redo_test.go:58`

确认点：

- `SchemaName/TableName/ExtraSchemaName/ExtraTableName` 是否继续表示 source name
- `TargetSchemaName/TargetTableName/TargetExtra*` 是否只承载 routed target name
- `GetEvents()` 拆分 `RENAME TABLES` 时，是否同时保留 source old/new 与 target old/new
- `GetDDLSchemaName()` 是否明确服务 sink 执行路径，而不是改变 source 字段语义

### [ ] 4. Router 核心与应用边界

入口：

- `/Users/edison/go/ticdc/downstreamadapter/routing/router.go:103`
- `/Users/edison/go/ticdc/downstreamadapter/routing/router.go:121`
- `/Users/edison/go/ticdc/downstreamadapter/routing/router.go:139`
- `/Users/edison/go/ticdc/downstreamadapter/routing/router.go:186`
- `/Users/edison/go/ticdc/downstreamadapter/routing/router_apply_test.go:25`
- `/Users/edison/go/ticdc/downstreamadapter/routing/router_apply_test.go:73`

确认点：

- `ApplyToTableInfo()` 是否仍然只在表级边界使用，而不是热路径每条 DML 都 clone
- `ApplyToDDLEvent()` 是否统一负责 query rewrite、target 字段填充、`BlockedTableNames` 改写
- `applyToMultipleTableInfos()` 是否继续保持 copy-on-write，而不是无差别分配

### [ ] 5. Dispatch 仍然只看 source identity

入口：

- `/Users/edison/go/ticdc/downstreamadapter/sink/eventrouter/event_router.go:88`
- `/Users/edison/go/ticdc/downstreamadapter/sink/eventrouter/event_router_test.go:161`
- `/Users/edison/go/ticdc/downstreamadapter/sink/eventrouter/event_router_test.go:253`
- `/Users/edison/go/ticdc/downstreamadapter/sink/eventrouter/event_router_test.go:321`

确认点：

- `matcher` / `topic` / `partition` 是否仍然只按 source schema/table 生效
- DDL topic 选择在 routed DDL 上是否依然只读 source old/new name
- table route 是否只改变 sink 输出名，而不改变 dispatch 结果

### [ ] 6. 各输出面是否都显式读取 target name

建议按 “codec -> storage -> sql sink / sqlmodel” 的顺序看：

- `/Users/edison/go/ticdc/pkg/sink/codec/open/codec.go:48`
- `/Users/edison/go/ticdc/pkg/sink/codec/open/codec.go:129`
- `/Users/edison/go/ticdc/pkg/sink/codec/simple/message.go:227`
- `/Users/edison/go/ticdc/pkg/sink/codec/simple/message.go:326`
- `/Users/edison/go/ticdc/pkg/sink/codec/canal/canal_json_encoder.go:161`
- `/Users/edison/go/ticdc/pkg/sink/codec/canal/canal_json_encoder.go:390`
- `/Users/edison/go/ticdc/pkg/sink/cloudstorage/path.go:234`
- `/Users/edison/go/ticdc/pkg/sink/cloudstorage/table_definition.go:216`
- `/Users/edison/go/ticdc/pkg/sink/mysql/sql_builder.go:181`
- `/Users/edison/go/ticdc/pkg/sink/mysql/sql_builder.go:309`
- `/Users/edison/go/ticdc/pkg/sink/sqlmodel/multi_row.go:228`
- `/Users/edison/go/ticdc/pkg/sink/sqlmodel/multi_row_v1.go:36`
- `/Users/edison/go/ticdc/pkg/common/event/redo_test.go:24`

确认点：

- MQ codec 是否已经统一输出 target schema/table
- CloudStorage 的 schema file、数据路径、DML writer 是否都已经看 target name
- MySQL / sqlmodel 最终 SQL 是否显式写 target table
- redo replay 回放出来的事件是否仍然保留 target 信息

### [ ] 7. 测试边界、验证状态、分支纯度

入口：

- `/Users/edison/go/ticdc/pkg/sink/codec/open/encoder_test.go:96`
- `/Users/edison/go/ticdc/pkg/sink/codec/open/encoder_test.go:688`
- `/Users/edison/go/ticdc/pkg/sink/codec/canal/canal_json_encoder_test.go:203`
- `/Users/edison/go/ticdc/pkg/sink/codec/simple/encoder_test.go:91`
- `/Users/edison/go/ticdc/pkg/sink/codec/debezium/codec_test.go:31`
- `/Users/edison/go/ticdc/pkg/sink/cloudstorage/table_definition_test.go:80`
- `/Users/edison/go/ticdc/downstreamadapter/sink/cloudstorage/dml_writers_routing_test.go:25`
- `/Users/edison/go/ticdc/pkg/sink/sqlmodel/multi_row_test.go:158`
- `/Users/edison/go/ticdc/pkg/sink/sqlmodel/row_change_test.go:166`
- `/Users/edison/go/ticdc/tests/integration_tests/table_route/run.sh:1`
- `/Users/edison/go/ticdc/tests/integration_tests/redo_apply_table_route/run.sh:1`

确认点：

- 新增测试是否覆盖了独立功能点，而不是重复证明同一条路径
- 事件相关测试是否优先使用 `EventTestHelper` 生成 source event，再在本 package 内补 target 字段
- codec / cloudstorage 这批新增测试是否已经回到各自 package 的职责边界，不再跨 package 直接测试 `routing`
- 当前分支是否还残留和 table route 无关的附带改动

## 当前验证状态

- [x] `go test ./downstreamadapter/routing -run 'TestError|TestResolveDDL|TestApplyToTableInfo|TestApplyToDDLEvent' -count=1`
- [x] `go test ./downstreamadapter/sink/eventrouter -run 'TestGetTopicForDDL|TestTableRoutingDoesNotAffectDDLTopicMatching|TestGetTopicForRowChange' -count=1`
- [x] `go test -tags=intest ./pkg/sink/codec/simple -run 'TestEncodeRoutedDMLEventUsesTargetNames|TestEncodeRoutedDDLEventUsesTargetNames' -count=1`
- [x] `go test -tags=intest ./pkg/sink/codec/open -run 'TestCreateTableDDL|TestEncodeRoutedDMLEventUsesTargetNames|TestEncodeRoutedDDLEventUsesTargetNames' -count=1`
- [x] `go test -tags=intest ./pkg/sink/codec/canal -run 'TestEncodeRoutedDMLEventUsesTargetNames|TestEncodeRoutedDDLEventUsesTargetNames' -count=1`
- [x] `go test -tags=intest ./pkg/sink/codec/debezium -run 'TestTableRouteDMLUsesTargetNames|TestTableRouteDDLRenameUsesTargetNames' -count=1`
- [x] `go test -tags=intest ./pkg/sink/cloudstorage -run 'TestFromDDLEventUsesCanonicalTargetNames' -count=1`
- [x] `go test -tags=intest ./downstreamadapter/sink/cloudstorage -run '^TestAddDMLEventUsesTargetNames$' -count=1`
- [ ] 集成执行仍需在完整外部环境确认

## PR 拆分建议

当前 diff 过大，不适合继续作为单个 PR review。建议按下面顺序拆成 6 个 PR。

### [ ] PR 1. 配置契约与外部接口

建议只包含：

- `pkg/config/sink.go`
- `pkg/config/changefeed.go`
- `pkg/config/sink_test.go`
- `api/v2/model.go`
- `tests/integration_tests/api_v2/model.go`
- `pkg/errors/error.go`

目标：

- 定义 `target-schema` / `target-table` 配置契约
- 完成表达式校验
- 保证配置裁剪不会误删 table route

### [ ] PR 2. source-preserving 的公共数据模型

建议只包含：

- `pkg/common/table_name.go`
- `pkg/common/table_name_gen.go`
- `pkg/common/table_info.go`
- `pkg/common/table_info_test.go`
- `pkg/common/event/ddl_event.go`
- `pkg/common/event/dml_event.go`
- `pkg/common/event/redo.go`
- `pkg/common/event/ddl_event_test.go`
- `pkg/common/event/redo_test.go`

目标：

- `Schema/Table` 保持 source
- `Target*` 只表示 routed target
- 定义 rename DDL 的 source / target old-new 语义

### [ ] PR 3. routing 核心与 shared router 接入

建议只包含：

- `downstreamadapter/routing/*`
- `downstreamadapter/dispatchermanager/dispatcher_manager.go`
- `downstreamadapter/dispatchermanager/dispatcher_manager_redo.go`
- `downstreamadapter/dispatchermanager/dispatcher_manager_test.go`
- `downstreamadapter/dispatcher/basic_dispatcher.go`
- `downstreamadapter/dispatcher/basic_dispatcher_info.go`
- `downstreamadapter/dispatcher/basic_dispatcher_active_active_test.go`
- `downstreamadapter/dispatcher/event_dispatcher_test.go`
- `downstreamadapter/dispatcher/redo_dispatcher_test.go`
- `downstreamadapter/eventcollector/dispatcher_stat.go`
- `downstreamadapter/eventcollector/dispatcher_stat_test.go`
- `downstreamadapter/eventcollector/event_collector_test.go`
- `downstreamadapter/sink/sink.go`

目标：

- 把 router 收口到 `downstreamadapter/routing`
- 只在 manager 级别初始化一次
- 在 collector / dispatcher 边界应用 routing

### [ ] PR 4. dispatch 保持 source identity

建议只包含：

- `downstreamadapter/sink/eventrouter/*`
- `downstreamadapter/sink/kafka/sink.go`
- `downstreamadapter/sink/kafka/sink_test.go`
- `downstreamadapter/sink/pulsar/sink.go`
- `downstreamadapter/sink/pulsar/sink_test.go`

目标：

- `matcher` / `topic` / `partition` 继续只看 source
- table route 不改变 dispatch 语义

### [ ] PR 5. 非 SQL 输出面读取 target

建议只包含：

- `pkg/sink/codec/open/*`
- `pkg/sink/codec/simple/*`
- `pkg/sink/codec/canal/canal_json_encoder.go`
- `pkg/sink/codec/canal/canal_json_encoder_test.go`
- `pkg/sink/codec/canal/canal_json_txn_encoder.go`
- `pkg/sink/codec/debezium/*`
- `pkg/sink/codec/avro/encoder.go`
- `pkg/sink/codec/csv/csv_message.go`
- `pkg/sink/codec/common/helper.go`
- `pkg/sink/cloudstorage/path.go`
- `pkg/sink/cloudstorage/path_test.go`
- `pkg/sink/cloudstorage/table_definition.go`
- `pkg/sink/cloudstorage/table_definition_test.go`
- `downstreamadapter/sink/cloudstorage/dml_writers.go`
- `downstreamadapter/sink/cloudstorage/dml_writers_routing_test.go`
- `downstreamadapter/sink/cloudstorage/sink.go`

目标：

- codec / cloudstorage 对外输出统一显式读取 `Target*`
- 测试边界回到各自 package

### [ ] PR 6. SQL sink、sqlmodel 与 redo

建议只包含：

- `pkg/sink/mysql/helper.go`
- `pkg/sink/mysql/mysql_writer_ddl.go`
- `pkg/sink/mysql/mysql_writer_test.go`
- `pkg/sink/mysql/progress_table_writer.go`
- `pkg/sink/mysql/sql_builder.go`
- `pkg/sink/mysql/sql_builder_test.go`
- `pkg/sink/sqlmodel/multi_row.go`
- `pkg/sink/sqlmodel/multi_row_test.go`
- `pkg/sink/sqlmodel/multi_row_v1.go`
- `pkg/sink/sqlmodel/row_change.go`
- `pkg/sink/sqlmodel/row_change_test.go`
- `downstreamadapter/sink/redo/sink.go`
- `tests/integration_tests/table_route/*`
- `tests/integration_tests/redo_apply_table_route/*`
- `tests/integration_tests/run_light_it_in_ci.sh`

目标：

- SQL 生成统一写 target table
- redo replay 保留 target 信息
- 最后补齐集成脚本和 CI case
