# Consumer 待完成任务

目标是高吞吐、低开销、高稳定，并使模块职责内聚、边界清楚。每个问题独立成章，按预估处理难度从低到高排列；有依赖的任务在依赖完成后处理。P1 表示正确性或资源安全风险，P2 表示可靠性、验证或维护问题。难度顺序不代表风险高低，最终通过 T16 验证整体结果。

Kafka commit 的最低约束：每个分区只确认已完成解码、且全部关联事件已写入下游的连续消息前缀；后续消息先完成不能越过前面的未完成消息。

## T33 Simple 缓存确认测试在输入解析结束前断言引用归零（低难度，P2）

- [ ] 修正 Simple 缓存确认测试的完成条件。
  - 入口：
    - [单行转换和 flush 完成后立即断言输入引用归零](cmd/consumer/consumer_test.go#L348)
    - [提交缓存转换后才释放输入的解析引用](cmd/consumer/assembler.go#L1347)
  - 问题：测试等待单行转换完成并执行 flush 后，输入解析可能仍持有合法引用。此时断言 refs 必须为 0 会随调度顺序失败，不能据此认定发生了引用泄漏。
  - 证据：本轮全量 race 检查在该断言失败；提取未修改 HEAD 的 consumer 代码单独运行同一 race 测试，20 次中失败 10 次，均为期望 0、实际 1。当前证据未显示数据竞争或生产确认提前。
  - 完成条件：在输入解析与关联写入均完成后验证引用归零；继续验证不同输入独立确认、未完成输入不可确认和最终预算回收；不通过 sleep 或放宽最终断言掩盖问题。
  - 验证：重复运行 `go test -race -tags=intest ./cmd/consumer -run '^TestSimpleCacheConfirmsEachInputIndependently$' -count=100`，随后执行完整 consumer race 测试。

## T20 输入引用计数与预算规则分散在多个模块（中等难度，P2，耦合）

- [ ] 封装输入引用与内存生命周期操作。
  - 入口：
    - [引用计数与输入预算的生命周期](cmd/consumer/ack.go#L24)
    - [assembler 直接增加写入引用](cmd/consumer/assembler.go#L481)
    - [转换 worker 释放解码引用](cmd/consumer/decoder.go#L112)
  - 问题：assembler 直接修改 refs，各 reader 直接读取计数；调用方需要共同掌握初始引用、解码引用、写入引用和确认后的预算释放规则。
  - 完成条件：通过少量有明确语义的操作表达保留、完成和可确认状态；集中维护计数不变量，明确原始输入与转换结果各自的预算所有者；保持现有 flush 回调路径。
  - 验证：覆盖一条输入拆成多批、缓存 schema、乱序完成及取消；确认不得提前，完成后的预算余额归零。

## T21 Kafka 缺少 DML 分区路由校验（中等难度，P2）

- [ ] 恢复 Kafka DML 分区路由校验。
  - 入口：
    - [当前路由计算仅用于推导等待分区](cmd/consumer/kafka.go#L273)
    - [接收 Kafka 分区消息](cmd/consumer/kafka.go#L193)
  - 问题：旧实现 `d1e7db93f:cmd/kafka-consumer/writer.go:659` 的 checkPartition 没有等价替代，错误分区可能被最终一致性检查遗漏。
  - 完成条件：根据实际行值和生产端规则核对分区；保留 Avro UPDATE 拆分等协议例外；失配信息能定位输入分区及预期分区。
  - 验证：覆盖 table、index-value、columns 路由和错误分区输入；运行 `mq_sink_dispatcher` 集成用例，并衡量校验开销。

## T22 Storage rename 进度依赖 assembler 修改同一个 DDL（中等难度，P2，耦合）

- [ ] 消除 Storage rename 进度对 DDL 原地修改的依赖。
  - 入口：
    - [assembler 补充 rename 旧表和新表名](cmd/consumer/assembler.go#L497)
    - [Storage 从修改后的 DDL 提取旧表名](cmd/consumer/storage.go#L517)
  - 问题：Storage Advance 直接读取 BlockedTableNames[0]，依赖 assembler 已按特定顺序解析并修改同一个 DDL 对象。这个前置条件没有体现在进度更新参数中。
  - 完成条件：通过明确的更新事实传递所需表范围，或在负责解析的边界形成完整元数据；Storage 更新进度时具备完整输入；避免重复解析同一 SQL。
  - 验证：覆盖带 schema、跨 schema 的 rename，以及旧路径文件回放；验证进度通知顺序变化时的行为与取消清理。

## T23 消费停滞与资源增长缺少诊断信号（中等难度，P2）

- [ ] 补齐 consumer 的进度与资源诊断信号。
  - 入口：
    - [现有诊断服务仅暴露 pprof](cmd/consumer/consumer.go#L248)
    - [依赖等待与继续读取决策](cmd/consumer/assembler.go#L903)
    - [写入进度与去重状态](cmd/consumer/writer.go#L143)
  - 问题：运行中缺少 consumer 自身的持续吞吐、积压、最老等待原因、去重键数量和分阶段进度信号，难以定位停滞及预算与实际内存的差异。
  - 完成条件：提供低频汇总或低基数指标，区分 schema、DDL、慢分区、预算和下游等待；覆盖预算用量、进程内存、在途数据和去重状态；避免逐行日志。
  - 验证：分别阻塞依赖与下游，确认可以定位原因并观察恢复；衡量采集开销。

## T24 Canal JSON 解析与转换产生较多中间分配（中等难度，P2）

- [ ] 减少 Canal JSON 解析与转换的中间分配。
  - 入口：
    - [延迟解析列字段](pkg/sink/codec/canal/canal_json_decoder.go#L319)
    - [构造格式化列值 map](pkg/sink/codec/canal/canal_json_decoder.go#L467)
    - [解析与转换基准](cmd/consumer/decoder_bench_test.go#L124)
  - 问题：同一行在 JSON 解析、列值格式化和 DML 转换之间多次构造 map 或复制列值，增加分配和 GC 成本；需要按 profile 确认哪些对象可以消除，并验证对端到端吞吐的影响。
  - 证据：本轮 Apple M3、Go 1.26.8、GOMAXPROCS=4 的 32 列基准约为 17 KiB/行、306 次堆分配/行；分配 profile 中约 52% 字节来自反射 map 写入，14% 来自格式化值 map，6% 来自 RawMessage 复制。该结果仅覆盖基准路径。
  - 完成条件：用新采集的 profile 确认热点，逐项减少不必要的列 map、复制或中间对象；保留列类型、旧值、checksum、大消息和 DDL schema 语义。
  - 验证：在固定工具链和负载下重复运行现有基准，记录 CPU、B/op、allocs/op 与方差；补充行为回归，并通过 T16 验证端到端效果。

## T25 构造 decoder 会重置其他实例共享的表 ID 状态（中高难度，P2，耦合）

- [ ] 消除 decoder 构造对全局 table ID 分配器的重置。
  - 入口：
    - [启动代码访问具体 Kafka reader 并预建全部游标](cmd/consumer/consumer.go#L93)
    - [Canal decoder 构造清空全局分配器](pkg/sink/codec/canal/canal_json_decoder.go#L143)
    - [Open decoder 构造清空全局分配器](pkg/sink/codec/open/decoder.go#L100)
  - 问题：构造一个 decoder 会影响同协议其他实例的 ID 状态，调用方必须保证先构造所有游标再开始解码；同一消费流又确实需要稳定、共享的表身份。
  - 完成条件：由消费会话明确拥有共享表身份状态，各游标在会话内复用；构造其他 decoder 不改变已有状态；通过必要的初始化信息获取 stream 范围，移除启动代码对 reader 内部 partitions 的访问。
  - 验证：创建额外游标前后表 ID 保持稳定；不同表无 ID 冲突；同流跨分区 DDL 与 DML 身份一致；覆盖同协议实例隔离及受影响 codec。

## T26 输入与输出类型包含不应跨模块暴露的中间状态（中高难度，P2，耦合）

- [ ] 明确输入变体与 writer 接收的已完成事件。
  - 入口：
    - [输入字段组合隐含多种事件类型](cmd/consumer/reader.go#L177)
    - [writeEvent 同时承载转换任务与可写事件](cmd/consumer/assembler.go#L311)
    - [转换结果收集后替换占位 DML](cmd/consumer/assembler.go#L250)
  - 问题：读入事实、等待转换和准备写入共用可变对象；writer 的输入类型还包含 decoding、boundary、stream 和 awaitingDDL 等 assembler 内部状态。
  - 完成条件：在边界明确合法输入变体及必需字段；writer 只接收完整可写事件、控制进度和完成通知；内部等待状态由 assembler 持有；选择能减少非法组合的最小类型调整。
  - 验证：覆盖输入变体、慢转换和控制事件；writer 无需检查未完成解码状态；比较调整前后的对象分配与吞吐。

## T27 assembler 的多类职责与两个执行循环共用一个状态对象（中高难度，P2，耦合）

- [ ] 提高 assembler 内聚性并区分两个执行循环的状态。
  - 入口：
    - [并行运行协议安排与转换结果收集](cmd/consumer/assembler.go#L46)
    - [集中承载多类职责和扫描缓存的 assembler 状态](cmd/consumer/assembler.go#L434)
    - [Simple 缓存恢复依赖具体 decoder](cmd/consumer/assembler.go#L1335)
  - 问题：协议解析、schema 缓存、依赖调度、转换收集和批处理集中于同一对象；两个执行循环的所有权主要靠注释区分，修改某一类状态需要理解远处的不变量。
  - 完成条件：按实际状态所有者和变更原因组织协议游标、依赖安排与转换收集；明确共享数据的可变阶段；将 Simple 特有的缓存操作限制在协议适配边界；保留统一的 DDL 影响范围语义。
  - 验证：通过现有顺序、schema、确认和取消测试及 race 检查；协议缓存行为的修改范围局限于所属边界；记录重组前后的基准结果。

## T28 Reader 的完成证明与超预算读取依赖隐含调用约定（高难度，P1，耦合）

- [ ] 明确 Reader 的完成证明与超预算读取契约。
  - 入口：
    - [读取、进度、边界和准入共用的 Reader 契约](cmd/consumer/reader.go#L29)
    - [共享可变 reached 与 watermark 指针](cmd/consumer/reader.go#L220)
    - [assembler 根据内部依赖反向驱动读取](cmd/consumer/assembler.go#L903)
    - [Kafka 按边界对象身份匹配完成通知](cmd/consumer/kafka.go#L246)
  - 问题：正确推进依赖 Boundary、Advance 和 ContinueReading 的调用顺序、共享对象身份及原地状态变化；布尔开关无法表达继续读取的范围和成本上限。
  - 完成条件：分别定义输入已解码、依赖已满足、下游已持久化、上游已确认的含义及所有者；用明确的请求与完成事实表达需要推进的范围；规定超预算读取的退出条件和资源边界；保留寻找缺失依赖的活性。
  - 验证：覆盖满预算、缺失 schema、DDL 副本先到、慢分区及取消；无提前确认、误完成或死锁；该契约支撑 T29 至 T32 的边界修复。

## T29 去重历史未计入预算，回收依赖全局 watermark（高难度，P1）

- [ ] 为去重状态建立可核算、可回收的资源边界。
  - 入口：
    - [逐行保存 handle 与 mutation 身份](cmd/consumer/writer.go#L550)
    - [依赖下游 watermark 回收历史](cmd/consumer/writer.go#L494)
  - 问题：去重 map、handle 字符串和键数组未纳入共享预算；全局 watermark 停滞时，已写入行的去重状态仍会持续累积。
  - 完成条件：核算去重保留成本，定义压力下的回收或有界存储策略；与 T28 的继续读取契约协调；保留未越过可靠边界的历史 mutation，避免重放覆盖新值。
  - 验证：在下游持续完成、全局 watermark 停滞时施加长时间输入，检查预算与实际内存；恢复后历史可回收，旧副本不会覆盖新状态。

## T30 CSV 同事务跨输出批次时可能先插入、后删除（高难度，P1）

- [ ] 保证 CSV 同事务跨输出批次的删除与插入顺序。
  - 入口：
    - [生产端将带旧值 UPDATE 编成独立删、插行](pkg/sink/codec/csv/csv_message.go#L104)
    - [按可用结果与大小截断 assembler 输出](cmd/consumer/assembler.go#L218)
    - [仅排序本次输出中的事件](cmd/consumer/assembler.go#L325)
    - [现有跨批测试只调用一次 prepare](cmd/consumer/assembler_test.go#L96)
  - 问题：代码路径显示，同事务 `D(1), I(2), D(2), I(1)` 跨两次输出后，前一批插入可能先于后一批删除；审阅尚未运行完整 TiDB 集成复现。旧路径的事务边界可参考 [同 commitTs 保持在解析批次内](cmd/util/event_group.go#L931)。
  - 完成条件：先用真实流水线复现，确定事务结束证明，在正确范围内保证删除阶段先完成；排序缓冲遵循 T28 的资源契约，并与 T31 协同限制内存。
  - 验证：覆盖两次独立输出、超过 1024 行或 2 MiB、跨文件、慢转换及主键/唯一键交换；检查最终数据，运行两个 `csv_storage_update_pk_*` 集成用例。

## T31 Storage 文件组重排与整文件读取缺少硬资源边界（高难度，P1）

- [ ] 让跨节点 Storage 文件组按有界内存完成重排。
  - 入口：
    - [跨节点文件序列启用整组排序](cmd/consumer/storage.go#L393)
    - [整文件分配并读取](cmd/consumer/storage.go#L482)
    - [等待文件组结束后排序](cmd/consumer/assembler.go#L1110)
  - 问题：组结束前保留该组的转换结果，期间可越过软预算继续读取；积压规模决定内存峰值，单个大文件也会直接分配完整大小。
  - 依赖：T28 的资源契约；与 T30 的事务排序边界共同设计。
  - 完成条件：常规顺序输入持续处理；需要重排的输入使用有界缓冲及必要的归并或外排；明确超大行、超大事务、取消和临时资源清理行为。
  - 验证：在固定内存预算下处理超预算文件组、多个 dispatcher、单个大文件及重放；检查数据一致性、峰值 RSS 和持续进度。

## T32 Pulsar checkpoint 尚不能证明其他分区已消费完成（最高难度，P1）

- [ ] 建立 Pulsar 多分区的可靠完成边界。
  - 入口：
    - [Pulsar 输入合并为单个逻辑 stream](cmd/consumer/pulsar.go#L193)
    - [生产端 broadcast 实际发送一次消息](downstreamadapter/sink/pulsar/ddl_producer.go#L98)
    - [后到的旧 DML 受已推进边界过滤](cmd/consumer/writer.go#L550)
  - 问题：生产端完成写入不能证明 consumer 已消费其他分区；某分区 checkpoint 先到时，其他分区尚未读到的旧 DML 可能被丢弃。旧 Pulsar consumer 存在类似假设，真实多分区场景仍需验证。
  - 依赖：T28 的完成证明契约；涉及上游协议调整时保持现有生产与消费的兼容性。
  - 完成条件：验证 checkpoint、DDL、DML 的跨分区到达顺序；建立可证明的完成边界，并明确验证通过的支持范围；所有相关分区未完成前，禁止据该 checkpoint 回收或过滤未消费数据。
  - 验证：使用真实多分区 Pulsar，主动延迟数据分区，使 checkpoint 和 DDL 先到；覆盖重启、重放与下游失败，检查数据一致性和确认位置。

## T16 最终 CI、真实环境与性能对照尚未完成（收尾验证）

- [ ] 执行最终 CI 并关闭迁移工作。
  - 问题：当前单测和微基准不能证明三个来源在真实环境中的数据正确性、恢复能力及端到端资源开销；迁移仍缺完整 CI 和与旧 consumer 的同条件对照。
  - 入口：
    - [统一启动脚本](tests/integration_tests/_utils/run_consumer#L1)
    - [consumer 镜像](deployments/consumer.Dockerfile#L1)
    - [PR #6466](https://github.com/pingcap/ticdc/pull/6466)
  - 前提：T20 至 T33 的修复与验证完成；仅完成低难度任务不能关闭 P1 问题。
  - 完成条件：`make check`、`make integration_test_build`、仓库完整 CI 通过；consumer 镜像可以构建并启动；按检查要求完成 error-log-review 审批。
  - 验证现有 Kafka/Pulsar/Storage 用例，重点检查 DDL、watermark、恢复、认证、默认 batch DML、共享预算下的连续确认、超大输入和跨节点文件组。
  - 补充 consumer 与受影响 codec 的 race 检查；在相同工具链、配置和负载下对照旧 consumer，记录端到端行吞吐、CPU、分配量、峰值 RSS、恢复耗时及测量方差。覆盖宽行、热点表、多分区、重放、慢下游和大文件组，区分微基准与端到端结果。

## T17 外部 CI 仍需完成旧构建入口迁移（迁移收尾）

- [ ] 外部 CI 切换后移除临时构建别名。
  - 问题：旧 consumer 的构建 target 和产物链接仍被保留以兼容外部 CI。外部调用尚未全部切换，直接删除会使这些任务构建或取用产物失败。
  - 入口：[三个兼容构建入口](Makefile#L178)。
  - 前提：T16 完成；外部 CI 已统一使用 `make consumer` 和 `cdc_consumer` 缓存产物。
  - 完成条件：删除三个旧 Make target 及旧产物链接；切换后的仓库 CI 全部通过。
