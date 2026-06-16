# weekly_rand_single 调查 Notebook

本 notebook 记录本 case 调查过程中固定会出现、但通常不是代码根因的错误/噪音，以及下一次遇到时的处理方式。

## 固定环境噪音

### TiDB 启动检查早期 `ERROR 2003`

现象：

```text
Verifying Upstream TiDB is started...
ERROR 2003 (HY000): Can't connect to MySQL server on '127.0.0.1:4000' (111)
```

判断：

- 这是启动检查刚开始时 TiDB 端口还没 ready 的 transient error。
- 如果后面能打印 `mysql.tidb` 变量表，或者继续进入 CDC/changefeed/workload，就不要当成 case 失败。

处理：

- 继续观察，不要因为这一行中断。
- 只有连续重试后脚本明确 `start tidb cluster failed` 并退出，才作为环境失败处理。

### `tiflash: command not found`

现象：

```text
Starting Upstream TiFlash...
.../start_tidb_cluster_impl: line 365: tiflash: command not found
start tidb cluster failed
The 2 times to try to start tidb cluster...
```

判断：

- 这是远端 PATH 没包含 TiFlash binary，不是 TiCDC 代码逻辑失败。
- 远端已有 TiFlash binary：
  - `/home/hongyunyan/.tiup/components/tiflash/v9.0.0-beta.2.pre-nightly/tiflash/tiflash`
- 当前这类错误可能出现在 cluster start retry 阶段；如果脚本后续进入 `bootstrap done`、创建 changefeed 并开始 workload，则无需处理。

处理：

- 如果脚本最终继续进入 workload：记录为环境噪音，继续跑。
- 如果脚本因为找不到 TiFlash 最终退出：补 PATH 后重跑：

```bash
export PATH=/home/hongyunyan/.tiup/components/tiflash/v9.0.0-beta.2.pre-nightly/tiflash:$PATH
```

### `go test` 默认 toolchain 不满足要求

现象：

```text
go.mod requires go >= 1.25.10
```

判断：

- 远端默认 go 版本可能低于 `go.mod` 要求，或者环境中 `GOTOOLCHAIN=local`。

处理：

- Go 测试统一带：

```bash
GOTOOLCHAIN=auto go test --tags=intest ./path -run TestName -count=1
```

### 直接 `go test` 跑到 failpoint 代码

现象：

```text
undefined: failpoint.Inject
undefined: failpoint.Return
```

判断：

- 这是 failpoint 代码没有被 rewrite 的编译错误，不是目标 case 的业务失败。
- 本仓库的 failpoint 相关测试需要走 make 目标，或者先启用 failpoint rewrite。

处理：

- 优先用包级 make 目标：

```bash
GOTOOLCHAIN=auto make unit_test_pkg PKG="./downstreamadapter/dispatcher ./maintainer"
```

- 如果必须直接 `go test`，先按仓库脚本启用 failpoint，结束后再 disable，避免污染后续测试。

### 长时间 5 连跑输出过大

现象：

- `weekly_rand_single` workload 每秒输出大量 DDL 行。
- 5 连跑如果直接 `2>&1 | tee -a log`，终端输出会非常大，远端 ssh 会话可能被输出拖慢。

判断：

- case 结果以 `/tmp/tidb_cdc_test/weekly_rand_single_5pass.log` 里的 pass/fail 标记和脚本退出码为准。
- 终端不需要实时接收完整 DDL 流，只需要日志文件完整保留。

处理：

- 下次启动 5 连跑时，直接把 stdout/stderr 写日志，不要 tee 到终端：

```bash
GOTOOLCHAIN=auto RUN_PROFILE=weekly RUN_DURATION=30m RUN_SEED=${seed} \
  tests/integration_tests/run.sh mysql weekly_rand_single \
  >> /tmp/tidb_cdc_test/weekly_rand_single_5pass.log 2>&1
```

- 如果已经启动成 `tee -a`，可以只重定向父 shell 或当前 `tee` 的 fd 1 到 `/dev/null`，不影响日志文件继续写入：

```bash
gdb -q -p <pid> -batch \
  -ex 'p (int) close(1)' \
  -ex 'p (int) open("/dev/null", 1)'
```

- 重定向之后继续用以下命令观察：

```bash
grep -n "weekly_rand_single passed\|weekly_rand_single failed\|five consecutive" \
  /tmp/tidb_cdc_test/weekly_rand_single_5pass.log
grep -a "health:" /tmp/tidb_cdc_test/weekly_rand_single_5pass.log | tail -n 8
```

### workload 结束瞬间的 `context deadline exceeded`

现象：

```text
ddl worker=3 kind=add_index ... err=context deadline exceeded
ddl worker=1 kind=split_add_index ... err=context deadline exceeded
```

判断：

- 如果这些行出现在 `workload finished, waiting for converge` 前后，通常只是 workload 总时长到期，DDL worker 被 context 取消。
- 这类行本身不是 case 失败；真正失败要看后续是否出现 `runner failed:`、checksum diff、panic/fatal/race，或脚本退出码非 0。

处理：

- 不要只因为 DDL worker 的 `context deadline exceeded` 判定代码错误。
- 继续看后面的 converge、finish mark、diff 和 log scan。

### weekly profile 的 `converge_timeout=30m` 过短

现象：

```text
workload finished, waiting for converge: 20s
converge: waiting for finish mark, checkpoint=...
runner failed: context deadline exceeded
```

本次 seed `2026061509` 的证据：

- workload 在 `2026-06-16 02:55:45` 结束并进入 converge。
- 上游 finish mark DDL commit 时间为 `2026-06-16 10:56:05.681`。
- 30 分钟 converge deadline 到期前，checkpoint 只到 `2026-06-16 10:38:29.999`。
- checkpoint 持续前进，不是完全卡死；但还差约 `17m36s` 逻辑时间，按当时速率需要额外约 `56-58m`。

判断：

- 如果上游 `db1.finish_mark` 已存在、下游还没有，且 CDC status 的 checkpoint 仍在推进，这更像 backlog 收敛窗口不足，不要立即当成 barrier 卡死。
- 如果 checkpoint 连续超过 `no_advance_hard` 没前进，或者 CDC state 变成 failed/error，再按 CDC 正确性问题调查。

处理：

- weekly profile 运行时使用更长收敛窗口：

```bash
export RUN_CONVERGE_TIMEOUT=120m
```

- 修改后的 weekly random DDL run.sh 会在 `RUN_PROFILE=weekly` 且未显式指定时默认使用 `120m`；smoke 仍默认 `30m`。
- 观察命令：

```bash
curl -s http://127.0.0.1:8300/api/v2/changefeeds/weeklyrand | tr ',' '\n' | grep -E 'state|checkpoint_ts|checkpoint_time|resolved_ts'
mysql -uroot -h127.0.0.1 -P4000 -Nse "SELECT COUNT(*), IFNULL(MAX(v),0) FROM db1.finish_mark;"
mysql -uroot -h127.0.0.1 -P3306 -Nse "SHOW TABLES FROM db1 LIKE 'finish_mark'; SELECT COUNT(*), IFNULL(MAX(v),0) FROM db1.finish_mark;" 2>&1 || true
```

### 随机 DDL 的 TiDB 业务错误

现象：

```text
err=Error 1071 (42000): Specified key was too long
err=Error 8200 (HY000): Unsupported modify charset from utf8mb4 to gbk
err=Error 1292 (22007): Truncated incorrect DOUBLE value
err=Error 1265 (01000): Data truncated for column
err=Error 1146 (42S02): Table ... doesn't exist
err=Error 1054 (42S22): Unknown column ...
```

判断：

- 这些错误来自 random DDL runner 故意尝试高风险 DDL：加索引、改字符集、改列类型、drop/recover/rename 竞态等。
- DDL worker 会记录错误并继续；只要 runner 没有最终 `runner failed:`，这些单条业务错误不是 case 失败。
- 需要区分两类 `1146`：
  - workload 中目标表被并发 drop/rename 后报 `1146`，通常是预期噪音；
  - converge 阶段查询下游 `db1.finish_mark` 报 `1146`，表示下游还没追到 finish mark，需要结合 checkpoint 判断。
- 需要区分两类 `1054`：
  - workload 中 random DDL worker 对已变化的列执行 `modify_column_type` 等操作后报 `Unknown column`，通常是预期噪音；
  - TiCDC sink 重试 DML 时在 `cdc.log` 出现 `ErrReachMaxTry`/`Unknown column`，或最终 `runner failed:` 关联到该错误，才是需要调查的同步错误。

处理：

- 不要因为单条 DDL business error 停测试。
- 真正需要处理的是：
  - `runner failed:` 后的最终错误；
  - sync diff 不一致；
  - panic/fatal/data race；
  - changefeed state failed/error；
  - checkpoint 在 `no_advance_hard` 窗口内完全不推进。

快速过滤：

```bash
grep -aE "runner failed|weekly_rand_single failed|checksum|panic|fatal|DATA RACE|state=failed|state=error" \
  /tmp/tidb_cdc_test/weekly_rand_single_5pass.log
```

### `RECOVER TABLE` 后下游 `Unknown column`

现象：

```text
changefeed state is not normal: warning
Error 1054 (42S22): Unknown column 'a' in 'field list'
REPLACE INTO `db1`.`t15_r_3235459` (`id`,`b`,`c`,`d`,`e`,`bin`,`a`) VALUES (...)
```

本次 seed `2026061509` 的证据：

- 上游在 `03:42:43` 对 `db1.t15_r_3235459` 执行过 `DROP COLUMN a`，随后在 `03:42:44` drop table。
- 上游在 `03:43:48` 执行 `RECOVER TABLE db1.t15_r_3235459`，TiCDC 事件中的 recovered `TableInfo` 带有列 `a`。
- MySQL sink 在下游直接执行原始 `RECOVER TABLE db1.t15_r_3235459` 成功。
- 后续 DML 按上游 recovered `TableInfo` 生成，SQL 包含 `a`；但下游实际恢复出来的表不带 `a`，因此报 `Unknown column 'a'` 并进入 warning。

判断：

- 这不是 converge timeout；延长时间不会恢复。
- 这也不是普通 random DDL business error；changefeed 已进入 warning，DML 会反复失败直到 runner 失败。
- 根因是 `RECOVER TABLE` 依赖执行集群本地 DDL history / recycle-bin / GC snapshot 状态。TiCDC 内部 schema timeline 来自上游，sink 执行的 raw SQL 却让下游自己选择历史表；同名表多次 drop/recover/drop-column 后，上下游可能恢复到不同 schema。
- `RECOVER TABLE BY JOB <id>` 不能直接用上游 drop job id 修复，因为下游执行时查的是下游自己的 DDL job id；TiCDC 当前没有维护上游 drop job 到下游 drop job 的映射。
- 把 recover 改成 `CREATE TABLE` 也不是完整修复，因为会丢失 `RECOVER TABLE` 应恢复的旧数据。

处理：

- weekly random DDL 默认集合不要生成 `recover_table`。
- 当前修复是在 `tests/utils/random_ddl_test_runner/defaultDDLKinds()` 中移除 `recover_table`，保留 `genRecoverTable` 供显式测试。
- 遇到类似日志时，先确认 random runner 是否又启用了 `recover_table`：

```bash
grep -RIn "name: *\"recover_table\"" tests/utils/random_ddl_test_runner
grep -a "kind=recover_table" /tmp/tidb_cdc_test/weekly_rand_single/ddl_trace.log | tail -n 20
grep -aE "Unknown column|state=warning|ErrReachMaxTry" /tmp/tidb_cdc_test/weekly_rand_single/cdc.log | tail -n 80
```

- 如果未来要正式支持 CDC 复制 `RECOVER TABLE`，需要单独设计：例如维护下游 drop/truncate job id 映射并处理路由/重试/GC，或在 recover 后做完整数据重建/快照补偿。不要把这个产品级语义问题混进 weekly random case 修复。

### sink-side `ErrReachMaxTry Unknown column` after dispatcher recreate

现象：

```text
runner failed: changefeed state is not normal: warning
[CDC:ErrReachMaxTry] ... REPLACE INTO `db2`.`t14_r_3402273` (`id`,`a`,`b`,`c`,`d`,`e`,`bin`) ...
Error 1054 (42S22): Unknown column 'bin' in 'field list'
Error 1054 (42S22): Unknown column 'e' in 'field list'
```

判断：

- 这不是 random DDL worker 的普通 `Unknown column` 业务噪音。
- 只要错误出现在 TiCDC sink retry / `ErrReachMaxTry` / changefeed warning 路径，就按同步正确性问题调查。
- 本次固定模式是：旧 dispatcher 已经执行过 table DDL 并 stopped 到更高 checkpoint；随后迟到的 add-table barrier 又从更旧 `startTs` 创建新 dispatcher，重放 DDL 之前的 DML，打到 DDL 之后的下游 schema。
- 延长 `converge_timeout` 不能修复这类问题；延长只解决 backlog 仍在正常推进的 timeout。

快速定位：

```bash
grep -aE "ErrReachMaxTry|Unknown column|changefeed state is not normal" \
  /tmp/tidb_cdc_test/weekly_rand_single/cdc.log | tail -n 120

grep -aE "new span replication created|add new table|dispatcher component has stopped|send reset dispatcher request|reset dispatcher" \
  /tmp/tidb_cdc_test/weekly_rand_single/cdc.log | tail -n 240

grep -a "<table_name_or_table_id>" /tmp/tidb_cdc_test/weekly_rand_single/ddl_trace.log | tail -n 80
```

处理：

- 查旧 dispatcher stopped checkpoint 是否大于新 dispatcher startTs。
- 查新 dispatcher replay 的第一条 DML commitTs 是否小于已经执行过的 DDL commitTs。
- 如果满足上述条件，优先检查 `maintainer/span.Controller` 的 removed table checkpoint 是否记录并 clamp 了 `AddNewTable` 起点。
- 当前修复点：`RecordRemovedSpanCheckpoint` + `AddNewSpans` table 级 startTs clamp；move 路径还需要确保 origin stopped status 写回 `replicaSet`。

### log scan `panic`/`fatal` false positives in random payload

现象：

```text
converge done: finish mark applied downstream
runner failed: log scan found 88 panic/fatal/race matches
```

判断：

- 如果 log scan 失败发生在 `converge done` 之后，先不要按 TiCDC runtime panic 处理。
- 抽样查看 `log scan match` 对应文件/行：

```bash
grep -aE "log scan match|runner failed|converge done" \
  /tmp/tidb_cdc_test/weekly_rand_single_5pass.log | tail -n 120

sed -n "<line-2>,<line+2>p" \
  /tmp/tidb_cdc_test/weekly_rand_single/<matched-log-file>
```

- 如果命中行是 `[DEBUG]` DML event / SQL builder 日志，并且 `panic`/`fatal` 只出现在随机字符串列值里，例如：

```text
Bb8bdTFTEIN9i3spwifGjZj3AmFAtalR
1YCs3x0WFrKYaheC3jpXpAnicxBqG3pe
```

则这是 log scan 误报，不是实际 panic/fatal。

处理：

- 默认 `panic`/`fatal` 不能用裸 substring 扫描随机 DML payload。
- 当前修复在 `tests/utils/random_ddl_test_runner/logscan.go` 中把默认关键字限定为真实严重日志模式：
  - `fatal`: `[FATAL]`、`level=fatal`、行首 `fatal error:`。
