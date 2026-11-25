# 用 tiup playground 启动集成测试集群

## P0｜classic 双集群 PoC

- [ ] 新增 `tests/integration_tests/_utils/start_tidb_cluster_tiup`，用两套 playground 起 upstream 与 downstream。
  - 对照实现：`tests/integration_tests/_utils/start_tidb_cluster_impl:95` 起 upstream PD，`:226` 起 upstream TiDB，`:246` 起 downstream TiDB，`:372` 起 TiFlash。
  - upstream 命令骨架：`tiup playground --tag <case>-up --pd 3 --kv 3 --db 2 --tiflash 0 --without-monitor --pd.binpath bin/pd-server --kv.binpath bin/tikv-server --db.binpath bin/tidb-server --pd.config $WORK_DIR/pd-config.toml --kv.config $WORK_DIR/tikv-config.toml --db.config $WORK_DIR/tidb-config.toml`。
  - playground 一个 tag 只对应一套拓扑，downstream 必须用第二个 tag 与独立端口段。
  - `--*.binpath` 指向 `bin/` 的本地构建产物时不再解析组件版本，但 playground 仍执行 `environment.InitEnv`（tiup `components/playground/main.go:173-177`）；离线启动能力需要在 PoC 中确认。
  - 完成条件：`cdc.test` 建 changefeed、上游写入一行数据、`check_sync_diff` 通过。
  - 验证：`cd tests/integration_tests && bash run.sh mysql common_1`，并把脚本临时指向 tiup 实现；记录启动到 `check_pd_health` 通过的耗时。

- [ ] 确定端口方案并同步 `tests/integration_tests/_utils/test_prepare`。
  - 现状：`test_prepare:39` PD 2379/2679/2779（peer 2380/2680/2780）、`:55` TiKV 20160-20162（status 20181-20183）、`:9` 与 `:16` TiDB 4000/4001（status 10080/10081）。
  - 约束：`--pd.port`/`--kv.port`/`--db.port` 只作用于第一个实例的 service 端口，peer 与 status 端口由 tiup 从 base 端口向上取空闲端口（tiup `pkg/utils/freeport.go:38`）；`tiup playground display` 只输出 Pid/Role/Uptime（tiup `components/playground/playground.go:116-131`），脚本无法据此发现端口。
  - 影响：多实例 PD 的 peer 端口无法保持 2680/2780，TiKV status 从 20180 起而非 20181，端口表必须改或者在 PoC 中接受这些差异。
  - 完成条件：使用多 PD 的三个 case 在选定方案下起集群并通过：`cli_with_auth/run.sh:29`、`http_api/run.sh:27`、`region_merge/run.sh:37`。

- [ ] 把 playground 组件日志接入 CI 收集路径。
  - 现状：`_utils/stop_test` 采集 `$WORK_DIR` 下的文件，`_utils/stop_tidb_cluster` 只负责杀进程。
  - 冲突：playground 把组件日志写在 `~/.tiup/data/<tag>/<component>-<port>/`，不会出现在 `$WORK_DIR`。
  - 完成条件：case 失败时 `$OUT_DIR/<case>/` 下能看到 PD/TiKV/TiDB 日志。

- [ ] 对齐清理路径。
  - 现状：`_utils/stop_tidb_cluster:9-11` 按进程名 `killall`/`pkill`，`:31-38` 按端口 `lsof` 兜底。
  - 需要补充：`tiup clean <tag>` 删除 `~/.tiup/data/<tag>`，并回收 `tiup` 进程本身；写法可参照 `cmd/config-converter/test.sh:35-37`。
  - 完成条件：连续运行两个 case，第二个 case 启动时无残留进程，端口不冲突。

## P0｜划定不可迁移的 case

- [ ] `synced_status` 与 `synced_status_with_redo` 保持现有实现。
  - 这两个 case 在测试中调用 `kill_pd`/`kill_tikv`/`kill_tidb`（`synced_status/run.sh:147,171,229`）。
  - 推断：playground 的实例均由 `exec.CommandContext` 启动（tiup `components/playground/instance/process.go:116-122`），任一实例退出使 `p.wait()` 返回错误、`RunE` 返回并取消 ctx（tiup `components/playground/main.go:237-240`），其余实例随之被 kill，downstream 也会消失，case 语义被改变。

- [ ] 四个 TLS case 保持 `start_tls_tidb_cluster_impl_classic`。
  - playground 没有 TLS 配置项；`cli_tls_with_auth`、`http_api_tls`、`http_api_tls_old_arch`、`http_api_tls_with_user_auth` 依赖证书与 PD 2579、TiDB 3307 等端口。

- [ ] CDC 进程继续由 `_utils/run_cdc_server` 启动，不使用 playground 的 `--ticdc`。
  - 现有 case 依赖 `--data-dir`、`--logsuffix`、`--addr`、`GO_FAILPOINTS` 以及多实例；`availability/capture.sh:93,124` 还会对 owner 执行 `kill -9` 与 `kill -STOP`。
  - 完成条件：PoC 脚本只接管 PD/TiKV/TiDB，`run.sh` 中的 CDC 启动逻辑不变。

## P1｜next-gen 启动脚本收敛

- [ ] 用 `tiup playground --mode nextgen` 复现 `_utils/start_tidb_cluster_nextgen` 的 upstream、downstream、keyspace 三套。
  - playground 已内置 tikv-worker 与 system role TiDB（tiup `components/playground/playground.go:1231-1244`），对应脚本中的 `_utils/start_tidb_cluster_nextgen:450` 与 system TiDB 部分。
  - 仍需脚本补齐：minio（`_utils/start_tidb_cluster_nextgen:174-182`）、keyspace 创建（`:132`）、多 keyspace 的额外 TiDB、`--cse.s3_endpoint`/`--cse.bucket`/`--cse.access_key`/`--cse.secret_key`。
  - 参考：远端分支 `tenfyzhong/feat-next-gen-integration-tests` 的 `tests/integration_tests/_utils/start_next_gen_cluster.sh` 已用 playground 起 next-gen，可对比参数。
  - 完成条件：至少一个 next-gen case 通过；`NEXT_GEN=1` 仍从 `_utils/start_tidb_cluster:22-23` 进入，脚本行数下降。

## P1｜CI 依赖与成本

- [ ] 确定 tiup 二进制来源。
  - 现状：CI 镜像未安装 tiup，`tests/scripts/download-integration-test-binaries.sh:34` 只从 fileserver 下载组件二进制。
  - 选项：镜像预装、prepare 阶段下载、或仅在启用 tiup 的 job 内安装。
  - 完成条件：新 job 在无外网环境下可运行（`--*.binpath` 加预置 `TIUP_HOME`）。

- [ ] 量化单 case 的额外开销。
  - 对比：`start_tidb_cluster_impl` 与「两套 playground + `tiup clean`」的启动和清理耗时。
  - 判断标准：单 case 增加超过 5 秒时，只对 next-gen 或长跑 case 启用，不做全量切换。

## P2｜落地与回退

- [ ] 用环境变量切换实现，默认路径不变。
  - `_utils/start_tidb_cluster:22-23` 已有 `NEXT_GEN` 分流，可增加同类开关（如 `USE_TIUP=1`）指向新脚本。
  - 完成条件：不设开关时 `run.sh` 行为与现在一致；CI 新增一组 tiup job，现有 job 不受影响。

- [ ] 记录取舍并保留回退方式。
  - 在 `docs/agents/testing.md` 写明两条启动路径的适用条件与对应环境变量。
  - 回退：删除 `start_tidb_cluster_tiup`、`USE_TIUP` 分支与新增 job，`start_tidb_cluster` 默认分支无需改动。
