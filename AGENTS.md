# Repository Guidelines

## Project Structure & Module Organization
- `cmd/`: CLI 入口，包括 `cmd/cdc` 和辅助工具入口。
- `pkg/`: 核心库层，涵盖同步逻辑、sink 实现与共用工具包。
- `maintainer/`, `coordinator/`, `logservice/`: 独立服务模块，负责调度、维护与日志能力。
- `tests/`: 集成测试套件；查看 `tests/integration_tests/` 了解具体场景脚本。
- `docs/`, `deployments/`, `scripts/`, `tools/`: 文档、集群部署模板、自动化脚本与本地工具链；`bin/` 存放已构建二进制与第三方依赖。

## Build, Test, and Development Commands
- `make cdc`: 构建主二进制 `bin/cdc`，注入版本元信息。
- `make fmt`: 运行 `gci`、`gofumports`、`shfmt` 与日志风格检查，统一代码与脚本格式。
- `make tidy`: 执行 `./tools/check/check-tidy.sh`，保持 `go.mod`/`go.sum` 干净。
- `make unit_test_in_verify_ci`: 以 race 检测运行关键包单测，生成 `cdc-junit-report.xml` 与覆盖率。
- `make integration_test_mysql CASE=bank`: 使用下载的 TiDB/TiKV 二进制跑集成流程；`CASE` 指定场景，`START_AT` 可跳过前置步骤。

## Coding Style & Naming Conventions
- Go 代码遵循 `gofumports` 输出；新增文件请保持 4 空格缩进、包名小写、文件名使用下划线分隔关键字。
- 导入顺序交给 `make fmt`；提交前务必保持工作树干净以便 diff 审查。
- Shell/脚本遵循 `shfmt`；日志格式需通过 `scripts/check-log-style.sh`。

## Testing Guidelines
- 单测使用 Go `testing` + `gotestsum`；保持覆盖率文件位于 `$(TEST_DIR)/cov.unit.out` 并由脚本转换。
- 集成测试脚本位于 `tests/integration_tests/run.sh`；确保本地提供 `bin/tidb-server` 等依赖，可用 `make prepare_test_binaries` 下载。
- 新增测试请按照目录命名：Go 单测以 `_test.go` 结尾，集成场景放入独立子目录并提供 `README.md` 说明。

## Commit & Pull Request Guidelines
- 提交信息遵循 `component: concise summary (#issue)` 形式，例如 `maintainer: fix checkpointTs update (#2864)`。
- PR 需说明动机、主要变更、验证方式；关联 Issue/需求并在需要时附运行截图或日志片段。
- 确保通过 `make fmt` 与必要测试后再发起评审；若引入新依赖，请同步更新 `OWNERS` 与相关文档。

## Agent 协作约定
- 与仓库维护者沟通时统一使用中文；提交的代码、配置与日志字符串中禁止出现中文字符。
- 执行测试前优先根据改动范围挑选包级或用例级命令，避免无差别运行整仓库单测以节省时间和资源。
- 所有对话、状态更新与评审反馈均使用中文表述，评审结论也需提供中文版本。
- PR 标题与描述必须使用英文书写。
- 代码、配置项名称及日志内容保持英文，不得混入中文。
