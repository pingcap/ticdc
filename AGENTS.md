# Repository Guidelines

## Agent Change Discipline

- Make surgical changes: touch only files required by the task.
- Do not refactor adjacent code, rename symbols, or reformat unrelated files unless the task requires it.
- Prefer the simplest solution that solves the problem; do not add speculative abstractions, features, or configurability.
- Match existing style and patterns. Remove only unused imports, variables, functions, or files introduced by your change.
- If requirements are ambiguous, state the assumption or ask before coding.

## Project Structure & Module Organization

- `cmd/`: buildable binaries (e.g. `cmd/cdc`, `cmd/kafka-consumer`).
- `downstreamadapter/`: downstream adapters and sinks (e.g. `downstreamadapter/sink/kafka`).
- `pkg/`: shared libraries (config, codec, sink implementations, errors, utilities).
- `server/`, `coordinator/`, `maintainer/`, `logservice/`: runtime components and orchestration.
- `tests/integration_tests/`: script-driven integration test suites.
- `scripts/`, `tools/`: codegen, linting, and build/test tooling.
- `docs/design/`: design docs; `server.toml` is a config example.

## Build, Test, and Development Commands

- `make cdc`: build main binary to `bin/cdc`.
- `make fmt`: run `gci` + `gofumports` + `shfmt`, plus log-style checks.
- `make check`: pre-submit checks (fmt, tidy, codegen, dashboards, Makefile formatting).
- `make unit_test`: unit tests with race + failpoints enabled (uses `--tags=intest`).
- `make unit_test_pkg PKG=./pkg/sink/...`: narrow unit test scope.
- `make generate_mock`: regenerate gomock-based mocks via `scripts/generate-mock.sh`.
- `make integration_test_kafka CASE=<name>` (and `*_mysql|*_storage|*_pulsar`): run integration suites; requires binaries in `bin/` (`make check_third_party_binary`).

## Go Coding Rules

- Formatting: keep `gofmt` clean; use `make fmt` before pushing.
- Naming:
  - Functions: use camel case and **do not** include `_` (e.g. `getPartitionNum`, not `get_partition_num`).
  - Variables: use lowerCamelCase (e.g. `flushInterval`, not `flush_interval`).
- Imports: do not rename imports unless required to resolve a package name conflict or to follow an existing local convention.

## Errors

- Use predefined errors from the repository error package; keep the local import name consistent with surrounding code.
- When an error comes from a third-party or library call, wrap it immediately at the boundary with `errors.WrapError(predefinedError, err, args...)`.
- After an error has been wrapped with `errors.WrapError`, propagate it directly; do not call `errors.Trace` again on later paths.
- When creating a TiCDC error, use `GenWithStack...` or `GenWithStackByArgs...` on a predefined error and pass concrete details through arguments when needed.
- Decide whether a newly generated error needs stack information. If a stack is unnecessary, especially on hot paths, use `FastGen...` or `FastGenByArgs...`.
- Avoid other error creation or wrapping styles, including `errors.New`, bare `fmt.Errorf`, and adding stack information multiple times.

## Logging

- Use structured logs via `github.com/pingcap/log` with `zap` fields.
- Treat logs as operational signals, not control-flow comments. Keep normal paths quiet.
- Default `INFO`/`WARN` logs should record high-value lifecycle events, state changes, external dependency abnormalities, or invariant violations.
- Choose log levels by required action:
  - `ERROR`: correctness, availability, or key progress is affected and needs attention.
  - `WARN`: the system is abnormal but can continue through recovery, retry, fallback, or degraded behavior.
  - `INFO`: key lifecycle events, important state changes, important configuration, or summary information.
  - `DEBUG`: bounded, low-frequency diagnostics with clear troubleshooting value.
- Do not add `DEBUG` logs by default. Delete low-value logs instead of moving them to `DEBUG`.
- Keep `message` stable and concise: summarize what happened, why it happened, and what the system will do next.
- Put object details in stable camelCase `zap` fields such as `changefeedID`, `nodeID`, `dispatcherID`, `regionID`, `subscriptionID`, and `requestID`.
- Message strings should not include function names and should avoid `-` (use spaces instead).
- Avoid per-object or per-iteration logs, duplicated logs on the same error path, large objects, raw payloads, and long error dumps in default logs.
- Use metrics for counts, scale, frequency, and trends; use windowed summaries or representative samples for high-cardinality events.
- Before adding, keeping, or rewriting a log, verify that it answers a real diagnostic question, identifies the object, reason, action, and impact, and will not grow linearly with object count or retry/loop frequency.

## Testing Guidelines

- Unit tests: `*_test.go`, favor deterministic tests; use `testify/require`.
- Unit tests should cover meaningful behavior only; avoid redundant or low-value cases.
- Do not test across feature boundaries. Keep each test focused on the behavior owned by the package or component under test.
- Reuse existing tests when possible. Add a new test only when reuse would make the existing test unclear or incomplete.
- If several test functions are highly related, merge them into one concise table-driven or scenario-based test.
- When a test needs mocked components, prefer existing gomock-generated mocks over handwritten mocks. If the required mock does not exist, add it to the mock generation flow and run `make generate_mock`.
- Keep tests efficient, simple, focused, and easy to update.
- Failpoints: `make unit_test` enables/disables automatically. If you enable manually, disable before committing to avoid a dirty tree.
- For documentation-only changes, unit tests are usually unnecessary. If tests are skipped, state in the final response that only documentation was changed.

## Commit & Pull Request Guidelines

- Commit/PR title format (see `CONTRIBUTING.md`): `<subsystem>[,subsystem2]: <what changed>` or `*: <what changed>`. Subject ≤70 chars; wrap body at ~80.
- PRs should follow `.github/pull_request_template.md` (include `Issue Number:` line, select tests, and fill the `release-note` block).
