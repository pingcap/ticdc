# Repository Guidelines

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
- `make integration_test_kafka CASE=<name>` (and `*_mysql|*_storage|*_pulsar`): run integration suites; requires binaries in `bin/` (`make check_third_party_binary`).

## Coding Style & Naming Conventions

- Go: keep `gofmt` clean; use `make fmt` before pushing.
- Naming:
  - Functions: use camel case and **do not** include `_` (e.g. `getPartitionNum`, not `get_partition_num`).
  - Variables: use lowerCamelCase (e.g. `flushInterval`, not `flush_interval`).
- Logging: structured logs via `github.com/pingcap/log` + `zap` fields; message strings should **not** include function names and should avoid `-` (use spaces instead).
- Errors: when an error comes from a third party/library call, wrap it immediately with `errors.Trace(err)` or `errors.WrapError(...)` to attach a stack trace; upstream callers should propagate wrapped errors without wrapping again.

## Testing Guidelines

- Unit tests: `*_test.go`, favor deterministic tests; use `testify/require`.
- Failpoints: `make unit_test` enables/disables automatically. If you enable manually, disable before committing to avoid a dirty tree.

## Commit & Pull Request Guidelines

- Commit/PR title format (see `CONTRIBUTING.md`): `<subsystem>[,subsystem2]: <what changed>` or `*: <what changed>`. Subject ≤70 chars; wrap body at ~80.
- PRs should follow `.github/pull_request_template.md` (include `Issue Number:` line, select tests, and fill the `release-note` block).
