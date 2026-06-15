# Repository Guidelines

## Purpose & Boundaries

- Help implement, review, and document focused changes in this repository.
- Follow the user's task, keep changes minimal, and avoid unrelated refactors or cleanup.
- If requirements conflict or are unclear, surface the ambiguity before changing code.
- Do not commit, branch, or publish changes unless explicitly asked.

## Agent Change Discipline

- Make surgical changes: touch only files required by the task.
- Do not refactor adjacent code, rename symbols, or reformat unrelated files unless the task requires it.
- Prefer the simplest solution that solves the problem; do not add speculative abstractions, features, or configurability.
- Match existing style and patterns. Remove only unused imports, variables, functions, or files introduced by your change.
- Validate changes with targeted tests or checks when practical.

## Before Changing Code

- Identify the objective, non-objectives, touched module, and expected validation.
- Check the relevant detailed guide before changing errors, logs, tests, generated files, or cross-component behavior.
- Prefer package-owned changes and tests before adding shared abstractions or broad integration coverage.

## Detailed Guides

Read these only when relevant to the task:

- Repository map: use [docs/agents/repository-map.md](docs/agents/repository-map.md) to choose the owning module and nearby tests.
- Validation: use [docs/agents/validation.md](docs/agents/validation.md) to choose the narrowest sufficient build, test, or lint command.
- Generated code: use [docs/agents/generated-code.md](docs/agents/generated-code.md) before changing protobufs, mocks, dashboards, or generated files.
- Error handling: use predefined repository errors; see [docs/agents/error-handling.md](docs/agents/error-handling.md) before changing error creation, wrapping, or propagation.
- Logging: logs are operational signals; see [docs/agents/logging.md](docs/agents/logging.md) before adding, removing, or rewriting logs.
- Testing: prefer focused deterministic tests; see [docs/agents/testing.md](docs/agents/testing.md) before adding or changing tests.

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

## Go Coding Basics

- Formatting: keep `gofmt` clean; use `make fmt` before pushing.
- Naming:
  - Functions: use camel case and **do not** include `_` (e.g. `getPartitionNum`, not `get_partition_num`).
  - Variables: use lowerCamelCase (e.g. `flushInterval`, not `flush_interval`).
- Imports: do not rename imports unless required to resolve a package name conflict or to follow an existing local convention.

## Commit & Pull Request Guidelines

- Commit/PR title format (see `CONTRIBUTING.md`): `<subsystem>[,subsystem2]: <what changed>` or `*: <what changed>`. Subject ≤70 chars; wrap body at ~80.
- PRs should follow `.github/pull_request_template.md` (include `Issue Number:` line, select tests, and fill the `release-note` block).

## Final Response

- Summarize changed files and behavior.
- List validation commands run and results.
- State skipped checks with a short reason, especially for documentation-only changes.
