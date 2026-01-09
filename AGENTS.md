# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Developing Environment Tips

### Prerequisites

- Go (see `go.mod`)
- `git`, `make`
- For code generation: `protoc` (optional; `make generate-protobuf` uses tools under `tools/`)
- For integration tests: TiDB/TiKV/PD binaries and related tools in `bin/` (see `make check_third_party_binary`)

### Code Organization

**Top-level:**

- `/cmd/` - Binary entry points (`cdc`, consumers, helpers).
- `/server/` - Server runtime modules: leader election, HTTP/gRPC modules, watchers.
- `/coordinator/` - Changefeed coordinator/controller and scheduling.
- `/maintainer/` - Maintainer role: barrier/replica scheduling and orchestration.
- `/downstreamadapter/` - Dispatcher and sink adapters for downstream systems.
- `/logservice/` - Log pulling, schema store, and event storage.
- `/pkg/` - Reusable libraries (config, etcd, messaging, sink, scheduler, redo, filter, metrics, ...).
- `/tests/integration_tests/` - Script-driven integration test suites.
- `/tools/` and `/scripts/` - Build/test toolchain and helpers.
- `/docs/design/` - Design docs for major features/architecture.

## Building

```bash
# Build TiCDC server
make cdc

# Other binaries
make kafka_consumer
make storage_consumer
make pulsar_consumer
make oauth2_server
make filter_helper
make config-converter

# Build with the `nextgen` build tag
NEXT_GEN=1 make cdc
```

## Testing

### Unit Tests

Prefer the Makefile targets (they enable/disable failpoints and set tags):

```bash
# Run all unit tests
make unit_test

# Run unit tests for a specific package/path
make unit_test_pkg PKG=./pkg/sink/...
```

If you run `go test` directly, make sure failpoints are disabled afterward:

```bash
make failpoint-enable
go test -v --race --tags=intest ./...
make failpoint-disable
```

### Integration Tests

Integration tests live under `tests/integration_tests/` and are driven by `tests/integration_tests/run.sh`.

```bash
# Build the test binary used by integration tests
make integration_test_build

# Run a specific suite
make integration_test_mysql CASE=<case-name>
make integration_test_kafka CASE=<case-name>
make integration_test_storage CASE=<case-name>
make integration_test_pulsar CASE=<case-name>
```

Integration tests require third-party binaries (TiDB/TiKV/PD, etc.) available in `bin/`:

```bash
make check_third_party_binary
```

## Code Quality

```bash
# Formatting (imports, gofmt, shell scripts) + log style checks
make fmt

# Static analysis
make check-static

# `go mod tidy` checks
make tidy

# Full pre-submit checks (fmt/tidy/generate/checks)
make check
```

## Pull Request Instructions

### PR title

The PR title **must** follow one of these formats (same convention as commit messages):

**Format 1 (Specific subsystems):** `subsystem [, subsystem2]: what is changed`

**Format 2 (Repository-wide):** `*: what is changed`

Examples:

- `maintainer: fix barrier regression on reschedule`
- `coordinator, scheduler: reduce tick overhead`
- `*: update build tooling`

### PR description

The PR description **must** follow the template at `.github/pull_request_template.md` and **must** keep the HTML comment blocks unchanged.

Key requirements:

- There MUST be a line starting with `Issue Number:` linking relevant issues via `close #xxx` or `ref #xxx`.
- Under `#### Tests`, ensure at least one item is selected.
- Provide a `release-note` block (or `None` if not applicable).
