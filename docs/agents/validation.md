# Validation Guidelines

Read this before choosing build, test, lint, or integration checks.

## Principles

- Start with the narrowest command that covers the changed behavior, then broaden only when risk or confidence requires it.
- Prefer package-scoped unit tests for code changes and reserve full-suite or integration tests for cross-component, protocol, sink, or deployment behavior.
- If a command is too expensive or requires unavailable services, state what was not run and why in the final response.
- Do not fix unrelated failures. Capture the failing command and the first relevant failure, then report it separately.

## Common Commands

- Build the main binary with `make cdc`.
- Format Go, shell, imports, and log style with `make fmt`.
- Run pre-submit checks with `make check`.
- Run all unit tests with `make unit_test`.
- Run focused unit tests with `make unit_test_pkg PKG=./pkg/sink/...`.
- Run targeted integration suites with `make integration_test_mysql CASE=<name>`, `make integration_test_kafka CASE=<name>`, `make integration_test_storage CASE=<name>`, or `make integration_test_pulsar CASE=<name>`.

## Task-to-Validation Matrix

- `pkg/` library changes: run `make unit_test_pkg PKG=./pkg/<package>/...`; broaden to `make unit_test` for shared utilities used widely.
- `downstreamadapter/sink/` changes: run the package unit tests, then the matching sink integration suite when behavior crosses process or external-system boundaries.
- `coordinator/`, `maintainer/`, or scheduling changes: run focused package tests and consider `make unit_test` when ownership, lifecycle, or concurrency invariants change.
- `logservice/` changes: run focused logservice package tests; consider broader unit tests for eventstore, logpuller, schema, or txn boundary changes.
- API, config, or CLI changes: run focused package tests and `make cdc` when command wiring or config loading changes.
- Documentation-only changes: unit tests are usually unnecessary; run markdown or whitespace checks when practical and state that only documentation changed.

## Reporting

In the final response, include the commands run, their result, and any checks skipped with a short reason.
