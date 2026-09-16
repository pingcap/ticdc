# Generated Code Guidelines

Read this before changing protobufs, mocks, generated Go files, dashboards, or files produced by `go generate`.

## Principles

- Prefer editing the source definition and regenerating output over manually patching generated files.
- Keep generated diffs paired with the source change that requires them.
- If generation produces unrelated churn, stop and inspect before including it.
- Do not commit local tool binaries, build outputs, coverage files, or temporary artifacts.

## Generation Commands

- Protobufs: run `make generate-protobuf` after changing `eventpb/**/*.proto`, `heartbeatpb/**/*.proto`, or `logservice/logservicepb/**/*.proto`.
- Mocks: run `make generate_mock` after changing interfaces listed in `scripts/generate-mock.sh` or adding a mock to that flow.
- Go generate: run `make go-generate` after changing files whose generated output is controlled by `//go:generate`.
- Next generation Grafana dashboards: run `make generate-next-gen-grafana` after changing inputs consumed by `scripts/generate-next-gen-metrics.sh`.
- Full pre-submit generation check: run `make check` when a change may affect formatting, generated files, dashboards, Makefile formatting, or module tidiness.

## Review Checklist

- Confirm generated files are deterministic and limited to the intended source change.
- Confirm generated files are not manually edited without a source-of-truth update.
- Confirm newly required generated files are included in the diff.
- Confirm no tool downloads, binaries, or temporary files are included.
