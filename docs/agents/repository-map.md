# Repository Map

Read this when choosing where to make a change or which tests to run.

## Runtime Components

- `cmd/`: buildable binaries such as `cmd/cdc`, `cmd/kafka-consumer`, `cmd/storage-consumer`, and helper tools.
- `server/`: server bootstrap and runtime service wiring.
- `coordinator/`: changefeed metadata, scheduling coordination, operators, drain, and GC coordination.
- `maintainer/`: table/span replication ownership, scheduling, split/range checks, and replica lifecycle.
- `logservice/`: eventstore, logpuller, schema store, transaction utilities, and protobuf definitions for log service internals.
- `downstreamadapter/`: dispatcher orchestration, routing, event collection, sinks, and syncpoint handling.

## Shared Libraries

- `pkg/config`: configuration types and validation.
- `pkg/errors`: predefined TiCDC errors and error helpers.
- `pkg/sink`: shared sink implementations, codecs, and sink utilities.
- `pkg/filter`, `pkg/binlog-filter`, and `pkg/integrity`: filtering and integrity-related logic.
- `pkg/etcd`, `pkg/pdutil`, `pkg/security`, and `pkg/server`: external dependency clients and shared service utilities.
- `pkg/orchestrator`, `pkg/scheduler`, and `pkg/messaging`: shared control-plane and messaging primitives.

## Tests and Tooling

- `tests/integration_tests/`: script-driven integration suites for MySQL, Kafka, storage, and Pulsar.
- `scripts/`: generation, lint, formatting, and integration helper scripts.
- `tools/`: pinned local tooling used by Make targets.
- `metrics/`: Grafana and next-generation dashboard assets.

## Placement Rules

- Put component-owned behavior close to the owning component instead of adding cross-cutting helpers prematurely.
- Put shared code under `pkg/` only when at least two components need the same abstraction.
- Prefer existing package tests before adding a new test package or broader integration test.
