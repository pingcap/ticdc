# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## IMPORTANT: Git Policy

**CRITICAL**: This file (`CLAUDE.md`) must **NEVER** be committed to git or included in any commits.

- ❌ Do NOT run `git add CLAUDE.md`
- ❌ Do NOT include CLAUDE.md in any commit
- ❌ Do NOT push CLAUDE.md to any remote repository
- ✅ This file should remain local only
- ✅ Add it to `.gitignore` if needed

## Communication Language

**Important**: Please communicate with me in **Chinese (中文)** during our conversations.

- Use Chinese for all explanations, discussions, and responses to my questions
- Code comments and documentation must still be in English (as required by project standards)
- Commit messages should be in English
- Technical discussions with me should be in Chinese

## Project Overview

TiCDC is a complete architectural redesign of PingCAP's change data capture system. It pulls change logs from TiDB clusters and pushes them to downstream systems (MySQL, TiDB, Kafka, Pulsar, Cloud Storage). This new architecture is built for:

- **Better scalability**: Support 1M+ tables
- **More efficiency**: Lower resource consumption for high volume
- **Better maintainability**: Simpler, human-readable code with clear modules
- **Cloud native**: Designed from ground up for cloud deployment

**Important**: This is the NEW architecture (replacing the old `tiflow` repository). The codebase requires Go 1.23.2+.

## Project Status

This is an **actively developed production codebase**. The architecture and implementation are continuously evolving. When working with this codebase:

- **Stay adaptive**: Your understanding of the codebase should evolve as you discover new patterns or changes
- **Verify assumptions**: Code structure may have changed since this document was written
- **Update knowledge**: If you find architectural changes, adjust your understanding accordingly
- **Check recent commits**: Recent changes may not be reflected in this document

## Code Standards

### Language Requirements

**CRITICAL**: All code comments, documentation, and identifiers must be in **English only**.

- ✅ Correct: `// Process events from the dispatcher`
- ❌ Wrong: `// 处理来自 dispatcher 的事件`
- ✅ Correct: `func processEvent(event Event)`
- ❌ Wrong: `func 处理事件(event Event)`

**No Chinese characters are allowed in:**
- Code comments
- Function/variable names
- Documentation strings
- Log messages (use English for log messages)

### Production Code Quality

This is a **production system** serving critical data replication workloads. Code must meet high standards:

**1. Rigorous and Clean Code**
- Write clear, maintainable code with proper error handling
- Follow Go best practices and idioms
- Maintain consistent code style (enforced by `make fmt`)
- Avoid unnecessary complexity
- Use meaningful variable and function names

**2. Appropriate and Necessary Comments**
- **DO** comment complex algorithms or non-obvious logic
- **DO** comment public APIs and exported functions
- **DO** explain "why" decisions were made, not just "what" the code does
- **DO** document important invariants and assumptions
- **DON'T** add redundant comments that just repeat the code
- **DON'T** leave TODO comments without tracking issues

**Examples of good comments**:
```go
// CalculateCheckpointTs returns the minimum checkpoint timestamp across all
// dispatchers. This represents the safe point up to which all events have
// been successfully replicated to the downstream.
func (m *Maintainer) CalculateCheckpointTs() uint64 {
    // We must consider barrier state here because DDL events create
    // synchronization points that affect the checkpoint calculation.
    if m.barrier.IsBlocking() {
        return m.barrier.GetBlockTs()
    }
    return m.minCheckpointFromDispatchers()
}
```

**Examples of bad comments**:
```go
// Bad: Just repeats what the code says
// Get checkpoint ts
func (m *Maintainer) CalculateCheckpointTs() uint64 {

// Bad: Chinese comment
// 计算检查点时间戳
func (m *Maintainer) CalculateCheckpointTs() uint64 {
```

**3. Testing Requirements**
- Add unit tests for new functionality
- Ensure integration tests pass before committing
- Use failpoint for fault injection testing when appropriate
- Maintain or improve code coverage

**4. Error Handling**
- Always handle errors explicitly
- Provide meaningful error messages with context
- Use error wrapping to maintain error chains
- Log errors at appropriate levels

## Build Commands

```bash
# Build main TiCDC binary
make cdc

# Build with failpoint support (for testing)
make build-cdc-with-failpoint

# Build consumers
make kafka_consumer
make storage_consumer
make pulsar_consumer

# Generate patchable tar for deployment
cd bin && tar -czf newarch_cdc.tar.gz cdc
```

## Testing Commands

```bash
# Run all unit tests (with failpoint)
make unit_test

# Run unit tests for CI (with coverage)
make unit_test_in_verify_ci

# Run unit tests for specific package
make unit_test_pkg PKG=./pkg/sink/mysql/...

# Run integration tests
make integration_test_mysql          # MySQL sink tests
make integration_test_kafka          # Kafka sink tests
make integration_test_storage        # Cloud storage sink tests
make integration_test_pulsar         # Pulsar sink tests

# Run specific integration test case
make integration_test_mysql CASE=basic START_AT=basic

# Run integration test build
make integration_test_build
```

## Code Quality Commands

```bash
# Format code (gofumports, gci, shfmt)
make fmt

# Run static analysis
make check-static

# Check everything (copyright, fmt, tidy, mock, etc.)
make check

# Tidy go modules
make tidy

# Generate protobuf
make generate-protobuf

# Generate mocks
make generate_mock
```

## Commit Message Format

Follow the subsystem-based format:

```
<subsystem>: <what changed>

<why this change was made>
```

Examples:
- `maintainer: add comment for variable declaration`
- `dispatcher,sink: improve batch processing`
- `*: update dependencies`

Keep subject line under 70 characters. Wrap body at 80 characters.

## High-Level Architecture

TiCDC uses a **layered, decentralized architecture** with clear separation of concerns:

### Core Data Flow (5 Phases)

1. **Collection**: `LogService` pulls change logs from TiKV, stores in `EventStore` (Pebble DB)
2. **Distribution**: `EventService` routes events from `EventStore` to `EventCollector`
3. **Processing**: `Dispatcher` (one per table) receives events and applies transformations
4. **Sinking**: `Sink` implementations write to downstream (MySQL, Kafka, etc.)
5. **Coordination**: `Maintainer` manages per-changefeed state, `Coordinator` manages cluster-wide state

### Key Components

**Server** (`server/`): Top-level orchestrator
- HTTP/gRPC/TCP server management
- Leader election
- Module lifecycle management
- Critical startup order: preServices → networkModules → nodeModules → subModules

**Coordinator** (`coordinator/`): Global decision maker (one per cluster)
- Manages all changefeeds
- Processes heartbeats from Maintainers
- Implements changefeed state machine (created → running → stopped)
- Persists state to etcd
- Schedules GC operations

**Maintainer** (`maintainer/`): Per-changefeed task manager (one per changefeed)
- Schedules table spans to DispatcherManagers
- Calculates per-changefeed checkpoint TS
- Handles barriers (DDL blocking, sync points)
- Uses local `ReplicationDB` (SQLite) for span state
- Contains: Controller, Barrier, SpanController, OperatorController, Scheduler

**LogService** (`logservice/`): Upstream data collection
- **LogPuller**: Pulls logs from TiKV via CDC gRPC
- **EventStore**: Stores raw events in Pebble DB
- **SchemaStore**: Maintains table schema versions
- **SubscriptionClient**: Manages dispatcher subscriptions

**DownstreamAdapter** (`downstreamadapter/`): Per-table processing
- **DispatcherManager**: Manages multiple dispatchers per changefeed
- **Dispatcher**: One per table span, routes events to Sink
- **EventCollector**: Buffers events, applies flow control
- **Sink**: Writes to MySQL, Kafka, Pulsar, Cloud Storage, etc.

**EventService** (`pkg/eventservice/`): Event broker
- Creates EventBroker per cluster
- Handles dispatcher registration/removal/reset
- Bridges EventStore and EventCollector

**MessageCenter** (`pkg/messaging/`): Inter-node communication
- Local (in-process) and Remote (gRPC) messaging
- Pub/sub routing for events and commands
- Thread-safe async communication

### Key Design Patterns

**Per-Component State Management**: Each component maintains local state
- Maintainer: ReplicationDB (SQLite)
- EventStore: Pebble DB
- Coordinator: etcd
- Dispatcher: in-memory queue

**Barrier Pattern (DDL Coordination)**: Ensures atomic DDL application across all affected tables
1. EventService receives DDL → sends to affected Dispatchers
2. Dispatcher blocks writes at DDL commitTs
3. Dispatcher waits for ACK from EventService
4. On ACK, Dispatcher writes blocked DDL & resumes
5. Barrier tracks completion across all dispatchers

**Watermark Pattern (Progress Tracking)**:
- `checkpointTs`: Last safely applied event
- `resolvedTs`: Last ts with no pending transactions
- `seq`: Sequence for staleness detection

**Flow Control (Backpressure)**:
- Path-level (per dispatcher): Pause at 20%, resume at 10%
- Area-level (per changefeed): Pause at 80%, resume at 50%
- Circuit breaker: Discard oldest events if extreme congestion

**Operator Pattern**: Split/merge operations execute in phases (Prepare → Execute → Finish)

## State Storage Locations

| Component | State Type | Storage | Scope |
|-----------|-----------|---------|-------|
| Coordinator | Global changefeed state | etcd | Cluster-wide |
| Maintainer | Per-CF table assignments | ReplicationDB (SQLite) | Per node |
| Dispatcher | Event queue, barrier state | Memory | Per node |
| EventStore | Raw CDC events | Pebble DB | Per node |
| SchemaStore | Table schema versions | Memory + Pebble | Per node |

## Important File Paths

**Entry points**:
- `cmd/cdc/main.go` - Main server binary
- `cmd/cdc/server/server.go` - Server startup

**Core coordination**:
- `server/server.go` - Module orchestration & startup order
- `coordinator/coordinator.go` - Global coordinator logic
- `coordinator/controller.go` - Changefeed state machine
- `maintainer/maintainer.go` - Per-changefeed control
- `maintainer/barrier.go` - DDL barrier coordination

**Event flow**:
- `logservice/logpuller/log_puller.go` - TiKV CDC client
- `logservice/eventstore/event_store.go` - Event storage
- `pkg/eventservice/event_service.go` - Event routing
- `downstreamadapter/eventcollector/event_collector.go` - Event buffering & flow control
- `downstreamadapter/dispatcher/basic_dispatcher.go` - Per-table dispatcher

**Sink implementations**:
- `downstreamadapter/sink/mysql/` - MySQL/TiDB sink
- `downstreamadapter/sink/kafka/` - Kafka sink
- `downstreamadapter/sink/cloudstorage/` - Cloud storage sink

**Protocol definitions**:
- `heartbeatpb/heartbeat.proto` - Heartbeat messages
- `eventpb/event.proto` - Event messages

**Configuration**:
- `pkg/config/server_config.go` - Server configuration
- `pkg/config/changefeed_config.go` - Changefeed configuration

## Debugging Entry Points

| Issue | Entry File | Key Variables |
|-------|-----------|--------------|
| Events not flowing | `pkg/eventservice/event_broker.go` | `brokers map`, `registered dispatchers` |
| Slow checkpoint | `maintainer/barrier.go` | `blockState`, `checkpointTs` |
| Memory usage | `downstreamadapter/eventcollector/event_collector.go` | `pendingEvents`, flow control thresholds |
| Dispatcher stuck | `downstreamadapter/dispatcher/basic_dispatcher.go` | `handleEvents()`, queue state |
| Replication lag | `maintainer/maintainer.go` | `watermark`, heartbeat collection |
| Schema mismatch | `logservice/schemastore/` | Schema version tracking |

## Failpoint Testing

This codebase uses failpoint for fault injection testing:

```bash
# Enable failpoints (transforms code)
make failpoint-enable

# Build with failpoints
make build-cdc-with-failpoint

# Disable failpoints (restore code)
make failpoint-disable
```

**Important**: Always run `make failpoint-disable` after testing, or you'll have modified source files.

## Metrics & Observability

Key metrics in `pkg/metrics/`:
- `EventStoreReceivedEventCount`: Events stored
- `EventStoreReadDurationHistogram`: Read latency
- `DispatcherEventCounter`: Events processed per dispatcher
- `HeartbeatCollectorChannelSize`: Heartbeat queue depth
- `BarrierStateUpdateCounter`: Barrier progress

Grafana dashboard: `<cluster-name>-TiCDC-New-Arch`

## Common Pitfalls

1. **Module startup order matters**: See `server/server.go` for correct ordering
2. **Graceful shutdown**: Must close in reverse order of startup
3. **Network timeouts**: MessageCenter can be congested, handle timeouts
4. **Async state propagation**: etcd updates are not instant
5. **Mixing timestamps**: Different scopes (node vs cluster, startTs vs checkpointTs)
6. **Barrier delays**: DDLs cause temporary stops across all affected tables

## Configuration Notes

**Enable new architecture** (required):
```bash
tiup cluster edit-config <cluster-name>
# Add under cdc_servers:
config:
  newarch: true
```

**Key server flags**:
- `--newarch`: Enable new architecture
- `--cluster-id`: Cluster identifier
- `--pd`: PD endpoints
- `--data-dir`: Local state storage directory

**Changefeed config**:
- `SinkURI`: Downstream connection string (mysql://, kafka://, s3://, etc.)
- `EnableTableAcrossNodes`: Allow table splitting across nodes
- `GcTTL`: GC safety duration

## How Scalability is Achieved

1. **Decentralized control**: Each Maintainer independently manages its changefeed
2. **Per-table isolation**: Each Dispatcher independently processes one table
3. **Async messaging**: MessageCenter enables non-blocking inter-node communication
4. **Layered storage**: etcd for global state, Pebble for events, memory for buffers
5. **Graceful degradation**: Flow control and circuit breakers prevent cascading failures
6. **Barrier coordination**: DDL blocking ensures atomic multi-table updates

The system avoids central bottlenecks and maintains consistent local state across nodes, enabling support for 1M+ tables.

## Example: DML Event Flow

```
TiDB COMMIT
  → TiKV CDC log
  → LogPuller (pull by region)
  → EventStore (store with key: region|ts)
  → EventService (dispatcher subscription match)
  → EventBroker (fetch & deliver)
  → EventCollector (buffer & route)
  → Dispatcher (receive & transform)
  → Sink (MySQL/Kafka/etc)
  → Report checkpoint TS → Maintainer → Coordinator
```

## Example: DDL Event Flow with Barrier

```
TiDB DDL COMMIT
  → LogPuller → EventStore
  → EventService → EventBroker
  → EventCollector → All affected Dispatchers
  → Dispatcher.Barrier (BLOCK state)
  → Dispatcher waits (no writes until all tables ready)
  → EventService sends DDL event
  → Dispatcher writes DDL to Sink
  → Dispatcher sends ACK back
  → EventService collects all ACKs
  → Barrier transitions to DONE
  → All affected dispatchers resume normal operation
```
