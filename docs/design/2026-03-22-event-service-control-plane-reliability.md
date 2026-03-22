# Event Service Control Plane Reliability

## Background

`event collector` and `event service` currently mix three concerns in one loose protocol:

1. binding a dispatcher to one event service node
2. starting or rebuilding a stream
3. garbage collecting stale dispatcher state

The old protocol mostly relied on best-effort requests plus periodic `ReadyEvent`.
That works in the common case, but several failure modes are hard to reason about:

- register/reset/remove requests can be dropped
- event service rolling restart loses in-memory dispatcher state
- network partition can make both sides hold incompatible views
- remote-read introduces concurrent local/remote registrations
- inactivity cleanup is doing both GC and recovery, which makes ownership unclear

This document defines a clearer protocol with explicit control-plane convergence while
preserving the current data-plane model.

## Goals

- Make dispatcher lifecycle state explicit and idempotent.
- Preserve rolling restart for TiCDC when remote read is disabled.
- Keep `epoch` as the dispatcher generation concept.
- Introduce `event service incarnation` to identify a specific process lifetime.
- Ensure local-only mode does not depend on log coordinator.
- Support remote-read without turning log coordinator into a single point of failure.
- Make request loss recoverable by protocol, not by ad hoc timeout handling.

## Non Goals

- This change does not redesign log coordinator scheduling itself.
- This change does not remove `ReadyEvent` from the startup path.
- This change does not make remote read mandatory today.

## Terms

### Epoch

`epoch` is the dispatcher generation.

- Every rebuild must move to a strictly larger epoch.
- Data events are accepted only from the current epoch.
- `reset` is the operation that commits a new epoch on event service.

### Event Service Incarnation

`incarnation` identifies one event service process lifetime.

- It changes on every event service restart.
- It is attached to control-plane responses and stream bootstrap signals.
- It lets collector distinguish "same node, new process" from delayed old messages.

## Core Model

The protocol is split into two planes.

### Control Plane

- `register`
- `reset`
- `remove`
- `DispatcherControlEvent` ACKs

These messages are idempotent and retried until the collector observes convergence.

### Data Plane

- `ReadyEvent`
- `HandshakeEvent`
- DML / DDL / resolved / sync point

`ReadyEvent` remains the gate before `reset`, because that preserves the current
local/remote selection behavior.

## Target Lifecycle

```text
collector                           event service
---------                           -------------
register -------------------------> create dispatcher shell (epoch=0)
                <----------------- DispatcherControl(register, accepted)
                <----------------- ReadyEvent(incarnation)
reset(epoch=N) -------------------> bind stream generation N
                <----------------- DispatcherControl(reset, accepted)
                <----------------- HandshakeEvent(epoch=N, incarnation)
                <----------------- data events(epoch=N)
remove(epoch=N) ------------------> delete if epoch is not newer
                <----------------- DispatcherControl(remove, accepted/not_found)
```

Key rule: `register` allocates presence, `reset` starts or rebuilds the stream.

## Reliable Control Plane

### 1. Explicit ACKs

Event service replies with `DispatcherControlEvent` for:

- `register`
- `reset`
- `remove`

Status values:

- `accepted`
- `stale`
- `not_found`
- `rejected`

### 2. Collector Reconcile

Collector keeps pending control intent:

- pending register per target event service
- pending reset for the current rebuild epoch
- remove tombstones after dispatcher removal

A periodic reconcile loop resends pending control messages until:

- an ACK arrives, or
- an equivalent stream signal proves convergence

Examples:

- `ReadyEvent` clears pending register for that server
- `HandshakeEvent(epoch)` clears pending reset for that epoch
- `remove` tombstone is cleared by remove ACK

### 3. Remove Tombstone

`remove` must survive local dispatcher deletion.

After collector removes a dispatcher from its active map, it keeps a tombstone
containing:

- dispatcher ID
- target event service
- remove request payload

This lets collector retry `remove` even after the active dispatcher state is gone.

### 4. Lease/Inactivity Is GC Only

Event service may still delete long-inactive dispatchers, but this is only orphan GC.
It is not the primary correctness mechanism.

Primary convergence comes from:

- ACK + retry
- epoch fencing
- incarnation fencing

## Local Only Path

When remote read is disabled:

- collector always registers local event service immediately
- startup still waits for local `ReadyEvent`
- local event service restart is recovered by:
  - heartbeat response / reset not found / new incarnation signal
  - collector reserving a new epoch
  - re-register
  - resend reset with that epoch

This path does not require log coordinator.

## Remote Read Path

Log coordinator remains a placement hint source, not the owner of correctness.

### Selection Rules

- collector always registers local event service
- remote candidates are optional hints
- remote register uses `onlyReuse=true`
- local ready still has final priority over remote ready

### Why Coordinator Is Not SPOF

- local-only still progresses without coordinator
- after remote candidate is chosen, collector/event service converge directly
- request loss and restart recovery are handled by collector/event service protocol

Coordinator failure may reduce remote-read quality, but must not block local progress.

## Reset Semantics

`reset` is the real rebuild primitive.

Rules:

- new rebuild => new epoch
- repeated resend before handshake => same epoch
- event service treats same-epoch reset as idempotent
- stale lower-epoch reset must not override newer state

Collector behavior:

- if stream is broken while epoch `E` is active, collector allocates `E+1` immediately
- this fences off delayed old data before the new stream is ready

## Failure Handling

### Request Loss

- lost `register`: reconcile resends register until ready or register ACK
- lost `reset`: reconcile resends same-epoch reset until reset ACK or handshake
- lost `remove`: tombstone keeps retrying until remove ACK

### Event Service Restart

Restart changes `incarnation`.

Collector reaction:

1. treat old binding as lost
2. reserve or reuse pending rebuild epoch
3. re-register to the same node
4. wait for ready
5. resend reset for the reserved epoch

### Network Partition

If collector and event service partition:

- event service may GC the dispatcher
- collector may still believe the stream exists

After partition heals, collector must not continue the old session.

Correct recovery is:

1. discover missing state through heartbeat response or reset `not_found`
2. move to a new epoch
3. re-register
4. rebuild from checkpoint

### Lost Remove After Partition

If `remove` was sent but lost, tombstone retry continues after connectivity returns.
If the dispatcher is already gone, `not_found` ACK clears the tombstone.

## Event Service Idempotency Rules

### Register

- existing epoch > request epoch => `stale`
- existing epoch == request epoch => `accepted`
- creation failure => `rejected`

### Reset

- dispatcher absent => `not_found`
- existing epoch > request epoch => `stale`
- existing epoch == request epoch => `accepted` and no-op
- existing epoch < request epoch => rebuild and `accepted`

### Remove

- dispatcher absent => `not_found`
- existing epoch > request epoch => `stale`
- otherwise remove and `accepted`

## Implementation Scope

This change lands the first reliability phase:

- `DispatcherControlEvent`
- event service `incarnation`
- ACKs for register/reset/remove
- collector reconcile loop
- collector remove tombstones
- reset resend with stable epoch
- collector recovery from heartbeat removed / reset not found

Future work can build on the same protocol:

- richer remote-read selection policy
- stronger server-level incarnation tracking
- eventual simplification of legacy ready/not-reusable signaling

## Code Pointers

- `downstreamadapter/eventcollector/dispatcher_stat.go`
- `downstreamadapter/eventcollector/event_collector.go`
- `pkg/eventservice/event_broker.go`
- `pkg/common/event/dispatcher_control_event.go`
- `pkg/common/event/ready_event.go`
- `pkg/common/event/not_reusable_event.go`
- `pkg/common/event/handshake_event.go`
