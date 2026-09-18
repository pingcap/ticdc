# allow-same-cluster Gate Design

- Author(s): [3AceShowHand](https://github.com/3AceShowHand)
- Last updated: 2026-09-18
- Status: Enable-time gates implemented; the runtime guard is open

## Background

TiCDC rejects a changefeed whose downstream is the same TiDB logical cluster as the upstream
(`IsSameUpstreamDownstream`), because such a changefeed can capture the writes of its own sink and
replicate them forever.

`allow-same-cluster` is the top-level changefeed config option that skips that check. Without an
additional limit, the option also permits the self-replication the check is meant to prevent, so
the option is only accepted when the changefeed provably cannot capture its own writes.

Unresolved:

- A table created after the changefeed exists is not covered by the enable-time gates.

## Invariant

Let `S` be the tables the changefeed replicates and `route(t)` the target name the dispatch rules
produce for table `t`. The changefeed cannot capture the writes of its own sink exactly when
`route(S) ∩ S = ∅`.

Two consequences drive the gates:

- A table without a route rule keeps its own name, so `route(t) = t`, which is in `S`. Every
  replicated table must therefore be routed.
- A route target that the filter replicates is in `S`. A routed table must therefore not collide
  with the replicated range, even when the target table does not exist yet.

## Gate 1: table routing must be enabled

`ReplicaConfig.ValidateAndAdjust` (`pkg/config/replica_config.go`) rejects `allow-same-cluster`
when `Sink.TableRouteEnabled()` is false, with
`[CDC:ErrInvalidReplicaConfig]allow-same-cluster requires table routing to be enabled`.

Create, update, and verify-table all call this function, both in the CLI pre-validation and on the
server, so the option fails fast before a changefeed is written. The gate does not depend on the
sink type, because routing itself is only allowed for MySQL-compatible sinks: the option has an
effect only for TiDB/MySQL downstreams, and a changefeed with an MQ or storage sink and the option
set is rejected instead of silently ignoring it.

## Gate 2: every replicated table must be routed outside the changefeed

The table-level check is `routing.ValidateSameClusterRouting`
(`downstreamadapter/routing/same_cluster.go`), called by `getVerifiedTables`
(`api/v2/changefeed.go`) right after `verifyRouteConflict`, and only when the option is set.

Its inputs are the dispatch rules, the tables the changefeed replicates (eligible tables, plus
ineligible tables when `force-replicate` is on), and the changefeed filter. For every replicated
table it wraps the dispatch rules in a `Router`, computes the target name, and rejects:

- a table that no route rule matches, or that a rule maps to itself;
- a table whose target is still matched by the filter.

The target is tested against the filter rather than against the current table list, so a target
table that appears later is rejected as well.

## New tables

The gates run when a changefeed is created, updated, or verified, so they cover the table set known
at that moment. A table created later that the filter matches and no route rule matches is
replicated into itself, and the sink writes are captured again.

Revisit when that case appears in practice: the same check can run where a dispatcher is created
(`downstreamadapter/dispatchermanager`), failing the changefeed with a configuration error instead
of replicating into itself.

## Verification

Unit tests:

- `pkg/config/replica_config_test.go`: `TestReplicaConfigTableRouteSupport` covers gate 1 — with
  routing, without routing, and with a Kafka sink.
- `downstreamadapter/routing/same_cluster_test.go`: `TestValidateSameClusterRouting` covers gate 2 —
  routing not enabled, no matching rule, a rule mapping the table to itself, a target inside the
  filter, and a target outside the filter.

Integration test `same_upstream_downstream`:

- a changefeed with `allow-same-cluster = true` and `allow_same_cluster_src.t1` routed to
  `allow_same_cluster_dst.t1_routed` is created against the upstream cluster and replicates its
  rows;
- create is rejected without table routing;
- create is rejected when the route target is still inside the filter range.
