# Materialized view behavior

This document describes how TiCDC handles TiDB materialized views (MVs) in the
8.5.8 materialized-view branch. TiCDC treats an MV as a physical table for
changefeed routing. It does not reproduce the MV definition or refresh
schedule downstream.

## DDL and table routing

| Source event | TiCDC behavior | Downstream behavior |
| --- | --- | --- |
| `CREATE MATERIALIZED VIEW` | Records the MV's table metadata and adds its physical table to the changefeed when it matches the table filter and eligibility rules. | Kafka sends `CREATE TABLE` for the MV name. The generated statement contains visible columns, their types, `NOT NULL` where applicable, and the primary key. It does not contain the MV query, refresh settings, or secondary indexes. Other sinks ignore this DDL. |
| `DROP MATERIALIZED VIEW` | Handles TiDB's `ActionDropTable` event and removes the MV's physical table. | Kafka sends `DROP TABLE` for the MV name. Other sinks ignore this DDL. |
| `CREATE MATERIALIZED VIEW LOG` | Tracks the log table in schema metadata, but excludes it from physical table dispatchers. Its create and drop events do not reach the downstream sink. | No log table is created or dropped. |
| TiDB internal `ActionCreateMaterializedViewShadow` | Tracks the shadow table in schema metadata, but excludes it from physical table dispatchers while it is a shadow table. | The shadow table's create event is not sent. |
| `REFRESH MATERIALIZED VIEW ... COMPLETE OUT OF PLACE` cutover | Replaces the old MV physical table ID with the shadow table ID under the MV name. For a partitioned MV, it replaces the old partition IDs with the new ones. | The cutover DDL is marked `NotSync` and is not sent to the sink. |

`ActionCreateMaterializedViewShadow` is not a standalone, user-facing DDL.
TiDB generates this internal action while executing
`REFRESH MATERIALIZED VIEW ... COMPLETE OUT OF PLACE`: it creates a temporary
physical table, loads the refreshed MV result into it, and then performs the
cutover in the following row. Users only issue the `REFRESH MATERIALIZED VIEW`
statement; there is no separate SQL statement for creating the shadow table.

MV create and drop DDLs are emitted only to Kafka. Other downstream sinks,
including the redo sink, advance the DDL event locally without writing it. The
events therefore continue to update TiCDC's schema metadata, table routing,
barriers, and checkpoints, but they are not executed or persisted by those
sinks. MV DML replication is unchanged; a non-Kafka downstream that receives
MV DML must already have a compatible target table.

The current physical MV table follows the same table filter and eligibility
rules as an ordinary table. Without `force-replicate`, it needs a primary key or
a non-null unique key. On restart or changefeed bootstrap, TiCDC reconstructs
the current MV table ID from schema history and continues to exclude MV log
and shadow tables from the physical table list.

This change has a dedicated refresh handler for the complete out-of-place
cutover. Shadow-table rows are not dispatched while the table is a shadow.

## Refresh data boundary

The cutover adds a dispatcher for the new physical table at the cutover commit
timestamp. It does not snapshot or backfill rows written to the shadow table
before that timestamp, and it does not run a refresh on the downstream. The
generated `CREATE TABLE` likewise does not copy any rows already present when
the MV is created. If the downstream needs the initial MV contents or the full
result of an out-of-place refresh, those contents require a separate load.

MV log table drops are suppressed. A standalone shadow-table drop is currently
handled by the general `DROP TABLE` path, so it can produce a downstream drop
event even though shadow-table creation is suppressed.

## Event filters

`CREATE MATERIALIZED VIEW` uses the `create table` event type for filtering.
The out-of-place cutover has the TiCDC-local event type
`mview refresh out of place cutover`; it can be named in an `ignore-event` rule,
and `ignore-sql` can match its SQL. The cutover is never written to the sink,
regardless of these rules, because TiCDC uses it to update table routing only.
