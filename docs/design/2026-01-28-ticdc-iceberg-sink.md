---
title: TiCDC Iceberg Sink (Glue-first)
summary: Design and usage notes for exporting TiDB changes into Apache Iceberg tables on S3 using AWS Glue catalog.
---

# TiCDC Iceberg Sink (Glue-first)

This document describes the **TiCDC Iceberg sink** (`iceberg://`) that exports TiDB changes into **Apache Iceberg** tables stored in an **S3 warehouse**, with **AWS Glue Data Catalog** as the default catalog for S3.

## Scope and current status

- **Storage**: S3 warehouse (required for `catalog=glue`).
- **Catalogs**
  - `glue`: AWS Glue Data Catalog (recommended on AWS; default when `warehouse` is `s3://...` and `catalog` is not specified).
  - `hadoop`: Hadoop catalog (metadata files in the warehouse; mainly for local/dev with `file://...`).
  - `rest`: reserved (not implemented yet).
- **Modes**
  - `append`: append-only changelog table (low risk).
  - `upsert`: produces Iceberg v2 **equality deletes** for PK / NOT NULL unique-key tables (merge-on-read).
- **File format**: Parquet data/delete files (Iceberg metadata/manifest files are Avro as per spec).
- **DDL support**:
  - schema evolution via `EnsureTable` for safe DDL
  - `RENAME TABLE(S)` supported for `catalog=glue`
  - `TRUNCATE TABLE` supported as an overwrite-to-empty snapshot
  - partition operations are blocked (fail-fast)
- **Partitioning**:
  - Supports Iceberg partition specs in table metadata (new tables), for example:
    - `days(_tidb_commit_time)` (default when `emit-metadata-columns=true`)
    - `hours(_tidb_commit_time)`
    - `months(_tidb_commit_time)` / `years(_tidb_commit_time)`
    - `identity(col)` (shorthand: `col`)
    - `truncate(col, 16)`
    - `bucket(<column>, <n>)`
  - `days(_tidb_commit_time)` requires `emit-metadata-columns=true`.
  - `bucket` supports `int|long|string|binary|decimal` source columns.

## Sink URI parameters

Minimal example (Glue + S3):

`iceberg://?warehouse=s3://<bucket>/<warehouse>&catalog=glue&region=<aws-region>&namespace=<ns>`

Common parameters:

- `warehouse` (required): `s3://...` or `file:///...`
- `catalog`: `glue|hadoop` (default: `glue` for `s3://...`, otherwise `hadoop`)
- `region`: AWS region for `catalog=glue` (or set `AWS_REGION`)
- `database` (optional, Glue): fixed Glue database name override
- `namespace`: root namespace used in the warehouse path and Glue name derivation
- `mode`: `append|upsert`
- `commit-interval`: commit cadence (for example `30s`)
- `target-file-size`: bytes, target data file size
- `auto-tune-file-size`: `true|false` (default: `true` for `mode=upsert`, otherwise `false`), adjusts file splitting based on observed file sizes
- `partitioning`: partition spec expression, for example:
  - `partitioning=days(_tidb_commit_time)` (default for `mode=append` when `emit-metadata-columns=true`)
  - `partitioning=hours(_tidb_commit_time)`
  - `partitioning=identity(id)` (or `partitioning=id`)
  - `partitioning=truncate(name,16)`
  - `partitioning=bucket(id,16)`
  - `partitioning=none` (unpartitioned)
- `schema-mode`: schema evolution policy:
  - `strict` (default): fail-fast on any column type change for an existing field ID
  - `evolve`: allow safe type widening (for example `int` → `long`, `float` → `double`, decimal precision increase with the same scale)
- `emit-metadata-columns`: `true|false`
- `enable-checkpoint-table`: `true|false` (default: `false`)
- `enable-global-checkpoint-table`: `true|false` (default: `false`)
- `allow-takeover`: `true|false` (default: `false`)
- `max-buffered-rows`: max buffered rows before fail-fast (default: `0` unlimited)
- `max-buffered-bytes`: max estimated buffered bytes before fail-fast (default: `0` unlimited)
- `max-buffered-rows-per-table`: per-table buffered rows limit (default: `0` unlimited)
- `max-buffered-bytes-per-table`: per-table buffered bytes limit (default: `0` unlimited)

The same parameters can also be set in `sink.iceberg-config` in the changefeed config file.

## Glue naming rules

Glue has a `(database, table)` namespace. TiDB has `(schema, table)`.

Default mapping (no `database` override):

- Glue database: `${namespace}_${tidb_schema}` (sanitized to `[a-z0-9_]+`)
- Glue table: `${tidb_table}` (sanitized)

With `database=<fixed_db>`:

- Glue database: `<fixed_db>` (sanitized)
- Glue table: `${tidb_schema}__${tidb_table}` (sanitized)

Sanitization:

- Lowercase; non-alphanumeric characters collapse to `_`
- Trim leading/trailing `_`
- If it starts with a digit, prefix with `t_`

## Table layout in the warehouse

For each TiDB table:

- Table root: `<warehouse>/<namespace>/<schema>/<table>/`
- Iceberg metadata: `metadata/`
- Data/delete files: `data/`

The sink updates Glue’s `metadata_location` parameter to the latest `metadata/vN.metadata.json`.

## Mode semantics

### `append`

- Writes one change row per DML with `_tidb_op`, `_tidb_commit_ts`, `_tidb_commit_time` (when enabled).
- Intended for Spark consumers to build their own “latest view” downstream.

### `upsert`

- Requires a **primary key** or **NOT NULL unique key** (TiCDC “handle key”).
- Partitioning must be **unpartitioned** or derived only from handle key columns (for example `bucket(pk,16)`), otherwise equality deletes may not be applied correctly.
- INSERT/UPDATE-after → data file
- DELETE/UPDATE-before → Iceberg v2 **equality delete** file (by handle key columns)
- Commit model:
  - data manifest: `content=data`, `sequence_number=new_seq`
  - delete manifest: `content=deletes`, `sequence_number=new_seq`, `min_sequence_number=base_seq`
  - delete entries use `sequence_number=base_seq` and `file_sequence_number=new_seq` to avoid “self-deletes”.

## Bootstrap helper (optional)

Phase 2 includes an operator-facing bootstrap helper that writes a TiDB snapshot into Iceberg and prints a recommended changefeed `start-ts`.

Tool: `cmd/iceberg-bootstrap`

Example:

```bash
go run ./cmd/iceberg-bootstrap \
  --sink-uri 'iceberg://?warehouse=s3://bucket/wh&catalog=glue&region=us-west-2&namespace=ns&mode=upsert' \
  --tidb-dsn 'user:pass@tcp(host:4000)/?charset=utf8mb4' \
  --schema sales \
  --table orders \
  --batch-rows 10000
```

Notes:
- The tool uses `@@tidb_current_ts` as the snapshot point and sets `@@tidb_snapshot` while reading.
- It writes in the same mode as the sink URI (`mode=append` or `mode=upsert`).
- For `mode=upsert`, the table must have a PK or NOT NULL unique key.

## DDL guardrails

Supported:

- Safe schema evolution via `EnsureTable` (add/drop/rename columns when TiDB DDL is compatible).
- `RENAME TABLE(S)` for `catalog=glue` (catalog-level rename; table location stays the same).
- `TRUNCATE TABLE` as an overwrite-to-empty snapshot.

The sink fails fast (returns an error) for DDL actions that would otherwise produce inconsistent tables:

- `TRUNCATE PARTITION`
- partition add/drop/reorg/exchange/partitioning changes

`DROP TABLE` / `DROP DATABASE` events are ignored for now.

## Single-writer ownership and takeover

To reduce the risk of metadata corruption, the sink treats each Iceberg table as **single-writer** by default.

- The sink records ownership in Iceberg table properties using:
  - `tidb.changefeed_id` (display name: `keyspace/name`)
  - `tidb.changefeed_gid` (internal GID; informational)
- If a table is already owned by another changefeed, the sink fails fast unless `allow-takeover=true` is set.

## Checkpoint index table (optional)

When `enable-checkpoint-table=true`, the sink appends a row into an internal Iceberg table after each successful table commit:

- Location: `<warehouse>/<namespace>/__ticdc/__tidb_checkpoints/`
- Contents: `changefeed_id`, `keyspace`, `schema`, `table`, `table_id`, `resolved_ts`, `snapshot_id`, `commit_uuid`, `metadata_location`, `committed_at`

This can be used by Spark jobs to pick a repeatable “cut” across multiple tables by joining on `resolved_ts`.

Notes:

- The checkpoint table is append-only and best-effort. It is not an atomic cross-table commit boundary.
- The checkpoint table uses the same catalog (Glue/Hadoop) and warehouse as normal tables.

## Global checkpoint table (optional)

When `enable-global-checkpoint-table=true`, the sink appends a row after each successful commit round:

- Location: `<warehouse>/<namespace>/__ticdc/__tidb_global_checkpoints/`
- Contents: `changefeed_id`, `keyspace`, `resolved_ts`, `committed_at`

This can be used by consumers to enumerate candidate `resolved_ts` cut points.

## Metrics

In addition to common TiCDC sink metrics, the Iceberg sink exposes per-table metrics:

- `ticdc_sink_iceberg_global_resolved_ts`
- `ticdc_sink_iceberg_commit_round_duration_seconds`
- `ticdc_sink_iceberg_commit_duration_seconds`
- `ticdc_sink_iceberg_last_committed_resolved_ts`
- `ticdc_sink_iceberg_resolved_ts_lag_seconds`
- `ticdc_sink_iceberg_last_committed_snapshot_id`
- `ticdc_sink_iceberg_commit_conflicts_total`
- `ticdc_sink_iceberg_commit_retries_total`
- `ticdc_sink_iceberg_files_written_total{type=data|delete}`
- `ticdc_sink_iceberg_bytes_written_total{type=data|delete}`
- `ticdc_sink_iceberg_buffered_rows`
- `ticdc_sink_iceberg_buffered_bytes`

## Spark read (Glue catalog)

Typical Spark Iceberg catalog settings (example):

```text
spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue.warehouse=s3://<bucket>/<warehouse>
spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Then read:

```text
SELECT * FROM glue.<db>.<table>
```

## Maintenance (recommended for long-running upsert workloads)

Upsert mode generates equality delete files over time. Without periodic maintenance, query performance and cost will degrade.

Example Spark SQL procedures (Iceberg) to run periodically:

```sql
CALL glue.system.rewrite_data_files(
  table => 'glue.<db>.<table>',
  options => map('target-file-size-bytes','536870912')
);

CALL glue.system.rewrite_delete_files(
  table => 'glue.<db>.<table>'
);

CALL glue.system.rewrite_manifests(
  table => 'glue.<db>.<table>'
);

CALL glue.system.expire_snapshots(
  table => 'glue.<db>.<table>',
  retain_last => 100
);

CALL glue.system.remove_orphan_files(
  table => 'glue.<db>.<table>',
  older_than => TIMESTAMP '2026-01-01 00:00:00'
);
```
