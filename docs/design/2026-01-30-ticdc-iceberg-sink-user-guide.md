---
title: TiCDC → Iceberg Sink User Guide (Glue-first)
summary: Step-by-step guide for bootstrapping and running TiCDC changefeeds into Apache Iceberg (AWS Glue + S3, or local Hadoop catalog for dev).
---

# TiCDC → Iceberg sink user guide (Glue-first)

This guide shows how to run TiCDC changefeeds into **Apache Iceberg** tables using the `iceberg://` sink.

For sink capabilities and config reference, see `docs/design/2026-01-28-ticdc-iceberg-sink.md`.

## 0) Choose your deployment mode

- **Production (recommended on AWS)**: `catalog=glue` + `warehouse=s3://...`
- **Local/dev**: `catalog=hadoop` + `warehouse=file:///...` (no AWS dependencies)

## 1) Prerequisites

### Production (Glue + S3)

- An S3 bucket/prefix to use as the Iceberg warehouse, for example `s3://my-bucket/my-warehouse/`.
- AWS credentials with permissions to:
  - read/write objects under the warehouse prefix
  - read/write Glue databases/tables (Glue Data Catalog)
- Spark with Iceberg runtime configured for Glue.

### Local/dev (Hadoop + file warehouse)

- Local filesystem path for the warehouse, for example `/tmp/iceberg_warehouse`.
- Spark with Iceberg runtime configured for Hadoop catalog (optional; you can validate by inspecting the warehouse files).

#### Install `spark-sql` (macOS)

```bash
brew install apache-spark
spark-sql --version
```

## 2) Decide table mode and changefeed settings

### Mode: `append`

- Use when you want a changelog table (one row per DML event).
- Works for tables without PK/UK.

### Mode: `upsert`

- Use when you want “latest row” semantics in Iceberg using **equality deletes** (Iceberg format v2).
- Requires each replicated table has a **primary key** or **NOT NULL unique key**.
- If your workload updates key columns, set `enable-old-value=true` for the changefeed so TiCDC can emit correct delete keys.
- Partitioning must be **unpartitioned** (`partitioning=none`) or derived only from the handle key columns (for example `bucket(pk,16)`), otherwise equality deletes may not be applied correctly.

## 3) Bootstrap (snapshot → Iceberg) (optional but recommended)

If you need an initial snapshot before streaming, use the bootstrap helper for a single table:

```bash
go run ./cmd/iceberg-bootstrap \
  --sink-uri 'iceberg://?warehouse=s3://bucket/wh&catalog=glue&region=us-west-2&namespace=ns&mode=upsert' \
  --tidb-dsn 'user:pass@tcp(host:4000)/?charset=utf8mb4' \
  --schema sales \
  --table orders \
  --batch-rows 10000
```

It prints a recommended `start-ts` you should use when creating the streaming changefeed.

Notes:
- The tool uses `@@tidb_current_ts` and reads data under `@@tidb_snapshot` to get a consistent snapshot.
- For multi-table bootstraps, run it per table (or use your existing snapshot export workflow) and start TiCDC at the snapshot boundary.

## 4) Create the changefeed (streaming)

Build TiCDC:

```bash
make cdc
```

### Production example (Glue + S3)

```bash
bin/cdc cli changefeed create \
  --sink-uri 'iceberg://?warehouse=s3://my-bucket/my-warehouse&catalog=glue&region=us-west-2&namespace=tidb_prod&mode=upsert&commit-interval=30s&enable-checkpoint-table=true&enable-global-checkpoint-table=true' \
  --start-ts <BOOTSTRAP_START_TS>
```

### Local/dev example (Hadoop + file)

```bash
bin/cdc cli changefeed create \
  --sink-uri "iceberg://?warehouse=file:///tmp/iceberg_warehouse&catalog=hadoop&namespace=dev&mode=append&commit-interval=5s" \
  --start-ts <START_TS>
```

Useful knobs:
- `partitioning=days(_tidb_commit_time)` (default for `mode=append` when `emit-metadata-columns=true`)
- `partitioning=none` or `partitioning=bucket(<pk>,16)` (recommended for `mode=upsert`)
- `max-buffered-rows`, `max-buffered-bytes` (fail-fast backpressure)
- `auto-tune-file-size=true` (default for `mode=upsert`, adjusts file splitting based on observed file sizes)
- `allow-takeover=true` (only for controlled ownership takeovers)

## 5) Verify output

### Warehouse layout

For a table `db.tbl`:

- Table root: `<warehouse>/<namespace>/<db>/<tbl>/`
- Iceberg metadata: `metadata/` (contains `vN.metadata.json`, manifest lists, manifests)
- Data + delete Parquet: `data/` (files like `snap-*.parquet`, and `delete-*.parquet` in upsert mode)

### Optional checkpoint tables

When enabled:
- Per-table checkpoint index: `<warehouse>/<namespace>/__ticdc/__tidb_checkpoints/`
- Global checkpoint list: `<warehouse>/<namespace>/__ticdc/__tidb_global_checkpoints/`

### Spark read examples

Glue catalog:

```text
spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue.warehouse=s3://my-bucket/my-warehouse
spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Then:

```sql
SELECT * FROM glue.<db>.<table>;
```

## 6) Maintenance (recommended for upsert)

Upsert mode generates equality delete files over time; periodic maintenance is required for good query performance.

Generate Spark SQL templates from Glue:

```bash
go run ./cmd/iceberg-maintenance --database <glue_db> --region <aws_region> --older-than '2026-01-01 00:00:00'
```

Run the generated procedures on a schedule (rewrite data/delete files, expire snapshots, remove orphan files).

## 7) Troubleshooting / runbook snippets

- **“table is owned by another changefeed”**: this is single-writer enforcement. Use `allow-takeover=true` only for controlled migrations.
- **Buffer limit errors** (`max-buffered-*`): increase the limits, decrease `commit-interval`, or reduce upstream write rate.
- **Unsupported DDL**: partition DDLs are blocked (fail-fast). Plan a manual backfill or rebuild if you must change partitioning.
- **Glue conflicts/retries**: check `ticdc_sink_iceberg_commit_conflicts_total` and `ticdc_sink_iceberg_commit_retries_total`.

## 8) Integration tests (local)

Two iceberg-focused integration tests are provided under `tests/integration_tests/`:

```bash
make integration_test_build
make integration_test_iceberg CASE="iceberg_append_basic iceberg_upsert_basic"
```

Optional Spark readback (for stronger validation):

- Install `spark-sql` and provide Iceberg Spark runtime (either via `--packages` or `--jars`).
- Then run:

```bash
ICEBERG_SPARK_READBACK=1 \
ICEBERG_SPARK_PACKAGES='org.apache.iceberg:iceberg-spark-runtime-<spark>_<scala>:<iceberg>' \
make integration_test_iceberg CASE="iceberg_append_basic iceberg_upsert_basic"
```

Example (Spark 4.1.x on macOS):

```bash
ICEBERG_SPARK_READBACK=1 \
ICEBERG_SPARK_PACKAGES='org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1' \
make integration_test_iceberg CASE="iceberg_append_basic iceberg_upsert_basic"
```
