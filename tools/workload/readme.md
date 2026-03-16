# Workload Tool Usage Guide

This tool helps generate and manipulate test data for database performance testing.

## Prerequisites

- Go environment (1.16 or later recommended)

- Access to a target database (e.g., TiDB, MySQL)

## Building the Tool

```bash
cd tools/workload
make
```

## Common Usage Scenarios

### 0. DDL Workload

Run DDL workload based on a TOML config file:

```bash
./bin/workload -action ddl \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name test \
    -ddl-config ./ddl.toml \
    -ddl-worker 1 \
    -ddl-timeout 2m
```

`ddl.toml` example (fixed mode):

```toml
mode = "fixed"

tables = [
  "test.sbtest1",
  "test.sbtest2",
]

[rate_per_minute]
add_column = 10
drop_column = 10
add_index = 5
drop_index = 5
truncate_table = 1
```

`ddl.toml` example (random mode, omit `tables`):

```toml
mode = "random"

[rate_per_minute]
add_column = 10
drop_column = 10
add_index = 5
drop_index = 5
truncate_table = 0
```

### 1. Sysbench-style Data Insertion

Insert test data using sysbench-compatible schema:

```bash
./bin/workload -action insert \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name db1 \
    -total-row-count 100000000 \
    -table-count 1000 \
    -workload-type sysbench \
    -thread 32 \
    -batch-size 64
```

Notes:
- This example runs **DML only**. DDL is disabled by default unless `-ddl-config` is provided.

### 2. Large Row Update Workload

Update existing data with large row operations:

```bash
./bin/workload -action update \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name large \
    -total-row-count 100000000 \
    -table-count 1 \
    -large-ratio 0.1 \
    -workload-type large_row \
    -thread 16 \
    -batch-size 64 \
    -percentage-for-update 1
```

Additional parameters for update:

- large-ratio: Ratio of large rows in the dataset
- percentage-for-update: Percentage of rows to update

### 3. DML Only (Explicit)

Run DML only (no DDL) while using `write` mode:

```bash
./workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name db1 \
    -table-count 1000 \
    -workload-type sysbench \
    -thread 32 \
    -batch-size 64 \
    -only-dml
```

### 4. DML + DDL Together

Run DDL concurrently with DML by providing a DDL config:

```bash
./bin/workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name db1 \
    -table-count 1000 \
    -workload-type sysbench \
    -thread 32 \
    -batch-size 64 \
    -ddl-config ./ddl.toml \
    -ddl-worker 1 \
    -ddl-timeout 2m
```

### 5. DDL Only

Run only DDL (useful for testing DDL concurrency/replication without DML):

```bash
./bin/workload -only-ddl \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name db1 \
    -table-count 1000 \
    -workload-type sysbench \
    -ddl-config ./ddl.toml \
    -ddl-worker 1 \
    -ddl-timeout 2m
```

(Equivalent: `-action ddl`.)

### 6. Insert + Update + DDL Together (No Delete)

Run insert and update concurrently, and execute DDL in parallel:

```bash
./bin/workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name db1 \
    -table-count 1000 \
    -workload-type sysbench \
    -thread 32 \
    -batch-size 64 \
    -percentage-for-update 0.5 \
    -percentage-for-delete 0 \
    -ddl-config ./ddl.toml \
    -ddl-worker 1 \
    -ddl-timeout 2m
```

### 7. JSON Zstd Workload

Generate writes for `wide_table_with_json_primary` and `wide_table_with_json_secondary` (two tables per shard). Use `-row-size` to control payload width and `-table-count` to control shard count.

```bash
./workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name json_payload \
    -total-row-count 1000000 \
    -table-count 16 \
    -workload-type wide_table_with_json \
    -row-size $((16 * 1024)) \
    -json-payload-mode zstd \
    -thread 32 \
    -batch-size 32 \
    -percentage-for-update 0.5 \
    -percentage-for-delete 0.05
```

## Notes

- Ensure the database is properly configured and has the necessary permissions.
- Adjust the thread and batch-size parameters based on your needs.
- Use `-batch-in-txn` to wrap each batch in a single explicit transaction (BEGIN/COMMIT).
- For `wide_table_with_json`, use `-json-payload-mode zstd` to generate a zstd friendly JSON-like payload, or `-json-payload-mode random` for incompressible payloads.
- For workloads that support partitioned tables (e.g. bank3), set `-partitioned=false` to create non-partitioned tables.
- `-bank3-partitioned` is deprecated; use `-partitioned`.
