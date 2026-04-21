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

Each DDL type is scheduled evenly across the minute instead of being burst-enqueued at the minute boundary.
For example, `truncate_table = 1` runs about once every 60s, and `add_column = 6` runs about once every 10s.

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

Prebuilt examples:

- `examples/ddl_truncate_table_mixed.toml`: periodically runs `TRUNCATE TABLE` while add/drop column and add/drop index continue in parallel.
- `examples/ddl_partition_table_mixed.toml`: targets partitioned `bank4` tables and mixes `TRUNCATE TABLE`, add/drop column, and add/drop index.

Truncate-table mixed DDL example:

```bash
./bin/workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name test \
    -workload-type sysbench \
    -table-count 4 \
    -thread 32 \
    -batch-size 64 \
    -ddl-config ./examples/ddl_truncate_table_mixed.toml \
    -ddl-worker 1 \
    -ddl-timeout 2m
```

Partition-table mixed DDL example (prepare 126-partition `bank4` tables first):

```bash
./bin/workload -action prepare \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name partition_ddl \
    -workload-type bank4 \
    -partitioned=true \
    -table-count 4 \
    -total-row-count 0

./bin/workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name partition_ddl \
    -workload-type bank4 \
    -partitioned=true \
    -table-count 4 \
    -thread 16 \
    -batch-size 64 \
    -percentage-for-update 0.5 \
    -percentage-for-delete 0.1 \
    -ddl-config ./examples/ddl_partition_table_mixed.toml \
    -ddl-worker 1 \
    -ddl-timeout 2m
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

### 7. Table Info Sharing Workload

Generate multiple tables with the same column layout and index layout, while making selected default values differ by table index. This workload covers a broad set of column types including numeric, bit, string, binary, temporal, enum/set, and JSON.

```bash
./workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name table_info \
    -table-count 16 \
    -workload-type table_info_sharing \
    -thread 32 \
    -batch-size 32 \
    -percentage-for-update 0.5 \
    -percentage-for-delete 0.1
```

### 8. Wide Table With JSON Workload

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
    -thread 32 \
    -batch-size 32 \
    -percentage-for-update 0.5 \
    -percentage-for-delete 0.05
```

## Notes

- Ensure the database is properly configured and has the necessary permissions.
- Adjust the thread and batch-size parameters based on your needs.
- Use `-batch-in-txn` to wrap each batch in a single explicit transaction (BEGIN/COMMIT).
- `wide_table_with_json` always generates JSON-like payload data.
- For workloads that support partitioned tables (e.g. bank3, bank4), set `-partitioned=false` to create non-partitioned tables.
- `bank4` partitioned mode creates 126 monthly partitions per table, which is suitable for partition-heavy DDL stress.
- `-bank3-partitioned` is deprecated; use `-partitioned`.
