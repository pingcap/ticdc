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

### 1. Sysbench-style Data Insertion

Insert test data using sysbench-compatible schema:

```bash
./workload -action insert \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name db1 \
    -total-row-count 100000000 \
    -table-count 1000 \
    -workload-type sysbench \
    -thread 32 \
    -batch-size 64
```

### 2. Large Row Update Workload

Update existing data with large row operations:

```bash
./workload -action update \
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

### 3. Fast/Slow Tables (CDC Lag Simulation)

Create a mix of fast tables (DML only) and slow tables (DML + periodic `ADD COLUMN` DDL on slow tables):

```bash
./workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name test \
    -workload-type fast_slow \
    -fast-table-count 8 \
    -slow-table-count 2 \
    -slow-ddl-interval 30s \
    -slow-ddl-start-delay 1m \
    -slow-ddl-max-columns 128
```

Notes:

- Fast tables are named `fast_sbtest<index>`, slow tables are named `slow_sbtest<index>` (suffix uses `table-start-index`).
- Slow table DDL uses `ALTER TABLE ... ADD COLUMN ...` periodically; customize with `-slow-ddl-options` (example: `ALGORITHM=COPY LOCK=EXCLUSIVE`).

## Notes

- Ensure the database is properly configured and has the necessary permissions.
- Adjust the thread and batch-size parameters based on your needs.
