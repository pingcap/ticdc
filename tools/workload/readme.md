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

### 3. Staging Forward Index Upsert Workload

Generate large-row upserts based on a `staging_forward_index`-style schema (BLOB/MEDIUMBLOB) and `INSERT ... ON DUPLICATE KEY UPDATE`.

```bash
./workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name ads_indexing \
    -total-row-count 1000000 \
    -table-count 1 \
    -workload-type staging_forward_index \
    -row-size $((512 * 1024)) \
    -thread 16 \
    -batch-size 8 \
    -percentage-for-update 0.5
```

### 4. BIS Metadata Workload

Generate writes for `bis_entity_metadata` and `bis_batch_metadata` (two tables per shard). Use `-row-size` to control payload width and `-table-count` to control shard count.

```bash
./workload -action write \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name bulk_ingestion \
    -total-row-count 1000000 \
    -table-count 16 \
    -workload-type bis_metadata \
    -row-size $((16 * 1024)) \
    -thread 32 \
    -batch-size 32 \
    -percentage-for-update 0.5 \
    -percentage-for-delete 0.05
```

## Notes

- Ensure the database is properly configured and has the necessary permissions.
- Adjust the thread and batch-size parameters based on your needs.
