# Sysbench Workload

## Overview

The Sysbench workload implements a simplified version of the classic sysbench OLTP test suite. It creates tables with a basic structure containing:

- `id`: Primary key
- `k`: Indexed integer column
- `c`: 30-character string column
- `pad`: 20-character string column

## How to Use

### Step 1: Prepare Data

```bash
./workload -database-host <host> -database-port 4000 \
  -action prepare -database-db-name test \
  -table-count 1 -workload-type sysbench \
  -thread 16 -batch-size 10 \
  -total-row-count 1000000
```

This command:
- Creates a sysbench table with 1 million rows
- Uses 16 concurrent threads for data insertion
- Inserts 10 rows per batch

### Step 2: Run Updates

```bash
./workload -database-host <host> -database-port 4000 \
  -action update -database-db-name test \
  -table-count 1 -workload-type sysbench \
  -thread 16 -percentage-for-update 1.0 \
  -range-num 5
```

This command:
- Runs updates on the `k` column using range-based updates
- Uses 16 threads dedicated to updates
- Divides the table into 5 update ranges for better concurrency

## Configuration Options

- `-table-count`: Number of tables to create (default: 1)
- `-thread`: Number of concurrent worker threads (default: 16)
- `-batch-size`: Number of rows to insert in each statement (default: 10)
- `-total-row-count`: Total number of rows to insert (default: 1 billion)
- `-percentage-for-update`: Portion of threads dedicated to updates (1.0 = all threads)
- `-range-num`: Number of ranges for dividing update operations (default: 5)

## Table Schema

```sql
CREATE TABLE sbtest%d (
  id bigint NOT NULL,
  k bigint NOT NULL DEFAULT '0',
  c char(30) NOT NULL DEFAULT '',
  pad char(20) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  KEY k_1 (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

## Notes

- Updates are performed using range-based operations for better efficiency
- The workload maintains a cache of update ranges to ensure even distribution
- Each update modifies the `k` column with random values within specified ID ranges
- Multiple tables are supported through the `-table-count` parameter