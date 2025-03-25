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
- Runs updates on the `pad` column using range-based updates
- Uses 16 threads dedicated to updates
- Divides the table into 5 update ranges for better concurrency
- Updates use pre-cached random 20-byte strings for better performance

### Multiple Databases Mode

For scenarios requiring higher concurrency or larger data volumes, you can distribute the workload across multiple databases.

#### Prepare Data Across Multiple Databases

```bash
./workload -database-host 127.0.0.1 -database-port 4000 \
  -action prepare \
  -db-prefix "db" -db-num 10 \
  -table-count 10 -workload-type sysbench \
  -thread 10 -total-row-count 10000000
```

This command:

- Creates 10 databases named `db1`, `db2`, ..., `db10`
- Each database contains 10 tables (`sbtest1` through `sbtest10`)
- Uses 10 concurrent threads for data insertion
- Inserts a total of 10 million rows distributed across all tables
- Each table will contain approximately (10,000,000 / (10 databases * 10 tables)) rows

#### Run Updates Across Multiple Databases

```bash
./workload -database-host 127.0.0.1 -database-port 4000 \
  -action update \
  -db-prefix "db" -db-num 10 \
  -table-count 10 -workload-type sysbench \
  -thread 10 -batch-size 100 \
  -range-num 5 -percentage-for-update 1.0
```

This command:
- Performs updates across all 10 databases concurrently
- Updates tables in all databases (`db1.sbtest1` through `db10.sbtest10`)
- Uses 10 concurrent threads for updates
- Updates 100 rows per batch
- Divides each table into 5 update ranges
- All threads (100%) are dedicated to updates

## Configuration Options

- `-table-count`: Number of tables to create (default: 1)
- `-thread`: Number of concurrent worker threads (default: 16)
- `-batch-size`: Number of rows to insert in each statement (default: 10)
- `-total-row-count`: Total number of rows to insert (default: 1 billion)
- `-percentage-for-update`: Portion of threads dedicated to updates (1.0 = all threads)
- `-range-num`: Number of ranges for dividing update operations (default: 5)

## Additional Configuration Options

In addition to the basic options, these parameters control multi-database operations:

- `-db-prefix`: Prefix for database names (e.g., "db" creates db1, db2, etc.)
- `-db-num`: Number of databases to create and use
- `-batch-size`: Number of rows per operation (insert/update)

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
- Each update modifies the `pad` column with pre-cached random strings within specified ID ranges
- The workload pre-generates 100,000 random 20-byte strings for pad values to improve performance
- Multiple tables are supported through the `-table-count` parameter
- When using multiple databases, the total workload is distributed evenly across all databases and tables
- Each thread operates independently on its assigned database and table
- The actual number of rows per table will be: total_row_count / (db_num * table_count)