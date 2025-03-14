# Bank Update Workload

- Author(s): [dongmen](https://github.com/asddongmen)
- Tracking Issue(s): [1111](https://github.com/pingcap/ticdc/issues/1111)

The Bank Update workload is designed to test TiCDC's processing capability for upstream real-time changes, particularly for high-throughput update scenarios.

It is also a good workload for testing the performance of TiKV's write amplification.

## Overview

This workload creates a wide table with 61 columns plus two special columns:

- `large_col`: A configurable-sized VARCHAR column (default 1KB)
- `small_col`: A small INT column that is the target for updates

The default row size is approximately 1.7KB, making this a wide table workload.

## Update Mode

The workload uses range updates, where each update operation modifies multiple rows within a specified ID range. This approach generates high-throughput updates efficiently.

## How to Use

### Step 1: Prepare Data

```bash
./workload -database-host <host> -database-port 4000 \
  -action prepare -database-db-name test \
  -table-count 1 -workload-type bank_update \
  -thread 16 -total-row-count 20000000 \
  -update-large-column-size 4000
```

This command creates a table with 20 million rows, each with a 4KB large column.

### Step 2: Run Updates

```bash
./workload -database-host <host> -database-port 4000 \
  -action update -database-db-name test \
  -table-count 1 -workload-type bank_update \
  -thread 16 -total-row-count 20000000 \
  -percentage-for-update 1.0 -batch-size 20000 \
  -update-large-column-size 4000
```

This command runs updates on the `small_col` field, updating 20,000 rows per statement.

You can also adjust the `-percentage-for-update` to control the percentage of workload that are dedicated to updates. If you set it to 0.5, then half of the workload will be dedicated to updates and the other half will be dedicated to inserts.

## Configuration Options

- `-update-large-column-size`: Controls the size of the large column (default: 1024 bytes)
- `-batch-size`: Number of rows to update in each statement (default: 10)
- `-percentage-for-update`: Portion of threads dedicated to updates (1.0 = all threads)
- `-total-row-count`: Total number of rows in the table
- `-thread`: Number of concurrent worker threads

## Performance

This workload can achieve update throughput of up to 2.3GB/s with 16 threads and 4000 bytes large column, making it ideal for testing TiCDC's ability to process high-volume data changes from upstream.

You can adjust the `-thread` and `-update-large-column-size` to achieve the throughput you want.

## Notes

- It only supports one table in each run, if you wan to test multiple table, you can create multiple databases and run different workload in each database.(I will support multiple table ASAP)
- The workload only updates the small `small_col` field, not the large column, which is aim for the better update performance.
- Updates are performed on random ID ranges within the table
- Each update statement modifies multiple rows for efficiency