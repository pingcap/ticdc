# eventStore Bench Tool

This tool generates synthetic workloads that stress the eventStore ingestion
path directly from the `tools/` tree.

## Build

```bash
cd tools
make bin/eventstore-bench
```

This produces `tools/bin/eventstore-bench`.

## Usage

```bash
./tools/bin/eventstore-bench \
  -data-dir /tmp/eventstore-bench \
  -duration 2m \
  -scenarios single-table,multi-table \
  -writers-per-table 4
```

Flags:

- `-data-dir`: base directory used to store the temporary pebble databases for
  each run.
- `-duration`: how long every scenario should run. `0` means run until the
  process receives `CTRL+C`.
- `-scenarios`: comma separated list of scenario names or `all`.
- `-writers-per-table`: optional override to force the same concurrency level
  for every scenario.

The tool prints per-scenario throughput (events/s and MB/s) and latency
statistics (avg, p50/p95/p99/max) and finishes with a condensed summary table.

## Built-in Scenarios

| Name          | Description                         |
| ------------- | ----------------------------------- |
| `single-table`| One table with balanced row sizes.  |
| `multi-table` | Many tables concurrently flushed.   |
| `wide-table`  | Large rows with updates (old value).|
| `narrow-table`| Small rows at high concurrency.     |

Each scenario has its own row width, batch size, and resolved-ts lag settings,
allowing you to understand how eventStore behaves in different shapes from
single-table to wide-row workloads.
