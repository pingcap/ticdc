# `customer_workload` workload

This workload generates anonymized DML traffic based on four production-shaped CDC models. It intentionally uses neutral model, table, and column names.

## Models

| Model | Shape | Main CDC pressure |
| --- | --- | --- |
| `A` | Large catalog-like rows, many changefeeds | High bytes/s and large Debezium messages |
| `B` | Small capture count, event-like rows | High per-capture Kafka sink density |
| `C` | One hot table spread across captures | Single-table dispatcher and eventservice scan pressure |
| `D` | Multi-feed object tables, update/delete-heavy | Old value, composite keys, delete-heavy DML |

## Tables

The generated table names are anonymized and suffixed by table index, for example:

- `catalog_large_0`
- `event_log_0`
- `graph_node_0`
- `object_main_0`

Column names are also generic: `entity_id`, `bucket_id`, `sequence_no`, `payload_text`, `payload_json`, `payload_blob`, and `aux_blob`.

For a full Model A run with `-table-count 27`, the table mix approximates the
row-share inferred from 286K rows/s and 6.66GB/s:

| Shape | Tables | Payload target | Approx row share |
| --- | ---: | ---: | ---: |
| `catalog_large_*` | 9 | 37KiB | 33.3% |
| `catalog_blended_*` | 9 | 20KiB | 33.3% |
| `catalog_json_*` | 7 | 17KiB | 25.9% |
| `catalog_compact_a_*` | 1 | 2.3KiB | 3.7% |
| `catalog_compact_b_*` | 1 | 1.1KiB | 3.7% |

## Example

Prepare and run a small model-A calibration against a TiDB cluster:

```bash
./bin/workload -action prepare \
  -workload-type customer_workload \
  -customer-model A \
  -database-host 10.2.15.7 \
  -database-port 4000 \
  -database-user root \
  -database-db-name test \
  -table-count 4 \
  -total-row-count 10000 \
  -thread 4 \
  -batch-size 16 \
  -max-runtime 5m

./bin/workload -action write \
  -workload-type customer_workload \
  -customer-model A \
  -database-host 10.2.15.7 \
  -database-port 4000 \
  -database-user root \
  -database-db-name test \
  -skip-create-table \
  -table-count 4 \
  -thread 8 \
  -batch-size 16 \
  -percentage-for-update 0.5 \
  -percentage-for-delete 0.05 \
  -target-rows-per-sec 1000 \
  -max-runtime 5m
```

## Notes

- `-customer-row-size` overrides the model default payload size for every table shape.
- `-customer-keyspace` controls the update/delete keyspace per table. For `write -skip-create-table` runs after a separate prepare phase, set this to the prepared rows per table.
- In non-prepare actions, customer-workload insert/upsert keys are randomized inside the prepared keyspace instead of appended after the current maximum key. This avoids artificial right-edge primary-key hotspots that are not representative of the target production traffic.
- `-customer-initial-seq` seeds prepare continuation and the upper bound used by randomized non-prepare insert/upsert. If omitted in non-prepare actions, it derives from `-customer-keyspace`.
- Avoid using `-total-row-count` to express write-phase keyspace; the generic metrics reporter may stop non-update actions after that many affected rows.
- `-target-rows-per-sec` throttles rows across all DML workers. Use TiCDC sink metrics and Kafka/object-store ingress as the source of truth for CDC throughput.
- The workload uses prepared statements for insert, update, and delete paths to avoid mixing large SQL string parsing cost into CDC capacity tests.
