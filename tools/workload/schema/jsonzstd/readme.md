# `json_zstd` workload

This workload generates **write/update/delete** traffic against a pair of generic metadata-style tables, with a focus on **large JSON-like payloads** and **compression behavior**.

It is useful for testing:

- Replication / CDC throughput with wide rows.
- Compression friendliness (highly compressible vs incompressible payloads).
- Mixed update patterns (status-only, metadata updates, key updates, timestamp updates).

## Tables

For each shard index `n`, the workload uses two tables:

- Entity table: `json_zstd_entity_metadata` / `json_zstd_entity_metadata_<n>`
- Batch table: `json_zstd_batch_metadata` / `json_zstd_batch_metadata_<n>`

`-table-count` controls how many shards to create, and `-table-start-index` controls the starting suffix.

## Payload sizing

`-row-size` controls the *approximate* payload width per row, and is split across columns:

- Entity: `media_metadata` (varchar, clamped to 6144 bytes)
- Batch: `aux_data` (varchar, up to 3072 bytes), `callback_metadata` (varchar, up to 3072 bytes), `metadata` (mediumblob, up to 16MB)

When `-row-size` is large, each column is clamped to its maximum.

## Payload modes

Use `-json-payload-mode` to control how payload bytes are generated:

- `const`: deterministic repeated bytes (fast, stable baseline).
- `zstd`: fills payload with repeated JSON-like records to be **zstd-friendly**.
- `random`: random bytes to be **incompressible**.

## Workload behavior (high level)

- Inserts are split between entity and batch tables (roughly 77% / 23%).
- Updates are split between entity and batch tables (roughly 51% / 49%).
- Entity updates include different access paths (by secondary id, by composite key, by primary id).
- Batch updates include status-only updates and metadata-heavy updates.
- Deletes target the entity table (by id or by composite key).

## Example

Create tables + run mixed writes with zstd-friendly payloads:

```bash
./workload -action write \
  -database-host 127.0.0.1 \
  -database-port 4000 \
  -database-db-name json_payload \
  -workload-type json_zstd \
  -table-count 16 \
  -row-size $((16 * 1024)) \
  -json-payload-mode zstd \
  -thread 32 \
  -batch-size 32 \
  -percentage-for-update 0.5 \
  -percentage-for-delete 0.05
```

## Notes

- The DDL contains TiDB-specific TTL hints; other MySQL-compatible databases may ignore them.
- `-total-row-count` is used to derive an internal per-table update keyspace, so setting it closer to the actual prepared row count gives more realistic update hit rates.
