# Plan

Focus on upsert-mode performance, prioritizing faster conversion and higher-quality Iceberg files while keeping row width and target file size configurable. The approach is to profile the current upsert path, reduce conversion/encode overhead, and improve file/manifest batching without changing user-visible semantics.

## Scope
- In: Upsert path in `downstreamadapter/sink/iceberg/sink.go`, Parquet encoding in `pkg/sink/iceberg/parquet.go`, file sizing in `pkg/sink/iceberg/file_split.go`, and manifest/commit generation in `pkg/sink/iceberg/upsert_table.go` + `pkg/sink/iceberg/hadoop_table.go`.
- Out: New catalogs, schema/DDL behavior changes, or cross-sink refactors.

## Action items
[ ] Review design docs and upsert code path; identify the highest-cost conversion and metadata steps.
[ ] Add or leverage lightweight timings/metrics for upsert conversion, Parquet encode, file counts, and bytes per commit.
[ ] Optimize conversion for upsert (reduce allocations, precompute key/value column access, reuse buffers/builders, avoid repeated parsing).
[ ] Improve Parquet writing to hit target sizes and reduce small files (row-group sizing, writer props, buffering/streaming choices).
[ ] Batch manifest entries to reduce metadata overhead in upsert commits without violating Iceberg semantics.
[ ] Validate edge cases (NULL keys, updates, deletes, partition safety, decimals/timestamps) and add focused unit tests.
[ ] Run tests: `make unit_test_pkg PKG=./pkg/sink/iceberg/...` and `make integration_test_iceberg CASE="iceberg_append_basic iceberg_upsert_basic"` if available.
[ ] Update docs with any new knobs/defaults or operational guidance for upsert file sizing.

## Open questions
- Target workload characteristics for tuning defaults (typical row width, desired file size, update/delete ratio)?
- Is a modest increase in commit latency acceptable for fewer/larger files?
- Are there constraints on adding new config knobs beyond existing `target-file-size` and buffering limits?

## Note
- Added `auto-tune-file-size` (default on for `mode=upsert`) and manifest batching; see `docs/design/2026-01-28-ticdc-iceberg-sink.md` and `docs/design/2026-01-30-ticdc-iceberg-sink-user-guide.md`.
