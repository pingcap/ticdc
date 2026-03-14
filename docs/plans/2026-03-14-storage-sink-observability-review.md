# Storage Sink Observability Review

Status: Proposed (Approved)
Date: 2026-03-14
Owner Team: TiCDC

## Background

The current cloud storage sink has already grown beyond a simple file writer. The main
runtime path now includes:

- an unbounded upstream task channel in `dmlWriters`
- encoder shard queues in `encoderGroup`
- writer shard batching
- local spool memory and spill files
- final data-file and index-file writes to external storage
- checkpoint metadata writes
- periodic cleanup jobs

This means production diagnosis is no longer answered by one latency metric or one error
counter. Operators need to know where time is spent, where bytes are buffered, which
stage is failing, and whether the sink is draining or stalling.

This document reviews the current observability surface of storage sink and proposes a
concrete improvement plan focused on cloud storage sink on the current `master` branch.

## Problem Statement

The current storage sink exposes some useful metrics and logs, but they are not yet
enough to answer common operational questions quickly:

- Is the sink blocked in encoding, batching, spool, or external storage?
- Are callbacks delayed because spool is suppressing wake?
- Are files flushed because of size, timer, or block-event barriers?
- Are checkpoint writes being dropped, delayed, or failing?
- Are cleanup jobs doing useful work, or just failing and retrying?
- When a metric changes, can it be correlated across all sink-specific dashboards?

The goal is not to add more telemetry blindly. The goal is to make the existing pipeline
diagnosable with low-noise, correctly typed, and consistently labeled signals.

## Goals

- Review the current cloud storage sink observability surface.
- Identify the highest-value blind spots in metrics and logs.
- Propose concrete additions with clear module boundaries.
- Propose a migration path for metrics whose current names or types are misleading.

## Non-Goals

- This document does not change sink behavior.
- This document does not redesign storage consumer.
- This document does not define spool correctness or quota semantics.
- This document does not require distributed tracing as a prerequisite.

## Current State

### Current data flow

```text
AddDMLEvent
  -> dmlWriters.msgCh
  -> encoderGroup input/output queues
  -> writer.inputCh
  -> writer batchedTask
  -> spool.Enqueue
  -> writer.toBeFlushedCh
  -> spool.Load
  -> schema file check/write
  -> data file write
  -> index file write
  -> callbacks
  -> spool.Release
```

Relevant code pointers:

- `downstreamadapter/sink/cloudstorage/sink.go`
- `downstreamadapter/sink/cloudstorage/dml_writers.go`
- `downstreamadapter/sink/cloudstorage/encoder_group.go`
- `downstreamadapter/sink/cloudstorage/writer.go`
- `downstreamadapter/sink/cloudstorage/spool/manager.go`
- `downstreamadapter/sink/cloudstorage/spool/quota.go`
- `downstreamadapter/sink/metrics/cloudstorage.go`
- `pkg/metrics/statistics.go`

### Existing metrics

Cloud-storage-specific metrics:

- `cloud_storage_write_bytes_total`
- `cloud_storage_file_count`
- `cloud_storage_write_duration_seconds`
- `cloud_storage_flush_duration_seconds`
- `cloud_storage_ddl_flush_duration_seconds`
- `cloud_storage_worker_busy_ratio`
- `cloud_storage_spool_memory_bytes`
- `cloud_storage_spool_disk_bytes`
- `cloud_storage_spool_total_bytes`
- `cloud_storage_wake_suppressed_total`

Generic sink metrics from `pkg/metrics.Statistics`:

- batch size histogram
- batch write bytes histogram
- total write bytes counter
- DML execution counter
- DDL execution histogram and counter
- DML / DDL execution error counters

Checkpoint metrics currently reused from MQ naming:

- `mq_checkpoint_ts_message_duration`
- `mq_checkpoint_ts_message_count`

### Existing logs

The current sink logs:

- DDL execution success
- schema file write failure
- data file path generation failure
- data file write failure
- index file write failure
- schema-version mismatch ignore path
- checkpoint write failure
- cleanup start / stop / success / failure / skip
- spool segment cleanup failure

The logs are useful, but the field set is not fully standardized.

## Key Findings

### 1. Metric type semantics are inconsistent or misleading

Code pointers:

- `downstreamadapter/sink/metrics/cloudstorage.go`
- `pkg/metrics/sink.go`

Problems:

- `cloud_storage_write_bytes_total` is implemented as a `GaugeVec`, but the name and
  usage are cumulative-counter semantics.
- `cloud_storage_file_count` is also a `GaugeVec`, but it is incremented monotonically.
- `cloud_storage_worker_busy_ratio` is implemented as a `CounterVec` of busy seconds,
  not a ratio.
- checkpoint metrics use `mq_*` names even when emitted by cloud storage sink.

Impact:

- dashboards have to rely on convention instead of type truth
- new operators can misread the data model
- metric migration becomes harder over time

### 2. Metric labels are not consistent across the sink

Code pointers:

- `downstreamadapter/sink/cloudstorage/writer.go`
- `downstreamadapter/sink/cloudstorage/sink.go`
- `pkg/metrics/statistics.go`

Problems:

- some metrics use `changefeedID.ID().String()`
- some generic sink metrics use `changefeed.Name()`
- logs also mix `changefeed`, `changefeedID`, and `namespace` / `keyspace`

Impact:

- metrics from the same sink cannot be joined reliably
- dashboard queries become sink-specific and fragile
- logs and metrics are harder to correlate during incidents

### 3. Queueing and backlog are largely invisible

Code pointers:

- `downstreamadapter/sink/cloudstorage/dml_writers.go`
- `downstreamadapter/sink/cloudstorage/encoder_group.go`
- `downstreamadapter/sink/cloudstorage/writer.go`

Problems:

- `dmlWriters.msgCh` is unbounded, but no depth or pending-age signal exists
- `encoderGroup` input and output queues have fixed size, but no queue occupancy metric
- `writer.inputCh` and `writer.toBeFlushedCh` have no queue length or saturation metric
- `batchedTask` keeps pending tables and entries, but there is no metric for current
  pending tables, entries, or bytes per writer shard

Impact:

- it is hard to tell whether slowness comes from encoding, batching, or flush
- there is no early signal before memory or spool bytes grow visibly

### 4. Flush behavior is observable only at the end, not by cause

Code pointers:

- `downstreamadapter/sink/cloudstorage/writer.go`
- `downstreamadapter/sink/cloudstorage/task.go`

Problems:

- flushes can be triggered by file size, flush interval, block-event barrier, or close
- current metrics do not distinguish flush reason
- current metrics do not expose batch age, batch table count, or batch entry count

Impact:

- when file count spikes or latency shifts, operators cannot explain why
- barrier-related stalls are difficult to separate from timer-based flushing

### 5. Error attribution is still too coarse

Code pointers:

- `downstreamadapter/sink/cloudstorage/sink.go`
- `downstreamadapter/sink/cloudstorage/writer.go`
- `pkg/metrics/statistics.go`

Problems:

- generic `ExecutionErrorCounter` only distinguishes DML vs DDL
- cloud storage sink has multiple concrete failure stages:
  - schema file write
  - data file path generation
  - data file write
  - index file write
  - checkpoint metadata write
  - cleanup jobs
  - spool read / spill / rotate / cleanup

Impact:

- a DML error spike still requires log spelunking to identify the failing stage
- repeated low-signal failures cannot be summarized on a dashboard

### 6. Background-task observability is weak

Code pointers:

- `downstreamadapter/sink/cloudstorage/sink.go:sendCheckpointTs`
- `downstreamadapter/sink/cloudstorage/sink.go:bgCleanup`
- `downstreamadapter/sink/cloudstorage/sink.go:genCleanupJob`

Problems:

- `AddCheckpointTs` drops when the channel is full, but there is no drop counter
- checkpoint write cadence is throttled by time, but no throttle / skip metric exists
- cleanup jobs log cost and count, but there are no cleanup run counters, skip counters,
  error counters, or duration histograms
- cleanup overlap prevention is only visible in warn logs

Impact:

- metadata lag and cleanup drift can accumulate without any dashboard signal
- operators cannot distinguish "idle", "skipped", and "failing" background jobs

### 7. Spool exposes capacity, but not enough lifecycle detail

Code pointers:

- `downstreamadapter/sink/cloudstorage/spool/quota.go`
- `downstreamadapter/sink/cloudstorage/spool/manager.go`

Current spool metrics already expose:

- memory bytes
- disk bytes
- total bytes
- wake-suppressed count

Missing:

- spill event count
- spill bytes total
- load count / load bytes / load failures
- current segment count
- current pending wake count
- current suppression state as a gauge
- segment rotation count

Impact:

- spool capacity can be seen, but spool behavior cannot be explained
- it is hard to distinguish "large but healthy" from "thrashing and rotating"

### 8. Metrics lifecycle and test coverage are incomplete

Code pointers:

- `downstreamadapter/sink/cloudstorage/sink.go:Close`
- `downstreamadapter/sink/cloudstorage/spool/quota.go:deleteMetrics`

Problems:

- spool metrics are cleaned up on close, but writer-level cloud-storage metrics are not
- there are no focused tests for metric registration, label cleanup, or semantic
  invariants

Impact:

- long-running clusters with many changefeeds may accumulate stale label values
- future refactors can silently change metric behavior without tests catching it

## Proposed Design

### Design principles

- Keep metric definitions centralized in `downstreamadapter/sink/metrics/cloudstorage.go`.
- Keep metric updates close to the state owner.
- Prefer counters for monotonic totals, gauges for instantaneous state, and histograms
  for latency and distribution.
- Standardize labels before adding more metrics.
- Add stage-specific observability only where it answers a real operational question.

### Recommended observability layers

```text
sink-level
  checkpoint / cleanup / ddl path health

dmlWriters-level
  upstream backlog and routing pressure

encoderGroup-level
  encode queue occupancy and encode latency

writer-level
  pending batch size, flush reason, external write latency, stage errors

spool-level
  capacity, spill/load lifecycle, suppression state, segment lifecycle
```

## Detailed Recommendations

### A. Fix metric semantics before adding too many new dashboards

Recommended changes:

- Introduce correctly typed replacements:
  - `cloud_storage_write_bytes_total` as a `CounterVec`
  - `cloud_storage_file_count_total` as a `CounterVec`
  - `cloud_storage_worker_busy_seconds_total` as a `CounterVec`
- Keep the old metrics during a migration window to avoid breaking existing dashboards.
- Introduce sink-generic checkpoint metric names for cloud storage:
  - `sink_checkpoint_ts_message_duration_seconds`
  - `sink_checkpoint_ts_message_total`

Migration rule:

- dual-publish first
- update Grafana dashboards second
- remove old names only after dashboards and alerts stop depending on them

### B. Standardize labels and log fields

Recommended standard:

- metrics:
  - `keyspace`
  - `changefeed`
  - optional `writer_id`, `encoder_id`, `job`, `reason`, `stage`
- logs:
  - `keyspace`
  - `changefeed_id`
  - `changefeed_name`
  - `writer_id`
  - `dispatcher_id`
  - `schema`
  - `table`
  - `commit_ts`
  - `path`
  - `stage`

Recommendation:

- choose one canonical metric label identity for changefeed and use it everywhere
- keep the alternate human-readable identifier in logs if needed

### C. Add queue and backlog metrics

Suggested metrics:

- `cloud_storage_dml_input_queue_length`
- `cloud_storage_encoder_input_queue_length{encoder_id}`
- `cloud_storage_encoder_output_queue_length{writer_id}`
- `cloud_storage_writer_input_queue_length{writer_id}`
- `cloud_storage_writer_flush_queue_length{writer_id}`
- `cloud_storage_writer_pending_tables{writer_id}`
- `cloud_storage_writer_pending_entries{writer_id}`
- `cloud_storage_writer_pending_bytes{writer_id}`
- `cloud_storage_writer_oldest_batch_age_seconds{writer_id}`

Where to update:

- `dmlWriters.addTasks`
- `encoderGroup.add`
- `writer.enqueueTask`
- `writer.genAndDispatchTask`

These metrics answer the first operational question: where is backlog accumulating?

### D. Make flush behavior diagnosable

Suggested metrics:

- `cloud_storage_flush_trigger_total{reason=size|interval|barrier|close}`
- `cloud_storage_flush_batch_tables`
- `cloud_storage_flush_batch_entries`
- `cloud_storage_flush_batch_bytes`
- `cloud_storage_flush_batch_age_seconds`

Where to update:

- `writer.genAndDispatchTask`
- `writer.flushMessages`

This makes it possible to distinguish:

- large-file flush pressure
- timer-driven churn
- barrier-driven flush amplification

### E. Add stage-specific error counters

Suggested metrics:

- `cloud_storage_stage_error_total{stage=schema_write}`
- `cloud_storage_stage_error_total{stage=data_path}`
- `cloud_storage_stage_error_total{stage=data_write}`
- `cloud_storage_stage_error_total{stage=index_write}`
- `cloud_storage_stage_error_total{stage=checkpoint_write}`
- `cloud_storage_stage_error_total{stage=cleanup_remove_expired}`
- `cloud_storage_stage_error_total{stage=cleanup_remove_empty_dirs}`
- `cloud_storage_stage_error_total{stage=spool_read}`
- `cloud_storage_stage_error_total{stage=spool_write}`
- `cloud_storage_stage_error_total{stage=spool_rotate}`

This should complement, not replace, the generic DML / DDL error counters.

### F. Improve checkpoint and cleanup observability

Suggested metrics:

- `cloud_storage_checkpoint_drop_total{reason=channel_full}`
- `cloud_storage_checkpoint_skip_total{reason=older_than_last|throttle_window}`
- `cloud_storage_checkpoint_write_duration_seconds`
- `cloud_storage_checkpoint_ts_last_sent`
- `cloud_storage_cleanup_run_total{job,result}`
- `cloud_storage_cleanup_duration_seconds{job,result}`
- `cloud_storage_cleanup_removed_total{job}`
- `cloud_storage_cleanup_skip_total{job,reason=already_running|disabled}`

This will make background work visible on dashboards instead of only in logs.

### G. Extend spool observability from capacity to behavior

Suggested metrics:

- `cloud_storage_spool_spill_total`
- `cloud_storage_spool_spill_bytes_total`
- `cloud_storage_spool_load_total`
- `cloud_storage_spool_load_bytes_total`
- `cloud_storage_spool_segment_count`
- `cloud_storage_spool_rotate_total`
- `cloud_storage_spool_pending_wake`
- `cloud_storage_spool_wake_suppressed`
- `cloud_storage_spool_error_total{stage=load|write|rotate|cleanup}`

Where to update:

- `spool.Manager.Enqueue`
- `spool.Manager.Load`
- `spool.Manager.rotateLocked`
- `spool.Manager.Release`
- `spool.Manager.Close`

### H. Tighten logging instead of adding more noisy logs

Recommended new logs:

- one sink-start summary log:
  - worker count
  - flush interval
  - file size
  - flush concurrency
  - spool quota
  - cleanup config
- one structured warn log when checkpoint drops occur
- one structured warn log when flush queue is saturated or skipped
- one structured info log when spool enters or exits suppression state

Recommended cleanup:

- standardize `keyspace` vs `namespace`
- standardize `changefeed` field meaning
- avoid mixing `changefeedID` and `changefeed` for different identities

## Performance Considerations

Observability must not add noticeable overhead to the hottest loops.

Guidelines:

- update cheap gauges and counters inline
- compute expensive per-batch summaries once, not per message
- avoid per-message logging entirely
- prefer queue length sampling over per-item log emission
- keep histogram updates at batch or flush boundaries

The most sensitive hot paths are:

- `encoderGroup.add`
- `writer.genAndDispatchTask`
- `spool.Manager.Enqueue`
- `spool.Manager.Load`
- `writer.writeDataFile`

## Testing Strategy

Recommended tests:

- metric type and name regression tests for cloud-storage-specific metrics
- label cleanup tests on `sink.Close` and `spool.Close`
- checkpoint drop-counter tests
- cleanup run / skip / error metric tests
- flush-reason counter tests
- spool segment-count / spill-count tests

The goal is not to test Prometheus itself. The goal is to lock down:

- metric contract
- label lifecycle
- stage attribution

## Observability / Operations

The minimum useful storage sink dashboard should answer:

1. Is backlog growing?
2. Where is backlog growing?
3. Is spool in memory-only mode, spill mode, or suppressed mode?
4. Which flush reason dominates?
5. Which stage is failing most often?
6. Are checkpoint and cleanup background tasks healthy?

Recommended dashboard groups:

- throughput and latency
- queue and backlog
- spool health
- checkpoint and cleanup
- error attribution

## Rollout Plan

### Phase 1: Low-risk additions

- add queue gauges
- add flush-reason counters
- add stage-specific error counters
- add checkpoint drop / skip metrics
- add cleanup run / duration / removed counters

### Phase 2: Metric contract cleanup

- dual-publish replacement metrics with correct types
- standardize labels for new metrics
- update dashboards and alerts

### Phase 3: Metrics lifecycle cleanup

- delete cloud-storage writer metric label values on sink close
- add tests for metric cleanup and semantic regressions

## Alternatives Considered

### Add only more logs

Rejected because logs are poor at showing backlog growth, long-term trends, and alertable
thresholds.

### Add tracing first

Rejected as the primary answer. Tracing may help later, but the current system first
needs correct counters, gauges, histograms, and stage attribution.

### Reuse existing generic sink metrics only

Rejected because the cloud storage sink now has storage-specific stages that generic DML
/ DDL counters cannot distinguish.

## Open Questions / Future Work

- Which canonical identity should be used for the `changefeed` metric label?
- Should per-writer metrics always be on, or guarded by worker-count/cardinality limits?
- Is it worth adding a lightweight debug endpoint for current queue depths and batch
  state, in addition to Prometheus metrics?

## References

- `downstreamadapter/sink/cloudstorage/sink.go`
- `downstreamadapter/sink/cloudstorage/dml_writers.go`
- `downstreamadapter/sink/cloudstorage/encoder_group.go`
- `downstreamadapter/sink/cloudstorage/task.go`
- `downstreamadapter/sink/cloudstorage/writer.go`
- `downstreamadapter/sink/cloudstorage/spool/manager.go`
- `downstreamadapter/sink/cloudstorage/spool/quota.go`
- `downstreamadapter/sink/metrics/cloudstorage.go`
- `pkg/metrics/statistics.go`
- `pkg/metrics/sink.go`
