// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spool

import (
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type quotaController struct {
	// These thresholds are all derived from one total budget.
	// The current design uses them as soft-control signals:
	// memoryQuotaBytes decides when new entries spill to disk, while the
	// high/low watermarks decide when to suppress or resume wake callbacks.
	memoryQuotaBytes   int64
	highWatermarkBytes int64
	lowWatermarkBytes  int64

	// memoryBytes and diskBytes track current staged bytes held by spool.
	// They are accounting values for tiering and wake control, not hard limits.
	memoryBytes int64
	diskBytes   int64

	// wakeSuppressed and pendingWake implement hysteresis for onEnqueued:
	// above the high watermark, new callbacks are delayed; once usage drops
	// below the low watermark, the delayed callbacks are released in batch.
	wakeSuppressed bool
	pendingWake    []func()

	// These metrics expose the current soft-control state so operators can see
	// where buffered bytes live and whether wake suppression is happening.
	metricMemoryBytes    prometheus.Gauge
	metricDiskBytes      prometheus.Gauge
	metricTotalBytes     prometheus.Gauge
	metricWakeSuppressed prometheus.Counter

	metricKeyspace        string
	metricChangefeedLabel string
}

func newQuotaController(
	changefeedID common.ChangeFeedID,
	options *Options,
) *quotaController {
	changefeedLabel := changefeedID.ID().String()
	controller := &quotaController{
		memoryQuotaBytes:   int64(float64(options.QuotaBytes) * options.MemoryRatio),
		highWatermarkBytes: int64(float64(options.QuotaBytes) * options.HighWatermarkRatio),
		lowWatermarkBytes:  int64(float64(options.QuotaBytes) * options.LowWatermarkRatio),

		metricMemoryBytes: metrics.CloudStorageSpoolMemoryBytesGauge.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricDiskBytes: metrics.CloudStorageSpoolDiskBytesGauge.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricTotalBytes: metrics.CloudStorageSpoolTotalBytesGauge.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricWakeSuppressed: metrics.CloudStorageWakeSuppressedCounter.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricKeyspace:        changefeedID.Keyspace(),
		metricChangefeedLabel: changefeedLabel,
	}
	controller.updateMetrics()
	return controller
}

// validateOptions checks the ratios needed by quotaController's soft-control
// model. The controller assumes one total budget that is split into a memory
// tier plus high/low watermarks for wake suppression.
func validateOptions(options *Options) error {
	if options.SegmentBytes <= 0 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool segment size must be greater than 0, but got %d",
			options.SegmentBytes,
		)
	}
	if options.QuotaBytes <= 0 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool disk quota must be greater than 0, but got %d",
			options.QuotaBytes,
		)
	}
	if options.MemoryRatio <= 0 || options.MemoryRatio >= 1 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool memory ratio must be in (0, 1), but got %f",
			options.MemoryRatio,
		)
	}
	if options.LowWatermarkRatio <= 0 || options.LowWatermarkRatio >= 1 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool low watermark ratio must be in (0, 1), but got %f",
			options.LowWatermarkRatio,
		)
	}
	if options.HighWatermarkRatio <= 0 || options.HighWatermarkRatio >= 1 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool high watermark ratio must be in (0, 1), but got %f",
			options.HighWatermarkRatio,
		)
	}
	if options.LowWatermarkRatio >= options.HighWatermarkRatio {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool low watermark ratio must be less than high watermark ratio, low: %f high: %f",
			options.LowWatermarkRatio,
			options.HighWatermarkRatio,
		)
	}
	return nil
}

// shouldSpill decides whether a new entry still fits in the in-memory tier.
// This is intentionally a memory-tier decision only; it does not enforce a
// hard total quota and does not reject new writes.
func (q *quotaController) shouldSpill(entryBytes int64) bool {
	return q.memoryBytes+entryBytes > q.memoryQuotaBytes
}

// reserve records a newly accepted entry and returns callbacks that may run
// immediately. If total staged bytes cross the high watermark, reserve enters
// wake suppression and queues later onEnqueued callbacks instead of running
// them inline.
func (q *quotaController) reserve(
	entryBytes int64,
	spilled bool,
	onEnqueued func(),
) []func() {
	if spilled {
		q.diskBytes += entryBytes
	}
	if !spilled {
		q.memoryBytes += entryBytes
	}
	if q.memoryBytes+q.diskBytes > q.highWatermarkBytes {
		q.wakeSuppressed = true
	}

	defer q.updateMetrics()
	if q.wakeSuppressed && onEnqueued != nil {
		q.pendingWake = append(q.pendingWake, onEnqueued)
		q.metricWakeSuppressed.Inc()
		return nil
	}
	if onEnqueued != nil {
		return []func(){onEnqueued}
	}
	return nil
}

// release records that an entry has been fully consumed or discarded. If the
// controller was suppressing wake callbacks, release only resumes them after
// total staged bytes fall back to the low watermark or below.
func (q *quotaController) release(entryBytes int64, spilled bool) []func() {
	if spilled {
		q.diskBytes -= entryBytes
	}
	if !spilled {
		q.memoryBytes -= entryBytes
	}
	if q.memoryBytes < 0 {
		q.memoryBytes = 0
	}
	if q.diskBytes < 0 {
		q.diskBytes = 0
	}
	if !q.wakeSuppressed {
		q.updateMetrics()
		return nil
	}
	if q.memoryBytes+q.diskBytes > q.lowWatermarkBytes {
		q.updateMetrics()
		return nil
	}

	q.wakeSuppressed = false
	callbacks := append([]func(){}, q.pendingWake...)
	q.pendingWake = nil
	q.updateMetrics()
	return callbacks
}

// reset clears runtime accounting when the spool is shutting down.
func (q *quotaController) reset() {
	q.memoryBytes = 0
	q.diskBytes = 0
	q.pendingWake = nil
	q.wakeSuppressed = false
	q.updateMetrics()
}

// deleteMetrics removes per-changefeed label values owned by this controller.
func (q *quotaController) deleteMetrics() {
	metrics.CloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageSpoolDiskBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageSpoolTotalBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageWakeSuppressedCounter.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
}

// updateMetrics publishes the controller's current accounting state.
func (q *quotaController) updateMetrics() {
	q.metricMemoryBytes.Set(float64(q.memoryBytes))
	q.metricDiskBytes.Set(float64(q.diskBytes))
	q.metricTotalBytes.Set(float64(q.memoryBytes + q.diskBytes))
}
