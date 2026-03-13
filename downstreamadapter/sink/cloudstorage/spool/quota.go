// Copyright 2025 PingCAP, Inc.
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
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type quotaController struct {
	memoryQuotaBytes   int64
	highWatermarkBytes int64
	lowWatermarkBytes  int64

	memoryBytes int64
	diskBytes   int64

	wakeSuppressed bool
	pendingWake    []func()

	metricMemoryBytes    prometheus.Gauge
	metricDiskBytes      prometheus.Gauge
	metricTotalBytes     prometheus.Gauge
	metricWakeSuppressed prometheus.Counter

	metricKeyspace        string
	metricChangefeedLabel string
}

func newQuotaController(
	changefeedID commonType.ChangeFeedID,
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

func (q *quotaController) shouldSpill(entryBytes int64) bool {
	return q.memoryBytes+entryBytes > q.memoryQuotaBytes
}

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

	if q.wakeSuppressed {
		if onEnqueued == nil {
			q.updateMetrics()
			return nil
		}
		q.pendingWake = append(q.pendingWake, onEnqueued)
		q.metricWakeSuppressed.Inc()
		q.updateMetrics()
		return nil
	}
	if onEnqueued != nil {
		q.updateMetrics()
		return []func(){onEnqueued}
	}
	q.updateMetrics()
	return nil
}

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

func (q *quotaController) reset() {
	q.memoryBytes = 0
	q.diskBytes = 0
	q.pendingWake = nil
	q.wakeSuppressed = false
	q.updateMetrics()
}

func (q *quotaController) deleteMetrics() {
	metrics.CloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageSpoolDiskBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageSpoolTotalBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageWakeSuppressedCounter.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
}

func (q *quotaController) updateMetrics() {
	q.metricMemoryBytes.Set(float64(q.memoryBytes))
	q.metricDiskBytes.Set(float64(q.diskBytes))
	q.metricTotalBytes.Set(float64(q.memoryBytes + q.diskBytes))
}
