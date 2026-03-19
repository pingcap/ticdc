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
	"github.com/prometheus/client_golang/prometheus"
)

// quotaAdapter adds spool-specific behavior on top of budgetCore.
// budgetCore only answers "how many bytes are staged" and "which watermark
// state are we in"; this adapter decides how spool reacts to that state.
type quotaAdapter struct {
	// budget owns threshold math and byte accounting. quotaAdapter builds
	// spool-specific behavior on top of it.
	budget *budgetCore

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

func newQuotaAdapter(
	changefeedID common.ChangeFeedID,
	options *Options,
) *quotaAdapter {
	changefeedLabel := changefeedID.ID().String()
	controller := &quotaAdapter{
		budget: newBudgetCore(options),
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

// shouldSpill decides whether a new entry still fits in the in-memory tier.
// This is intentionally a memory-tier decision only; it does not enforce a
// hard total quota and does not reject new writes.
func (q *quotaAdapter) shouldSpill(entryBytes int64) bool {
	return q.budget.shouldSpill(entryBytes)
}

// reserve records a newly accepted entry and returns callbacks that may run
// immediately. If total staged bytes cross the high watermark, reserve enters
// wake suppression and queues later onEnqueued callbacks instead of running
// them inline.
func (q *quotaAdapter) reserve(
	entryBytes int64,
	spilled bool,
	onEnqueued func(),
) []func() {
	q.budget.reserve(entryBytes, spilled)
	if q.budget.overHighWatermark() {
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
// adapter was suppressing wake callbacks, release only resumes them after
// total staged bytes fall back to the low watermark or below.
func (q *quotaAdapter) release(entryBytes int64, spilled bool) []func() {
	q.budget.release(entryBytes, spilled)
	if !q.wakeSuppressed {
		q.updateMetrics()
		return nil
	}
	if !q.budget.atOrBelowLowWatermark() {
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
func (q *quotaAdapter) reset() {
	q.budget.reset()
	q.pendingWake = nil
	q.wakeSuppressed = false
	q.updateMetrics()
}

// deleteMetrics removes per-changefeed label values owned by this adapter.
func (q *quotaAdapter) deleteMetrics() {
	metrics.CloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageSpoolDiskBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageSpoolTotalBytesGauge.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
	metrics.CloudStorageWakeSuppressedCounter.DeleteLabelValues(q.metricKeyspace, q.metricChangefeedLabel)
}

// updateMetrics publishes the adapter's current accounting state.
func (q *quotaAdapter) updateMetrics() {
	snapshot := q.budget.snapshot()
	q.metricMemoryBytes.Set(float64(snapshot.memoryBytes))
	q.metricDiskBytes.Set(float64(snapshot.diskBytes))
	q.metricTotalBytes.Set(float64(snapshot.totalBytes()))
}
