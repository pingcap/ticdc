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
	"sync"

	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
)

// quotaController adds spool-specific behavior on top of budget.
// budget only answers "how many bytes are staged" and "which watermark
// state are we in"; this adapter decides how spool reacts to that state.
type quotaController struct {
	// budget owns threshold math and byte accounting. quotaAdapter builds
	// spool-specific behavior on top of it.
	budget *budget

	// postEnqueuePaused becomes true after local buffered bytes cross
	// the high watermark. While it is true, new PostEnqueue callbacks are kept
	// in memory instead of being executed immediately.
	postEnqueuePaused bool
	// pendingPostEnqueue stores the PostEnqueue callbacks that were
	// held back while local buffered bytes stayed above the high watermark.
	// They are run together after usage drops to the low watermark or below.
	pendingPostEnqueue []func()

	// These metrics expose the current local buffer state so operators can see
	// where buffered bytes live and how often PostEnqueue callbacks were moved
	// into the pending queue.
	metricMemoryBytes        prometheus.Gauge
	metricDiskBytes          prometheus.Gauge
	metricTotalBytes         prometheus.Gauge
	metricPendingPostEnqueue prometheus.Counter
	metricPendingCallbacks   prometheus.Gauge
	metricPostEnqueuePaused  prometheus.Gauge
	metricSpillCount         prometheus.Counter
	metricSpillBytes         prometheus.Counter
	metricLoadCount          prometheus.Counter
	metricLoadBytes          prometheus.Counter
	metricRotateCount        prometheus.Counter
	metricSegments           prometheus.Gauge
	metricStageErrors        *prometheus.CounterVec

	keyspace   string
	changefeed string

	diskQuotaChanged chan struct{}
	waitMu           sync.Mutex
}

func newQuotaController(
	changefeedID common.ChangeFeedID,
	options *options,
) *quotaController {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.ID().String()
	controller := &quotaController{
		keyspace:   keyspace,
		changefeed: changefeed,

		budget: newBudget(options),

		metricMemoryBytes:        metrics.CloudStorageSpoolMemoryBytesGauge.WithLabelValues(keyspace, changefeed),
		metricDiskBytes:          metrics.CloudStorageSpoolDiskBytesGauge.WithLabelValues(keyspace, changefeed),
		metricTotalBytes:         metrics.CloudStorageSpoolTotalBytesGauge.WithLabelValues(keyspace, changefeed),
		metricPendingPostEnqueue: metrics.CloudStoragePendingPostEnqueueCounter.WithLabelValues(keyspace, changefeed),
		metricPendingCallbacks:   metrics.CloudStoragePendingPostEnqueueGauge.WithLabelValues(keyspace, changefeed),
		metricPostEnqueuePaused:  metrics.CloudStoragePostEnqueuePausedGauge.WithLabelValues(keyspace, changefeed),
		metricSpillCount:         metrics.CloudStorageSpillCountCounter.WithLabelValues(keyspace, changefeed),
		metricSpillBytes:         metrics.CloudStorageSpillBytesCounter.WithLabelValues(keyspace, changefeed),
		metricLoadCount:          metrics.CloudStorageLoadCountCounter.WithLabelValues(keyspace, changefeed),
		metricLoadBytes:          metrics.CloudStorageLoadBytesCounter.WithLabelValues(keyspace, changefeed),
		metricRotateCount:        metrics.CloudStorageRotateCountCounter.WithLabelValues(keyspace, changefeed),
		metricSegments:           metrics.CloudStorageSpoolSegmentGauge.WithLabelValues(keyspace, changefeed),
		metricStageErrors:        metrics.CloudStorageSpoolErrorCounter,
		diskQuotaChanged:         make(chan struct{}),
	}
	controller.updateMetrics()
	return controller
}

// shouldSpill decides whether a new entry still fits in the in-memory tier.
// This is intentionally a memory-tier decision only; it does not enforce a
// hard total quota and does not reject new writes.
func (q *quotaController) shouldSpill(entryBytes int64) bool {
	return q.budget.shouldSpill(entryBytes)
}

func (q *quotaController) exceedsDiskQuota(entryBytes int64) bool {
	return q.budget.exceedsDiskQuota(entryBytes)
}

func (q *quotaController) wouldExceedDiskQuota(entryBytes int64) bool {
	return q.budget.wouldExceedDiskQuota(entryBytes)
}

func (q *quotaController) diskQuotaWaitCh() <-chan struct{} {
	return q.diskQuotaChanged
}

// acquire records a newly accepted entry and returns callbacks that may run
// immediately. If total staged bytes cross the high watermark, acquire puts
// new PostEnqueue callbacks into the pending queue instead of running them inline.
func (q *quotaController) acquire(
	entryBytes int64,
	spilled bool,
	postEnqueue func(),
) []func() {
	if q.budget.acquire(entryBytes, spilled) {
		q.postEnqueuePaused = true
	}
	if spilled {
		q.metricSpillCount.Inc()
		q.metricSpillBytes.Add(float64(entryBytes))
	}

	defer q.updateMetrics()
	if q.postEnqueuePaused && postEnqueue != nil {
		q.pendingPostEnqueue = append(q.pendingPostEnqueue, postEnqueue)
		q.metricPendingPostEnqueue.Inc()
		return nil
	}
	if postEnqueue != nil {
		return []func(){postEnqueue}
	}
	return nil
}

// release records that an entry has been fully consumed or discarded. If the
// adapter was holding PostEnqueue callbacks in the pending queue, release only
// runs them after total staged bytes fall back to the low watermark or below.
func (q *quotaController) release(entryBytes int64, spilled bool) []func() {
	atOrBelowLowWatermark := q.budget.release(entryBytes, spilled)
	if spilled {
		q.notifyDiskQuotaChanged()
	}
	defer q.updateMetrics()

	if !q.postEnqueuePaused {
		return nil
	}
	if !atOrBelowLowWatermark {
		return nil
	}

	q.postEnqueuePaused = false
	callbacks := append([]func(){}, q.pendingPostEnqueue...)
	q.pendingPostEnqueue = nil
	return callbacks
}

// reset clears runtime accounting when the spool is shutting down.
func (q *quotaController) reset() {
	q.budget.reset()
	q.pendingPostEnqueue = nil
	q.postEnqueuePaused = false
	q.notifyDiskQuotaChanged()
	q.updateMetrics()
}

// deleteMetrics removes per-changefeed label values owned by this adapter.
func (q *quotaController) deleteMetrics() {
	metrics.CloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpoolDiskBytesGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpoolTotalBytesGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStoragePendingPostEnqueueCounter.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStoragePendingPostEnqueueGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStoragePostEnqueuePausedGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpillCountCounter.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpillBytesCounter.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageLoadCountCounter.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageLoadBytesCounter.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageRotateCountCounter.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpoolSegmentGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpoolErrorCounter.DeleteLabelValues(q.keyspace, q.changefeed, spoolErrorStageLoad)
	metrics.CloudStorageSpoolErrorCounter.DeleteLabelValues(q.keyspace, q.changefeed, spoolErrorStageWrite)
	metrics.CloudStorageSpoolErrorCounter.DeleteLabelValues(q.keyspace, q.changefeed, spoolErrorStageRotate)
}

// updateMetrics publishes the adapter's current accounting state.
func (q *quotaController) updateMetrics() {
	q.metricMemoryBytes.Set(float64(q.budget.memoryBytes))
	q.metricDiskBytes.Set(float64(q.budget.diskBytes))
	q.metricTotalBytes.Set(float64(q.budget.totalBytes()))
	q.metricPendingCallbacks.Set(float64(len(q.pendingPostEnqueue)))
	if q.postEnqueuePaused {
		q.metricPostEnqueuePaused.Set(1)
		return
	}
	q.metricPostEnqueuePaused.Set(0)
}

func (q *quotaController) notifyDiskQuotaChanged() {
	q.waitMu.Lock()
	close(q.diskQuotaChanged)
	q.diskQuotaChanged = make(chan struct{})
	q.waitMu.Unlock()
}

func (q *quotaController) recordLoad(bytes int64) {
	q.metricLoadCount.Inc()
	q.metricLoadBytes.Add(float64(bytes))
}

func (q *quotaController) recordRotate(segmentCount int) {
	q.metricRotateCount.Inc()
	q.metricSegments.Set(float64(segmentCount))
}

func (q *quotaController) setSegmentCount(segmentCount int) {
	q.metricSegments.Set(float64(segmentCount))
}

func (q *quotaController) recordStageError(stage string) {
	q.metricStageErrors.WithLabelValues(q.keyspace, q.changefeed, stage).Inc()
}
