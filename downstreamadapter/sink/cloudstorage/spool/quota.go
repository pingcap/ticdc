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
	// budget owns threshold math and byte accounting.
	budget *budget

	// postEnqueuePaused is true will hold PostEnqueue callbacks in memory
	postEnqueuePaused bool
	// pendingPostEnqueue stores the held PostEnqueue callbacks,
	// they will be executed after the budget drop below the low watermark.
	pendingPostEnqueue []func()

	metricMemoryBytes        prometheus.Gauge
	metricDiskBytes          prometheus.Gauge
	metricPendingPostEnqueue prometheus.Gauge
	metricSpilledBytes       prometheus.Observer

	keyspace   string
	changefeed string

	waitersMu    sync.Mutex
	nextWaiterID uint64
	waiters      map[uint64]chan struct{}
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
		metricPendingPostEnqueue: metrics.CloudStoragePendingPostEnqueueGauge.WithLabelValues(keyspace, changefeed),
		metricSpilledBytes:       metrics.CloudStorageSpillBytesHistogram.WithLabelValues(keyspace, changefeed),
		waiters:                  make(map[uint64]chan struct{}),
	}
	return controller
}

func (q *quotaController) shouldSpill(entryBytes int64) bool {
	return q.budget.shouldSpill(entryBytes)
}

func (q *quotaController) exceedsDiskQuota(entryBytes int64) bool {
	return q.budget.exceedsDiskQuota(entryBytes)
}

func (q *quotaController) wouldExceedDiskQuota(entryBytes int64) bool {
	return q.budget.wouldExceedDiskQuota(entryBytes)
}

func (q *quotaController) addDiskQuotaWaiter() (uint64, <-chan struct{}) {
	q.waitersMu.Lock()
	defer q.waitersMu.Unlock()

	q.nextWaiterID++
	waiterID := q.nextWaiterID
	waitCh := make(chan struct{}, 1)
	q.waiters[waiterID] = waitCh
	return waiterID, waitCh
}

func (q *quotaController) removeDiskQuotaWaiter(waiterID uint64) {
	q.waitersMu.Lock()
	delete(q.waiters, waiterID)
	q.waitersMu.Unlock()
}

// acquire records a newly accepted entry and returns callbacks that may run
// immediately. If total staged bytes cross the high watermark, acquire puts
// new PostEnqueue callbacks into the pending queue instead of running them inline.
func (q *quotaController) acquire(
	entryBytes int64,
	spilled bool,
	postEnqueue func(),
) func() {
	if q.budget.acquire(entryBytes, spilled) {
		q.postEnqueuePaused = true
	}
	if spilled {
		q.metricSpilledBytes.Observe(float64(entryBytes))
	}

	defer q.updateMetrics()
	if postEnqueue == nil {
		return nil
	}
	if q.postEnqueuePaused {
		q.pendingPostEnqueue = append(q.pendingPostEnqueue, postEnqueue)
		return nil
	}
	return postEnqueue
}

// release should be called after make sure the entry has been fully consumed or discarded.
// return all pending callbacks
func (q *quotaController) release(entryBytes int64, spilled bool) []func() {
	atOrBelowLowWatermark := q.budget.release(entryBytes, spilled)
	if spilled {
		q.wakeDiskQuotaWaiters()
	}
	defer q.updateMetrics()

	if !q.postEnqueuePaused {
		return nil
	}
	if !atOrBelowLowWatermark {
		return nil
	}

	callbacks := append([]func(){}, q.pendingPostEnqueue...)
	q.pendingPostEnqueue = nil
	q.postEnqueuePaused = false
	return callbacks
}

// deleteMetrics removes per-changefeed label values owned by this adapter.
func (q *quotaController) deleteMetrics() {
	metrics.CloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpoolDiskBytesGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStoragePendingPostEnqueueGauge.DeleteLabelValues(q.keyspace, q.changefeed)
	metrics.CloudStorageSpillBytesHistogram.DeleteLabelValues(q.keyspace, q.changefeed)
}

func (q *quotaController) updateMetrics() {
	q.metricMemoryBytes.Set(float64(q.budget.memoryBytes))
	q.metricDiskBytes.Set(float64(q.budget.diskBytes))
	q.metricPendingPostEnqueue.Set(float64(len(q.pendingPostEnqueue)))
}

func (q *quotaController) wakeDiskQuotaWaiters() {
	q.waitersMu.Lock()
	waiters := make([]chan struct{}, 0, len(q.waiters))
	for _, waitCh := range q.waiters {
		waiters = append(waiters, waitCh)
	}
	q.waiters = make(map[uint64]chan struct{})
	q.waitersMu.Unlock()

	for _, waitCh := range waiters {
		waitCh <- struct{}{}
	}
}
