// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	execDMLEventRowsAffectedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "dml_event_affected_row_count",
			Help:      "Total count of affected rows.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "count_type", "row_type"},
	)

	activeActiveConflictSkipRowsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "active_active_conflict_skip_rows_total",
			Help:      "Total number of rows skipped due to last-write-wins conflict resolution in TiDB active-active replication.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// ConflictDetectDuration records the duration of detecting conflict.
	ConflictDetectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_conflict_detect_duration",
			Help:      "Bucketed histogram of conflict detect time (s) for single DML statement.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	WorkerBatchFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_batch_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	WorkerFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	WorkerTotalDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_total_duration",
			Help:      "total duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	WorkerHandledRows = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_handled_rows",
			Help:      "Busy ratio (X ms in 1s) for all workers.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	WorkerEventRowCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_event_row_count",
			Help:      "Row count number for a single DML event handled by txn sink worker.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12), // 1~2048
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})
)

type rowsAffectedLabels struct {
	countType string
	rowType   string
}

// InitMetrics registers MySQL sink metrics.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(execDMLEventRowsAffectedCounter)
	registry.MustRegister(activeActiveConflictSkipRowsCounter)
	registry.MustRegister(ConflictDetectDuration)
	registry.MustRegister(WorkerBatchFlushDuration)
	registry.MustRegister(WorkerFlushDuration)
	registry.MustRegister(WorkerTotalDuration)
	registry.MustRegister(WorkerHandledRows)
	registry.MustRegister(WorkerEventRowCount)
}

func (w *Writer) recordTotalRowsAffected(actualRowsAffected, expectedRowsAffected int64) {
	w.getRowsAffectedCounter("actual", "total").Add(float64(actualRowsAffected))
	w.getRowsAffectedCounter("expected", "total").Add(float64(expectedRowsAffected))
}

func (w *Writer) recordRowsAffected(rowsAffected int64, rowType common.RowType) {
	w.getRowsAffectedCounter("actual", rowType.String()).Add(float64(rowsAffected))
	w.getRowsAffectedCounter("expected", rowType.String()).Add(1)
	w.recordTotalRowsAffected(rowsAffected, 1)
}

func (w *Writer) getRowsAffectedCounter(countType, rowType string) prometheus.Counter {
	labels := rowsAffectedLabels{countType: countType, rowType: rowType}
	counter, loaded := w.rowsAffectedCounters.Load(labels)
	if !loaded {
		counter := execDMLEventRowsAffectedCounter.WithLabelValues(
			w.ChangefeedID.Keyspace(), w.ChangefeedID.Name(), countType, rowType)
		w.rowsAffectedCounters.Store(labels, counter)
		return counter
	}
	return counter.(prometheus.Counter)
}

// DeleteDMLEventRowsAffectedMetrics deletes affected-row metric series for a MySQL sink.
func DeleteDMLEventRowsAffectedMetrics(changefeedID common.ChangeFeedID) {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	rowTypes := [...]common.RowType{
		common.RowTypeDelete,
		common.RowTypeInsert,
		common.RowTypeUpdate,
	}
	for _, countType := range [...]string{"actual", "expected"} {
		execDMLEventRowsAffectedCounter.DeleteLabelValues(keyspace, changefeed, countType, "total")
		for _, rowType := range rowTypes {
			execDMLEventRowsAffectedCounter.DeleteLabelValues(
				keyspace, changefeed, countType, rowType.String())
		}
	}
}
