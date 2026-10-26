// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"strconv"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

// NewStatistics creates a statistics
func NewStatistics(
	changefeed common.ChangeFeedID,
	keyspaceID uint32,
) *Statistics {
	statistics := &Statistics{
		changefeedID: changefeed,
		keyspaceID:   strconv.FormatUint(uint64(keyspaceID), 10),
	}

	keyspace := changefeed.Keyspace()
	changefeedID := changefeed.Name()
	statistics.metricExecDDLHis = execDDLHistogram.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecDDLRunningCnt = execDDLRunningGauge.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecBatchHis = execBatchHistogram.WithLabelValues(keyspace, changefeedID, statistics.keyspaceID)
	statistics.metricExecBatchBytesHis = execBatchWriteBytesHistogram.WithLabelValues(keyspace, changefeedID)
	statistics.metricTotalWriteBytesCnt = totalWriteBytesCounter.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecErrCntForDDL = executionErrorCounter.WithLabelValues(keyspace, changefeedID, "ddl")
	statistics.metricExecErrCntForDML = executionErrorCounter.WithLabelValues(keyspace, changefeedID, "dml")
	statistics.metricExecDMLCnt = execDMLEventCounter.WithLabelValues(keyspace, changefeedID)

	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	closed       atomic.Bool
	changefeedID common.ChangeFeedID
	keyspaceID   string

	// metricExecDDLHis records each DDL execution time duration.
	metricExecDDLHis prometheus.Observer
	// metricExecDDLRunningCnt records the count of running DDL.
	metricExecDDLRunningCnt prometheus.Gauge
	// metricExecBatchHis records the row count of each flushed DML event.
	metricExecBatchHis prometheus.Observer
	// metricExecBatchBytesHis records the approximate original size of each flushed DML event.
	metricExecBatchBytesHis prometheus.Observer
	// metricTotalWriteBytesCnt records the executed DML event size.
	metricTotalWriteBytesCnt prometheus.Counter

	// metricExecErrCntForDDL records the error count of the Sink for DDL.
	metricExecErrCntForDDL prometheus.Counter
	// metricExecErrCntForDML records the error count of the Sink for DML.
	metricExecErrCntForDML prometheus.Counter
	// metricExecDMLCnt records the executed DML event count of the Sink.
	metricExecDMLCnt prometheus.Counter
}

// TrackDMLEvent records rows and approximate original bytes when the event is flushed.
func (b *Statistics) TrackDMLEvent(event *commonEvent.DMLEvent) {
	rows, size := int(event.Len()), event.GetSize()
	event.AddPostFlushFunc(func() {
		b.RecordDMLResult(rows, size, nil)
	})
}

// RecordDMLResult records a completed downstream DML write. Successful writes
// contribute rows and bytes; failed writes increment the error counter.
func (b *Statistics) RecordDMLResult(batchSize int, batchWriteBytes int64, err error) {
	if b.closed.Load() {
		return
	}
	if err != nil {
		b.metricExecErrCntForDML.Inc()
		return
	}
	b.metricExecBatchHis.Observe(float64(batchSize))
	b.metricExecBatchBytesHis.Observe(float64(batchWriteBytes))
	b.metricExecDMLCnt.Add(float64(batchSize))
	b.metricTotalWriteBytesCnt.Add(float64(batchWriteBytes))
}

// RecordDDLExecution executes a DDL and records its duration and result.
func (b *Statistics) RecordDDLExecution(executor func() (string, error)) error {
	if b.closed.Load() {
		_, err := executor()
		return err
	}
	b.metricExecDDLRunningCnt.Inc()
	defer b.metricExecDDLRunningCnt.Dec()
	start := time.Now()
	ddlType, err := executor()
	if b.closed.Load() {
		return err
	}
	if err != nil {
		b.metricExecErrCntForDDL.Inc()
		return err
	}
	execDDLCounter.WithLabelValues(b.changefeedID.Keyspace(), b.changefeedID.Name(), ddlType).Inc()
	b.metricExecDDLHis.Observe(time.Since(start).Seconds())
	return nil
}

// Close release some internal resources.
func (b *Statistics) Close() {
	if b.closed.Swap(true) {
		return
	}
	keyspace := b.changefeedID.Keyspace()
	changefeedID := b.changefeedID.Name()
	execDDLHistogram.DeleteLabelValues(keyspace, changefeedID)
	execDDLRunningGauge.DeleteLabelValues(keyspace, changefeedID)
	execBatchHistogram.DeleteLabelValues(keyspace, changefeedID, b.keyspaceID)
	execBatchWriteBytesHistogram.DeleteLabelValues(keyspace, changefeedID)
	eventSizeHistogram.DeleteLabelValues(keyspace, changefeedID)
	executionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "ddl")
	executionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "dml")
	totalWriteBytesCounter.DeleteLabelValues(keyspace, changefeedID)
	execDMLEventCounter.DeleteLabelValues(keyspace, changefeedID)
	execDDLCounter.DeletePartialMatch(prometheus.Labels{
		getKeyspaceLabel(): keyspace,
		"changefeed":       changefeedID,
	})
}
