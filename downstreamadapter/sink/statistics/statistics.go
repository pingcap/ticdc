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
	statistics.metricExecErrCntForDDL = executionErrorCounter.WithLabelValues(keyspace, changefeedID, "ddl")
	statistics.metricExecErrCntForDML = executionErrorCounter.WithLabelValues(keyspace, changefeedID, "dml")

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
	// metricExecBatchHis records the row count of each successfully delivered DML batch.
	metricExecBatchHis prometheus.Observer
	// metricExecBatchBytesHis records the write bytes of each successfully delivered DML batch.
	metricExecBatchBytesHis prometheus.Observer

	// metricExecErrCntForDDL records the error count of the Sink for DDL.
	metricExecErrCntForDDL prometheus.Counter
	// metricExecErrCntForDML records the error count of the Sink for DML.
	metricExecErrCntForDML prometheus.Counter
}

// RecordDMLBatch records a successfully delivered DML batch.
func (b *Statistics) RecordDMLBatch(rows int, writeBytes int64) {
	if b.closed.Load() {
		return
	}
	b.metricExecBatchHis.Observe(float64(rows))
	b.metricExecBatchBytesHis.Observe(float64(writeBytes))
}

// RecordDMLError records a failed downstream DML write.
func (b *Statistics) RecordDMLError() {
	if !b.closed.Load() {
		b.metricExecErrCntForDML.Inc()
	}
}

// RecordDDLExecution executes a DDL and records its duration and result.
func (b *Statistics) RecordDDLExecution(executor func() (string, error)) error {
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
	executionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "ddl")
	executionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "dml")
	execDDLCounter.DeletePartialMatch(prometheus.Labels{
		getKeyspaceLabel(): keyspace,
		"changefeed":       changefeedID,
	})
}
