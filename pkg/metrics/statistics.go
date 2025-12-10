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

package metrics

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
)

// NewStatistics creates a statistics
func NewStatistics(
	changefeed common.ChangeFeedID,
	sinkType string,
) *Statistics {
	statistics := &Statistics{
		sinkType:     sinkType,
		changefeedID: changefeed,
	}

	keyspace := changefeed.Keyspace()
	changefeedID := changefeed.Name()
	statistics.metricExecDDLHis = ExecDDLHistogram.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricExecDDLRunningCnt = ExecDDLRunningGauge.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricExecBatchHis = ExecBatchHistogram.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricExecBatchBytesHis = ExecBatchWriteBytesHistogram.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricTotalWriteBytesCnt = TotalWriteBytesCounter.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricExecErrCnt = ExecutionErrorCounter.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricExecDMLCnt = ExecDMLEventCounter.WithLabelValues(keyspace, changefeedID)
	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	sinkType     string
	changefeedID common.ChangeFeedID

	// metricExecDDLHis records each DDL execution time duration.
	metricExecDDLHis prometheus.Observer
	// metricExecDDLRunningCnt records the count of running DDL.
	metricExecDDLRunningCnt prometheus.Gauge
	// metricExecBatchHis records the executed DML batch size.
	// this should be only useful for the MySQL Sink, and Kafka Sink with batched protocol, such as open-protocol.
	metricExecBatchHis prometheus.Observer
	// metricExecBatchBytesHis records the executed batch write bytes.
	metricExecBatchBytesHis prometheus.Observer
	// metricTotalWriteBytesCnt records the executed DML event size.
	metricTotalWriteBytesCnt prometheus.Counter

	// metricExecErrCnt records the error count of the Sink.
	metricExecErrCnt prometheus.Counter
	// metricExecDMLCnt records the executed DML event count of the Sink.
	metricExecDMLCnt prometheus.Counter
}

// RecordBatchExecution stats batch executors which return (batchRowCount, batchWriteBytes, error).
func (b *Statistics) RecordBatchExecution(executor func() (int, int64, error)) error {
	batchSize, batchWriteBytes, err := executor()
	if err != nil {
		b.metricExecErrCnt.Inc()
		return err
	}
	b.metricExecBatchHis.Observe(float64(batchSize))
	b.metricExecBatchBytesHis.Observe(float64(batchWriteBytes))
	b.metricExecDMLCnt.Add(float64(batchSize))
	b.metricTotalWriteBytesCnt.Add(float64(batchWriteBytes))
	return nil
}

// RecordDDLExecution record the time cost of execute ddl
func (b *Statistics) RecordDDLExecution(executor func() error) error {
	b.metricExecDDLRunningCnt.Inc()
	defer b.metricExecDDLRunningCnt.Dec()

	start := time.Now()
	if err := executor(); err != nil {
		b.metricExecErrCnt.Inc()
		return err
	}
	b.metricExecDDLHis.Observe(time.Since(start).Seconds())
	return nil
}

// Close release some internal resources.
func (b *Statistics) Close() {
	keyspace := b.changefeedID.Keyspace()
	changefeedID := b.changefeedID.Name()
	ExecDDLHistogram.DeleteLabelValues(keyspace, changefeedID)
	ExecBatchHistogram.DeleteLabelValues(keyspace, changefeedID)
	ExecBatchWriteBytesHistogram.DeleteLabelValues(keyspace, changefeedID)
	EventSizeHistogram.DeleteLabelValues(keyspace, changefeedID)
	ExecutionErrorCounter.DeleteLabelValues(keyspace, changefeedID)
	TotalWriteBytesCounter.DeleteLabelValues(keyspace, changefeedID)
	ExecDMLEventCounter.DeleteLabelValues(keyspace, changefeedID)
}
