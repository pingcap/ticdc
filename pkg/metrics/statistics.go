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

	namespace := changefeed.Namespace()
	changefeedID := changefeed.Name()
	statistics.metricExecDDLHis = ExecDDLHistogram.WithLabelValues(namespace, changefeedID, sinkType)
	statistics.metricExecBatchHis = ExecBatchHistogram.WithLabelValues(namespace, changefeedID, sinkType)
	statistics.metricTotalWriteBytesCnt = TotalWriteBytesCounter.WithLabelValues(namespace, changefeedID, sinkType)
	statistics.metricExecErrCnt = ExecutionErrorCounter.WithLabelValues(namespace, changefeedID, sinkType)
	statistics.metricExecDMLCnt = ExecDMLEventCounter.WithLabelValues(namespace, changefeedID)
	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	sinkType     string
	changefeedID common.ChangeFeedID

	// metricExecDDLHis record each DDL execution time duration.
	metricExecDDLHis prometheus.Observer
	// metricExecBatchHis record the executed DML batch size.
	// this should be only useful for the MySQL Sink, and Kafka Sink with batched protocol, such as open-protocol.
	metricExecBatchHis prometheus.Observer
	// metricTotalWriteBytesCnt record the executed DML event size.
	metricTotalWriteBytesCnt prometheus.Counter

	// metricExecErrCnt record the error count of the Sink.
	metricExecErrCnt prometheus.Counter
	// metricExecDMLCnt record the executed DML event count of the Sink.
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
	b.metricExecDMLCnt.Add(float64(batchSize))
	b.metricTotalWriteBytesCnt.Add(float64(batchWriteBytes))
	return nil
}

// RecordDDLExecution record the time cost of execute ddl
func (b *Statistics) RecordDDLExecution(executor func() error) error {
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
	namespace := b.changefeedID.Namespace()
	changefeedID := b.changefeedID.Name()
	ExecDDLHistogram.DeleteLabelValues(namespace, changefeedID)
	ExecBatchHistogram.DeleteLabelValues(namespace, changefeedID)
	EventSizeHistogram.DeleteLabelValues(namespace, changefeedID)
	ExecutionErrorCounter.DeleteLabelValues(namespace, changefeedID)
	TotalWriteBytesCounter.DeleteLabelValues(namespace, changefeedID)
	ExecDMLEventCounter.DeleteLabelValues(namespace, changefeedID)
}
