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
	"fmt"
	"strings"
	"sync"
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
		sinkType:        sinkType,
		changefeedID:    changefeed,
		ddlTypes:        sync.Map{},
		rowsAffectedMap: sync.Map{},
	}

	keyspace := changefeed.Keyspace()
	changefeedID := changefeed.Name()
	statistics.metricExecDDLHis = ExecDDLHistogram.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecDDLRunningCnt = ExecDDLRunningGauge.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecBatchHis = ExecBatchHistogram.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricExecBatchBytesHis = ExecBatchWriteBytesHistogram.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricTotalWriteBytesCnt = TotalWriteBytesCounter.WithLabelValues(keyspace, changefeedID, sinkType)
	statistics.metricExecErrCntForDDL = ExecutionErrorCounter.WithLabelValues(keyspace, changefeedID, "ddl")
	statistics.metricExecErrCntForDML = ExecutionErrorCounter.WithLabelValues(keyspace, changefeedID, "dml")
	statistics.metricExecDMLCnt = ExecDMLEventCounter.WithLabelValues(keyspace, changefeedID)

	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	sinkType        string
	changefeedID    common.ChangeFeedID
	ddlTypes        sync.Map
	rowsAffectedMap sync.Map

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

	// metricExecErrCntForDDL records the error count of the Sink for DDL.
	metricExecErrCntForDDL prometheus.Counter
	// metricExecErrCntForDML records the error count of the Sink for DML.
	metricExecErrCntForDML prometheus.Counter
	// metricExecDMLCnt records the executed DML event count of the Sink.
	metricExecDMLCnt prometheus.Counter
}

// RecordBatchExecution stats batch executors which return (batchRowCount, batchWriteBytes, error).
func (b *Statistics) RecordBatchExecution(executor func() (int, int64, error)) error {
	batchSize, batchWriteBytes, err := executor()
	if err != nil {
		b.metricExecErrCntForDML.Inc()
		return err
	}
	b.metricExecBatchHis.Observe(float64(batchSize))
	b.metricExecBatchBytesHis.Observe(float64(batchWriteBytes))
	b.metricExecDMLCnt.Add(float64(batchSize))
	b.metricTotalWriteBytesCnt.Add(float64(batchWriteBytes))
	return nil
}

// RecordDDLExecution record the time cost of execute ddl
func (b *Statistics) RecordDDLExecution(executor func() (string, error)) error {
	b.metricExecDDLRunningCnt.Inc()
	defer b.metricExecDDLRunningCnt.Dec()

	var (
		ddlType string
		err     error
	)
	start := time.Now()
	if ddlType, err = executor(); err != nil {
		b.metricExecErrCntForDDL.Inc()
		return err
	}
	metricExecDDLCounter := ExecDDLCounter.WithLabelValues(
		b.changefeedID.Keyspace(), b.changefeedID.Name(), ddlType)
	metricExecDDLCounter.Inc()
	b.ddlTypes.Store(ddlType, struct{}{})
	b.metricExecDDLHis.Observe(time.Since(start).Seconds())
	return nil
}

func (b *Statistics) RecordTotalRowsAffected(actualRowsAffected, expectedRowsAffected int64) {
	b.getRowsAffected("actual", "total").Add(float64(actualRowsAffected))
	b.getRowsAffected("expected", "total").Add(float64(expectedRowsAffected))
}

func (b *Statistics) RecordRowsAffected(rowsAffected int64, rowType common.RowType) {
	b.getRowsAffected("actual", rowType.String()).Add(float64(rowsAffected))
	b.getRowsAffected("expected", rowType.String()).Add(1)
	b.RecordTotalRowsAffected(rowsAffected, 1)
}

func (b *Statistics) getRowsAffected(count_type, row_type string) prometheus.Counter {
	key := fmt.Sprintf("%s-%s", count_type, row_type)
	counter, loaded := b.rowsAffectedMap.Load(key)
	if !loaded {
		keyspace := b.changefeedID.Keyspace()
		changefeedID := b.changefeedID.Name()
		counter := ExecDMLEventRowsAffectedCounter.WithLabelValues(keyspace, changefeedID, count_type, row_type)
		b.rowsAffectedMap.Store(key, counter)
		return counter
	}
	return counter.(prometheus.Counter)
}

// Close release some internal resources.
func (b *Statistics) Close() {
	keyspace := b.changefeedID.Keyspace()
	changefeedID := b.changefeedID.Name()
	ExecDDLHistogram.DeleteLabelValues(keyspace, changefeedID)
	ExecBatchHistogram.DeleteLabelValues(keyspace, changefeedID)
	ExecBatchWriteBytesHistogram.DeleteLabelValues(keyspace, changefeedID)
	EventSizeHistogram.DeleteLabelValues(keyspace, changefeedID)
	ExecutionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "ddl")
	ExecutionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "dml")
	b.ddlTypes.Range(func(key, value any) bool {
		ddlType := key.(string)
		ExecDDLCounter.DeleteLabelValues(keyspace, changefeedID, ddlType)
		return true
	})
	b.rowsAffectedMap.Range(func(key, value any) bool {
		countTypeAndRowType := key.(string)
		splitTypes := strings.Split(countTypeAndRowType, "-")
		countType, rowType := splitTypes[0], splitTypes[1]
		ExecDMLEventRowsAffectedCounter.DeleteLabelValues(keyspace, changefeedID, countType, rowType)
		return true
	})
	TotalWriteBytesCounter.DeleteLabelValues(keyspace, changefeedID)
	ExecDMLEventCounter.DeleteLabelValues(keyspace, changefeedID)
}
