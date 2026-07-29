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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// New creates a Statistics.
func New(changefeed common.ChangeFeedID, keyspaceID uint32) *Statistics {
	statistics := &Statistics{
		changefeedID:    changefeed,
		keyspaceID:      metrics.FormatKeyspaceID(keyspaceID),
		ddlTypes:        sync.Map{},
		rowsAffectedMap: sync.Map{},
	}

	keyspace := changefeed.Keyspace()
	changefeedID := changefeed.Name()
	statistics.metricExecDDLHis = metrics.ExecDDLHistogram.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecDDLRunningCnt = metrics.ExecDDLRunningGauge.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecBatchHis = metrics.ExecBatchHistogram.WithLabelValues(keyspace, changefeedID, statistics.keyspaceID)
	statistics.metricTotalWriteBytesCnt = metrics.TotalWriteBytesCounter.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecErrCntForDDL = metrics.ExecutionErrorCounter.WithLabelValues(keyspace, changefeedID, "ddl")
	statistics.metricExecErrCntForDML = metrics.ExecutionErrorCounter.WithLabelValues(keyspace, changefeedID, "dml")

	return statistics
}

// Statistics maintains some status and metrics of the Sink
// Note: All methods of Statistics should be thread-safe.
type Statistics struct {
	changefeedID    common.ChangeFeedID
	keyspaceID      string
	ddlTypes        sync.Map
	rowsAffectedMap sync.Map

	// metricExecDDLHis records each DDL execution time duration.
	metricExecDDLHis prometheus.Observer
	// metricExecDDLRunningCnt records the count of running DDL.
	metricExecDDLRunningCnt prometheus.Gauge
	// metricExecBatchHis records the executed DML batch size.
	// this should be only useful for the MySQL Sink, and Kafka Sink with batched protocol, such as open-protocol.
	metricExecBatchHis prometheus.Observer
	// metricTotalWriteBytesCnt records the executed DML event size.
	metricTotalWriteBytesCnt prometheus.Counter

	// metricExecErrCntForDDL records the error count of the Sink for DDL.
	metricExecErrCntForDDL prometheus.Counter
	// metricExecErrCntForDML records the error count of the Sink for DML.
	metricExecErrCntForDML prometheus.Counter
}

// RecordBatchExecution stats batch executors which return (batchRowCount, batchWriteBytes, error).
func (b *Statistics) RecordBatchExecution(executor func() (int, int64, error)) error {
	batchSize, batchWriteBytes, err := executor()
	if err != nil {
		b.metricExecErrCntForDML.Inc()
		return err
	}
	b.metricExecBatchHis.Observe(float64(batchSize))
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
	metricExecDDLCounter := metrics.ExecDDLCounter.WithLabelValues(
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

func (b *Statistics) getRowsAffected(countType, rowType string) prometheus.Counter {
	key := fmt.Sprintf("%s-%s", countType, rowType)
	counter, loaded := b.rowsAffectedMap.Load(key)
	if !loaded {
		keyspace := b.changefeedID.Keyspace()
		changefeedID := b.changefeedID.Name()
		counter := metrics.ExecDMLEventRowsAffectedCounter.WithLabelValues(keyspace, changefeedID, countType, rowType)
		b.rowsAffectedMap.Store(key, counter)
		return counter
	}
	return counter.(prometheus.Counter)
}

// Close release some internal resources.
func (b *Statistics) Close() {
	keyspace := b.changefeedID.Keyspace()
	changefeedID := b.changefeedID.Name()
	metrics.ExecDDLHistogram.DeleteLabelValues(keyspace, changefeedID)
	metrics.ExecDDLRunningGauge.DeleteLabelValues(keyspace, changefeedID)
	metrics.ExecBatchHistogram.DeleteLabelValues(keyspace, changefeedID, b.keyspaceID)
	metrics.ExecutionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "ddl")
	metrics.ExecutionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "dml")
	b.ddlTypes.Range(func(key, value any) bool {
		ddlType := key.(string)
		metrics.ExecDDLCounter.DeleteLabelValues(keyspace, changefeedID, ddlType)
		return true
	})
	b.rowsAffectedMap.Range(func(key, value any) bool {
		countTypeAndRowType := key.(string)
		splitTypes := strings.Split(countTypeAndRowType, "-")
		countType, rowType := splitTypes[0], splitTypes[1]
		metrics.ExecDMLEventRowsAffectedCounter.DeleteLabelValues(keyspace, changefeedID, countType, rowType)
		return true
	})
	metrics.TotalWriteBytesCounter.DeleteLabelValues(keyspace, changefeedID)
}
