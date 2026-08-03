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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/prometheus/client_golang/prometheus"
)

// New creates a Statistics.
func New(changefeed common.ChangeFeedID, keyspaceID uint32) *Statistics {
	statistics := &Statistics{
		changefeedID:    changefeed,
		keyspaceID:      strconv.FormatUint(uint64(keyspaceID), 10),
		ddlTypes:        sync.Map{},
		rowsAffectedMap: sync.Map{},
	}

	keyspace := changefeed.Keyspace()
	changefeedID := changefeed.Name()
	statistics.metricExecDDLHis = execDDLHistogram.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecDDLRunningCnt = execDDLRunningGauge.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecBatchHis = execBatchHistogram.WithLabelValues(keyspace, changefeedID, statistics.keyspaceID)
	statistics.metricTotalWriteBytesCnt = totalWriteBytesCounter.WithLabelValues(keyspace, changefeedID)
	statistics.metricExecErrCntForDDL = executionErrorCounter.WithLabelValues(keyspace, changefeedID, "ddl")
	statistics.metricExecErrCntForDML = executionErrorCounter.WithLabelValues(keyspace, changefeedID, "dml")

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

// RecordDMLResult records the result of one downstream DML execution attempt.
// Successful attempts contribute their row count; failed attempts increment the
// DML execution error counter. DML event bytes are tracked separately because
// they are recorded only after the whole transaction is flushed.
func (b *Statistics) RecordDMLResult(rowCount int, err error) {
	if err != nil {
		b.metricExecErrCntForDML.Inc()
		return
	}
	b.metricExecBatchHis.Observe(float64(rowCount))
}

// TrackDMLEvent records the approximate size reported by a DML event after the
// whole transaction has been flushed to downstream. The size is snapshotted
// here so the callback does not retain the event.
func (b *Statistics) TrackDMLEvent(event *commonEvent.DMLEvent) {
	writeBytes := event.GetSize()
	event.AddPostFlushFunc(func() {
		b.metricTotalWriteBytesCnt.Add(float64(writeBytes))
	})
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
	metricExecDDLCounter := execDDLCounter.WithLabelValues(
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
		counter := execDMLEventRowsAffectedCounter.WithLabelValues(keyspace, changefeedID, countType, rowType)
		b.rowsAffectedMap.Store(key, counter)
		return counter
	}
	return counter.(prometheus.Counter)
}

// Close release some internal resources.
func (b *Statistics) Close() {
	keyspace := b.changefeedID.Keyspace()
	changefeedID := b.changefeedID.Name()
	execDDLHistogram.DeleteLabelValues(keyspace, changefeedID)
	execDDLRunningGauge.DeleteLabelValues(keyspace, changefeedID)
	execBatchHistogram.DeleteLabelValues(keyspace, changefeedID, b.keyspaceID)
	executionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "ddl")
	executionErrorCounter.DeleteLabelValues(keyspace, changefeedID, "dml")
	b.ddlTypes.Range(func(key, value any) bool {
		ddlType := key.(string)
		execDDLCounter.DeleteLabelValues(keyspace, changefeedID, ddlType)
		return true
	})
	b.rowsAffectedMap.Range(func(key, value any) bool {
		countTypeAndRowType := key.(string)
		splitTypes := strings.Split(countTypeAndRowType, "-")
		countType, rowType := splitTypes[0], splitTypes[1]
		execDMLEventRowsAffectedCounter.DeleteLabelValues(keyspace, changefeedID, countType, rowType)
		return true
	})
	totalWriteBytesCounter.DeleteLabelValues(keyspace, changefeedID)
}
