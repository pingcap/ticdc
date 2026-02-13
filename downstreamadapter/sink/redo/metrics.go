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
// See the License for the specific language governing permissions and
// limitations under the License.

package redo

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/prometheus/client_golang/prometheus"
)

type redoSinkMetrics struct {
	changefeedID common.ChangeFeedID

	rowWriteLogDuration prometheus.Observer
	ddlWriteLogDuration prometheus.Observer

	rowTotalCount prometheus.Counter
	ddlTotalCount prometheus.Counter

	rowWorkerBusyRatio prometheus.Counter
	ddlWorkerBusyRatio prometheus.Counter
}

func newRedoSinkMetrics(changefeedID common.ChangeFeedID) *redoSinkMetrics {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	return &redoSinkMetrics{
		changefeedID: changefeedID,
		rowWriteLogDuration: metrics.RedoWriteLogDurationHistogram.
			WithLabelValues(keyspace, changefeed, redo.RedoRowLogFileType),
		ddlWriteLogDuration: metrics.RedoWriteLogDurationHistogram.
			WithLabelValues(keyspace, changefeed, redo.RedoDDLLogFileType),
		rowTotalCount: metrics.RedoTotalRowsCountGauge.
			WithLabelValues(keyspace, changefeed, redo.RedoRowLogFileType),
		ddlTotalCount: metrics.RedoTotalRowsCountGauge.
			WithLabelValues(keyspace, changefeed, redo.RedoDDLLogFileType),
		rowWorkerBusyRatio: metrics.RedoWorkerBusyRatio.
			WithLabelValues(keyspace, changefeed, redo.RedoRowLogFileType),
		ddlWorkerBusyRatio: metrics.RedoWorkerBusyRatio.
			WithLabelValues(keyspace, changefeed, redo.RedoDDLLogFileType),
	}
}

func (m *redoSinkMetrics) observeRowWrite(rows int, duration time.Duration) {
	if rows > 0 {
		m.rowTotalCount.Add(float64(rows))
	}
	m.rowWriteLogDuration.Observe(duration.Seconds())
	m.rowWorkerBusyRatio.Add(duration.Seconds())
}

func (m *redoSinkMetrics) observeDDLWrite(duration time.Duration) {
	m.ddlTotalCount.Inc()
	m.ddlWriteLogDuration.Observe(duration.Seconds())
	m.ddlWorkerBusyRatio.Add(duration.Seconds())
}

func (m *redoSinkMetrics) close() {
	keyspace := m.changefeedID.Keyspace()
	changefeed := m.changefeedID.Name()

	metrics.RedoWriteLogDurationHistogram.DeleteLabelValues(keyspace, changefeed, redo.RedoRowLogFileType)
	metrics.RedoWriteLogDurationHistogram.DeleteLabelValues(keyspace, changefeed, redo.RedoDDLLogFileType)
	metrics.RedoTotalRowsCountGauge.DeleteLabelValues(keyspace, changefeed, redo.RedoRowLogFileType)
	metrics.RedoTotalRowsCountGauge.DeleteLabelValues(keyspace, changefeed, redo.RedoDDLLogFileType)
	metrics.RedoWorkerBusyRatio.DeleteLabelValues(keyspace, changefeed, redo.RedoRowLogFileType)
	metrics.RedoWorkerBusyRatio.DeleteLabelValues(keyspace, changefeed, redo.RedoDDLLogFileType)
}
