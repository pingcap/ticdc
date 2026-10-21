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
	"github.com/pingcap/ticdc/pkg/redo"
	redocommon "github.com/pingcap/ticdc/pkg/redo/common"
	"github.com/prometheus/client_golang/prometheus"
)

type metricCollector struct {
	keyspace string
	name     string

	rowWriteLogDuration prometheus.Observer
	ddlWriteLogDuration prometheus.Observer

	rowTotalCount prometheus.Counter
	ddlTotalCount prometheus.Counter

	ddlWorkerBusyRatio prometheus.Counter
}

func newMetricCollector(changefeedID common.ChangeFeedID) *metricCollector {
	keyspace := changefeedID.Keyspace()
	name := changefeedID.Name()
	return &metricCollector{
		keyspace:            keyspace,
		name:                name,
		rowTotalCount:       redoTotalRowsCountGauge.WithLabelValues(keyspace, name, redo.RedoRowLogFileType),
		rowWriteLogDuration: redoWriteLogDurationHistogram.WithLabelValues(keyspace, name, redo.RedoRowLogFileType),

		ddlTotalCount:       redoTotalRowsCountGauge.WithLabelValues(keyspace, name, redo.RedoDDLLogFileType),
		ddlWriteLogDuration: redoWriteLogDurationHistogram.WithLabelValues(keyspace, name, redo.RedoDDLLogFileType),
		ddlWorkerBusyRatio:  redocommon.RedoWorkerBusyRatio.WithLabelValues(keyspace, name, redo.RedoDDLLogFileType),
	}
}

func (m *metricCollector) observeRowWrite(rows int, duration time.Duration) {
	if rows > 0 {
		m.rowTotalCount.Add(float64(rows))
	}
	// DML write duration is the enqueue latency exposed by AddDMLEvents.
	// The actual DML worker busy time is recorded by fileWorkerGroup.
	m.rowWriteLogDuration.Observe(duration.Seconds())
}

func (m *metricCollector) observeDDLWrite(duration time.Duration) {
	m.ddlTotalCount.Inc()
	m.ddlWriteLogDuration.Observe(duration.Seconds())
	m.ddlWorkerBusyRatio.Add(duration.Seconds())
}

func (m *metricCollector) close() {
	for _, logType := range []string{redo.RedoRowLogFileType, redo.RedoDDLLogFileType} {
		redoWriteLogDurationHistogram.DeleteLabelValues(m.keyspace, m.name, logType)
		redoTotalRowsCountGauge.DeleteLabelValues(m.keyspace, m.name, logType)
	}
	redocommon.RedoWorkerBusyRatio.DeleteLabelValues(m.keyspace, m.name, redo.RedoDDLLogFileType)
}
