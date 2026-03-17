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

type metricCollector struct {
	keyspace string
	name     string

	rowWriteLogDuration prometheus.Observer
	ddlWriteLogDuration prometheus.Observer

	rowTotalCount prometheus.Counter
	ddlTotalCount prometheus.Counter

	rowWorkerBusyRatio prometheus.Counter
	ddlWorkerBusyRatio prometheus.Counter
}

func newMetricCollector(changefeedID common.ChangeFeedID) *metricCollector {
	keyspace := changefeedID.Keyspace()
	name := changefeedID.Name()
	return &metricCollector{
		keyspace:            keyspace,
		name:                name,
		rowTotalCount:       metrics.RedoTotalRowsCountGauge.WithLabelValues(keyspace, name, redo.RedoRowLogFileType),
		rowWriteLogDuration: metrics.RedoWriteLogDurationHistogram.WithLabelValues(keyspace, name, redo.RedoRowLogFileType),
		rowWorkerBusyRatio:  metrics.RedoWorkerBusyRatio.WithLabelValues(keyspace, name, redo.RedoRowLogFileType),

		ddlTotalCount:       metrics.RedoTotalRowsCountGauge.WithLabelValues(keyspace, name, redo.RedoDDLLogFileType),
		ddlWriteLogDuration: metrics.RedoWriteLogDurationHistogram.WithLabelValues(keyspace, name, redo.RedoDDLLogFileType),
		ddlWorkerBusyRatio:  metrics.RedoWorkerBusyRatio.WithLabelValues(keyspace, name, redo.RedoDDLLogFileType),
	}
}

func (m *metricCollector) observeRowWrite(rows int, duration time.Duration) {
	if rows > 0 {
		m.rowTotalCount.Add(float64(rows))
	}
	m.rowWriteLogDuration.Observe(duration.Seconds())
	m.rowWorkerBusyRatio.Add(duration.Seconds())
}

func (m *metricCollector) observeDDLWrite(duration time.Duration) {
	m.ddlTotalCount.Inc()
	m.ddlWriteLogDuration.Observe(duration.Seconds())
	m.ddlWorkerBusyRatio.Add(duration.Seconds())
}

func (m *metricCollector) close() {
	for _, logType := range []string{redo.RedoRowLogFileType, redo.RedoDDLLogFileType} {
		metrics.RedoWriteLogDurationHistogram.DeleteLabelValues(m.keyspace, m.name, logType)
		metrics.RedoTotalRowsCountGauge.DeleteLabelValues(m.keyspace, m.name, logType)
		metrics.RedoWorkerBusyRatio.DeleteLabelValues(m.keyspace, m.name, logType)
	}
}
