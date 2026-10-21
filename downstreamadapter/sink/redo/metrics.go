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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redo

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// redoResolvedTsGauge records the resolved ts persisted by redo meta.
	redoResolvedTsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "redo",
		Name:      "resolved_ts",
		Help:      "Resolved ts persisted by redo meta",
	}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// redoCheckpointTsGauge records the checkpoint ts persisted by redo meta.
	redoCheckpointTsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "redo",
		Name:      "checkpoint_ts",
		Help:      "Checkpoint ts persisted by redo meta",
	}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// redoTotalRowsCountGauge records the total number of rows written to redo log.
	redoTotalRowsCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "redo",
		Name:      "total_rows_count",
		Help:      "The total count of rows that are processed by redo writer",
	}, []string{metrics.GetKeyspaceLabel(), "changefeed", "type"})

	// redoWriteLogDurationHistogram records the latency distributions of writeLog.
	redoWriteLogDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "redo",
		Name:      "write_log_duration_seconds",
		Help:      "The latency distributions of writeLog called by redo sink",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16),
	}, []string{metrics.GetKeyspaceLabel(), "changefeed", "type"})

	// redoFlushLogDurationHistogram records the latency distributions of flushLog.
	redoFlushLogDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "redo",
		Name:      "flush_log_duration_seconds",
		Help:      "The latency distributions of flushLog called by redo sink",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16),
	}, []string{metrics.GetKeyspaceLabel(), "changefeed", "type"})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(redoResolvedTsGauge)
	registry.MustRegister(redoCheckpointTsGauge)
	registry.MustRegister(redoTotalRowsCountGauge)
	registry.MustRegister(redoWriteLogDurationHistogram)
	registry.MustRegister(redoFlushLogDurationHistogram)
}
