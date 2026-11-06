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

package mysql

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// changefeedDownstreamIsTiDBGauge indicates whether the downstream of a
	// MySQL-compatible sink is confirmed to be TiDB (1 means yes).
	//
	// This metric is only set when the sink can positively identify TiDB (for
	// example by executing `SELECT tidb_version()`), and is intentionally absent
	// for MySQL or unknown downstreams. Dashboards can use it to override the
	// scheme-derived "mysql" label value with "tidb" for running changefeeds.
	changefeedDownstreamIsTiDBGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "changefeed_downstream_is_tidb",
			Help:      "Whether the downstream of a changefeed is confirmed to be TiDB (1 means yes).",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	workerBatchFlushDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_batch_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	workerFlushDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_flush_duration",
			Help:      "Flush duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	workerTotalDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_total_duration",
			Help:      "total duration (s) for txn worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	workerHandledRowsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_handled_rows",
			Help:      "Busy ratio (X ms in 1s) for all workers.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})

	workerEventRowCountHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_worker_event_row_count",
			Help:      "Row count number for a single DML event handled by txn sink worker.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12), // 1~2048
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "id"})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(changefeedDownstreamIsTiDBGauge)
	registry.MustRegister(workerBatchFlushDurationHistogram)
	registry.MustRegister(workerFlushDurationHistogram)
	registry.MustRegister(workerTotalDurationHistogram)
	registry.MustRegister(workerHandledRowsCounter)
	registry.MustRegister(workerEventRowCountHistogram)
}
