// Copyright 2025 PingCAP, Inc.
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

import "github.com/prometheus/client_golang/prometheus"

var (
	IcebergGlobalResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_global_resolved_ts",
			Help:      "Latest global resolved ts (TSO) observed by iceberg sink per changefeed.",
		}, []string{getKeyspaceLabel(), "changefeed"})

	IcebergCommitRoundDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_commit_round_duration_seconds",
			Help:      "Bucketed histogram of iceberg commit round duration (s) per changefeed.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20), // 10ms~5243s
		}, []string{getKeyspaceLabel(), "changefeed"})

	IcebergCommitConflictsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_commit_conflicts_total",
			Help:      "Total iceberg commit conflicts per table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})

	IcebergCommitRetriesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_commit_retries_total",
			Help:      "Total iceberg commit retries per table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})

	IcebergCommitDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_commit_duration_seconds",
			Help:      "Bucketed histogram of iceberg commit duration (s) per table.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20), // 10ms~5243s
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})

	IcebergLastCommittedResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_last_committed_resolved_ts",
			Help:      "Last committed resolved ts (TSO) per iceberg table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})

	IcebergResolvedTsLagSecondsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_resolved_ts_lag_seconds",
			Help:      "Lag (s) between now and resolved ts physical time per iceberg table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})

	IcebergLastCommittedSnapshotIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_last_committed_snapshot_id",
			Help:      "Last committed snapshot id per iceberg table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})

	IcebergFilesWrittenCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_files_written_total",
			Help:      "Total iceberg data and delete files written per table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table", "type"})

	IcebergBytesWrittenCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_bytes_written_total",
			Help:      "Total iceberg data and delete bytes written per table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table", "type"})

	IcebergBufferedRowsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_buffered_rows",
			Help:      "Buffered rows waiting to be committed per iceberg table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})

	IcebergBufferedBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "iceberg_buffered_bytes",
			Help:      "Estimated buffered bytes waiting to be committed per iceberg table.",
		}, []string{getKeyspaceLabel(), "changefeed", "schema", "table"})
)

func initIcebergMetrics(registry *prometheus.Registry) {
	registry.MustRegister(IcebergGlobalResolvedTsGauge)
	registry.MustRegister(IcebergCommitRoundDurationHistogram)
	registry.MustRegister(IcebergCommitConflictsCounter)
	registry.MustRegister(IcebergCommitRetriesCounter)
	registry.MustRegister(IcebergCommitDurationHistogram)
	registry.MustRegister(IcebergLastCommittedResolvedTsGauge)
	registry.MustRegister(IcebergResolvedTsLagSecondsGauge)
	registry.MustRegister(IcebergLastCommittedSnapshotIDGauge)
	registry.MustRegister(IcebergFilesWrittenCounter)
	registry.MustRegister(IcebergBytesWrittenCounter)
	registry.MustRegister(IcebergBufferedRowsGauge)
	registry.MustRegister(IcebergBufferedBytesGauge)
}
