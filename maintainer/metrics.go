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

package maintainer

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	MaintainerCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of maintainer",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	MaintainerCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "checkpoint_ts_lag",
			Help:      "checkpoint ts lag of maintainer in seconds",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	MaintainerResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "resolved_ts",
			Help:      "resolved ts of maintainer",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	MaintainerResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of maintainer in seconds",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	MaintainerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "changefeed",
			Name:      "maintainer_counter",
			Help:      "The counter of changefeed maintainer",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// ExecDDLBlockingGauge records the count of blocking DDL.
	ExecDDLBlockingGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_blocking",
			Help:      "Total count of blocking ddl.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	MaintainerHandleEventDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "handle_event_duration",
			Help:      "Bucketed histogram of maintainer handle event time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	MaintainerEventChLenGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "event_ch_len",
			Help:      "length of maintainer event channel",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	ScheduleTaskGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task",
			Help:      "The total number of scheduler tasks",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	SpanCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "span_count",
			Help:      "The total number of spans",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	TableCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_count",
			Help:      "The total number of tables",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	TableStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_replication_state",
			Help:      "The total number of tables in different replication states",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "state", "mode"})

	SlowestTableIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_id",
			Help:      "The table ID of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	SlowestTableCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_checkpoint_ts",
			Help:      "The checkpoint ts of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	SlowestTableResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_resolved_ts",
			Help:      "The resolved ts of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	SlowestTableStageCheckpointTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts",
			Help:      "Checkpoint ts of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	SlowestTableStageResolvedTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts",
			Help:      "Resolved ts of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	SlowestTableStageCheckpointTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag",
			Help:      "Checkpoint ts lag of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	SlowestTableStageResolvedTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag",
			Help:      "Resolved ts lag of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	SlowestTableStageCheckpointTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag_histogram",
			Help:      "Histogram of the slowest table checkpoint ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	SlowestTableStageResolvedTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag_histogram",
			Help:      "Histogram of the slowest table resolved ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	SlowestTableRegionGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_region_count",
			Help:      "The number of regions captured by the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	SlowestTablePullerResolvedTs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts",
			Help:      "Puller Slowest ResolvedTs",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	SlowestTablePullerResolvedTsLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts_lag",
			Help:      "Puller Slowest ResolvedTs lag",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(MaintainerCheckpointTsGauge)
	registry.MustRegister(MaintainerCheckpointTsLagGauge)
	registry.MustRegister(MaintainerResolvedTsGauge)
	registry.MustRegister(MaintainerResolvedTsLagGauge)
	registry.MustRegister(MaintainerGauge)
	registry.MustRegister(ExecDDLBlockingGauge)
	registry.MustRegister(MaintainerHandleEventDuration)
	registry.MustRegister(MaintainerEventChLenGauge)
	registry.MustRegister(ScheduleTaskGauge)
	registry.MustRegister(SpanCountGauge)
	registry.MustRegister(TableCountGauge)
	registry.MustRegister(TableStateGauge)
	registry.MustRegister(SlowestTableIDGauge)
	registry.MustRegister(SlowestTableCheckpointTsGauge)
	registry.MustRegister(SlowestTableResolvedTsGauge)
	registry.MustRegister(SlowestTableStageCheckpointTsGaugeVec)
	registry.MustRegister(SlowestTableStageResolvedTsGaugeVec)
	registry.MustRegister(SlowestTableStageCheckpointTsLagGaugeVec)
	registry.MustRegister(SlowestTableStageResolvedTsLagGaugeVec)
	registry.MustRegister(SlowestTableStageCheckpointTsLagHistogramVec)
	registry.MustRegister(SlowestTableStageResolvedTsLagHistogramVec)
	registry.MustRegister(SlowestTableRegionGaugeVec)
	registry.MustRegister(SlowestTablePullerResolvedTs)
	registry.MustRegister(SlowestTablePullerResolvedTsLag)
}
