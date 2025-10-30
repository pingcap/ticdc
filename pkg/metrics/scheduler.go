// Copyright 2024 PingCAP, Inc.
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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ScheduleTaskGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task",
			Help:      "The total number of scheduler tasks",
		}, []string{getKeyspaceLabel(), "changefeed", "mode"})

	SpanCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "span_count",
			Help:      "The total number of spans",
		}, []string{getKeyspaceLabel(), "changefeed", "mode"})
	TableCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_count",
			Help:      "The total number of tables",
		}, []string{getKeyspaceLabel(), "changefeed", "mode"})
	TableStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_replication_state",
			Help:      "The total number of tables in different replication states",
		}, []string{getKeyspaceLabel(), "changefeed", "state", "mode"})
	SlowestTableIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_id",
			Help:      "The table ID of the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed"})
	SlowestTableCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_checkpoint_ts",
			Help:      "The checkpoint ts of the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed"})
	SlowestTableResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_resolved_ts",
			Help:      "The resolved ts of the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed"})
	SlowestTableStageCheckpointTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts",
			Help:      "Checkpoint ts of each stage of the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed", "stage"})
	SlowestTableStageResolvedTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts",
			Help:      "Resolved ts of each stage of the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed", "stage"})
	SlowestTableStageCheckpointTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag",
			Help:      "Checkpoint ts lag of each stage of the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed", "stage"})
	SlowestTableStageResolvedTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag",
			Help:      "Resolved ts lag of each stage of the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed", "stage"})
	SlowestTableStageCheckpointTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag_histogram",
			Help:      "Histogram of the slowest table checkpoint ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{getKeyspaceLabel(), "changefeed", "stage"})
	SlowestTableStageResolvedTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag_histogram",
			Help:      "Histogram of the slowest table resolved ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{getKeyspaceLabel(), "changefeed", "stage"})
	SlowestTableRegionGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_region_count",
			Help:      "The number of regions captured by the slowest table",
		}, []string{getKeyspaceLabel(), "changefeed"})

	SlowestTablePullerResolvedTs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts",
			Help:      "Puller Slowest ResolvedTs",
		}, []string{getKeyspaceLabel(), "changefeed"})
	SlowestTablePullerResolvedTsLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts_lag",
			Help:      "Puller Slowest ResolvedTs lag",
		}, []string{getKeyspaceLabel(), "changefeed"})

	// checker related
	SplitSpanCheckDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "split_span_check_duration",
			Help:      "Bucketed histogram of split span check time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{getKeyspaceLabel(), "changefeed", "group_id"})
)

func initSchedulerMetrics(registry *prometheus.Registry) {
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

	registry.MustRegister(SplitSpanCheckDuration)
}
