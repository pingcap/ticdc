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
	maintainerCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of maintainer",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	maintainerCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "checkpoint_ts_lag",
			Help:      "checkpoint ts lag of maintainer in seconds",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	maintainerResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "resolved_ts",
			Help:      "resolved ts of maintainer",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	maintainerResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of maintainer in seconds",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	maintainerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "changefeed",
			Name:      "maintainer_counter",
			Help:      "The counter of changefeed maintainer",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// execDDLBlockingGauge records the count of blocking DDL.
	execDDLBlockingGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_blocking",
			Help:      "Total count of blocking ddl.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	maintainerHandleEventDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "handle_event_duration",
			Help:      "Bucketed histogram of maintainer handle event time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	maintainerEventChLenGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "event_ch_len",
			Help:      "length of maintainer event channel",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	scheduleTaskGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "task",
			Help:      "The total number of scheduler tasks",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	spanCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "span_count",
			Help:      "The total number of spans",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	tableCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_count",
			Help:      "The total number of tables",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})

	tableStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "table_replication_state",
			Help:      "The total number of tables in different replication states",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "state", "mode"})

	slowestTableIDGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_id",
			Help:      "The table ID of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	slowestTableCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_checkpoint_ts",
			Help:      "The checkpoint ts of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	slowestTableResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_resolved_ts",
			Help:      "The resolved ts of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	slowestTableStageCheckpointTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts",
			Help:      "Checkpoint ts of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	slowestTableStageResolvedTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts",
			Help:      "Resolved ts of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	slowestTableStageCheckpointTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag",
			Help:      "Checkpoint ts lag of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	slowestTableStageResolvedTsLagGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag",
			Help:      "Resolved ts lag of each stage of the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	slowestTableStageCheckpointTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_checkpoint_ts_lag_histogram",
			Help:      "Histogram of the slowest table checkpoint ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	slowestTableStageResolvedTsLagHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_stage_resolved_ts_lag_histogram",
			Help:      "Histogram of the slowest table resolved ts lag of each stage",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 36),
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "stage"})

	slowestTableRegionGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_region_count",
			Help:      "The number of regions captured by the slowest table",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	slowestTablePullerResolvedTs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts",
			Help:      "Puller Slowest ResolvedTs",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	slowestTablePullerResolvedTsLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "scheduler",
			Name:      "slow_table_puller_resolved_ts_lag",
			Help:      "Puller Slowest ResolvedTs lag",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	captureLeaseResponseCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_lease_response_total",
			Help:      "Number of capture write lease responses by handling result.",
		}, []string{"result"})

	captureLeaseResponseRejectedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_lease_response_rejected_total",
			Help:      "Number of rejected P2P write lease responses.",
		}, []string{"reason"})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(captureLeaseResponseCounter)
	registry.MustRegister(captureLeaseResponseRejectedCounter)
	registry.MustRegister(maintainerCheckpointTsGauge)
	registry.MustRegister(maintainerCheckpointTsLagGauge)
	registry.MustRegister(maintainerResolvedTsGauge)
	registry.MustRegister(maintainerResolvedTsLagGauge)
	registry.MustRegister(maintainerGauge)
	registry.MustRegister(execDDLBlockingGauge)
	registry.MustRegister(maintainerHandleEventDuration)
	registry.MustRegister(maintainerEventChLenGauge)
	registry.MustRegister(scheduleTaskGauge)
	registry.MustRegister(spanCountGauge)
	registry.MustRegister(tableCountGauge)
	registry.MustRegister(tableStateGauge)
	registry.MustRegister(slowestTableIDGauge)
	registry.MustRegister(slowestTableCheckpointTsGauge)
	registry.MustRegister(slowestTableResolvedTsGauge)
	registry.MustRegister(slowestTableStageCheckpointTsGaugeVec)
	registry.MustRegister(slowestTableStageResolvedTsGaugeVec)
	registry.MustRegister(slowestTableStageCheckpointTsLagGaugeVec)
	registry.MustRegister(slowestTableStageResolvedTsLagGaugeVec)
	registry.MustRegister(slowestTableStageCheckpointTsLagHistogramVec)
	registry.MustRegister(slowestTableStageResolvedTsLagHistogramVec)
	registry.MustRegister(slowestTableRegionGaugeVec)
	registry.MustRegister(slowestTablePullerResolvedTs)
	registry.MustRegister(slowestTablePullerResolvedTsLag)
}
