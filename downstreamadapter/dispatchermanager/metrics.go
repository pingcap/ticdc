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

package dispatchermanager

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	tableTriggerEventDispatcherGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "table_trigger_dispatcher_count",
			Help:      "The number of table dispatchers",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "event_type"})

	eventDispatcherGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "table_dispatcher_count",
			Help:      "The number of table dispatchers",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "event_type"})

	createDispatcherDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "create_dispatcher_duration",
			Help:      "Bucketed histogram of create dispatcher time (s) for table span.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 20), // 1us~524ms
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "event_type"})

	dispatcherManagerResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "resolved_ts",
			Help:      "Resolved ts of event dispatcher manager(changefeed)",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	dispatcherManagerResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "resolved_ts_lag",
			Help:      "Resolved ts lag of event dispatcher manager(changefeed) in seconds",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	dispatcherManagerCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "checkpoint_ts",
			Help:      "Checkpoint ts of event dispatcher manager(changefeed)",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	dispatcherManagerCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "checkpoint_ts_lag",
			Help:      "Checkpoint ts lag of event dispatcher manager(changefeed) in seconds",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	dispatcherManagerBlockStatusesChanLenGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "block_statuses_chan_len",
			Help:      "length of dispatcher manager block statuses channel",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	handleDispatcherRequsetCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "handle_dispatcher_request",
			Help:      "Total count of dispatcher request.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "type"})

	heartbeatCollectorBlockStatusRequestQueueLenGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "heartbeat_collector",
			Name:      "block_status_request_queue_len",
			Help:      "length of heartbeat collector block status request queue",
		})
)

// RegisterMetrics registers metrics owned by the dispatcher manager.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(tableTriggerEventDispatcherGauge)
	registry.MustRegister(eventDispatcherGauge)
	registry.MustRegister(createDispatcherDuration)
	registry.MustRegister(dispatcherManagerResolvedTsGauge)
	registry.MustRegister(dispatcherManagerResolvedTsLagGauge)
	registry.MustRegister(dispatcherManagerCheckpointTsGauge)
	registry.MustRegister(dispatcherManagerCheckpointTsLagGauge)
	registry.MustRegister(dispatcherManagerBlockStatusesChanLenGauge)
	registry.MustRegister(handleDispatcherRequsetCounter)
	registry.MustRegister(heartbeatCollectorBlockStatusRequestQueueLenGauge)
}
