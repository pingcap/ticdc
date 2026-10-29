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

import "github.com/prometheus/client_golang/prometheus"

var (
	TableTriggerEventDispatcherGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "table_trigger_dispatcher_count",
			Help:      "The number of table dispatchers",
		}, []string{GetKeyspaceLabel(), "changefeed", "event_type"})

	EventDispatcherGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "table_dispatcher_count",
			Help:      "The number of table dispatchers",
		}, []string{GetKeyspaceLabel(), "changefeed", "event_type"})

	CreateDispatcherDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "create_dispatcher_duration",
			Help:      "Bucketed histogram of create dispatcher time (s) for table span.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 20), // 1us~524ms
		}, []string{GetKeyspaceLabel(), "changefeed", "event_type"})

	DispatcherManagerResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "resolved_ts",
			Help:      "Resolved ts of event dispatcher manager(changefeed)",
		}, []string{GetKeyspaceLabel(), "changefeed"})

	DispatcherManagerResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "resolved_ts_lag",
			Help:      "Resolved ts lag of event dispatcher manager(changefeed) in seconds",
		}, []string{GetKeyspaceLabel(), "changefeed"})

	DispatcherManagerCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "checkpoint_ts",
			Help:      "Checkpoint ts of event dispatcher manager(changefeed)",
		}, []string{GetKeyspaceLabel(), "changefeed"})

	DispatcherManagerCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "checkpoint_ts_lag",
			Help:      "Checkpoint ts lag of event dispatcher manager(changefeed) in seconds",
		}, []string{GetKeyspaceLabel(), "changefeed"})

	DispatcherManagerBlockStatusesChanLenGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dispatchermanager",
			Name:      "block_statuses_chan_len",
			Help:      "length of dispatcher manager block statuses channel",
		}, []string{GetKeyspaceLabel(), "changefeed"})

	HandleDispatcherRequsetCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "handle_dispatcher_request",
			Help:      "Total count of dispatcher request.",
		}, []string{GetKeyspaceLabel(), "changefeed", "type"})

	HeartbeatCollectorBlockStatusRequestQueueLenGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "heartbeat_collector",
			Name:      "block_status_request_queue_len",
			Help:      "length of heartbeat collector block status request queue",
		})
)

func initDispatcherMetrics(registry *prometheus.Registry) {
	registry.MustRegister(TableTriggerEventDispatcherGauge)
	registry.MustRegister(EventDispatcherGauge)
	registry.MustRegister(CreateDispatcherDuration)
	registry.MustRegister(DispatcherManagerResolvedTsGauge)
	registry.MustRegister(DispatcherManagerResolvedTsLagGauge)
	registry.MustRegister(DispatcherManagerCheckpointTsGauge)
	registry.MustRegister(DispatcherManagerCheckpointTsLagGauge)
	registry.MustRegister(DispatcherManagerBlockStatusesChanLenGauge)
	registry.MustRegister(HandleDispatcherRequsetCounter)
	registry.MustRegister(HeartbeatCollectorBlockStatusRequestQueueLenGauge)
}
