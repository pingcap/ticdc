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
	EventServiceChannelSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "channel_size",
		Help:      "The size of the event service channel",
	}, []string{"type"})

	// EventServiceSendEventCount is the metric that counts events sent by the event service.
	EventServiceSendEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "send_event_count",
		Help:      "The number of events sent by the event service",
	}, []string{"type", "mode"})

	// EventServiceSendEventDuration is the metric that records the duration of sending events by the event service.
	EventServiceSendEventDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "send_event_duration",
		Help:      "The duration of sending events by the event service",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
	}, []string{"type"})
	EventServiceResolvedTsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "resolved_ts",
			Help:      "resolved ts of eventService",
		})
	EventServiceResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of eventService in seconds",
		}, []string{"type"})
	EventServiceScanDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_duration",
			Help:      "The duration of scanning a data range from eventStore",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 19), // 40us to 10s
		})
	EventServiceScannedCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scanned_count",
			Help:      "The number of events scanned from eventStore",
			Buckets:   prometheus.ExponentialBuckets(8, 2.0, 12), // 8 ~ 16384
		})

	EventServiceDispatcherGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "dispatcher_count",
			Help:      "The number of dispatchers in event service",
		}, []string{"cluster"})
	EventServiceScanTaskCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_task_count",
			Help:      "The number of scan tasks that have finished",
		})
	EventServicePendingScanTaskCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "pending_scan_task_count",
			Help:      "The number of pending scan tasks",
		})
	EventServiceDispatcherStatusCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "dispatcher_status_count",
			Help:      "The number of different dispatcher status",
		}, []string{"status"})
	EventServiceDispatcherUpdateResolvedTsDiff = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "dispatcher_update_resolved_ts_diff",
			Help:      "The lag difference between received and sent resolved ts of dispatchers",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})
	EventServiceSkipResolvedTsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "skip_resolved_ts_count",
			Help:      "The number of skipped resolved ts",
		}, []string{"mode"})

	EventServiceAvailableMemoryQuotaGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "available_memory_quota",
	}, []string{"changefeed"})

	EventServiceScannedDMLSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "scanned_dml_size",
		Help:      "The size of scanned DML events from eventStore",
		Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 16), // 1KB to 64MB
	})
	EventServiceScannedTxnCount = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "scanned_txn_count",
		Help:      "The number of transactions scanned from eventStore",
		Buckets:   prometheus.ExponentialBuckets(1, 2.0, 8), // 1 ~ 256
	})

	EventServiceSkipScanCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "skip_scan_count",
			Help:      "The number of scans skipped",
		}, []string{"reason"})

	EventServiceGetDDLEventDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "get_ddl_event_duration",
			Help:      "The duration of getting DDL events from eventStore",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})

	EventServiceInterruptScanCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "interrupt_scan_count",
			Help:      "The number of scans interrupted",
		})

	EventServiceResetDispatcherCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "reset_dispatcher_count",
			Help:      "The number of event dispatcher reset operations performed",
		})
)

// initEventServiceMetrics registers all metrics in this file.
func initEventServiceMetrics(registry *prometheus.Registry) {
	registry.MustRegister(EventServiceChannelSizeGauge)
	registry.MustRegister(EventServiceSendEventCount)
	registry.MustRegister(EventServiceSendEventDuration)
	registry.MustRegister(EventServiceResolvedTsGauge)
	registry.MustRegister(EventServiceResolvedTsLagGauge)
	registry.MustRegister(EventServiceScanDuration)
	registry.MustRegister(EventServiceScannedCount)
	registry.MustRegister(EventServiceDispatcherGauge)
	registry.MustRegister(EventServiceScanTaskCount)
	registry.MustRegister(EventServiceDispatcherStatusCount)
	registry.MustRegister(EventServicePendingScanTaskCount)
	registry.MustRegister(EventServiceDispatcherUpdateResolvedTsDiff)
	registry.MustRegister(EventServiceSkipResolvedTsCount)
	registry.MustRegister(EventServiceAvailableMemoryQuotaGaugeVec)
	registry.MustRegister(EventServiceScannedDMLSize)
	registry.MustRegister(EventServiceScannedTxnCount)
	registry.MustRegister(EventServiceSkipScanCount)
	registry.MustRegister(EventServiceInterruptScanCount)
	registry.MustRegister(EventServiceGetDDLEventDuration)
	registry.MustRegister(EventServiceResetDispatcherCount)
}
