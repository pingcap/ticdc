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

package eventservice

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	eventServiceChannelSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "channel_size",
		Help:      "The size of the event service channel",
	}, []string{"type"})

	// eventServiceSendEventCount is the metric that counts events sent by the event service.
	eventServiceSendEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "send_event_count",
		Help:      "The number of events sent by the event service",
	}, []string{"type", "mode"})

	// eventServiceSendEventDuration is the metric that records the duration of sending events by the event service.
	eventServiceSendEventDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "send_event_duration",
		Help:      "The duration of sending events by the event service",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
	}, []string{"type"})

	eventServiceResolvedTsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "resolved_ts",
			Help:      "resolved ts of eventService",
		})

	eventServiceResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of eventService in seconds",
		}, []string{"type"})

	eventServiceScanWindowBaseTsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_base_ts",
			Help:      "The base ts of the scan window for each changefeed",
		}, []string{"changefeed"})

	eventServiceScanWindowIntervalGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_interval",
			Help:      "The scan window interval in seconds for each changefeed",
		}, []string{"changefeed"})

	eventServiceScanWindowUsageRatioGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_usage_ratio",
			Help:      "The usage ratio observed by the scan window controller for each changefeed",
		}, []string{"changefeed", "type"})

	eventServiceScanWindowUsageEMAGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_usage_ema",
			Help:      "The usage EMA values used by the scan window controller for each changefeed",
		}, []string{"changefeed", "type"})

	eventServiceScanWindowTargetBandGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_target_band",
			Help:      "Whether the observed scan window value is currently inside the target band for each changefeed",
		}, []string{"changefeed", "type"})

	eventServiceScanWindowTargetBandCrossCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_target_band_cross_count",
			Help:      "The number of target band state changes observed by the scan window controller for each changefeed",
		}, []string{"changefeed", "type"})

	eventServiceScanWindowPressureScoreGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_pressure_score",
			Help:      "The pressure score maintained by the scan window controller for each changefeed",
		}, []string{"changefeed"})

	eventServiceScanWindowMemoryReleaseCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_memory_release_count",
			Help:      "The number of memory release events reported to the scan window controller for each changefeed",
		}, []string{"changefeed"})

	eventServiceScanWindowAdjustCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_window_adjust_count",
			Help:      "The number of scan window adjustments made by the controller for each changefeed",
		}, []string{"changefeed", "reason"})

	eventServiceScanDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_duration",
			Help:      "The duration of scanning a data range from eventStore",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 19), // 40us to 10s
		})

	eventServiceScannedCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scanned_count",
			Help:      "The number of events scanned from eventStore",
			Buckets:   prometheus.ExponentialBuckets(8, 2.0, 12), // 8 ~ 16384
		})

	eventServiceDispatcherGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "dispatcher_count",
			Help:      "The number of dispatchers in event service",
		}, []string{"cluster"})

	eventServiceScanTaskCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "scan_task_count",
			Help:      "The number of scan tasks that have finished",
		})

	eventServicePendingScanTaskCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "pending_scan_task_count",
			Help:      "The number of pending scan tasks",
		})

	eventServiceDroppedScanTaskCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "dropped_scan_task_count",
			Help:      "The number of scan tasks dropped because the worker queue is full",
		})

	eventServiceDispatcherStatusCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "dispatcher_status_count",
			Help:      "The number of different dispatcher status",
		}, []string{"status"})

	eventServiceDispatcherUpdateResolvedTsDiff = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "dispatcher_update_resolved_ts_diff",
			Help:      "The lag difference between received and sent resolved ts of dispatchers",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})

	eventServiceSkipResolvedTsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "skip_resolved_ts_count",
			Help:      "The number of skipped resolved ts",
		}, []string{"mode"})

	eventServiceAvailableMemoryQuotaGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "available_memory_quota",
	}, []string{"changefeed"})

	eventServiceScannedDMLSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "scanned_dml_size",
		Help:      "The size of scanned DML events from eventStore",
		Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 16), // 1KB to 64MB
	})

	eventServiceScannedTxnCount = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "scanned_txn_count",
		Help:      "The number of transactions scanned from eventStore",
		Buckets:   prometheus.ExponentialBuckets(1, 2.0, 8), // 1 ~ 256
	})

	eventServiceBigTxnSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "big_txn_size",
		Help:      "The raw KV size of big transactions scanned from eventStore",
		Buckets:   prometheus.ExponentialBuckets(1024*1024, 2.0, 16), // 1MB to 32GB
	})

	eventServiceBigTxnCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "big_txn_count",
		Help:      "The number of big transactions scanned from eventStore",
	})

	eventServiceSkipScanCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "skip_scan_count",
			Help:      "The number of scans skipped",
		}, []string{"reason"})

	eventServiceGetDDLEventDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "get_ddl_event_duration",
			Help:      "The duration of getting DDL events from eventStore",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})

	eventServiceInterruptScanCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "interrupt_scan_count",
			Help:      "The number of scans interrupted",
		})

	eventServiceResetDispatcherCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_service",
			Name:      "reset_dispatcher_count",
			Help:      "The number of event dispatcher reset operations performed",
		})

	eventServiceSendDMLTypeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_service",
		Name:      "send_dml_type_count",
		Help:      "The number of different dml events type sent by the event service,  it is potentially inaccurat if some dml events are filter",
	}, []string{"mode", "dml_type"})

	// eventStoreOutputEventCount is the metric that counts events output by the sorter.
	eventStoreOutputEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "output_event_count",
		Help:      "The number of events output by the sorter",
	}, []string{"type", "mode"}) // types : kv, resolved.
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(eventServiceChannelSizeGauge)
	registry.MustRegister(eventServiceSendEventCount)
	registry.MustRegister(eventServiceSendEventDuration)
	registry.MustRegister(eventServiceResolvedTsGauge)
	registry.MustRegister(eventServiceResolvedTsLagGauge)
	registry.MustRegister(eventServiceScanWindowBaseTsGaugeVec)
	registry.MustRegister(eventServiceScanWindowIntervalGaugeVec)
	registry.MustRegister(eventServiceScanWindowUsageRatioGaugeVec)
	registry.MustRegister(eventServiceScanWindowUsageEMAGaugeVec)
	registry.MustRegister(eventServiceScanWindowTargetBandGaugeVec)
	registry.MustRegister(eventServiceScanWindowTargetBandCrossCount)
	registry.MustRegister(eventServiceScanWindowPressureScoreGaugeVec)
	registry.MustRegister(eventServiceScanWindowMemoryReleaseCount)
	registry.MustRegister(eventServiceScanWindowAdjustCount)
	registry.MustRegister(eventServiceScanDuration)
	registry.MustRegister(eventServiceScannedCount)
	registry.MustRegister(eventServiceDispatcherGauge)
	registry.MustRegister(eventServiceScanTaskCount)
	registry.MustRegister(eventServicePendingScanTaskCount)
	registry.MustRegister(eventServiceDroppedScanTaskCount)
	registry.MustRegister(eventServiceDispatcherStatusCount)
	registry.MustRegister(eventServiceDispatcherUpdateResolvedTsDiff)
	registry.MustRegister(eventServiceSkipResolvedTsCount)
	registry.MustRegister(eventServiceAvailableMemoryQuotaGaugeVec)
	registry.MustRegister(eventServiceScannedDMLSize)
	registry.MustRegister(eventServiceScannedTxnCount)
	registry.MustRegister(eventServiceBigTxnSize)
	registry.MustRegister(eventServiceBigTxnCount)
	registry.MustRegister(eventServiceSkipScanCount)
	registry.MustRegister(eventServiceGetDDLEventDuration)
	registry.MustRegister(eventServiceInterruptScanCount)
	registry.MustRegister(eventServiceResetDispatcherCount)
	registry.MustRegister(eventServiceSendDMLTypeCount)
	registry.MustRegister(eventStoreOutputEventCount)
}
