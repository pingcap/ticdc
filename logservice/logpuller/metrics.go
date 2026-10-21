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

package logpuller

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	grpcMetrics = grpc_prometheus.NewClientMetrics()

	eventFeedErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_feed_error_count",
			Help:      "The number of error return by tikv",
		}, []string{"type"})

	pullerEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "pull_event_count",
			Help:      "event count received by this puller",
		}, []string{"type"})

	batchResolvedEventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "batch_resolved_event_size",
			Help:      "The number of region in one batch resolved ts event",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		}, []string{"type"})

	logPullerPrewriteCacheRowNum = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "prewrite_cache_row_num",
			Help:      "The number of rows in prewrite cache",
		})

	logPullerMatcherCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "matcher_count",
			Help:      "The number of matchers",
		})

	logPullerResolvedTsLag = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "resolved_ts_lag",
			Help:      "The lag of resolved ts",
		})

	logPullerMemoryQuota = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "memory_quota",
			Help:      "The log puller local memory quota usage.",
		}, []string{"type"})

	logPullerMemoryQuotaEventWaiterCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "memory_quota_event_waiter_count",
			Help:      "The number of event receivers waiting at the log puller memory hard limit.",
		})

	logPullerMemoryQuotaEventWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "memory_quota_event_wait_duration",
			Help:      "The duration in seconds that an event receiver waits at the log puller memory hard limit.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 24),
		})

	logPullerMemoryQuotaScanWaiterCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "memory_quota_scan_waiter_count",
			Help:      "The number of region scans waiting at the log puller memory quota gate.",
		})

	logPullerMemoryQuotaScanWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "memory_quota_scan_wait_duration",
			Help:      "The duration in seconds that a region scan waits at the log puller memory quota gate.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 24),
		})

	subscriptionClientResolvedTsLagGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolved_ts_lag",
			Help:      "The resolved ts lag of subscription client.",
		})

	subscriptionClientRequestedRegionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "requested_region_count",
			Help:      "The number of region requests by state.",
		}, []string{"state"})

	regionRequestFinishScanDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "region_request_finish_scan_duration",
			Help:      "duration (s) for region request to be finished.",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})

	subscriptionClientSubscribedRegionCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "subscribed_region_count",
			Help:      "The number of locked ranges",
		})

	subscriptionClientResolveLockTaskDropCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolve_lock_task_drop_count",
			Help:      "The number of resolve lock tasks dropped before being processed",
		})

	subscriptionClientResolveLockCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolve_lock_count",
			Help:      "The number of resolve lock executions",
		}, []string{"status"})

	subscriptionClientRegionEventHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "region_event_handle_duration",
			Help:      "duration (s) for subscription client to handle region events and build KV cache",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		}, []string{"type"}) // types: entries, resolved, mixed, error.

	subscriptionClientConsumeKVEventsCallbackDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "consume_kv_events_callback_duration",
			Help:      "duration (s) from calling consumeKVEvents to wake callback execution",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		}, []string{"type"})
)

func GetGlobalGrpcMetrics() *grpc_prometheus.ClientMetrics {
	return grpcMetrics
}

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(grpcMetrics)
	registry.MustRegister(eventFeedErrorCounter)
	registry.MustRegister(pullerEventCounter)
	registry.MustRegister(batchResolvedEventSize)
	registry.MustRegister(logPullerPrewriteCacheRowNum)
	registry.MustRegister(logPullerMatcherCount)
	registry.MustRegister(logPullerResolvedTsLag)
	registry.MustRegister(logPullerMemoryQuota)
	registry.MustRegister(logPullerMemoryQuotaEventWaiterCount)
	registry.MustRegister(logPullerMemoryQuotaEventWaitDuration)
	registry.MustRegister(logPullerMemoryQuotaScanWaiterCount)
	registry.MustRegister(logPullerMemoryQuotaScanWaitDuration)
	registry.MustRegister(subscriptionClientRequestedRegionCount)
	registry.MustRegister(regionRequestFinishScanDuration)
	registry.MustRegister(subscriptionClientSubscribedRegionCount)
	registry.MustRegister(subscriptionClientResolveLockTaskDropCounter)
	registry.MustRegister(subscriptionClientResolveLockCounter)
	registry.MustRegister(subscriptionClientRegionEventHandleDuration)
	registry.MustRegister(subscriptionClientConsumeKVEventsCallbackDuration)
}
