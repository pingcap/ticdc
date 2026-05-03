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

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	grpcMetrics = grpc_prometheus.NewClientMetrics()

	EventFeedErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_feed_error_count",
			Help:      "The number of error return by tikv",
		}, []string{"type"})
	PullerEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "pull_event_count",
			Help:      "event count received by this puller",
		}, []string{"type"})
	BatchResolvedEventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "batch_resolved_event_size",
			Help:      "The number of region in one batch resolved ts event",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		}, []string{"type"})
	LogPullerPrewriteCacheRowNum = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "prewrite_cache_row_num",
			Help:      "The number of rows in prewrite cache",
		})
	LogPullerMatcherCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "matcher_count",
			Help:      "The number of matchers",
		})
	LogPullerResolvedTsLag = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "log_puller",
			Name:      "resolved_ts_lag",
			Help:      "The lag of resolved ts",
		})

	SubscriptionClientResolvedTsLagGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolved_ts_lag",
			Help:      "The resolved ts lag of subscription client.",
		})

	SubscriptionClientRequestedRegionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "requested_region_count",
			Help:      "The number of requested regions",
		}, []string{"state"})
	RegionRequestFinishScanDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "region_request_finish_scan_duration",
			Help:      "duration (s) for region request to be finished.",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})
	SubscriptionClientAddRegionRequestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "add_region_request_duration",
			Help:      "The cost of adding region request",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})
	SubscriptionClientSubscribedRegionCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "subscribed_region_count",
			Help:      "The number of locked ranges",
		})
	SubscriptionClientResolveLockTaskDropCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolve_lock_task_drop_count",
			Help:      "The number of resolve lock tasks dropped before being processed",
		})
	SubscriptionClientResolveLockCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolve_lock_count",
			Help:      "The number of resolve lock executions",
		}, []string{"status"})

	SubscriptionClientRegionEventHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "region_event_handle_duration",
			Help:      "duration (s) for subscription client to handle region events and build KV cache",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		}, []string{"type"}) // types: entries, resolved, mixed, error.

	SubscriptionClientConsumeKVEventsCallbackDuration = prometheus.NewHistogramVec(
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

func initLogPullerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(grpcMetrics)
	registry.MustRegister(EventFeedErrorCounter)
	registry.MustRegister(PullerEventCounter)
	registry.MustRegister(BatchResolvedEventSize)
	registry.MustRegister(LogPullerPrewriteCacheRowNum)
	registry.MustRegister(LogPullerMatcherCount)
	registry.MustRegister(LogPullerResolvedTsLag)
	registry.MustRegister(SubscriptionClientRequestedRegionCount)
	registry.MustRegister(SubscriptionClientAddRegionRequestDuration)
	registry.MustRegister(RegionRequestFinishScanDuration)
	registry.MustRegister(SubscriptionClientSubscribedRegionCount)
	registry.MustRegister(SubscriptionClientResolveLockTaskDropCounter)
	registry.MustRegister(SubscriptionClientResolveLockCounter)
	registry.MustRegister(SubscriptionClientRegionEventHandleDuration)
	registry.MustRegister(SubscriptionClientConsumeKVEventsCallbackDuration)
}
