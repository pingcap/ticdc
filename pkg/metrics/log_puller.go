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
)

func InitLogPullerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(LogPullerPrewriteCacheRowNum)
	registry.MustRegister(LogPullerMatcherCount)
	registry.MustRegister(LogPullerResolvedTsLag)
	registry.MustRegister(SubscriptionClientRequestedRegionCount)
	registry.MustRegister(SubscriptionClientAddRegionRequestDuration)
	registry.MustRegister(RegionRequestFinishScanDuration)
	registry.MustRegister(SubscriptionClientSubscribedRegionCount)
}
