// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnutil

import "github.com/prometheus/client_golang/prometheus"

var (
	subscriptionClientResolveLockProcessedLockCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolve_lock_processed_lock_count",
			Help:      "The number of locks found or resolved by resolve lock",
		}, []string{"status"})
	subscriptionClientResolveLockOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "subscription_client",
			Name:      "resolve_lock_operation_duration",
			Help:      "duration (s) for successful scan lock and resolve lock operations",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		}, []string{"type"})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(subscriptionClientResolveLockProcessedLockCounter)
	registry.MustRegister(subscriptionClientResolveLockOperationDuration)
}
