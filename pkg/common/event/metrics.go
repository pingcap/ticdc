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

package event

import "github.com/prometheus/client_golang/prometheus"

var (
	DMLDecodeDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event",
			Name:      "decode_duration",
			Help:      "The duration of decoding a row from eventStore",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})

	DMLIgnoreComputeDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event",
			Name:      "ignore_compute_duration",
			Help:      "The duration of computing if a row should be ignored",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})
)

func InitEventMetrics(registry *prometheus.Registry) {
	registry.MustRegister(DMLDecodeDuration)
	registry.MustRegister(DMLIgnoreComputeDuration)
}
