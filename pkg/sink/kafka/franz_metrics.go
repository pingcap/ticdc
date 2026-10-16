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
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import "github.com/prometheus/client_golang/prometheus"

var (
	outgoingBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_outgoing_bytes_total",
		Help:      "Total bytes written by Kafka producer requests, excluding TLS overhead.",
	}, []string{"namespace", "changefeed", "broker"})

	requestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_requests_total",
		Help:      "Total Kafka producer request writes by broker.",
	}, []string{"namespace", "changefeed", "broker"})

	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_request_duration_seconds",
		Help:      "Successful Kafka producer request end-to-end duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"namespace", "changefeed", "broker"})

	throttleTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_throttle_time_seconds",
		Help:      "Kafka broker throttle time reported to the producer in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"namespace", "changefeed", "broker"})

	recordsPerBatch = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_records_per_batch",
		Help:      "Records in each successfully produced topic-partition batch.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
	}, []string{"namespace", "changefeed"})

	compressionRatio = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_producer_batch_compression_ratio",
		Help:      "Distribution of uncompressed-to-compressed record batch size ratios multiplied by 100.",
		Buckets:   prometheus.ExponentialBuckets(25, 2, 10),
	}, []string{"namespace", "changefeed"})
)
