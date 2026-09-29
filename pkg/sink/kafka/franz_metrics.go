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
	requestsInFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_in_flight_requests",
		Help:      "Current franz-go requests awaiting a response.",
	}, []string{"namespace", "changefeed", "broker"})

	outgoingBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_outgoing_bytes_total",
		Help:      "Total bytes written by franz-go, excluding TLS overhead.",
	}, []string{"namespace", "changefeed", "broker"})

	requestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_requests_total",
		Help:      "Total franz-go requests by broker and write result.",
	}, []string{"namespace", "changefeed", "broker", "result"})

	responsesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_responses_total",
		Help:      "Total franz-go responses by broker and read result.",
	}, []string{"namespace", "changefeed", "broker", "result"})

	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_request_duration_seconds",
		Help:      "Franz-go request end-to-end duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"namespace", "changefeed", "broker"})

	throttleTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_throttle_time_seconds",
		Help:      "Kafka broker throttle time reported to the producer in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"namespace", "changefeed", "broker"})

	recordsPerBatch = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_records_per_batch",
		Help:      "Records in each successfully written franz-go topic-partition batch.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
	}, []string{"namespace", "changefeed"})

	uncompressedBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_uncompressed_bytes_total",
		Help:      "Total record bytes before compression in successfully written franz-go batches.",
	}, []string{"namespace", "changefeed"})

	compressedBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "kafka_franz_producer_compressed_bytes_total",
		Help:      "Total record bytes after compression in successfully written franz-go batches.",
	}, []string{"namespace", "changefeed"})
)
