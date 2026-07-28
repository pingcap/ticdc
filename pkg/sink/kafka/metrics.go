// Copyright 2022 PingCAP, Inc.
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

import (
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/kafka/claimcheck"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	requestsInFlightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_in_flight_requests",
			Help: "The current number of in-flight requests" +
				" awaiting a response for all brokers.",
		}, []string{"namespace", "changefeed", "broker"})
	outgoingBytesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_outgoing_bytes_total",
			Help:      "Total bytes written to Kafka brokers, excluding TLS overhead.",
		}, []string{"namespace", "changefeed", "broker"})
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_requests_total",
			Help:      "Total Kafka requests by broker and write result.",
		}, []string{"namespace", "changefeed", "broker", "result"})
	responsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_responses_total",
			Help:      "Total Kafka responses by broker and read result.",
		}, []string{"namespace", "changefeed", "broker", "result"})
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_request_duration_seconds",
			Help:      "Kafka request end-to-end duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"namespace", "changefeed", "broker"})
	recordsPerBatch = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_records_per_batch",
			Help:      "Number of records in each successfully written topic-partition batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		}, []string{"namespace", "changefeed"})
	uncompressedBytesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_uncompressed_bytes_total",
			Help:      "Total serialized record bytes before compression in successfully written batches.",
		}, []string{"namespace", "changefeed"})
	compressedBytesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_producer_compressed_bytes_total",
			Help:      "Total serialized record bytes after compression in successfully written batches.",
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(outgoingBytesTotal)
	registry.MustRegister(requestsTotal)
	registry.MustRegister(responsesTotal)
	registry.MustRegister(requestDuration)
	registry.MustRegister(recordsPerBatch)
	registry.MustRegister(uncompressedBytesTotal)
	registry.MustRegister(compressedBytesTotal)
	registry.MustRegister(requestsInFlightGauge)

	claimcheck.InitMetrics(registry)
	codec.InitMetrics(registry)
}
