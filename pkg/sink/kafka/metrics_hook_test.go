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

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetricsHookRecordsLegacyMetricsAndCleanup(t *testing.T) {
	outgoingByteRate := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kafka_producer_outgoing_byte_rate"},
		[]string{"namespace", "changefeed", "broker"},
	)
	requestRate := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kafka_producer_request_rate"},
		[]string{"namespace", "changefeed", "broker"},
	)
	requestsInFlight := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kafka_producer_request_in_flight"},
		[]string{"namespace", "changefeed", "broker"},
	)
	recordsPerRequest := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kafka_producer_records_per_request"},
		[]string{"namespace", "changefeed", "type"},
	)
	compressionRatio := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kafka_producer_compression_ratio"},
		[]string{"namespace", "changefeed", "type"},
	)

	hook := newMetricsHook("async_producer")
	hook.bindMetrics("default", "cf", metricVectors{
		LegacyOutgoingByteRate:  outgoingByteRate,
		LegacyRequestRate:       requestRate,
		LegacyRequestsInFlight:  requestsInFlight,
		LegacyRecordsPerRequest: recordsPerRequest,
		LegacyCompressionRatio:  compressionRatio,
	})

	hook.RecordBrokerWrite(1, 42, nil)
	hook.RecordProduceBatchWritten(3, 100, 50)

	require.Equal(t, float64(42), testutil.ToFloat64(outgoingByteRate.WithLabelValues("default", "cf", "1")))
	require.Equal(t, float64(1), testutil.ToFloat64(requestRate.WithLabelValues("default", "cf", "1")))
	require.Equal(t, float64(1), testutil.ToFloat64(requestsInFlight.WithLabelValues("default", "cf", "1")))
	require.Equal(t, float64(3), testutil.ToFloat64(recordsPerRequest.WithLabelValues("default", "cf", legacyMetricAvg)))
	require.Equal(t, float64(3), testutil.ToFloat64(recordsPerRequest.WithLabelValues("default", "cf", legacyMetricP99)))
	require.Equal(t, float64(200), testutil.ToFloat64(compressionRatio.WithLabelValues("default", "cf", legacyMetricAvg)))
	require.Equal(t, float64(200), testutil.ToFloat64(compressionRatio.WithLabelValues("default", "cf", legacyMetricP99)))

	hook.cleanupMetrics()

	require.Equal(t, 0, testutil.CollectAndCount(outgoingByteRate))
	require.Equal(t, 0, testutil.CollectAndCount(requestRate))
	require.Equal(t, 0, testutil.CollectAndCount(requestsInFlight))
	require.Equal(t, 0, testutil.CollectAndCount(recordsPerRequest))
	require.Equal(t, 0, testutil.CollectAndCount(compressionRatio))
}
