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
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	kafkafranz "github.com/pingcap/ticdc/pkg/sink/kafka/franz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzFactoryMetricsCollectorType(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "franz-metrics-type")
	f := &franzFactory{
		changefeedID: changefeedID,
		metricsHook:  kafkafranz.NewMetricsHook(),
	}
	collector := f.MetricsCollector(nil)

	_, ok := collector.(*kafkafranz.MetricsHook)
	require.True(t, ok)
}

func TestFranzMetricsHookWritePrometheusMetricsDirectly(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "franz-metrics")
	hook := kafkafranz.NewMetricsHook()
	hook.BindPrometheusMetrics(
		changefeedID.Keyspace(),
		changefeedID.Name(),
		kafkafranz.PrometheusMetrics{
			RequestsInFlight:  requestsInFlightGauge,
			OutgoingByteRate:  OutgoingByteRateGauge,
			RequestRate:       RequestRateGauge,
			RequestLatency:    franzRequestLatencyHistogram,
			ResponseRate:      responseRateGauge,
			CompressionRatio:  franzCompressionRatioHistogram,
			RecordsPerRequest: franzRecordsPerRequestHistogram,
		},
	)
	t.Cleanup(func() {
		hook.CleanupPrometheusMetrics()
	})

	hook.RecordBrokerWrite(1, 128, nil)
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	require.Equal(t, float64(1), testutil.ToFloat64(requestsInFlightGauge.WithLabelValues(keyspace, changefeed, "1")))
	require.Greater(t, testutil.ToFloat64(RequestRateGauge.WithLabelValues(keyspace, changefeed, "1")), 0.0)
	require.Greater(t, testutil.ToFloat64(OutgoingByteRateGauge.WithLabelValues(keyspace, changefeed, "1")), 0.0)

	hook.OnBrokerE2E(kgo.BrokerMetadata{NodeID: 1}, 0, kgo.BrokerE2E{
		BytesRead:   128,
		TimeToWrite: time.Millisecond,
		ReadWait:    time.Millisecond,
		TimeToRead:  time.Millisecond,
	})
	require.Equal(t, float64(0), testutil.ToFloat64(requestsInFlightGauge.WithLabelValues(keyspace, changefeed, "1")))
	require.Greater(t, testutil.ToFloat64(responseRateGauge.WithLabelValues(keyspace, changefeed, "1")), 0.0)
	require.Greater(t, histogramSampleCount(t, franzRequestLatencyHistogram.WithLabelValues(keyspace, changefeed, "1")), uint64(0))

	hook.RecordProduceBatchWritten(8, 400, 200)
	require.Greater(t, histogramSampleCount(t, franzCompressionRatioHistogram.WithLabelValues(keyspace, changefeed)), uint64(0))
	require.Greater(t, histogramSampleCount(t, franzRecordsPerRequestHistogram.WithLabelValues(keyspace, changefeed)), uint64(0))
}

func histogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()

	histogram, ok := observer.(prometheus.Histogram)
	require.True(t, ok)

	metric := &dto.Metric{}
	require.NoError(t, histogram.Write(metric))
	require.NotNil(t, metric.Histogram)

	return metric.Histogram.GetSampleCount()
}
