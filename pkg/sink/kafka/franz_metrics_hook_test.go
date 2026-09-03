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

package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMetricsHook(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "metrics-hook")
	cleanupMetrics(changefeedID)
	t.Cleanup(func() { cleanupMetrics(changefeedID) })
	hook := newMetricsHook(changefeedID)
	meta := kgo.BrokerMetadata{NodeID: 1}

	hook.OnBrokerWrite(meta, 0, 12, 0, 0, nil)
	hook.OnBrokerE2E(meta, 0, kgo.BrokerE2E{
		BytesRead:   8,
		TimeToWrite: time.Millisecond,
		TimeToRead:  time.Millisecond,
	})
	hook.OnProduceBatchWritten(meta, "topic", 0, kgo.ProduceBatchMetrics{
		NumRecords:        3,
		UncompressedBytes: 10,
		CompressedBytes:   5,
	})

	metrics := hook.broker(1)
	require.Same(t, metrics, hook.broker(1))
	require.Equal(t, float64(12), testutil.ToFloat64(metrics.outgoingBytesTotal))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.requestsSuccess))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.responsesSuccess))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.requestsInFlight))
	require.Equal(t, float64(10), testutil.ToFloat64(
		uncompressedBytesTotal.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
	))
	require.Equal(t, float64(5), testutil.ToFloat64(
		compressedBytesTotal.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
	))
	batchMetric, ok := hook.recordsPerBatch.(prometheus.Metric)
	require.True(t, ok)
	batchHistogram := &dto.Metric{}
	require.NoError(t, batchMetric.Write(batchHistogram))
	require.Equal(t, uint64(1), batchHistogram.GetHistogram().GetSampleCount())
	require.Equal(t, float64(3), batchHistogram.GetHistogram().GetSampleSum())

	registry := prometheus.NewRegistry()
	InitMetrics(registry)
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(metricFamilies))
	for _, family := range metricFamilies {
		if strings.HasPrefix(family.GetName(), "ticdc_sink_kafka_franz_producer_") {
			names = append(names, family.GetName())
		}
	}
	require.ElementsMatch(t, []string{
		"ticdc_sink_kafka_franz_producer_compressed_bytes_total",
		"ticdc_sink_kafka_franz_producer_in_flight_requests",
		"ticdc_sink_kafka_franz_producer_outgoing_bytes_total",
		"ticdc_sink_kafka_franz_producer_records_per_batch",
		"ticdc_sink_kafka_franz_producer_request_duration_seconds",
		"ticdc_sink_kafka_franz_producer_requests_total",
		"ticdc_sink_kafka_franz_producer_responses_total",
		"ticdc_sink_kafka_franz_producer_throttle_time_seconds",
		"ticdc_sink_kafka_franz_producer_uncompressed_bytes_total",
	}, names)

	hook.OnBrokerWrite(meta, 0, 0, 0, 0, nil)
	hook.OnBrokerE2E(meta, 0, kgo.BrokerE2E{
		BytesRead:   8,
		TimeToWrite: 3 * time.Millisecond,
		ReadWait:    time.Millisecond,
		TimeToRead:  4 * time.Millisecond,
	})

	metric, ok := metrics.requestDuration.(prometheus.Metric)
	require.True(t, ok)

	histogram := &dto.Metric{}
	require.NoError(t, metric.Write(histogram))
	require.Equal(t, uint64(2), histogram.GetHistogram().GetSampleCount())
	require.InDelta(t, 0.010, histogram.GetHistogram().GetSampleSum(), 0.000001)

	hook.OnBrokerWrite(meta, 0, 0, 0, 0, context.DeadlineExceeded)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.requestsWriteError))

	hook.OnBrokerWrite(meta, 0, 0, 0, 0, nil)
	hook.OnBrokerE2E(meta, 0, kgo.BrokerE2E{ReadErr: context.Canceled})
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.responsesReadError))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.requestsInFlight))

	hook.OnBrokerWrite(kgo.BrokerMetadata{NodeID: -1}, 0, 1, 0, 0, nil)
	hook.OnBrokerE2E(kgo.BrokerMetadata{NodeID: -1}, 0, kgo.BrokerE2E{})

	hook.OnBrokerThrottle(meta, 10*time.Millisecond, true)
	hook.OnBrokerThrottle(meta, 50*time.Millisecond, false)
	hook.OnBrokerThrottle(kgo.BrokerMetadata{NodeID: 2}, 30*time.Millisecond, true)
	hook.OnBrokerThrottle(kgo.BrokerMetadata{NodeID: -1}, time.Second, true)

	throttleMetric, ok := metrics.throttleTime.(prometheus.Metric)
	require.True(t, ok)
	throttleHistogram := &dto.Metric{}
	require.NoError(t, throttleMetric.Write(throttleHistogram))
	require.Equal(t, uint64(2), throttleHistogram.GetHistogram().GetSampleCount())
	require.InDelta(t, 0.06, throttleHistogram.GetHistogram().GetSampleSum(), 0.000001)
	keyspace, changefeed, broker := changefeedID.Keyspace(), changefeedID.Name(), "1"
	cleanupMetrics(changefeedID)
	require.False(t, outgoingBytesTotal.DeleteLabelValues(keyspace, changefeed, broker))
	require.False(t, requestsTotal.DeleteLabelValues(keyspace, changefeed, broker, metricResultSuccess))
	require.False(t, requestsTotal.DeleteLabelValues(keyspace, changefeed, broker, metricResultWriteError))
	require.False(t, responsesTotal.DeleteLabelValues(keyspace, changefeed, broker, metricResultSuccess))
	require.False(t, responsesTotal.DeleteLabelValues(keyspace, changefeed, broker, metricResultReadError))
	require.False(t, requestsInFlight.DeleteLabelValues(keyspace, changefeed, broker))
	require.False(t, requestDuration.DeleteLabelValues(keyspace, changefeed, broker))
	require.False(t, throttleTime.DeleteLabelValues(keyspace, changefeed, broker))
	require.False(t, recordsPerBatch.DeleteLabelValues(keyspace, changefeed))
	require.False(t, uncompressedBytesTotal.DeleteLabelValues(keyspace, changefeed))
	require.False(t, compressedBytesTotal.DeleteLabelValues(keyspace, changefeed))
}
