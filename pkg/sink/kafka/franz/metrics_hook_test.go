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

package franz

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestInitMetrics(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "metrics-registration")
	CleanupMetrics(changefeedID)
	t.Cleanup(func() { CleanupMetrics(changefeedID) })

	hook := newMetricsHook(changefeedID)
	hook.OnProduceBatchWritten(
		kgo.BrokerMetadata{},
		"topic",
		0,
		kgo.ProduceBatchMetrics{
			NumRecords:        1,
			UncompressedBytes: 2,
			CompressedBytes:   1,
		},
	)

	registry := prometheus.NewRegistry()
	InitMetrics(registry)

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	names := make([]string, 0, len(metricFamilies))
	for _, family := range metricFamilies {
		names = append(names, family.GetName())
	}

	require.Contains(t, names, "ticdc_sink_kafka_franz_producer_records_per_batch")
	require.Contains(t, names, "ticdc_sink_kafka_franz_producer_uncompressed_bytes_total")
	require.Contains(t, names, "ticdc_sink_kafka_franz_producer_compressed_bytes_total")
}

func TestMetricsHookRecordsRawValues(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "metrics-hook")
	CleanupMetrics(changefeedID)
	t.Cleanup(func() { CleanupMetrics(changefeedID) })
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
}
