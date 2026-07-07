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
	"strconv"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// metricsHook adapts franz-go client callbacks to TiCDC's Kafka sink metrics.
// franz-go calls these hook methods while writing requests, receiving responses,
// and flushing produce batches. The hook does not poll Kafka; it only converts
// the callback payloads into the existing TiCDC Kafka metric vectors.
type metricsHook struct {
	keyspace   string
	changefeed string
}

const (
	metricAvg = "avg"
	metricP99 = "p99"
)

func newKafkaMetricsHook(changefeedID common.ChangeFeedID) *metricsHook {
	return &metricsHook{
		keyspace:   changefeedID.Keyspace(),
		changefeed: changefeedID.Name(),
	}
}

// CleanupMetrics removes Kafka sink metric series for a changefeed when its sink exits.
func CleanupMetrics(changefeedID common.ChangeFeedID) {
	labels := prometheus.Labels{
		"namespace":  changefeedID.Keyspace(),
		"changefeed": changefeedID.Name(),
	}
	OutgoingByteRateGauge.DeletePartialMatch(labels)
	RequestRateGauge.DeletePartialMatch(labels)
	responseRateGauge.DeletePartialMatch(labels)
	requestsInFlightGauge.DeletePartialMatch(labels)
	RequestLatencyGauge.DeletePartialMatch(labels)
	compressionRatioGauge.DeletePartialMatch(labels)
	recordsPerRequestGauge.DeletePartialMatch(labels)
}

func (h *metricsHook) OnBrokerWrite(
	meta kgo.BrokerMetadata,
	_ int16,
	bytesWritten int,
	_ time.Duration,
	_ time.Duration,
	err error,
) {
	if meta.NodeID < 0 {
		return
	}
	brokerID := strconv.Itoa(int(meta.NodeID))

	if bytesWritten > 0 {
		OutgoingByteRateGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(float64(bytesWritten))
	}
	RequestRateGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(1)
	if err == nil {
		requestsInFlightGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(1)
	}
}

func (h *metricsHook) OnBrokerE2E(
	meta kgo.BrokerMetadata,
	_ int16,
	e2e kgo.BrokerE2E,
) {
	if meta.NodeID < 0 {
		return
	}
	brokerID := strconv.Itoa(int(meta.NodeID))

	if e2e.WriteErr == nil {
		requestsInFlightGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(-1)
	}
	if e2e.BytesRead > 0 && e2e.ReadErr == nil {
		responseRateGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(1)
	}
	if e2e.Err() == nil {
		latencyMs := float64(e2e.DurationE2E().Microseconds()) / 1000
		RequestLatencyGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID, metricAvg).Set(latencyMs)
		RequestLatencyGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID, metricP99).Set(latencyMs)
	}
}

func (h *metricsHook) OnProduceBatchWritten(
	_ kgo.BrokerMetadata,
	_ string,
	_ int32,
	m kgo.ProduceBatchMetrics,
) {
	if m.NumRecords > 0 {
		records := float64(m.NumRecords)
		recordsPerRequestGauge.WithLabelValues(h.keyspace, h.changefeed, metricAvg).Set(records)
		recordsPerRequestGauge.WithLabelValues(h.keyspace, h.changefeed, metricP99).Set(records)
	}
	if m.UncompressedBytes > 0 && m.CompressedBytes > 0 {
		ratio := float64(m.UncompressedBytes) / float64(m.CompressedBytes) * 100
		compressionRatioGauge.WithLabelValues(h.keyspace, h.changefeed, metricAvg).Set(ratio)
		compressionRatioGauge.WithLabelValues(h.keyspace, h.changefeed, metricP99).Set(ratio)
	}
}
