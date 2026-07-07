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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type metricsHook struct {
	keyspace   string
	changefeed string
	metrics    metricVectors
}

type metricVectors struct {
	RequestsInFlight  *prometheus.GaugeVec
	OutgoingByteRate  *prometheus.GaugeVec
	RequestRate       *prometheus.GaugeVec
	RequestLatency    *prometheus.GaugeVec
	ResponseRate      *prometheus.GaugeVec
	CompressionRatio  *prometheus.GaugeVec
	RecordsPerRequest *prometheus.GaugeVec
}

const (
	legacyMetricAvg = "avg"
	legacyMetricP99 = "p99"
)

func newMetricsHook(
	keyspace string,
	changefeed string,
	metrics metricVectors,
) *metricsHook {
	return &metricsHook{
		keyspace:   keyspace,
		changefeed: changefeed,
		metrics:    metrics,
	}
}

func (h *metricsHook) cleanupMetrics() {
	labels := prometheus.Labels{
		"namespace":  h.keyspace,
		"changefeed": h.changefeed,
	}
	for _, gaugeVec := range []*prometheus.GaugeVec{
		h.metrics.OutgoingByteRate,
		h.metrics.RequestRate,
		h.metrics.ResponseRate,
		h.metrics.RequestsInFlight,
		h.metrics.RequestLatency,
		h.metrics.CompressionRatio,
		h.metrics.RecordsPerRequest,
	} {
		if gaugeVec != nil {
			gaugeVec.DeletePartialMatch(labels)
		}
	}
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

	if h.metrics.OutgoingByteRate != nil && bytesWritten > 0 {
		h.metrics.OutgoingByteRate.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(float64(bytesWritten))
	}
	if h.metrics.RequestRate != nil {
		h.metrics.RequestRate.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(1)
	}
	if err == nil && h.metrics.RequestsInFlight != nil {
		h.metrics.RequestsInFlight.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(1)
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

	if e2e.WriteErr == nil && h.metrics.RequestsInFlight != nil {
		h.metrics.RequestsInFlight.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(-1)
	}
	if e2e.BytesRead > 0 && e2e.ReadErr == nil && h.metrics.ResponseRate != nil {
		h.metrics.ResponseRate.WithLabelValues(h.keyspace, h.changefeed, brokerID).Add(1)
	}
	if e2e.Err() == nil && h.metrics.RequestLatency != nil {
		latencyMs := float64(e2e.DurationE2E().Microseconds()) / 1000
		h.metrics.RequestLatency.WithLabelValues(h.keyspace, h.changefeed, brokerID, legacyMetricAvg).Set(latencyMs)
		h.metrics.RequestLatency.WithLabelValues(h.keyspace, h.changefeed, brokerID, legacyMetricP99).Set(latencyMs)
	}
}

func (h *metricsHook) OnProduceBatchWritten(
	_ kgo.BrokerMetadata,
	_ string,
	_ int32,
	m kgo.ProduceBatchMetrics,
) {
	if h.metrics.RecordsPerRequest != nil && m.NumRecords > 0 {
		records := float64(m.NumRecords)
		h.metrics.RecordsPerRequest.WithLabelValues(h.keyspace, h.changefeed, legacyMetricAvg).Set(records)
		h.metrics.RecordsPerRequest.WithLabelValues(h.keyspace, h.changefeed, legacyMetricP99).Set(records)
	}
	if h.metrics.CompressionRatio != nil && m.UncompressedBytes > 0 && m.CompressedBytes > 0 {
		ratio := float64(m.UncompressedBytes) / float64(m.CompressedBytes) * 100
		h.metrics.CompressionRatio.WithLabelValues(h.keyspace, h.changefeed, legacyMetricAvg).Set(ratio)
		h.metrics.CompressionRatio.WithLabelValues(h.keyspace, h.changefeed, legacyMetricP99).Set(ratio)
	}
}
