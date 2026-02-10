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

package franz

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type MetricsHook struct {
	promMu     sync.RWMutex
	promBound  bool
	keyspace   string
	changefeed string
	prom       PrometheusMetrics
}

type PrometheusMetrics struct {
	RequestsInFlight  *prometheus.GaugeVec
	OutgoingByteRate  *prometheus.GaugeVec
	RequestRate       *prometheus.GaugeVec
	RequestLatency    *prometheus.HistogramVec
	ResponseRate      *prometheus.GaugeVec
	CompressionRatio  *prometheus.HistogramVec
	RecordsPerRequest *prometheus.HistogramVec
}

func NewMetricsHook() *MetricsHook {
	return &MetricsHook{}
}

func (h *MetricsHook) BindPrometheusMetrics(
	keyspace string,
	changefeed string,
	_ time.Duration,
	metrics PrometheusMetrics,
) {
	h.promMu.Lock()
	defer h.promMu.Unlock()

	h.keyspace = keyspace
	h.changefeed = changefeed
	h.prom = metrics
	h.promBound = true
}

func (h *MetricsHook) loadPrometheusMetrics() (string, string, PrometheusMetrics, bool) {
	h.promMu.RLock()
	defer h.promMu.RUnlock()

	return h.keyspace, h.changefeed, h.prom, h.promBound
}

func (h *MetricsHook) Run(ctx context.Context) {
	_, _, _, bound := h.loadPrometheusMetrics()

	if !bound {
		<-ctx.Done()
		return
	}

	<-ctx.Done()
	h.CleanupPrometheusMetrics()
}

func (h *MetricsHook) FlushPrometheusMetrics(interval time.Duration) {
	_ = interval
}

func (h *MetricsHook) CleanupPrometheusMetrics() {
	keyspace, changefeed, metrics, bound := h.loadPrometheusMetrics()

	if !bound {
		return
	}

	labels := prometheus.Labels{
		"namespace":  keyspace,
		"changefeed": changefeed,
	}
	if metrics.OutgoingByteRate != nil {
		metrics.OutgoingByteRate.MetricVec.DeletePartialMatch(labels)
	}
	if metrics.RequestRate != nil {
		metrics.RequestRate.MetricVec.DeletePartialMatch(labels)
	}
	if metrics.ResponseRate != nil {
		metrics.ResponseRate.MetricVec.DeletePartialMatch(labels)
	}
	if metrics.RequestsInFlight != nil {
		metrics.RequestsInFlight.MetricVec.DeletePartialMatch(labels)
	}
	if metrics.RequestLatency != nil {
		metrics.RequestLatency.MetricVec.DeletePartialMatch(labels)
	}
	if metrics.CompressionRatio != nil {
		metrics.CompressionRatio.MetricVec.DeletePartialMatch(labels)
	}
	if metrics.RecordsPerRequest != nil {
		metrics.RecordsPerRequest.MetricVec.DeletePartialMatch(labels)
	}
}

func (h *MetricsHook) RecordBrokerWrite(nodeID int32, bytesWritten int, err error) {
	if nodeID < 0 {
		return
	}

	keyspace, changefeed, metrics, bound := h.loadPrometheusMetrics()
	if !bound {
		return
	}
	brokerID := strconv.Itoa(int(nodeID))

	if metrics.OutgoingByteRate != nil && bytesWritten > 0 {
		metrics.OutgoingByteRate.WithLabelValues(keyspace, changefeed, brokerID).Add(float64(bytesWritten))
	}
	if metrics.RequestRate != nil {
		metrics.RequestRate.WithLabelValues(keyspace, changefeed, brokerID).Add(1)
	}
	if err == nil && metrics.RequestsInFlight != nil {
		metrics.RequestsInFlight.WithLabelValues(keyspace, changefeed, brokerID).Add(1)
	}
}

func (h *MetricsHook) OnBrokerWrite(
	meta kgo.BrokerMetadata,
	_ int16,
	bytesWritten int,
	_ time.Duration,
	_ time.Duration,
	err error,
) {
	h.RecordBrokerWrite(meta.NodeID, bytesWritten, err)
}

func (h *MetricsHook) OnBrokerE2E(
	meta kgo.BrokerMetadata,
	_ int16,
	e2e kgo.BrokerE2E,
) {
	if meta.NodeID < 0 {
		return
	}

	keyspace, changefeed, metrics, bound := h.loadPrometheusMetrics()
	if !bound {
		return
	}
	brokerID := strconv.Itoa(int(meta.NodeID))

	if e2e.WriteErr == nil && metrics.RequestsInFlight != nil {
		metrics.RequestsInFlight.WithLabelValues(keyspace, changefeed, brokerID).Add(-1)
	}
	if e2e.BytesRead > 0 && e2e.ReadErr == nil && metrics.ResponseRate != nil {
		metrics.ResponseRate.WithLabelValues(keyspace, changefeed, brokerID).Add(1)
	}
	if e2e.Err() == nil && metrics.RequestLatency != nil {
		latencyMs := float64(e2e.DurationE2E().Microseconds()) / 1000
		metrics.RequestLatency.WithLabelValues(keyspace, changefeed, brokerID).Observe(latencyMs)
	}
}

func (h *MetricsHook) OnProduceBatchWritten(
	_ kgo.BrokerMetadata,
	_ string,
	_ int32,
	m kgo.ProduceBatchMetrics,
) {
	h.RecordProduceBatchWritten(m.NumRecords, m.UncompressedBytes, m.CompressedBytes)
}

func (h *MetricsHook) RecordProduceBatchWritten(numRecords int, uncompressedBytes int, compressedBytes int) {
	keyspace, changefeed, metrics, bound := h.loadPrometheusMetrics()
	if !bound {
		return
	}

	if metrics.RecordsPerRequest != nil && numRecords > 0 {
		records := float64(numRecords)
		metrics.RecordsPerRequest.WithLabelValues(keyspace, changefeed).Observe(records)
	}
	if metrics.CompressionRatio != nil && uncompressedBytes > 0 && compressedBytes > 0 {
		ratio := float64(uncompressedBytes) / float64(compressedBytes) * 100
		metrics.CompressionRatio.WithLabelValues(keyspace, changefeed).Observe(ratio)
	}
}
