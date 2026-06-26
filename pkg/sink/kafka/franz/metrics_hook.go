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
	clientType string
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

	LegacyRequestsInFlight  *prometheus.GaugeVec
	LegacyOutgoingByteRate  *prometheus.GaugeVec
	LegacyRequestRate       *prometheus.GaugeVec
	LegacyRequestLatency    *prometheus.GaugeVec
	LegacyResponseRate      *prometheus.GaugeVec
	LegacyCompressionRatio  *prometheus.GaugeVec
	LegacyRecordsPerRequest *prometheus.GaugeVec
}

const (
	legacyMetricAvg = "avg"
	legacyMetricP99 = "p99"
)

func NewMetricsHook(clientType string) *MetricsHook {
	return &MetricsHook{clientType: clientType}
}

func (h *MetricsHook) BindPrometheusMetrics(
	keyspace string,
	changefeed string,
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

func (h *MetricsHook) CleanupPrometheusMetrics() {
	keyspace, changefeed, metrics, bound := h.loadPrometheusMetrics()

	if !bound {
		return
	}

	labels := prometheus.Labels{
		"namespace":  keyspace,
		"changefeed": changefeed,
		"client":     h.clientType,
	}
	deleteGaugeVecPartialMatch(metrics.OutgoingByteRate, labels)
	deleteGaugeVecPartialMatch(metrics.RequestRate, labels)
	deleteGaugeVecPartialMatch(metrics.ResponseRate, labels)
	deleteGaugeVecPartialMatch(metrics.RequestsInFlight, labels)
	deleteHistogramVecPartialMatch(metrics.RequestLatency, labels)
	deleteHistogramVecPartialMatch(metrics.CompressionRatio, labels)
	deleteHistogramVecPartialMatch(metrics.RecordsPerRequest, labels)

	legacyLabels := prometheus.Labels{
		"namespace":  keyspace,
		"changefeed": changefeed,
	}
	deleteGaugeVecPartialMatch(metrics.LegacyOutgoingByteRate, legacyLabels)
	deleteGaugeVecPartialMatch(metrics.LegacyRequestRate, legacyLabels)
	deleteGaugeVecPartialMatch(metrics.LegacyResponseRate, legacyLabels)
	deleteGaugeVecPartialMatch(metrics.LegacyRequestsInFlight, legacyLabels)
	deleteGaugeVecPartialMatch(metrics.LegacyRequestLatency, legacyLabels)
	deleteGaugeVecPartialMatch(metrics.LegacyCompressionRatio, legacyLabels)
	deleteGaugeVecPartialMatch(metrics.LegacyRecordsPerRequest, legacyLabels)
}

func (h *MetricsHook) RecordBrokerWrite(nodeID int32, bytesWritten int, err error) {
	if nodeID < 0 {
		return
	}

	ctx, ok := h.loadMetricsContext()
	if !ok {
		return
	}
	brokerID := strconv.Itoa(int(nodeID))

	if ctx.metrics.OutgoingByteRate != nil && bytesWritten > 0 {
		ctx.metrics.OutgoingByteRate.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType, brokerID).Add(float64(bytesWritten))
	}
	if ctx.metrics.LegacyOutgoingByteRate != nil && bytesWritten > 0 {
		ctx.metrics.LegacyOutgoingByteRate.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(float64(bytesWritten))
	}
	if ctx.metrics.RequestRate != nil {
		ctx.metrics.RequestRate.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType, brokerID).Add(1)
	}
	if ctx.metrics.LegacyRequestRate != nil {
		ctx.metrics.LegacyRequestRate.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(1)
	}
	if err == nil && ctx.metrics.RequestsInFlight != nil {
		ctx.metrics.RequestsInFlight.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType, brokerID).Add(1)
	}
	if err == nil && ctx.metrics.LegacyRequestsInFlight != nil {
		ctx.metrics.LegacyRequestsInFlight.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(1)
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

	ctx, ok := h.loadMetricsContext()
	if !ok {
		return
	}
	brokerID := strconv.Itoa(int(meta.NodeID))

	if e2e.WriteErr == nil && ctx.metrics.RequestsInFlight != nil {
		ctx.metrics.RequestsInFlight.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType, brokerID).Add(-1)
	}
	if e2e.WriteErr == nil && ctx.metrics.LegacyRequestsInFlight != nil {
		ctx.metrics.LegacyRequestsInFlight.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(-1)
	}
	if e2e.BytesRead > 0 && e2e.ReadErr == nil && ctx.metrics.ResponseRate != nil {
		ctx.metrics.ResponseRate.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType, brokerID).Add(1)
	}
	if e2e.BytesRead > 0 && e2e.ReadErr == nil && ctx.metrics.LegacyResponseRate != nil {
		ctx.metrics.LegacyResponseRate.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(1)
	}
	if e2e.Err() == nil && ctx.metrics.RequestLatency != nil {
		latencyMs := float64(e2e.DurationE2E().Microseconds()) / 1000
		ctx.metrics.RequestLatency.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType, brokerID).Observe(latencyMs)
	}
	if e2e.Err() == nil && ctx.metrics.LegacyRequestLatency != nil {
		latencyMs := float64(e2e.DurationE2E().Microseconds()) / 1000
		ctx.metrics.LegacyRequestLatency.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID, legacyMetricAvg).Set(latencyMs)
		ctx.metrics.LegacyRequestLatency.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID, legacyMetricP99).Set(latencyMs)
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
	ctx, ok := h.loadMetricsContext()
	if !ok {
		return
	}

	if ctx.metrics.RecordsPerRequest != nil && numRecords > 0 {
		records := float64(numRecords)
		ctx.metrics.RecordsPerRequest.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType).Observe(records)
	}
	if ctx.metrics.LegacyRecordsPerRequest != nil && numRecords > 0 {
		records := float64(numRecords)
		ctx.metrics.LegacyRecordsPerRequest.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricAvg).Set(records)
		ctx.metrics.LegacyRecordsPerRequest.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricP99).Set(records)
	}
	if ctx.metrics.CompressionRatio != nil && uncompressedBytes > 0 && compressedBytes > 0 {
		ratio := float64(uncompressedBytes) / float64(compressedBytes) * 100
		ctx.metrics.CompressionRatio.WithLabelValues(ctx.keyspace, ctx.changefeed, ctx.clientType).Observe(ratio)
	}
	if ctx.metrics.LegacyCompressionRatio != nil && uncompressedBytes > 0 && compressedBytes > 0 {
		ratio := float64(uncompressedBytes) / float64(compressedBytes) * 100
		ctx.metrics.LegacyCompressionRatio.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricAvg).Set(ratio)
		ctx.metrics.LegacyCompressionRatio.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricP99).Set(ratio)
	}
}

type metricsContext struct {
	keyspace   string
	changefeed string
	clientType string
	metrics    PrometheusMetrics
}

func (h *MetricsHook) loadMetricsContext() (metricsContext, bool) {
	keyspace, changefeed, metrics, bound := h.loadPrometheusMetrics()
	if !bound {
		return metricsContext{}, false
	}
	return metricsContext{
		keyspace:   keyspace,
		changefeed: changefeed,
		clientType: h.clientType,
		metrics:    metrics,
	}, true
}

func deleteGaugeVecPartialMatch(gaugeVec *prometheus.GaugeVec, labels prometheus.Labels) {
	if gaugeVec != nil {
		gaugeVec.MetricVec.DeletePartialMatch(labels)
	}
}

func deleteHistogramVecPartialMatch(histogramVec *prometheus.HistogramVec, labels prometheus.Labels) {
	if histogramVec != nil {
		histogramVec.MetricVec.DeletePartialMatch(labels)
	}
}
