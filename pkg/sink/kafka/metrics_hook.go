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
	deleteGaugeVecPartialMatch(h.metrics.OutgoingByteRate, labels)
	deleteGaugeVecPartialMatch(h.metrics.RequestRate, labels)
	deleteGaugeVecPartialMatch(h.metrics.ResponseRate, labels)
	deleteGaugeVecPartialMatch(h.metrics.RequestsInFlight, labels)
	deleteGaugeVecPartialMatch(h.metrics.RequestLatency, labels)
	deleteGaugeVecPartialMatch(h.metrics.CompressionRatio, labels)
	deleteGaugeVecPartialMatch(h.metrics.RecordsPerRequest, labels)
}

func (h *metricsHook) RecordBrokerWrite(nodeID int32, bytesWritten int, err error) {
	if nodeID < 0 {
		return
	}

	ctx, ok := h.loadMetricsContext()
	if !ok {
		return
	}
	brokerID := strconv.Itoa(int(nodeID))

	if ctx.metrics.OutgoingByteRate != nil && bytesWritten > 0 {
		ctx.metrics.OutgoingByteRate.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(float64(bytesWritten))
	}
	if ctx.metrics.RequestRate != nil {
		ctx.metrics.RequestRate.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(1)
	}
	if err == nil && ctx.metrics.RequestsInFlight != nil {
		ctx.metrics.RequestsInFlight.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(1)
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
	h.RecordBrokerWrite(meta.NodeID, bytesWritten, err)
}

func (h *metricsHook) OnBrokerE2E(
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
		ctx.metrics.RequestsInFlight.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(-1)
	}
	if e2e.BytesRead > 0 && e2e.ReadErr == nil && ctx.metrics.ResponseRate != nil {
		ctx.metrics.ResponseRate.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID).Add(1)
	}
	if e2e.Err() == nil && ctx.metrics.RequestLatency != nil {
		latencyMs := float64(e2e.DurationE2E().Microseconds()) / 1000
		ctx.metrics.RequestLatency.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID, legacyMetricAvg).Set(latencyMs)
		ctx.metrics.RequestLatency.WithLabelValues(ctx.keyspace, ctx.changefeed, brokerID, legacyMetricP99).Set(latencyMs)
	}
}

func (h *metricsHook) OnProduceBatchWritten(
	_ kgo.BrokerMetadata,
	_ string,
	_ int32,
	m kgo.ProduceBatchMetrics,
) {
	h.RecordProduceBatchWritten(m.NumRecords, m.UncompressedBytes, m.CompressedBytes)
}

func (h *metricsHook) RecordProduceBatchWritten(numRecords int, uncompressedBytes int, compressedBytes int) {
	ctx, ok := h.loadMetricsContext()
	if !ok {
		return
	}

	if ctx.metrics.RecordsPerRequest != nil && numRecords > 0 {
		records := float64(numRecords)
		ctx.metrics.RecordsPerRequest.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricAvg).Set(records)
		ctx.metrics.RecordsPerRequest.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricP99).Set(records)
	}
	if ctx.metrics.CompressionRatio != nil && uncompressedBytes > 0 && compressedBytes > 0 {
		ratio := float64(uncompressedBytes) / float64(compressedBytes) * 100
		ctx.metrics.CompressionRatio.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricAvg).Set(ratio)
		ctx.metrics.CompressionRatio.WithLabelValues(ctx.keyspace, ctx.changefeed, legacyMetricP99).Set(ratio)
	}
}

type metricsContext struct {
	keyspace   string
	changefeed string
	metrics    metricVectors
}

func (h *metricsHook) loadMetricsContext() (metricsContext, bool) {
	return metricsContext{
		keyspace:   h.keyspace,
		changefeed: h.changefeed,
		metrics:    h.metrics,
	}, true
}

func deleteGaugeVecPartialMatch(gaugeVec *prometheus.GaugeVec, labels prometheus.Labels) {
	if gaugeVec != nil {
		gaugeVec.DeletePartialMatch(labels)
	}
}
