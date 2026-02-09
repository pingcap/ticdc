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
	"sync"
	"sync/atomic"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type brokerMetrics struct {
	outgoingByteRate metrics.Meter
	requestRate      metrics.Meter
	requestLatency   metrics.Histogram
	responseRate     metrics.Meter
	inFlight         int64
}

func newBrokerMetrics() *brokerMetrics {
	return &brokerMetrics{
		outgoingByteRate: metrics.NewMeter(),
		requestRate:      metrics.NewMeter(),
		requestLatency:   metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
		responseRate:     metrics.NewMeter(),
	}
}

type MetricsHook struct {
	mu      sync.RWMutex
	brokers map[int32]*brokerMetrics

	compressionRatio metrics.Histogram
	recordsPerReq    metrics.Histogram
}

func NewMetricsHook() *MetricsHook {
	return &MetricsHook{
		brokers:          make(map[int32]*brokerMetrics),
		compressionRatio: metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
		recordsPerReq:    metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
	}
}

func (h *MetricsHook) RecordBrokerWrite(nodeID int32, bytesWritten int, err error) {
	broker := h.getBroker(nodeID)
	if broker == nil {
		return
	}

	if bytesWritten > 0 {
		broker.outgoingByteRate.Mark(int64(bytesWritten))
	}
	broker.requestRate.Mark(1)
	if err == nil {
		atomic.AddInt64(&broker.inFlight, 1)
	}
}

func (h *MetricsHook) getBroker(nodeID int32) *brokerMetrics {
	if nodeID < 0 {
		return nil
	}

	h.mu.RLock()
	broker := h.brokers[nodeID]
	h.mu.RUnlock()
	if broker != nil {
		return broker
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	broker = h.brokers[nodeID]
	if broker != nil {
		return broker
	}
	broker = newBrokerMetrics()
	h.brokers[nodeID] = broker
	return broker
}

type BrokerMetricsSnapshot struct {
	OutgoingByteRate      float64
	RequestRate           float64
	RequestLatencyMeanMic float64
	RequestLatencyP99Mic  float64
	InFlight              int64
	ResponseRate          float64
}

type MetricsSnapshot struct {
	CompressionMean float64
	CompressionP99  float64
	RecordsMean     float64
	RecordsP99      float64
	Brokers         map[int32]BrokerMetricsSnapshot
}

func (h *MetricsHook) Snapshot() MetricsSnapshot {
	h.mu.RLock()
	brokers := make(map[int32]*brokerMetrics, len(h.brokers))
	for id, broker := range h.brokers {
		brokers[id] = broker
	}
	compression := h.compressionRatio.Snapshot()
	records := h.recordsPerReq.Snapshot()
	h.mu.RUnlock()

	result := MetricsSnapshot{
		CompressionMean: compression.Mean(),
		CompressionP99:  compression.Percentile(0.99),
		RecordsMean:     records.Mean(),
		RecordsP99:      records.Percentile(0.99),
		Brokers:         make(map[int32]BrokerMetricsSnapshot, len(brokers)),
	}

	for id, broker := range brokers {
		latencySnapshot := broker.requestLatency.Snapshot()
		result.Brokers[id] = BrokerMetricsSnapshot{
			OutgoingByteRate:      broker.outgoingByteRate.Snapshot().Rate1(),
			RequestRate:           broker.requestRate.Snapshot().Rate1(),
			RequestLatencyMeanMic: latencySnapshot.Mean(),
			RequestLatencyP99Mic:  latencySnapshot.Percentile(0.99),
			InFlight:              atomic.LoadInt64(&broker.inFlight),
			ResponseRate:          broker.responseRate.Snapshot().Rate1(),
		}
	}
	return result
}

func (h *MetricsHook) snapshotBrokers() map[int32]*brokerMetrics {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make(map[int32]*brokerMetrics, len(h.brokers))
	for id, broker := range h.brokers {
		result[id] = broker
	}
	return result
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
	broker := h.getBroker(meta.NodeID)
	if broker == nil {
		return
	}

	if e2e.WriteErr == nil {
		if atomic.AddInt64(&broker.inFlight, -1) < 0 {
			atomic.StoreInt64(&broker.inFlight, 0)
		}
	}
	if e2e.BytesRead > 0 && e2e.ReadErr == nil {
		broker.responseRate.Mark(1)
	}
	if e2e.Err() == nil {
		broker.requestLatency.Update(e2e.DurationE2E().Microseconds())
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
	if numRecords > 0 {
		h.recordsPerReq.Update(int64(numRecords))
	}
	if uncompressedBytes > 0 && compressedBytes > 0 {
		ratio := int64(float64(uncompressedBytes) / float64(compressedBytes) * 100)
		h.compressionRatio.Update(ratio)
	}
}
