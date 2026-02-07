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
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/rcrowley/go-metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type franzBrokerMetrics struct {
	outgoingByteRate metrics.Meter
	requestRate      metrics.Meter
	requestLatency   metrics.Histogram
	responseRate     metrics.Meter
	inFlight         int64
}

func newFranzBrokerMetrics() *franzBrokerMetrics {
	return &franzBrokerMetrics{
		outgoingByteRate: metrics.NewMeter(),
		requestRate:      metrics.NewMeter(),
		requestLatency:   metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
		responseRate:     metrics.NewMeter(),
	}
}

type franzMetricsHook struct {
	mu      sync.RWMutex
	brokers map[int32]*franzBrokerMetrics

	compressionRatio metrics.Histogram
	recordsPerReq    metrics.Histogram
}

func newFranzMetricsHook() *franzMetricsHook {
	return &franzMetricsHook{
		brokers:          make(map[int32]*franzBrokerMetrics),
		compressionRatio: metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
		recordsPerReq:    metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
	}
}

func (h *franzMetricsHook) getBroker(nodeID int32) *franzBrokerMetrics {
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
	broker = newFranzBrokerMetrics()
	h.brokers[nodeID] = broker
	return broker
}

func (h *franzMetricsHook) snapshotBrokers() map[int32]*franzBrokerMetrics {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make(map[int32]*franzBrokerMetrics, len(h.brokers))
	for id, broker := range h.brokers {
		result[id] = broker
	}
	return result
}

func (h *franzMetricsHook) OnBrokerWrite(
	meta kgo.BrokerMetadata,
	_ int16,
	bytesWritten int,
	_ time.Duration,
	_ time.Duration,
	err error,
) {
	broker := h.getBroker(meta.NodeID)
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

func (h *franzMetricsHook) OnBrokerE2E(
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

func (h *franzMetricsHook) OnProduceBatchWritten(
	_ kgo.BrokerMetadata,
	_ string,
	_ int32,
	m kgo.ProduceBatchMetrics,
) {
	if m.NumRecords > 0 {
		h.recordsPerReq.Update(int64(m.NumRecords))
	}
	if m.UncompressedBytes > 0 && m.CompressedBytes > 0 {
		ratio := int64(float64(m.UncompressedBytes) / float64(m.CompressedBytes) * 100)
		h.compressionRatio.Update(ratio)
	}
}

type franzMetricsCollector struct {
	changefeedID common.ChangeFeedID
	hook         *franzMetricsHook
}

func (m *franzMetricsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(refreshMetricsInterval)
	defer func() {
		ticker.Stop()
		m.cleanupMetrics()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("franz kafka metrics collector stopped",
				zap.String("keyspace", m.changefeedID.Keyspace()),
				zap.String("changefeed", m.changefeedID.Name()))
			return
		case <-ticker.C:
			m.collectMetrics()
		}
	}
}

func (m *franzMetricsCollector) collectMetrics() {
	keyspace := m.changefeedID.Keyspace()
	changefeedID := m.changefeedID.Name()

	compressionSnapshot := m.hook.compressionRatio.Snapshot()
	compressionRatioGauge.WithLabelValues(keyspace, changefeedID, avg).Set(compressionSnapshot.Mean())
	compressionRatioGauge.WithLabelValues(keyspace, changefeedID, p99).Set(compressionSnapshot.Percentile(0.99))

	recordsSnapshot := m.hook.recordsPerReq.Snapshot()
	recordsPerRequestGauge.WithLabelValues(keyspace, changefeedID, avg).Set(recordsSnapshot.Mean())
	recordsPerRequestGauge.WithLabelValues(keyspace, changefeedID, p99).Set(recordsSnapshot.Percentile(0.99))

	for id, broker := range m.hook.snapshotBrokers() {
		brokerID := strconv.Itoa(int(id))
		OutgoingByteRateGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(
			broker.outgoingByteRate.Snapshot().Rate1(),
		)
		RequestRateGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(
			broker.requestRate.Snapshot().Rate1(),
		)
		RequestLatencyGauge.WithLabelValues(keyspace, changefeedID, brokerID, avg).Set(
			broker.requestLatency.Snapshot().Mean() / 1000,
		)
		RequestLatencyGauge.WithLabelValues(keyspace, changefeedID, brokerID, p99).Set(
			broker.requestLatency.Snapshot().Percentile(0.99) / 1000,
		)
		requestsInFlightGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(
			float64(atomic.LoadInt64(&broker.inFlight)),
		)
		responseRateGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(
			broker.responseRate.Snapshot().Rate1(),
		)
	}
}

func (m *franzMetricsCollector) cleanupMetrics() {
	keyspace := m.changefeedID.Keyspace()
	changefeedID := m.changefeedID.Name()
	compressionRatioGauge.DeleteLabelValues(keyspace, changefeedID, avg)
	compressionRatioGauge.DeleteLabelValues(keyspace, changefeedID, p99)
	recordsPerRequestGauge.DeleteLabelValues(keyspace, changefeedID, avg)
	recordsPerRequestGauge.DeleteLabelValues(keyspace, changefeedID, p99)

	for id := range m.hook.snapshotBrokers() {
		brokerID := strconv.Itoa(int(id))
		OutgoingByteRateGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
		RequestRateGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
		RequestLatencyGauge.DeleteLabelValues(keyspace, changefeedID, brokerID, avg)
		RequestLatencyGauge.DeleteLabelValues(keyspace, changefeedID, brokerID, p99)
		requestsInFlightGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
		responseRateGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
	}
}
