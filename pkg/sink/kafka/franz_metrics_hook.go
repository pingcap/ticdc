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
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// metricsHook adapts client callbacks to TiCDC's Kafka sink metrics.
// franz-go calls these hook methods while writing requests, receiving responses,
// and flushing produce batches. The hook does not poll Kafka; it only records
// raw callback values for Prometheus.
type metricsHook struct {
	keyspace   string
	changefeed string

	brokers sync.Map

	recordsPerBatch  prometheus.Observer
	compressionRatio prometheus.Observer
}

type brokerMetrics struct {
	outgoingBytesTotal prometheus.Counter
	requestsTotal      prometheus.Counter
	requestsInFlight   prometheus.Gauge
	requestDuration    prometheus.Observer
	throttleTime       prometheus.Observer
}

func newMetricsHook(changefeedID common.ChangeFeedID) *metricsHook {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	return &metricsHook{
		keyspace:         keyspace,
		changefeed:       changefeed,
		recordsPerBatch:  recordsPerBatch.WithLabelValues(keyspace, changefeed),
		compressionRatio: compressionRatio.WithLabelValues(keyspace, changefeed),
	}
}

func (h *metricsHook) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	if meta.NodeID < 0 {
		return
	}

	h.broker(meta.NodeID).throttleTime.Observe(throttleInterval.Seconds())
}

// broker returns the metric handles for one broker. They are cached here so the
// per-request hook paths resolve each label set only once.
func (h *metricsHook) broker(nodeID int32) *brokerMetrics {
	if cached, ok := h.brokers.Load(nodeID); ok {
		return cached.(*brokerMetrics)
	}

	brokerID := strconv.Itoa(int(nodeID))
	metrics := &brokerMetrics{
		outgoingBytesTotal: outgoingBytesTotal.WithLabelValues(h.keyspace, h.changefeed, brokerID),
		requestsTotal:      requestsTotal.WithLabelValues(h.keyspace, h.changefeed, brokerID),
		requestsInFlight:   requestsInFlightGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID),
		requestDuration:    requestDuration.WithLabelValues(h.keyspace, h.changefeed, brokerID),
		throttleTime:       throttleTime.WithLabelValues(h.keyspace, h.changefeed, brokerID),
	}

	actual, _ := h.brokers.LoadOrStore(nodeID, metrics)
	return actual.(*brokerMetrics)
}

// cleanupMetrics removes producer series after all clients are closed.
func cleanupMetrics(changefeedID common.ChangeFeedID) {
	labels := prometheus.Labels{
		"namespace":  changefeedID.Keyspace(),
		"changefeed": changefeedID.Name(),
	}

	outgoingBytesTotal.DeletePartialMatch(labels)
	requestsTotal.DeletePartialMatch(labels)
	requestsInFlightGauge.DeletePartialMatch(labels)
	requestDuration.DeletePartialMatch(labels)
	throttleTime.DeletePartialMatch(labels)
	recordsPerBatch.DeletePartialMatch(labels)
	compressionRatio.DeletePartialMatch(labels)
}

func (h *metricsHook) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _ time.Duration, _ time.Duration, err error) {
	if meta.NodeID < 0 {
		return
	}

	metrics := h.broker(meta.NodeID)
	metrics.requestsTotal.Inc()

	if bytesWritten > 0 {
		metrics.outgoingBytesTotal.Add(float64(bytesWritten))
	}

	if err == nil {
		metrics.requestsInFlight.Inc()
	}
}

func (h *metricsHook) OnBrokerE2E(meta kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	if meta.NodeID < 0 {
		return
	}

	metrics := h.broker(meta.NodeID)

	if e2e.WriteErr == nil {
		metrics.requestsInFlight.Dec()
	}

	if e2e.Err() == nil {
		metrics.requestDuration.Observe(e2e.DurationE2E().Seconds())
	}
}

func (h *metricsHook) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.ProduceBatchMetrics) {
	if m.NumRecords > 0 {
		h.recordsPerBatch.Observe(float64(m.NumRecords))
	}

	if m.CompressionType != 0 && m.UncompressedBytes > 0 && m.CompressedBytes > 0 {
		ratio := float64(m.UncompressedBytes) / float64(m.CompressedBytes) * 100
		h.compressionRatio.Observe(ratio)
	}
}
