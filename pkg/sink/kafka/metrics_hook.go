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

// metricsHook adapts franz-go client callbacks to TiCDC's Kafka sink metrics.
// franz-go calls these hook methods while writing requests, receiving responses,
// and flushing produce batches. The hook does not poll Kafka; it only converts
// the callback payloads into the existing TiCDC Kafka metric vectors.
type metricsHook struct {
	keyspace   string
	changefeed string

	brokers sync.Map

	recordsPerBatch        prometheus.Observer
	uncompressedBytesTotal prometheus.Counter
	compressedBytesTotal   prometheus.Counter
}

type brokerMetrics struct {
	outgoingBytesTotal prometheus.Counter
	requestsSuccess    prometheus.Counter
	requestsWriteError prometheus.Counter
	responsesSuccess   prometheus.Counter
	responsesReadError prometheus.Counter
	requestsInFlight   prometheus.Gauge
	requestDuration    prometheus.Observer
}

const (
	metricResultSuccess    = "success"
	metricResultWriteError = "write_error"
	metricResultReadError  = "read_error"
)

func newKafkaMetricsHook(changefeedID common.ChangeFeedID) *metricsHook {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	return &metricsHook{
		keyspace:               keyspace,
		changefeed:             changefeed,
		recordsPerBatch:        recordsPerBatch.WithLabelValues(keyspace, changefeed),
		uncompressedBytesTotal: uncompressedBytesTotal.WithLabelValues(keyspace, changefeed),
		compressedBytesTotal:   compressedBytesTotal.WithLabelValues(keyspace, changefeed),
	}
}

func (h *metricsHook) broker(nodeID int32) *brokerMetrics {
	if cached, ok := h.brokers.Load(nodeID); ok {
		return cached.(*brokerMetrics)
	}

	brokerID := strconv.Itoa(int(nodeID))
	metrics := &brokerMetrics{
		outgoingBytesTotal: outgoingBytesTotal.WithLabelValues(h.keyspace, h.changefeed, brokerID),
		requestsSuccess: requestsTotal.WithLabelValues(
			h.keyspace, h.changefeed, brokerID, metricResultSuccess),
		requestsWriteError: requestsTotal.WithLabelValues(
			h.keyspace, h.changefeed, brokerID, metricResultWriteError),
		responsesSuccess: responsesTotal.WithLabelValues(
			h.keyspace, h.changefeed, brokerID, metricResultSuccess),
		responsesReadError: responsesTotal.WithLabelValues(
			h.keyspace, h.changefeed, brokerID, metricResultReadError),
		requestsInFlight: requestsInFlightGauge.WithLabelValues(h.keyspace, h.changefeed, brokerID),
		requestDuration:  requestDuration.WithLabelValues(h.keyspace, h.changefeed, brokerID),
	}
	actual, _ := h.brokers.LoadOrStore(nodeID, metrics)
	return actual.(*brokerMetrics)
}

// CleanupMetrics removes Kafka sink metric series after all of its clients are closed.
func CleanupMetrics(changefeedID common.ChangeFeedID) {
	labels := prometheus.Labels{
		"namespace":  changefeedID.Keyspace(),
		"changefeed": changefeedID.Name(),
	}
	outgoingBytesTotal.DeletePartialMatch(labels)
	requestsTotal.DeletePartialMatch(labels)
	responsesTotal.DeletePartialMatch(labels)
	requestsInFlightGauge.DeletePartialMatch(labels)
	requestDuration.DeletePartialMatch(labels)
	recordsPerBatch.DeletePartialMatch(labels)
	uncompressedBytesTotal.DeletePartialMatch(labels)
	compressedBytesTotal.DeletePartialMatch(labels)
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
	metrics := h.broker(meta.NodeID)

	if bytesWritten > 0 {
		metrics.outgoingBytesTotal.Add(float64(bytesWritten))
	}
	if err != nil {
		metrics.requestsWriteError.Inc()
	} else {
		metrics.requestsSuccess.Inc()
		metrics.requestsInFlight.Inc()
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
	metrics := h.broker(meta.NodeID)

	if e2e.WriteErr == nil {
		metrics.requestsInFlight.Dec()
		if e2e.BytesRead > 0 || e2e.ReadErr != nil {
			if e2e.ReadErr != nil {
				metrics.responsesReadError.Inc()
			} else {
				metrics.responsesSuccess.Inc()
			}
		}
	}
	if e2e.Err() == nil {
		metrics.requestDuration.Observe(e2e.DurationE2E().Seconds())
	}
}

func (h *metricsHook) OnProduceBatchWritten(
	_ kgo.BrokerMetadata,
	_ string,
	_ int32,
	m kgo.ProduceBatchMetrics,
) {
	if m.NumRecords > 0 {
		h.recordsPerBatch.Observe(float64(m.NumRecords))
	}
	if m.UncompressedBytes > 0 {
		h.uncompressedBytesTotal.Add(float64(m.UncompressedBytes))
	}
	if m.CompressedBytes > 0 {
		h.compressedBytesTotal.Add(float64(m.CompressedBytes))
	}
}
