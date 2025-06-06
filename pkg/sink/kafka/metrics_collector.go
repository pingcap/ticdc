// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
)

// MetricsCollector is the interface for kafka metrics collector.
type MetricsCollector interface {
	Run(ctx context.Context)
}

const (
	// refreshMetricsInterval specifies the interval of refresh kafka client metrics.
	refreshMetricsInterval = 5 * time.Second
	// refreshClusterMetaInterval specifies the interval of refresh kafka cluster meta.
	// Do not set it too small, because it will cause too many requests to kafka cluster.
	// Every request will get all topics and all brokers information.
	refreshClusterMetaInterval = 30 * time.Minute
)

// Sarama metrics names, see https://pkg.go.dev/github.com/IBM/sarama#pkg-overview.
const (
	// Producer level.
	compressionRatioMetricName  = "compression-ratio"
	recordsPerRequestMetricName = "records-per-request"

	// Broker level.
	outgoingByteRateMetricNamePrefix   = "outgoing-byte-rate-for-broker-"
	requestRateMetricNamePrefix        = "request-rate-for-broker-"
	requestLatencyInMsMetricNamePrefix = "request-latency-in-ms-for-broker-"
	requestsInFlightMetricNamePrefix   = "requests-in-flight-for-broker-"
	responseRateMetricNamePrefix       = "response-rate-for-broker-"

	p99 = "p99"
	avg = "avg"
)

type saramaMetricsCollector struct {
	changefeedID common.ChangeFeedID
	// adminClient is used to get broker infos from broker.
	adminClient ClusterAdminClient
	brokers     map[int32]struct{}
	registry    metrics.Registry
}

func (m *saramaMetricsCollector) Run(ctx context.Context) {
	// Initialize brokers.
	m.updateBrokers(ctx)

	refreshMetricsTicker := time.NewTicker(refreshMetricsInterval)
	refreshClusterMetaTicker := time.NewTicker(refreshClusterMetaInterval)
	defer func() {
		refreshMetricsTicker.Stop()
		refreshClusterMetaTicker.Stop()
		m.cleanupMetrics()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("kafka metrics collector stopped",
				zap.String("namespace", m.changefeedID.Namespace()),
				zap.String("changefeed", m.changefeedID.Name()))
			return
		case <-refreshMetricsTicker.C:
			m.collectBrokerMetrics()
			m.collectProducerMetrics()
		case <-refreshClusterMetaTicker.C:
			m.updateBrokers(ctx)
		}
	}
}

func (m *saramaMetricsCollector) updateBrokers(ctx context.Context) {
	brokers := m.adminClient.GetAllBrokers(ctx)
	for _, b := range brokers {
		m.brokers[b.ID] = struct{}{}
	}
}

func (m *saramaMetricsCollector) collectProducerMetrics() {
	namespace := m.changefeedID.Namespace()
	changefeedID := m.changefeedID.Name()
	compressionRatioMetric := m.registry.Get(compressionRatioMetricName)
	if histogram, ok := compressionRatioMetric.(metrics.Histogram); ok {
		compressionRatioGauge.
			WithLabelValues(namespace, changefeedID, avg).
			Set(histogram.Snapshot().Mean())
		compressionRatioGauge.WithLabelValues(namespace, changefeedID, p99).
			Set(histogram.Snapshot().Percentile(0.99))
	}

	recordsPerRequestMetric := m.registry.Get(recordsPerRequestMetricName)
	if histogram, ok := recordsPerRequestMetric.(metrics.Histogram); ok {
		recordsPerRequestGauge.
			WithLabelValues(namespace, changefeedID, avg).
			Set(histogram.Snapshot().Mean())
		recordsPerRequestGauge.
			WithLabelValues(namespace, changefeedID, p99).
			Set(histogram.Snapshot().Percentile(0.99))
	}
}

func (m *saramaMetricsCollector) collectBrokerMetrics() {
	namespace := m.changefeedID.Namespace()
	changefeedID := m.changefeedID.Name()
	for id := range m.brokers {
		brokerID := strconv.Itoa(int(id))
		outgoingByteRateMetric := m.registry.Get(
			getBrokerMetricName(outgoingByteRateMetricNamePrefix, brokerID))
		if meter, ok := outgoingByteRateMetric.(metrics.Meter); ok {
			OutgoingByteRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		requestRateMetric := m.registry.Get(
			getBrokerMetricName(requestRateMetricNamePrefix, brokerID))
		if meter, ok := requestRateMetric.(metrics.Meter); ok {
			RequestRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().Rate1())
		}

		requestLatencyMetric := m.registry.Get(
			getBrokerMetricName(requestLatencyInMsMetricNamePrefix, brokerID))
		if histogram, ok := requestLatencyMetric.(metrics.Histogram); ok {
			RequestLatencyGauge.
				WithLabelValues(namespace, changefeedID, brokerID, avg).
				Set(histogram.Snapshot().Mean() / 1000)
			RequestLatencyGauge.
				WithLabelValues(namespace, changefeedID, brokerID, p99).
				Set(histogram.Snapshot().Percentile(0.99) / 1000)
		}

		requestsInFlightMetric := m.registry.Get(getBrokerMetricName(
			requestsInFlightMetricNamePrefix, brokerID))
		if counter, ok := requestsInFlightMetric.(metrics.Counter); ok {
			requestsInFlightGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(float64(counter.Snapshot().Count()))
		}

		responseRateMetric := m.registry.Get(getBrokerMetricName(
			responseRateMetricNamePrefix, brokerID))
		if meter, ok := responseRateMetric.(metrics.Meter); ok {
			responseRateGauge.
				WithLabelValues(namespace, changefeedID, brokerID).
				Set(meter.Snapshot().Rate1())
		}
	}
}

func getBrokerMetricName(prefix, brokerID string) string {
	return prefix + brokerID
}

func (m *saramaMetricsCollector) cleanupProducerMetrics() {
	compressionRatioGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), avg)
	compressionRatioGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), p99)

	recordsPerRequestGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), avg)
	recordsPerRequestGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), p99)
}

func (m *saramaMetricsCollector) cleanupBrokerMetrics() {
	namespace := m.changefeedID.Namespace()
	changefeedID := m.changefeedID.Name()
	for id := range m.brokers {
		brokerID := strconv.Itoa(int(id))
		OutgoingByteRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		RequestRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		RequestLatencyGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID, avg)
		RequestLatencyGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID, p99)
		requestsInFlightGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)
		responseRateGauge.
			DeleteLabelValues(namespace, changefeedID, brokerID)

	}
}

func (m *saramaMetricsCollector) cleanupMetrics() {
	m.cleanupProducerMetrics()
	m.cleanupBrokerMetrics()
}
