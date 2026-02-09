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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	kafkafranz "github.com/pingcap/ticdc/pkg/sink/kafka/franz"
	"go.uber.org/zap"
)

type franzMetricsCollector struct {
	changefeedID common.ChangeFeedID
	hook         *kafkafranz.MetricsHook
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

	snapshot := m.hook.Snapshot()
	compressionRatioGauge.WithLabelValues(keyspace, changefeedID, avg).Set(snapshot.CompressionMean)
	compressionRatioGauge.WithLabelValues(keyspace, changefeedID, p99).Set(snapshot.CompressionP99)
	recordsPerRequestGauge.WithLabelValues(keyspace, changefeedID, avg).Set(snapshot.RecordsMean)
	recordsPerRequestGauge.WithLabelValues(keyspace, changefeedID, p99).Set(snapshot.RecordsP99)

	for id, broker := range snapshot.Brokers {
		brokerID := strconv.Itoa(int(id))
		OutgoingByteRateGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(broker.OutgoingByteRate)
		RequestRateGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(broker.RequestRate)
		RequestLatencyGauge.WithLabelValues(keyspace, changefeedID, brokerID, avg).Set(
			broker.RequestLatencyMeanMic / 1000,
		)
		RequestLatencyGauge.WithLabelValues(keyspace, changefeedID, brokerID, p99).Set(
			broker.RequestLatencyP99Mic / 1000,
		)
		requestsInFlightGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(float64(broker.InFlight))
		responseRateGauge.WithLabelValues(keyspace, changefeedID, brokerID).Set(broker.ResponseRate)
	}
}

func (m *franzMetricsCollector) cleanupMetrics() {
	keyspace := m.changefeedID.Keyspace()
	changefeedID := m.changefeedID.Name()
	compressionRatioGauge.DeleteLabelValues(keyspace, changefeedID, avg)
	compressionRatioGauge.DeleteLabelValues(keyspace, changefeedID, p99)
	recordsPerRequestGauge.DeleteLabelValues(keyspace, changefeedID, avg)
	recordsPerRequestGauge.DeleteLabelValues(keyspace, changefeedID, p99)

	snapshot := m.hook.Snapshot()
	for id := range snapshot.Brokers {
		brokerID := strconv.Itoa(int(id))
		OutgoingByteRateGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
		RequestRateGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
		RequestLatencyGauge.DeleteLabelValues(keyspace, changefeedID, brokerID, avg)
		RequestLatencyGauge.DeleteLabelValues(keyspace, changefeedID, brokerID, p99)
		requestsInFlightGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
		responseRateGauge.DeleteLabelValues(keyspace, changefeedID, brokerID)
	}
}
