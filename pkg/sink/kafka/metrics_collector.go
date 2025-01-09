// Copyright 2024 PingCAP, Inc.
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
	"encoding/json"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	// RefreshMetricsInterval specifies the interval of refresh kafka client metrics.
	RefreshMetricsInterval = 5 * time.Second
)

// The stats are provided as a JSON object string, see https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
type kafkaMetrics struct {
	// Instance type (producer or consumer)
	Role string `json:"type"`
	// Wall clock time in seconds since the epoch
	Time int `json:"time"`
	// Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()
	Ops int `json:"replyq"`
	// Current number of messages in producer queues
	MsgCount int `json:"msg_cnt"`
	// Current total size of messages in producer queues
	MsgSize int `json:"msg_size"`

	// Total number of requests sent to Kafka brokers
	Tx int `json:"tx"`
	// Total number of bytes transmitted to Kafka brokers
	TxBytes int `json:"tx_bytes"`
	// Total number of responses received from Kafka brokers
	Rx int `json:"rx"`
	// Total number of bytes received from Kafka brokers
	RxBytes int `json:"rx_bytes"`
	// Number of topics in the metadata cache.
	MetadataCacheCnt int `json:"metadata_cache_cnt"`

	Brokers []broker `json:"brokers"`
}

type broker struct {
	Name     string `json:"name"`
	Nodeid   int    `json:"nodeid"`
	Nodename string `json:"nodename"`
	State    string `json:"state"`
	Rtt      window `json:"rtt"`
}

type window struct {
	Min int `json:"min"`
	Max int `json:"max"`
	Avg int `json:"avg"`
	P99 int `json:"p99"`
}

type metricsCollector struct {
	changefeedID common.ChangeFeedID
	config       *kafka.ConfigMap
}

// NewMetricsCollector return a kafka metrics collector based on  library.
func NewMetricsCollector(
	changefeedID common.ChangeFeedID,
	config *kafka.ConfigMap,
) MetricsCollector {
	return &metricsCollector{changefeedID: changefeedID, config: config}
}

func (m *metricsCollector) Run(ctx context.Context) {
	_ = m.config.SetKey("statistics.interval.ms", RefreshMetricsInterval.Milliseconds())
	_ = m.config.SetKey("stats_cb", m.collect)
	client, err := kafka.NewAdminClient(m.config)
	if err != nil {
		log.Error("create client failed", zap.Error(err))
		return
	}
	for {
		select {
		case <-ctx.Done():
			log.Info("Kafka metrics collector stopped",
				zap.String("namespace", m.changefeedID.String()))
			client.Close()
			m.cleanupMetrics()
			return
		}
	}
}
func (m *metricsCollector) collect(data string) {
	var statistics kafkaMetrics
	if err := json.Unmarshal([]byte(data), &statistics); err != nil {
		log.Error("kafka metrics collect failed", zap.Error(err))
	}
	// metrics is collected each 5 seconds, divide by 5 to get per seconds average.
	// compressionRatioGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String()).
	// 	Set(float64(statistics.Writes / 5))
	recordsPerRequestGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String()).
		Set(float64(statistics.Tx) / 5)
	requestsInFlightGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String()).
		Set(float64(statistics.MsgCount) / 5)
	responseRateGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String()).
		Set(float64(statistics.Rx) / 5)
	// RequestRateGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String()).
	// 	Set(float64(statistics.Writes / 5))

	latency := 0
	for _, broker := range statistics.Brokers {
		latency += broker.Rtt.Avg
	}
	latency = latency / len(statistics.Brokers)
	// latency is in milliseconds
	RequestLatencyGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String(), "avg").
		Set(float64(latency) * 1000 / 5)
	// OutgoingByteRateGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String()).
	// 	Set(float64(statistics.Bytes / 5))
}

func (m *metricsCollector) cleanupMetrics() {
	// compressionRatioGauge.
	// 	DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String())
	recordsPerRequestGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String())
	requestsInFlightGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String())
	responseRateGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String())
	// RequestRateGauge.
	// 	DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String())

	RequestLatencyGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String(), "avg")
	OutgoingByteRateGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Id.String())
}
