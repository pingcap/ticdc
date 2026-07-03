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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	adminMethodGetBrokerConfig     = "get_broker_config"
	adminMethodGetTopicConfig      = "get_topic_config"
	adminMethodGetTopicsMeta       = "get_topics_meta"
	adminMethodGetTopicsPartitions = "get_topics_partitions_num"
	adminMethodCreateTopic         = "create_topic"
	adminCallStatusOK              = "ok"
	adminCallStatusError           = "error"
)

var (
	adminCallCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_client_admin_call_total",
			Help:      "Total kafka admin calls by method and result.",
		}, []string{"namespace", "changefeed", "method", "result"})
	adminCallLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "kafka_client_admin_call_duration_seconds",
			Help:      "Latency of kafka admin calls by method and result.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"namespace", "changefeed", "method", "result"})
)

func initAdminMetrics(registry *prometheus.Registry) {
	registry.MustRegister(adminCallCount)
	registry.MustRegister(adminCallLatency)
}

func cleanupAdminMetrics(keyspace string, changefeed string) {
	labels := prometheus.Labels{
		"namespace":  keyspace,
		"changefeed": changefeed,
	}
	adminCallCount.DeletePartialMatch(labels)
	adminCallLatency.DeletePartialMatch(labels)
}

func observeAdminCall(
	keyspace string,
	changefeed string,
	method string,
	callErr error,
	duration time.Duration,
) {
	status := adminCallStatusOK
	if callErr != nil {
		status = adminCallStatusError
	}
	adminCallCount.WithLabelValues(keyspace, changefeed, method, status).Inc()
	adminCallLatency.WithLabelValues(keyspace, changefeed, method, status).Observe(duration.Seconds())
}
