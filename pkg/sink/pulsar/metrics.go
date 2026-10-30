// Copyright 2025 PingCAP, Inc.
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

package pulsar

import (
	"sync"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// pulsar producer DDL scheme, type: count/success/fail, May there are too many tables
	publishedDDLSchemaTableCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      "published_DDL_schema_table_count",
			Help:      "pulsar published schema count",
		}, []string{"topic", "changefeed", "type"})

	// pulsar producer DML scheme, type:count/success/fail , May there are too many tables
	publishedDMLSchemaTableCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      "published_DML_schema_table_count",
			Help:      "pulsar published dml message count",
		}, []string{"topic", "changefeed", "type"})

	// pulsar producer WATER count, type:count/success/fail , May there are too many tables
	publishedMessageTypeResolvedCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      "published_message_type_resolved_count",
			Help:      "pulsar published message type resolved count ",
		}, []string{"topic", "changefeed", "type"})
)

var (
	metricRegistryMu sync.RWMutex
	// metricRegistry is shared by all Pulsar clients on the node. Bootstrap can
	// create multiple changefeeds concurrently, so access must be synchronized.
	metricRegistry *prometheus.Registry
)

// RegisterMetrics registers Pulsar sink metrics and configures the shared registry for client metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(publishedDDLSchemaTableCount)
	registry.MustRegister(publishedDMLSchemaTableCount)
	registry.MustRegister(publishedMessageTypeResolvedCount)

	metricRegistryMu.Lock()
	metricRegistry = registry
	metricRegistryMu.Unlock()
}

func getMetricRegistry() *prometheus.Registry {
	metricRegistryMu.RLock()
	registry := metricRegistry
	metricRegistryMu.RUnlock()
	if registry != nil {
		return registry
	}

	metricRegistryMu.Lock()
	defer metricRegistryMu.Unlock()
	// Use the default registry when clients are created before server bootstrap.
	if metricRegistry == nil {
		metricRegistry = prometheus.DefaultRegisterer.(*prometheus.Registry)
	}
	return metricRegistry
}

// IncPublishedDDLCount DDL
func IncPublishedDDLCount(topic, changefeed string, messageType common.MessageType) {
	if isResolvedMessage(messageType) {
		publishedMessageTypeResolvedCount.WithLabelValues(topic, changefeed, "count").Inc()
		return
	}
	publishedDDLSchemaTableCount.WithLabelValues(topic, changefeed, "count").Inc()
}

// IncPublishedDDLSuccess success
func IncPublishedDDLSuccess(topic, changefeed string, messageType common.MessageType) {
	if isResolvedMessage(messageType) {
		publishedMessageTypeResolvedCount.WithLabelValues(topic, changefeed, "success").Inc()
		return
	}
	publishedDDLSchemaTableCount.WithLabelValues(topic, changefeed, "success").Inc()
}

// IncPublishedDDLFail fail
func IncPublishedDDLFail(topic, changefeed string, messageType common.MessageType) {
	if isResolvedMessage(messageType) {
		publishedMessageTypeResolvedCount.WithLabelValues(topic, changefeed, "fail").Inc()
		return
	}
	publishedDDLSchemaTableCount.WithLabelValues(topic, changefeed, "fail").Inc()
}

// IncPublishedDMLCount count
func IncPublishedDMLCount(topic, changefeed string) {
	publishedDMLSchemaTableCount.WithLabelValues(topic, changefeed, "count").Inc()
}

// IncPublishedDMLSuccess success
func IncPublishedDMLSuccess(topic, changefeed string) {
	publishedDMLSchemaTableCount.WithLabelValues(topic, changefeed, "success").Inc()
}

// IncPublishedDMLFail fail
func IncPublishedDMLFail(topic, changefeed string) {
	publishedDMLSchemaTableCount.WithLabelValues(topic, changefeed, "fail").Inc()
}

func isResolvedMessage(messageType common.MessageType) bool {
	return messageType == common.MessageTypeResolved
}
