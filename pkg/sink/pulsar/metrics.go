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

	ticdcMetrics "github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricRegistryMu sync.RWMutex
	// metricRegistry is shared by all Pulsar clients on the node. Bootstrap can
	// create multiple changefeeds concurrently, so access must be synchronized.
	metricRegistry *prometheus.Registry
)

// InitMetrics configures the shared registry for Pulsar client metrics.
func InitMetrics(registry *prometheus.Registry) {
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
		ticdcMetrics.PulsarPublishedMessageTypeResolvedCount.WithLabelValues(topic, changefeed, "count").Inc()
		return
	}
	ticdcMetrics.PublishedDDLSchemaTableCount.WithLabelValues(topic, changefeed, "count").Inc()
}

// IncPublishedDDLSuccess success
func IncPublishedDDLSuccess(topic, changefeed string, messageType common.MessageType) {
	if isResolvedMessage(messageType) {
		ticdcMetrics.PulsarPublishedMessageTypeResolvedCount.WithLabelValues(topic, changefeed, "success").Inc()
		return
	}
	ticdcMetrics.PublishedDDLSchemaTableCount.WithLabelValues(topic, changefeed, "success").Inc()
}

// IncPublishedDDLFail fail
func IncPublishedDDLFail(topic, changefeed string, messageType common.MessageType) {
	if isResolvedMessage(messageType) {
		ticdcMetrics.PulsarPublishedMessageTypeResolvedCount.WithLabelValues(topic, changefeed, "fail").Inc()
		return
	}
	ticdcMetrics.PublishedDDLSchemaTableCount.WithLabelValues(topic, changefeed, "fail").Inc()
}

// IncPublishedDMLCount count
func IncPublishedDMLCount(topic, changefeed string) {
	ticdcMetrics.PublishedDMLSchemaTableCount.WithLabelValues(topic, changefeed, "count").Inc()
}

// IncPublishedDMLSuccess success
func IncPublishedDMLSuccess(topic, changefeed string) {
	ticdcMetrics.PublishedDMLSchemaTableCount.WithLabelValues(topic, changefeed, "success").Inc()
}

// IncPublishedDMLFail fail
func IncPublishedDMLFail(topic, changefeed string) {
	ticdcMetrics.PublishedDMLSchemaTableCount.WithLabelValues(topic, changefeed, "fail").Inc()
}

func isResolvedMessage(messageType common.MessageType) bool {
	return messageType == common.MessageTypeResolved
}
