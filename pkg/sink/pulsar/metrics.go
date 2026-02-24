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
	ticdcMetrics "github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

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
