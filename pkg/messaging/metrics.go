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

package messaging

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	messagingSendMsgCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "send_message_counter",
			Help:      "The counter of messages sent by a message center",
		}, []string{"type"}) // target: its addr, type: event, command

	messagingReceiveMsgCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "receive_message_counter",
			Help:      "The counter of messages received by a message center",
		}, []string{"type"}) // target: its addr, type: event, command

	messagingDropMsgCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "drop_message_counter",
			Help:      "The counter of messages dropped by a message center",
		}, []string{"type"}) // target: its addr, type: event, command

	messagingErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "error_counter",
			Help:      "The counter of errors occurred in a message center",
		}, []string{"type", "message"}) // type: event, command, message: error info
	messagingStreamGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "stream_gauge",
			Help:      "The gauge of streams in a message center",
		}, []string{"from"}) // target: its addr

	messagingReceiveChannelLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "receive_channel_length",
			Help:      "The length of the receive channel in a message center",
		}, []string{"type"}) // type: event, command

	messagingSlowHandleCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "messaging",
			Name:      "slow_handle_counter",
			Help:      "The counter of messages that took more than 100ms to handle",
		}, []string{"type"}) // type: message type
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(messagingSendMsgCounter)
	registry.MustRegister(messagingReceiveMsgCounter)
	registry.MustRegister(messagingDropMsgCounter)
	registry.MustRegister(messagingErrorCounter)
	registry.MustRegister(messagingStreamGauge)
	registry.Register(messagingReceiveChannelLength)
	registry.MustRegister(messagingSlowHandleCounter)
}
