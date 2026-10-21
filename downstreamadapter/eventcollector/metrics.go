// Copyright 2020 PingCAP, Inc.
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

package eventcollector

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	DispatcherReceivedEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "dispatcher",
		Name:      "received_event_count",
		Help:      "The number of events received by the dispatcher",
	}, []string{"type", "event_type"})

	EventCollectorRegisteredDispatcherCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "dispatcher",
		Name:      "event_collector_registered_dispatcher_count",
		Help:      "The number of registered dispatchers in the event collector",
	})

	EventCollectorReceivedEventLagDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "dispatcher",
			Name:      "received_event_lag_duration",
			Help:      "The duration of lag between the event collector received event and the event's ts",
			Buckets:   metrics.LagBucket(),
		}, []string{"type"})

	EventCollectorHandleEventDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_collector",
			Name:      "handle_event_duration",
			Help:      "The duration of handling events",
		})

	EventCollectorDroppedEventCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_collector",
		Name:      "dropped_event_count",
		Help:      "The number of events dropped by the event collector",
	})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(DispatcherReceivedEventCount)
	registry.MustRegister(EventCollectorRegisteredDispatcherCount)
	registry.MustRegister(EventCollectorReceivedEventLagDuration)
	registry.MustRegister(EventCollectorHandleEventDuration)
	registry.MustRegister(EventCollectorDroppedEventCount)
}
