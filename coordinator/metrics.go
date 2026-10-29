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

package coordinator

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	coordinatorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "ownership_counter",
			Help:      "The counter of ownership increases every 5 seconds on a owner capture",
		})

	changefeedStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "changefeed_state",
			Help:      "The total number of changefeed in different replication states",
		}, []string{"state"})

	captureLeaseHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "capture_lease_heartbeat_total",
			Help:      "Number of capture write lease heartbeats by handling result.",
		}, []string{"result"})

	captureP2PWitnessAvailable = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "capture_p2p_witness_available",
			Help:      "Whether a remote capture is available to witness the coordinator capture.",
		})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(captureLeaseHeartbeatCounter)
	registry.MustRegister(captureP2PWitnessAvailable)
	registry.MustRegister(coordinatorCounter)
	registry.MustRegister(changefeedStateGauge)
}
