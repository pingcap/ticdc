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

package coordinator

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	CoordinatorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "ownership_counter",
			Help:      "The counter of ownership increases every 5 seconds on a owner capture",
		})

	ChangefeedStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "changefeed_state",
			Help:      "The total number of changefeed in different replication states",
		}, []string{"state"})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(CoordinatorCounter)
	registry.MustRegister(ChangefeedStateGauge)
}
