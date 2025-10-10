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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	MaintainerCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of maintainer",
		}, []string{"namespace", "changefeed"})

	MaintainerCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "checkpoint_ts_lag",
			Help:      "checkpoint ts lag of maintainer in seconds",
		}, []string{"namespace", "changefeed"})

	MaintainerResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "resolved_ts",
			Help:      "resolved ts of maintainer",
		}, []string{"namespace", "changefeed"})
	MaintainerResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of maintainer in seconds",
		}, []string{"namespace", "changefeed"})

	CoordinatorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "ownership_counter",
			Help:      "The counter of ownership increases every 5 seconds on a owner capture",
		})

	MaintainerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "changefeed",
			Name:      "maintainer_counter",
			Help:      "The counter of changefeed maintainer",
		}, []string{"namespace", "changefeed"})

	ChangefeedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "status",
			Help:      "The status of changefeeds",
		}, []string{"namespace", "changefeed"})

	ChangefeedCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts_lag",
			Help:      "changefeed checkpoint ts lag in changefeeds in seconds",
		}, []string{"namespace", "changefeed"})
)

func initChangefeedMetrics(registry *prometheus.Registry) {
	registry.MustRegister(MaintainerCheckpointTsGauge)
	registry.MustRegister(MaintainerCheckpointTsLagGauge)
	registry.MustRegister(MaintainerResolvedTsGauge)
	registry.MustRegister(MaintainerResolvedTsLagGauge)
	registry.MustRegister(CoordinatorCounter)
	registry.MustRegister(MaintainerGauge)
	registry.MustRegister(ChangefeedStatusGauge)
	registry.MustRegister(ChangefeedCheckpointTsLagGauge)
}
