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
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ChangefeedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "status",
			Help:      "The status of changefeeds",
		}, []string{GetKeyspaceLabel(), "changefeed", "keyspace_id"})

	// ChangefeedErrorInfoGauge records the current warning or failed reason and its occurrence time
	// for each changefeed.
	ChangefeedErrorInfoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_error_info",
			Help:      "The current warning or failed reason and occurrence time of changefeeds",
		}, []string{GetKeyspaceLabel(), "changefeed", "state", "error_time", "code", "message"})

	ChangefeedCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts_lag",
			Help:      "changefeed checkpoint ts lag in changefeeds in seconds",
		}, []string{GetKeyspaceLabel(), "changefeed", "keyspace_id"})

	// it's a metrics used in a large number of tcms, we should always keep this metrics
	ChangefeedCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of changefeeds",
		}, []string{GetKeyspaceLabel(), "changefeed"})

	// ChangefeedDownstreamInfoGauge is a metric with a constant '1' value,
	// labeled by the downstream type of each changefeed.
	//
	// It is used by dashboards to show a changefeed -> downstream type mapping
	// without leaking sensitive information in sink-uri.
	ChangefeedDownstreamInfoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_downstream_info",
			Help:      "Downstream type information of changefeeds exposed as labels.",
		}, []string{GetKeyspaceLabel(), "changefeed", "downstream_type"})
)

func DeleteChangefeedCheckpointMetrics(keyspace, changefeed string, keyspaceID uint32) {
	ChangefeedCheckpointTsGauge.DeleteLabelValues(keyspace, changefeed)
	ChangefeedCheckpointTsLagGauge.DeleteLabelValues(keyspace, changefeed, FormatKeyspaceID(keyspaceID))
}

// FormatKeyspaceID formats a keyspace ID as a metric label value.
func FormatKeyspaceID(keyspaceID uint32) string {
	return strconv.FormatUint(uint64(keyspaceID), 10)
}

func ResetOwnerChangefeedMetrics() {
	ChangefeedStatusGauge.Reset()
	ChangefeedErrorInfoGauge.Reset()
	ChangefeedCheckpointTsGauge.Reset()
	ChangefeedCheckpointTsLagGauge.Reset()
	ChangefeedDownstreamInfoGauge.Reset()
}

func initChangefeedMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ChangefeedStatusGauge)
	registry.MustRegister(ChangefeedErrorInfoGauge)
	registry.MustRegister(ChangefeedCheckpointTsLagGauge)
	registry.MustRegister(ChangefeedCheckpointTsGauge)
	registry.MustRegister(ChangefeedDownstreamInfoGauge)
}
