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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"github.com/pingcap/ticdc/pkg/metrics"
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

	// changefeedErrorInfoGauge records the current warning or failed reason and its occurrence time
	// for each changefeed.
	changefeedErrorInfoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_error_info",
			Help:      "The current warning or failed reason and occurrence time of changefeeds",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "state", "error_time", "code", "message"})

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

	meteringEnabledGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "metering",
			Name:      "enabled",
			Help:      "Whether traffic metering reporting is enabled on the coordinator (1 = enabled, 0 = disabled).",
		})

	meteringReportCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "metering",
			Name:      "report_total",
			Help:      "Total number of traffic metering upload attempts by outcome.",
		}, []string{"result"})

	meteringLastSuccessTimestampGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "metering",
			Name:      "last_success_timestamp_seconds",
			Help:      "Unix timestamp of the last successful traffic metering upload.",
		})

	meteringReportDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "metering",
			Name:      "report_duration_seconds",
			Help:      "Duration in seconds of traffic metering upload attempts.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
		})

	meteringPendingBytesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "metering",
			Name:      "pending_bytes",
			Help:      "Total unreported traffic bytes currently buffered in coordinator memory.",
		})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(coordinatorCounter)
	registry.MustRegister(changefeedStateGauge)
	registry.MustRegister(captureLeaseHeartbeatCounter)
	registry.MustRegister(captureP2PWitnessAvailable)
	registry.MustRegister(changefeedErrorInfoGauge)
	registry.MustRegister(meteringEnabledGauge)
	registry.MustRegister(meteringReportCounter)
	registry.MustRegister(meteringLastSuccessTimestampGauge)
	registry.MustRegister(meteringReportDuration)
	registry.MustRegister(meteringPendingBytesGauge)
}
