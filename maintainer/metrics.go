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

package maintainer

import (
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	maintainerHandleEventDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "handle_event_duration",
			Help:      "Bucketed histogram of maintainer handle event time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	maintainerEventChLenGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "maintainer",
			Name:      "event_ch_len",
			Help:      "length of maintainer event channel",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	captureLeaseResponseCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_lease_response_total",
			Help:      "Number of capture write lease responses by handling result.",
		}, []string{"result"})

	captureLeaseResponseRejectedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_lease_response_rejected_total",
			Help:      "Number of rejected P2P write lease responses.",
		}, []string{"reason"})

	// execDDLBlockingGauge records the count of blocking DDL.
	execDDLBlockingGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_blocking",
			Help:      "Total count of blocking ddl.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "mode"})
)

// RegisterMetrics registers maintainer, operator, and replica metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(maintainerHandleEventDuration)
	registry.MustRegister(maintainerEventChLenGauge)
	registry.MustRegister(captureLeaseResponseCounter)
	registry.MustRegister(captureLeaseResponseRejectedCounter)
	registry.MustRegister(execDDLBlockingGauge)
	operator.RegisterMetrics(registry)
	replica.RegisterMetrics(registry)
}
