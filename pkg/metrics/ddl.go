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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ExecDDLHistogram records the exexution time of a DDL.
	ExecDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{getKeyspaceLabel(), "changefeed"})

	// ExecDDLRunningGauge records the count of running DDL.
	ExecDDLRunningGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_running",
			Help:      "Total count of running ddl.",
		}, []string{getKeyspaceLabel(), "changefeed"})

	// ExecDDLBlockingGauge records the count of blocking DDL.
	ExecDDLBlockingGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_blocking",
			Help:      "Total count of blocking ddl.",
		}, []string{getKeyspaceLabel(), "changefeed", "mode"})

	// ExecDDLCounter records the execution count of different DDL types
	ExecDDLCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "execution",
			Help:      "Total execution count of different DDL types.",
		}, []string{getKeyspaceLabel(), "changefeed", "ddl_type"})
)

func initDDLMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ExecDDLHistogram)
	registry.MustRegister(ExecDDLRunningGauge)
	registry.MustRegister(ExecDDLBlockingGauge)
	registry.MustRegister(ExecDDLCounter)
}
