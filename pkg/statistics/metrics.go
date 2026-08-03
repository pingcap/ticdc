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

package statistics

import (
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	execDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{getKeyspaceLabel(), "changefeed"})

	execDDLRunningGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_running",
			Help:      "Total count of running ddl.",
		}, []string{getKeyspaceLabel(), "changefeed"})

	execDDLCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "execution",
			Help:      "Total execution count of different DDL types.",
		}, []string{getKeyspaceLabel(), "changefeed", "ddl_type"})

	execBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_row_count",
			Help:      "Row count number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{getKeyspaceLabel(), "changefeed", "keyspace_id"})

	totalWriteBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "write_bytes_total",
			Help:      "Total approximate raw bytes of DML events successfully written to downstream.",
		}, []string{getKeyspaceLabel(), "changefeed"})

	executionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "execution_error",
			Help:      "Total count of execution errors.",
		}, []string{getKeyspaceLabel(), "changefeed", "event_type"})
)

// InitMetrics registers the metrics maintained by Statistics.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(execDDLHistogram)
	registry.MustRegister(execDDLRunningGauge)
	registry.MustRegister(execDDLCounter)
	registry.MustRegister(execBatchHistogram)
	registry.MustRegister(totalWriteBytesCounter)
	registry.MustRegister(executionErrorCounter)
}

func getKeyspaceLabel() string {
	if kerneltype.IsNextGen() {
		return "keyspace_name"
	}
	return "namespace"
}
