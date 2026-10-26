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

package statistics

import (
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/prometheus/client_golang/prometheus"
)

// ---------- Metrics used in Statistics. ---------- //
var (
	// execBatchHistogram records the row count of each flushed DML event.
	execBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_row_count",
			Help:      "Row count of a DML event confirmed flushed by sink.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{getKeyspaceLabel(), "changefeed", "keyspace_id"})

	// execBatchWriteBytesHistogram records the approximate original size of each flushed DML event.
	execBatchWriteBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_write_bytes",
			Help:      "Approximate original size in bytes of a DML event confirmed flushed by sink.",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 18), // 1KB~128MB
		}, []string{getKeyspaceLabel(), "changefeed"})

	// executionErrorCounter is the counter of execution errors.
	executionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "execution_error",
			Help:      "Total count of execution errors.",
		}, []string{getKeyspaceLabel(), "changefeed", "event_type"})
)

var (
	// execDDLHistogram records the execution time of a DDL.
	execDDLHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a ddl.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
		}, []string{getKeyspaceLabel(), "changefeed"})

	// execDDLRunningGauge records the count of running DDL.
	execDDLRunningGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "exec_running",
			Help:      "Total count of running ddl.",
		}, []string{getKeyspaceLabel(), "changefeed"})

	// execDDLCounter records the execution count of different DDL types
	execDDLCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "ddl",
			Name:      "execution",
			Help:      "Total execution count of different DDL types.",
		}, []string{getKeyspaceLabel(), "changefeed", "ddl_type"})
)

// InitMetrics registers the metrics maintained by Statistics.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(execBatchHistogram)
	registry.MustRegister(execBatchWriteBytesHistogram)
	registry.MustRegister(executionErrorCounter)
	registry.MustRegister(execDDLHistogram)
	registry.MustRegister(execDDLRunningGauge)
	registry.MustRegister(execDDLCounter)
}

func getKeyspaceLabel() string {
	if kerneltype.IsNextGen() {
		return "keyspace_name"
	}
	return "namespace"
}
