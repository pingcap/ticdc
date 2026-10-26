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
	// execBatchHistogram records batch size of a txn.
	execBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_row_count",
			Help:      "Row count number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{getKeyspaceLabel(), "changefeed", "keyspace_id"})

	// execBatchWriteBytesHistogram records bytes written for each batch.
	execBatchWriteBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_write_bytes",
			Help:      "Bytes number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 18), // 1KB~128MB
		}, []string{getKeyspaceLabel(), "changefeed"})

	// totalWriteBytesCounter records the total number of bytes written by sink.
	totalWriteBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "write_bytes_total",
			Help:      "Total number of bytes written by sink",
		}, []string{getKeyspaceLabel(), "changefeed"})

	eventSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "event_size",
			Help:      "The size of changed events (in bytes).",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 30), // 0~32M
		}, []string{getKeyspaceLabel(), "changefeed"})

	execDMLEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "dml_event_count",
			Help:      "Total count of DML events.",
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
	registry.MustRegister(totalWriteBytesCounter)
	registry.MustRegister(eventSizeHistogram)
	registry.MustRegister(execDMLEventCounter)
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
