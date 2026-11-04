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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// LargeRowSizeLowBound is set to 2K, only track data event with size not smaller than it.
const LargeRowSizeLowBound = 2 * 1024

<<<<<<< HEAD:pkg/metrics/sink.go
// ---------- Metrics used in Statistics. ---------- //
var (
	// ExecBatchHistogram records batch size of a txn.
	ExecBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_row_count",
			Help:      "Row count number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{GetKeyspaceLabel(), "changefeed", "type", "keyspace_id"}) // type is for `sinkType`

	// ExecBatchWriteBytesHistogram records bytes written for each batch.
	ExecBatchWriteBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "batch_write_bytes",
			Help:      "Bytes number for a given batch.",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 18), // 1KB~128MB
		}, []string{GetKeyspaceLabel(), "changefeed", "type"}) // type is for `sinkType`

	// ExecWriteBytesGauge records the total number of bytes written by sink.
	TotalWriteBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "write_bytes_total",
			Help:      "Total number of bytes written by sink",
		}, []string{GetKeyspaceLabel(), "changefeed", "type"}) // type is for `sinkType`

	EventSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "event_size",
			Help:      "The size of changed events (in bytes).",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 30), // 0~32M
		}, []string{GetKeyspaceLabel(), "changefeed"})

	ExecDMLEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "dml_event_count",
			Help:      "Total count of DML events.",
		}, []string{GetKeyspaceLabel(), "changefeed"})

	// ExecutionErrorCounter is the counter of execution errors.
	ExecutionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "execution_error",
			Help:      "Total count of execution errors.",
		}, []string{GetKeyspaceLabel(), "changefeed", "event_type"})
)

// ---------- Metrics for txn sink and backends. ---------- //
=======
>>>>>>> af8dc9454 (metrics: register component collectors during server startup (#6663)):downstreamadapter/sink/metrics/metrics.go
var (
	// queueDuration = ConflictDetectDuration + (queue time in txn workers).
	queueDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_queue_duration",
			Help:      "Bucketed histogram of queue time (s) for single DML statement.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	sinkDMLBatchCommit = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_sink_dml_batch_commit",
			Help:      "Duration of committing a DML batch",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1310s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	sinkDMLBatchCallback = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_sink_dml_batch_callback",
			Help:      "Duration of execuing a batch of callbacks",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms~1300s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	prepareStatementErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "txn_prepare_statement_errors",
			Help:      "Prepare statement errors",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// WorkerSendMessageDuration records the duration of flushing a group messages.
	WorkerSendMessageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_worker_send_message_duration",
			Help:      "Send Message duration(s) for MQ worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// WorkerBatchSize record the size of each batched messages.
	WorkerBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_worker_batch_size",
			Help:      "Batch size for MQ worker.",
			Buckets:   prometheus.ExponentialBuckets(4, 2, 10), // 4 ~ 2048
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	// WorkerBatchDuration record the time duration cost on batch messages.
	WorkerBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_worker_batch_duration",
			Help:      "Batch duration for MQ worker.",
			Buckets:   prometheus.ExponentialBuckets(0.004, 2, 10), // 4ms ~ 2s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	CheckpointTsMessageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_checkpoint_ts_message_duration",
			Help:      "Duration of sending checkpoint ts message.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	CheckpointTsMessageCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "mq_checkpoint_ts_message_count",
			Help:      "Number of checkpoint ts messages sent.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})
)

<<<<<<< HEAD:pkg/metrics/sink.go
// InitMetrics registers all metrics in this file.
func initSinkMetrics(registry *prometheus.Registry) {
	// common sink metrics
	registry.MustRegister(ExecBatchHistogram)
	registry.MustRegister(ExecBatchWriteBytesHistogram)
	registry.MustRegister(TotalWriteBytesCounter)
	registry.MustRegister(EventSizeHistogram)
	registry.MustRegister(ExecDMLEventCounter)
	registry.MustRegister(ExecutionErrorCounter)

	// txn sink metrics
	registry.MustRegister(QueueDuration)
	registry.MustRegister(SinkDMLBatchCommit)
	registry.MustRegister(SinkDMLBatchCallback)
	registry.MustRegister(PrepareStatementErrors)

	// kafka sink metrics
=======
// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(queueDuration)
	registry.MustRegister(sinkDMLBatchCommit)
	registry.MustRegister(sinkDMLBatchCallback)
	registry.MustRegister(prepareStatementErrors)
>>>>>>> af8dc9454 (metrics: register component collectors during server startup (#6663)):downstreamadapter/sink/metrics/metrics.go
	registry.MustRegister(WorkerSendMessageDuration)
	registry.MustRegister(WorkerBatchSize)
	registry.MustRegister(WorkerBatchDuration)
	registry.MustRegister(CheckpointTsMessageDuration)
	registry.MustRegister(CheckpointTsMessageCount)

	// pulsar sink metrics
	initPulsarMetrics(registry)
}
