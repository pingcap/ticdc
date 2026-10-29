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

package metrics

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// LargeRowSizeLowBound is set to 2K, only track data event with size not smaller than it.
const LargeRowSizeLowBound = 2 * 1024

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

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(queueDuration)
	registry.MustRegister(sinkDMLBatchCommit)
	registry.MustRegister(sinkDMLBatchCallback)
	registry.MustRegister(prepareStatementErrors)
	registry.MustRegister(WorkerSendMessageDuration)
	registry.MustRegister(WorkerBatchSize)
	registry.MustRegister(WorkerBatchDuration)
	registry.MustRegister(CheckpointTsMessageDuration)
	registry.MustRegister(CheckpointTsMessageCount)
}
