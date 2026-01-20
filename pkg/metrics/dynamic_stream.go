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

import "github.com/prometheus/client_golang/prometheus"

var (
	DynamicStreamMemoryUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "memory_usage",
		}, []string{"module", "type", getKeyspaceLabel(), "area"})
	DynamicStreamEventChanSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "event_chan_size",
		}, []string{"module"})
	DynamicStreamPendingQueueLen = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "pending_queue_len",
		}, []string{"module"})
	DynamicStreamAddPathNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "add_path_num",
			Help:      "The number of add path command",
		}, []string{"module"})
	DynamicStreamRemovePathNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "remove_path_num",
			Help:      "The number of remove path command",
		}, []string{"module"})

	DynamicStreamBatchCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "batch_count",
			Help:      "The number of events in each batch processed by dynamic stream",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1 ~ 16384
		}, []string{"module", "area"})
	DynamicStreamBatchBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "batch_bytes",
			Help:      "The total bytes in each batch processed by dynamic stream",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 18), // 1KB ~ 128MB
		}, []string{"module", "area"})
	DynamicStreamBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "dynamic_stream",
			Name:      "batch_duration",
			Help:      "The duration of batch each batch in dynamic stream",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 18), // 40us to 5s
		}, []string{"module", "area"})
)

func initDynamicStreamMetrics(registry *prometheus.Registry) {
	registry.MustRegister(DynamicStreamMemoryUsage)
	registry.MustRegister(DynamicStreamEventChanSize)
	registry.MustRegister(DynamicStreamPendingQueueLen)
	registry.MustRegister(DynamicStreamAddPathNum)
	registry.MustRegister(DynamicStreamRemovePathNum)
	registry.MustRegister(DynamicStreamBatchCount)
	registry.MustRegister(DynamicStreamBatchBytes)
	registry.MustRegister(DynamicStreamBatchDuration)
}
