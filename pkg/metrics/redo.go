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

const (
	namespace = "ticdc"
	subsystem = "redo"
)

var (
	// RedoWriteBytesGauge records the total number of bytes written to redo log.
	RedoWriteBytesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "write_bytes_total",
		Help:      "Total number of bytes redo log written",
	}, []string{"namespace", "changefeed", "type"})

	// RedoFsyncDurationHistogram records the latency distributions of fsync called by redo writer.
	RedoFsyncDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "fsync_duration_seconds",
		Help:      "The latency distributions of fsync called by redo writer",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16),
	}, []string{"namespace", "changefeed", "type"})

	// RedoFlushAllDurationHistogram records the latency distributions of flushAll
	// called by redo writer.
	RedoFlushAllDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "flush_all_duration_seconds",
		Help:      "The latency distributions of flushall called by redo writer",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16),
	}, []string{"namespace", "changefeed", "type"})

	// RedoFlushLogDurationHistogram records the latency distributions of flushLog.
	RedoFlushLogDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "flush_log_duration_seconds",
		Help:      "The latency distributions of flushLog called by redo sink",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16),
	}, []string{"namespace", "changefeed", "type"})
)

func InitRedoMetrics(registry *prometheus.Registry) {
	registry.MustRegister(RedoFsyncDurationHistogram)
	registry.MustRegister(RedoWriteBytesGauge)
	registry.MustRegister(RedoFlushAllDurationHistogram)
	registry.MustRegister(RedoFlushLogDurationHistogram)
}
