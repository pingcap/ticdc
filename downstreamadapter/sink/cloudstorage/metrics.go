// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "ticdc"
	subsystem = "sink"
)

// Metrics for cloud storage sink
var (
	// cloudStorageFlushBytesHist records the total number of bytes flushed to external storage.
	cloudStorageFlushBytesHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_flush_bytes",
		Help:      "Total number of bytes written to cloud storage",
		Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 20), // 1KiB ~ 512MiB
	}, []string{"namespace", "changefeed"})

	// cloudStorageFlushDurationHistogram records the latency distributions of flushLog.
	cloudStorageFlushDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_flush_duration_seconds",
		Help:      "The latency distributions of flush storage by a cloud storage sink",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 15), // 1ms ~ 16s
	}, []string{"namespace", "changefeed"})

	// cloudStorageDDLFlushDMLDurationHistogram records DDL flush duration.
	cloudStorageDDLFlushDMLDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_ddl_flush_dml_duration_seconds",
		Help:      "DDL flush duration for cloud storage sink",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16), // 1ms ~ 32s
	}, []string{"namespace", "changefeed"})

	// cloudStorageWorkerBusyRatio records the busy ratio of CloudStorage writers.
	cloudStorageWorkerBusyRatio = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cloud_storage_worker_busy_ratio",
			Help:      "Busy ratio for cloud storage sink dml writers.",
		}, []string{"namespace", "changefeed", "id"})

	cloudStorageSpoolMemoryBytesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_spool_memory_bytes",
		Help:      "Current memory bytes used by cloud storage spool",
	}, []string{"namespace", "changefeed"})

	cloudStorageSpoolDiskBytesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_spool_disk_bytes",
		Help:      "Current disk bytes used by cloud storage spool",
	}, []string{"namespace", "changefeed"})

	cloudStoragePendingPostEnqueueGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_pending_post_enqueue",
		Help:      "Current number of PostEnqueue callbacks waiting in cloud storage spool",
	}, []string{"namespace", "changefeed"})

	cloudStorageSpoolDiskQuotaWaitDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_spool_disk_quota_wait_duration_seconds",
		Help:      "Wait duration for cloud storage spool disk quota",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 16), // 1ms ~ 32s
	}, []string{"namespace", "changefeed"})

	cloudStorageSpoolDiskQuotaWaitersGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_spool_disk_quota_waiters",
		Help:      "Current number of waiters blocked by cloud storage spool disk quota",
	}, []string{"namespace", "changefeed"})

	// cloudStorageLoadBytesHistogram records loaded batch sizes.
	cloudStorageLoadBytesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_load_bytes",
		Help:      "Byte distribution of encoded batches loaded from cloud storage spool",
		Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 20), // 1KiB ~ 512MiB
	}, []string{"namespace", "changefeed"})

	cloudStorageRotateCountCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_rotate_total",
		Help:      "Total number of local spool segment rotations in cloud storage sink",
	}, []string{"namespace", "changefeed"})

	cloudStorageSpoolSegmentCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_spool_segment_count",
		Help:      "Current number of live cloud storage spool segments",
	}, []string{"namespace", "changefeed"})

	cloudStorageFlushReasonCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_flush_total",
		Help:      "Total number of flush batches emitted by cloud storage buffer manager",
	}, []string{"namespace", "changefeed", "reason"})
)

// InitMetrics registers all cloud storage sink metrics.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(cloudStorageFlushBytesHist)
	registry.MustRegister(cloudStorageFlushDurationHistogram)
	registry.MustRegister(cloudStorageDDLFlushDMLDurationHistogram)
	registry.MustRegister(cloudStorageWorkerBusyRatio)
	registry.MustRegister(cloudStorageSpoolMemoryBytesGauge)
	registry.MustRegister(cloudStorageSpoolDiskBytesGauge)
	registry.MustRegister(cloudStoragePendingPostEnqueueGauge)
	registry.MustRegister(cloudStorageSpoolDiskQuotaWaitDurationHistogram)
	registry.MustRegister(cloudStorageSpoolDiskQuotaWaitersGauge)
	registry.MustRegister(cloudStorageLoadBytesHistogram)
	registry.MustRegister(cloudStorageRotateCountCounter)
	registry.MustRegister(cloudStorageSpoolSegmentCountGauge)
	registry.MustRegister(cloudStorageFlushReasonCounter)
}
