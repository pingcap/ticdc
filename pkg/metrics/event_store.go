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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	EventStoreSubscriptionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "subscription_num",
			Help:      "The number of subscriptions in event store",
		})

	EventStoreReceivedEventCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "input_event_count",
			Help:      "The number of events received by event store.",
		}, []string{"type"}) // types : kv, resolved.

	// EventStoreCompressedRowsCount is the counter of compressed rows.
	EventStoreCompressedRowsCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "compressed_rows_count",
			Help:      "The total number of rows compressed by event store.",
		})
	// EventStoreOutputEventCount is the metric that counts events output by the sorter.
	EventStoreOutputEventCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "output_event_count",
		Help:      "The number of events output by the sorter",
	}, []string{"type", "mode"}) // types : kv, resolved.

	EventStoreWriteDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "write_duration",
		Help:      "Bucketed histogram of event store write duration",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 10),
	})

	EventStoreScanRequestsCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "scan_requests_count",
			Help:      "The number of scan requests received by event store.",
		})

	EventStoreScanBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "scan_bytes",
		Help:      "The number of bytes scanned by event store.",
	}, []string{"type"})

	EventStoreDeleteRangeCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "delete_range_count",
			Help:      "The number of delete range operations executed by event store gc manager (after coalescing).",
		})

	EventStoreDeleteRangeFetchedCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "delete_range_fetched_count",
			Help:      "The number of delete range items fetched by event store gc manager (before coalescing).",
		})

	EventStoreSubscriptionResolvedTsLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "subscription_resolved_ts_lag",
			Help:      "The Resolved Ts lag of subscriptions for event store.",
			Buckets:   LagBucket(),
		})

	EventStoreSubscriptionDataGCLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "subscription_data_gc_lag",
			Help:      "The data gc lag of subscriptions for event store.",
			Buckets:   LagBucket(),
		})

	EventStoreOnDiskDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "on_disk_data_size",
		Help:      "The amount of pending data stored on-disk for event store",
	}, []string{"id"})

	EventStoreInMemoryDataSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "in_memory_data_size",
		Help:      "The amount of pending data stored in-memory for event store",
	}, []string{"id"})

	EventStoreResolvedTsLagGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "resolved_ts_lag",
			Help:      "The resolved ts lag of event store.",
		})

	EventStoreWriteBatchEventsCountHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_batch_events_count",
			Help:      "Batch event count histogram for write task pool.",
			Buckets:   prometheus.ExponentialBuckets(8, 2, 20),
		})

	EventStoreWriteBatchSizeHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_batch_size",
			Help:      "Batch event size histogram for write task pool.",
			Buckets:   prometheus.ExponentialBuckets(32, 2, 20),
		})

	EventStoreWriteBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_bytes",
			Help:      "The number of bytes written by event store.",
		})

	EventStoreWriteRequestsCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_requests_count",
			Help:      "The number of write requests received by event store.",
		})

	EventStoreReadDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "read_duration",
		Help:      "Bucketed histogram of event store sorter iterator read duration",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
	}, []string{"type"})

	EventStoreNotifyDispatcherCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "notify_dispatcher_count",
			Help:      "The number of times event store notifies dispatchers with resolved ts.",
		})

	EventStoreNotifyDispatcherDurationHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "notify_dispatcher_duration",
			Help:      "The duration of notifying dispatchers with resolved ts.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 20), // 10us ~ 5.2s,
		})

	// EventStoreRegisterDispatcherStartTsLagHist is the histogram of startTs lag when registering a dispatcher.
	EventStoreRegisterDispatcherStartTsLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "register_dispatcher_start_ts_lag",
			Help:      "The lag of startTs when registering a dispatcher.",
			Buckets:   LagBucket(),
		})

	EventStoreWriteWorkerIODuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_worker_io_duration",
			Help:      "IO duration (s) for event store write worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"db", "worker"})

	EventStoreWriteWorkerTotalDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event_store",
			Name:      "write_worker_total_duration",
			Help:      "total duration (s) event store write worker.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
		}, []string{"db", "worker"})

	EventStoreWriteQueueDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "write_queue_duration",
		Help:      "duration (s) events wait in event store write queue before being processed",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
	})

	EventStoreWritePrepareDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "write_prepare_duration",
		Help:      "duration (s) for event store to build pebble write batch",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
	})

	EventStorePebbleIteratorGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_iterator_count",
		Help:      "The number of sstable iterators by event store pebble DB",
	}, []string{"id"})

	EventStorePebbleLevelFilesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_level_files",
		Help:      "The number of files in each level by event store pebble DB",
	}, []string{"id", "level"})

	EventStorePebbleBlockCacheAccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_block_cache_access_total",
		Help:      "The total number of event store pebble block cache access",
	}, []string{"id", "type"})

	EventStorePebbleWriteStallCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_write_stall_total",
		Help:      "The total number of event store pebble write stalls",
	}, []string{"id", "reason"})

	EventStorePebbleWriteStallDuration = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_write_stall_duration_seconds_total",
		Help:      "The total duration (s) of event store pebble write stalls",
	}, []string{"id", "reason"})

	EventStorePebbleCompactionDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_compaction_duration_seconds",
		Help:      "Bucketed histogram of event store pebble compaction duration (s)",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 20),
	}, []string{"id"})

	EventStorePebbleFlushDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_flush_duration_seconds",
		Help:      "Bucketed histogram of event store pebble flush duration (s)",
		Buckets:   prometheus.ExponentialBuckets(0.004, 2.0, 20),
	}, []string{"id"})

	EventStorePebbleCompactionDebtGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_compaction_debt_bytes",
		Help:      "The estimated compaction debt in bytes for event store pebble DB",
	}, []string{"id"})

	EventStorePebbleCompactionInProgressGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_compaction_in_progress",
		Help:      "The number of in progress compactions for event store pebble DB",
	}, []string{"id"})

	EventStorePebbleCompactionInProgressBytesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_compaction_in_progress_bytes",
		Help:      "The total bytes of in progress compactions for event store pebble DB",
	}, []string{"id"})

	EventStorePebbleFlushInProgressGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_flush_in_progress",
		Help:      "The number of in progress flushes for event store pebble DB",
	}, []string{"id"})

	EventStorePebbleReadAmpGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "event_store",
		Name:      "pebble_read_amplification",
		Help:      "The read amplification for event store pebble DB",
	}, []string{"id"})
)

func initEventStoreMetrics(registry *prometheus.Registry) {
	registry.MustRegister(EventStoreSubscriptionGauge)
	registry.MustRegister(EventStoreCompressedRowsCount)
	registry.MustRegister(EventStoreReceivedEventCount)
	registry.MustRegister(EventStoreOutputEventCount)
	registry.MustRegister(EventStoreWriteDurationHistogram)
	registry.MustRegister(EventStoreScanRequestsCount)
	registry.MustRegister(EventStoreScanBytes)
	registry.MustRegister(EventStoreDeleteRangeCount)
	registry.MustRegister(EventStoreDeleteRangeFetchedCount)
	registry.MustRegister(EventStoreSubscriptionResolvedTsLagHist)
	registry.MustRegister(EventStoreOnDiskDataSizeGauge)
	registry.MustRegister(EventStoreInMemoryDataSizeGauge)
	registry.MustRegister(EventStoreResolvedTsLagGauge)
	registry.MustRegister(EventStoreWriteBytes)
	registry.MustRegister(EventStoreSubscriptionDataGCLagHist)
	registry.MustRegister(EventStoreWriteBatchEventsCountHist)
	registry.MustRegister(EventStoreWriteBatchSizeHist)
	registry.MustRegister(EventStoreWriteRequestsCount)
	registry.MustRegister(EventStoreReadDurationHistogram)
	registry.MustRegister(EventStoreNotifyDispatcherCount)
	registry.MustRegister(EventStoreNotifyDispatcherDurationHist)
	registry.MustRegister(EventStoreRegisterDispatcherStartTsLagHist)
	registry.MustRegister(EventStoreWriteWorkerIODuration)
	registry.MustRegister(EventStoreWriteWorkerTotalDuration)
	registry.MustRegister(EventStoreWriteQueueDurationHistogram)
	registry.MustRegister(EventStoreWritePrepareDurationHistogram)
	registry.MustRegister(EventStorePebbleIteratorGauge)
	registry.MustRegister(EventStorePebbleLevelFilesGauge)
	registry.MustRegister(EventStorePebbleBlockCacheAccess)
	registry.MustRegister(EventStorePebbleWriteStallCount)
	registry.MustRegister(EventStorePebbleWriteStallDuration)
	registry.MustRegister(EventStorePebbleCompactionDurationHistogram)
	registry.MustRegister(EventStorePebbleFlushDurationHistogram)
	registry.MustRegister(EventStorePebbleCompactionDebtGauge)
	registry.MustRegister(EventStorePebbleCompactionInProgressGauge)
	registry.MustRegister(EventStorePebbleCompactionInProgressBytesGauge)
	registry.MustRegister(EventStorePebbleFlushInProgressGauge)
	registry.MustRegister(EventStorePebbleReadAmpGauge)
}
