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

package eventservice

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

var (
	metricEventServiceSendEventDuration   = metrics.EventServiceSendEventDuration.WithLabelValues("txn")
	metricEventBrokerScanTaskCount        = metrics.EventServiceScanTaskCount
	metricEventBrokerPendingScanTaskCount = metrics.EventServicePendingScanTaskCount
	metricEventStoreOutputKv              = metrics.EventStoreOutputEventCount.WithLabelValues("kv")
	metricEventStoreOutputResolved        = metrics.EventStoreOutputEventCount.WithLabelValues("resolved")

	metricEventServiceSendKvCount         = metrics.EventServiceSendEventCount.WithLabelValues("kv")
	metricEventServiceSendResolvedTsCount = metrics.EventServiceSendEventCount.WithLabelValues("resolved_ts")
	metricEventServiceSendDDLCount        = metrics.EventServiceSendEventCount.WithLabelValues("ddl")
	metricEventServiceSendCommandCount    = metrics.EventServiceSendEventCount.WithLabelValues("command")
	metricEventServiceSkipResolvedTsCount = metrics.EventServiceSkipResolvedTsCount
)

// metricsSnapshot holds all metrics data collected at a point in time
type metricsSnapshot struct {
	receivedMinResolvedTs  uint64
	sentMinResolvedTs      uint64
	dispatcherCount        int
	runningDispatcherCount int
	pausedDispatcherCount  int
	pendingTaskCount       int
	slowestDispatcher      *dispatcherStat
	pdTime                 time.Time
}

// metricsCollector is responsible for collecting and reporting metrics for the event broker
type metricsCollector struct {
	broker *eventBroker
	// Prometheus metrics
	metricDispatcherCount                   prometheus.Gauge
	metricEventServiceReceivedResolvedTsLag prometheus.Gauge
	metricEventServiceSentResolvedTsLag     prometheus.Gauge
}

// newMetricsCollector creates a new MetricsCollector instance
func newMetricsCollector(broker *eventBroker) *metricsCollector {
	return &metricsCollector{
		broker:                                  broker,
		metricDispatcherCount:                   metrics.EventServiceDispatcherGauge.WithLabelValues(strconv.FormatUint(broker.tidbClusterID, 10)),
		metricEventServiceReceivedResolvedTsLag: metrics.EventServiceResolvedTsLagGauge.WithLabelValues("received"),
		metricEventServiceSentResolvedTsLag:     metrics.EventServiceResolvedTsLagGauge.WithLabelValues("sent"),
	}
}

// Run starts the metrics collection loop
func (mc *metricsCollector) Run(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Info("metrics collector started")
	for {
		select {
		case <-ctx.Done():
			log.Info("metrics collector stopped")
			return context.Cause(ctx)
		case <-ticker.C:
			snapshot := mc.collectMetrics()
			mc.updateMetricsFromSnapshot(snapshot)
			mc.logSlowDispatchers(snapshot)
		}
	}
}

// collectMetrics gathers all metrics data from the event broker
func (mc *metricsCollector) collectMetrics() *metricsSnapshot {
	snapshot := &metricsSnapshot{
		receivedMinResolvedTs: uint64(math.MaxUint64),
		sentMinResolvedTs:     uint64(math.MaxUint64),
		pdTime:                mc.broker.pdClock.CurrentTime(),
	}

	mc.collectDispatcherMetrics(snapshot)
	mc.collectPendingTaskMetrics(snapshot)

	// If there are no dispatchers, use current time as resolved timestamps
	if snapshot.dispatcherCount == 0 {
		pdTSO := oracle.GoTimeToTS(snapshot.pdTime)
		snapshot.receivedMinResolvedTs = uint64(pdTSO)
		snapshot.sentMinResolvedTs = uint64(pdTSO)
	}

	return snapshot
}

// collectDispatcherMetrics collects metrics related to dispatchers
func (mc *metricsCollector) collectDispatcherMetrics(snapshot *metricsSnapshot) {
	mc.broker.dispatchers.Range(func(key, value any) bool {
		snapshot.dispatcherCount++
		dispatcher := value.(*dispatcherStat)

		if dispatcher.isRunning.Load() {
			snapshot.runningDispatcherCount++
		} else {
			snapshot.pausedDispatcherCount++
		}

		// Record update time difference
		updateDiff := dispatcher.lastReceivedResolvedTsTime.Load().Sub(dispatcher.lastSentResolvedTsTime.Load())
		metrics.EventServiceDispatcherUpdateResolvedTsDiff.Observe(updateDiff.Seconds())

		// Track min resolved timestamps
		resolvedTs := dispatcher.eventStoreResolvedTs.Load()
		if resolvedTs < snapshot.receivedMinResolvedTs {
			snapshot.receivedMinResolvedTs = resolvedTs
		}

		watermark := dispatcher.sentResolvedTs.Load()
		if watermark < snapshot.sentMinResolvedTs {
			snapshot.sentMinResolvedTs = watermark
		}

		// Track slowest dispatcher
		if snapshot.slowestDispatcher == nil || snapshot.slowestDispatcher.sentResolvedTs.Load() < watermark {
			snapshot.slowestDispatcher = dispatcher
		}

		return true
	})
}

// collectPendingTaskMetrics collects metrics about pending tasks
func (mc *metricsCollector) collectPendingTaskMetrics(snapshot *metricsSnapshot) {
	for _, ch := range mc.broker.taskChan {
		snapshot.pendingTaskCount += len(ch)
	}
}

// updateMetricsFromSnapshot updates all prometheus metrics based on the snapshot
func (mc *metricsCollector) updateMetricsFromSnapshot(snapshot *metricsSnapshot) {
	// Update lag metrics
	receivedLag := float64(oracle.GetPhysical(snapshot.pdTime)-oracle.ExtractPhysical(snapshot.receivedMinResolvedTs)) / 1e3
	mc.metricEventServiceReceivedResolvedTsLag.Set(receivedLag)

	sentLag := float64(oracle.GetPhysical(snapshot.pdTime)-oracle.ExtractPhysical(snapshot.sentMinResolvedTs)) / 1e3
	mc.metricEventServiceSentResolvedTsLag.Set(sentLag)

	// Update task count metrics
	metricEventBrokerPendingScanTaskCount.Set(float64(snapshot.pendingTaskCount))

	// Update dispatcher status metrics
	metrics.EventServiceDispatcherStatusCount.WithLabelValues("running").Set(float64(snapshot.runningDispatcherCount))
	metrics.EventServiceDispatcherStatusCount.WithLabelValues("paused").Set(float64(snapshot.pausedDispatcherCount))
	metrics.EventServiceDispatcherStatusCount.WithLabelValues("total").Set(float64(snapshot.dispatcherCount))
}

// logSlowDispatchers logs warnings for dispatchers that are too slow
func (mc *metricsCollector) logSlowDispatchers(snapshot *metricsSnapshot) {
	if snapshot.slowestDispatcher == nil {
		return
	}

	lag := time.Since(oracle.GetTimeFromTS(snapshot.slowestDispatcher.sentResolvedTs.Load()))
	if lag <= 30*time.Second {
		return
	}

	log.Warn("slowest dispatcher",
		zap.Stringer("dispatcherID", snapshot.slowestDispatcher.id),
		zap.Uint64("sentResolvedTs", snapshot.slowestDispatcher.sentResolvedTs.Load()),
		zap.Uint64("receivedResolvedTs", snapshot.slowestDispatcher.eventStoreResolvedTs.Load()),
		zap.Duration("lag", lag),
		zap.Duration("updateDiff",
			time.Since(snapshot.slowestDispatcher.lastReceivedResolvedTsTime.Load())-
				time.Since(snapshot.slowestDispatcher.lastSentResolvedTsTime.Load())),
		zap.Bool("isPaused", !snapshot.slowestDispatcher.isRunning.Load()),
		zap.Bool("isHandshaked", snapshot.slowestDispatcher.isHandshaked.Load()),
		zap.Bool("isTaskScanning", snapshot.slowestDispatcher.isTaskScanning.Load()),
	)
}
