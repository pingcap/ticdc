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
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	metricEventServiceSendEventDuration   = metrics.EventServiceSendEventDuration.WithLabelValues("txn")
	metricEventBrokerScanTaskCount        = metrics.EventServiceScanTaskCount
	metricEventBrokerPendingScanTaskCount = metrics.EventServicePendingScanTaskCount
	metricEventStoreOutputKv              = metrics.EventStoreOutputEventCount.WithLabelValues("kv", "default")
	metricEventStoreOutputResolved        = metrics.EventStoreOutputEventCount.WithLabelValues("resolved", "default")
	metricEventServiceSendKvCount         = metrics.EventServiceSendEventCount.WithLabelValues("kv", "default")
	metricEventServiceSendResolvedTsCount = metrics.EventServiceSendEventCount.WithLabelValues("resolved_ts", "default")
	metricEventServiceSendDDLCount        = metrics.EventServiceSendEventCount.WithLabelValues("ddl", "default")
	metricEventServiceSendCommandCount    = metrics.EventServiceSendEventCount.WithLabelValues("command", "default")
	metricEventServiceSkipResolvedTsCount = metrics.EventServiceSkipResolvedTsCount.WithLabelValues("default")

	metricRedoEventStoreOutputKv              = metrics.EventStoreOutputEventCount.WithLabelValues("kv", "redo")
	metricRedoEventStoreOutputResolved        = metrics.EventStoreOutputEventCount.WithLabelValues("resolved", "redo")
	metricRedoEventServiceSendKvCount         = metrics.EventServiceSendEventCount.WithLabelValues("kv", "redo")
	metricRedoEventServiceSendResolvedTsCount = metrics.EventServiceSendEventCount.WithLabelValues("resolved_ts", "redo")
	metricRedoEventServiceSendDDLCount        = metrics.EventServiceSendEventCount.WithLabelValues("ddl", "redo")
	metricRedoEventServiceSendCommandCount    = metrics.EventServiceSendEventCount.WithLabelValues("command", "redo")
	metricRedoEventServiceSkipResolvedTsCount = metrics.EventServiceSkipResolvedTsCount.WithLabelValues("redo")
)

func updateCounter(mode int64, defaultCounter, redoCounter prometheus.Counter) {
	if common.IsDefaultMode(mode) {
		defaultCounter.Inc()
	} else {
		redoCounter.Inc()
	}
}

func updateCounterWithValue(mode int64, defaultCounter, redoCounter prometheus.Counter, val float64) {
	if common.IsDefaultMode(mode) {
		defaultCounter.Add(val)
	} else {
		redoCounter.Add(val)
	}
}

func updateMetricEventStoreOutputKv(mode int64, val float64) {
	updateCounterWithValue(mode, metricEventStoreOutputKv, metricRedoEventStoreOutputKv, val)
}

func updateMetricEventStoreOutputResolved(mode int64) {
	updateCounter(mode, metricEventStoreOutputResolved, metricRedoEventStoreOutputResolved)
}

func updateMetricEventServiceSendKvCount(mode int64, val float64) {
	updateCounterWithValue(mode, metricEventServiceSendKvCount, metricRedoEventServiceSendKvCount, val)
}

func updateMetricEventServiceSendResolvedTsCount(mode int64) {
	updateCounter(mode, metricEventServiceSendResolvedTsCount, metricRedoEventServiceSendResolvedTsCount)
}

func updateMetricEventServiceSendDDLCount(mode int64) {
	updateCounter(mode, metricEventServiceSendDDLCount, metricRedoEventServiceSendDDLCount)
}

func updateMetricEventServiceSendCommandCount(mode int64) {
	updateCounter(mode, metricEventServiceSendCommandCount, metricRedoEventServiceSendCommandCount)
}

func updateMetricEventServiceSkipResolvedTsCount(mode int64) {
	updateCounter(mode, metricEventServiceSkipResolvedTsCount, metricRedoEventServiceSkipResolvedTsCount)
}

// metricsSnapshot holds all metrics data collected at a point in time
type metricsSnapshot struct {
	receivedMinResolvedTs uint64
	sentMinResolvedTs     uint64
	dispatcherCount       int
	pendingTaskCount      int
	slowestDispatcher     *dispatcherStat
	pdTime                time.Time
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
	// note: this ticker cannot be frequent,
	// otherwise it may influence the performance of data sync
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	// need a more frequent ticker to report changefeed metrics to log coordinator for accurate metrics
	// and the frequency is ok because the reporting doesn't hold any lock which may influence data sync.
	reportTicker := time.NewTicker(1 * time.Second)
	defer reportTicker.Stop()

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
		case <-reportTicker.C:
			mc.reportChangefeedStatesToLogCoordinator()
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
		snapshot.receivedMinResolvedTs = pdTSO
		snapshot.sentMinResolvedTs = pdTSO
	}

	return snapshot
}

// collectDispatcherMetrics collects metrics related to dispatchers
func (mc *metricsCollector) collectDispatcherMetrics(snapshot *metricsSnapshot) {
	collect := func(dispatcher *dispatcherStat) {
		// Record update time difference
		updateDiff := dispatcher.lastReceivedResolvedTsTime.Load().Sub(dispatcher.lastSentResolvedTsTime.Load())
		metrics.EventServiceDispatcherUpdateResolvedTsDiff.Observe(updateDiff.Seconds())

		// Track min resolved timestamps
		resolvedTs := dispatcher.receivedResolvedTs.Load()
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
	}

	mc.broker.dispatchers.Range(func(key, value any) bool {
		snapshot.dispatcherCount++
		dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
		collect(dispatcher)
		return true
	})
	mc.broker.tableTriggerDispatchers.Range(func(key, value any) bool {
		snapshot.dispatcherCount++
		dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
		collect(dispatcher)
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
		zap.Uint64("receivedResolvedTs", snapshot.slowestDispatcher.receivedResolvedTs.Load()),
		zap.Duration("lag", lag),
		zap.Duration("updateDiff",
			time.Since(snapshot.slowestDispatcher.lastSentResolvedTsTime.Load())-
				time.Since(snapshot.slowestDispatcher.lastReceivedResolvedTsTime.Load())),
		zap.Uint64("epoch", snapshot.slowestDispatcher.epoch),
		zap.Uint64("seq", snapshot.slowestDispatcher.seq.Load()),
		zap.Bool("isTaskScanning", snapshot.slowestDispatcher.isTaskScanning.Load()),
	)
}

// reportChangefeedStatesToLogCoordinator collects and reports the state of all changefeeds to the log coordinator.
func (mc *metricsCollector) reportChangefeedStatesToLogCoordinator() {
	var states []*logservicepb.ChangefeedStateEntry
	mc.broker.changefeedMap.Range(func(key, value any) bool {
		cfStatus := value.(*changefeedStatus)
		minResolvedTs := uint64(math.MaxUint64)
		cfStatus.dispatchers.Range(func(key, value any) bool {
			dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
			resolvedTs := dispatcher.receivedResolvedTs.Load()
			if resolvedTs < minResolvedTs {
				minResolvedTs = resolvedTs
			}
			return true
		})
		states = append(states, &logservicepb.ChangefeedStateEntry{
			ChangefeedID: cfStatus.changefeedID.ToPB(),
			ResolvedTs:   minResolvedTs,
		})
		return true
	})
	coordinatorID := mc.broker.eventStore.GetLogCoordinatorNodeID()
	if coordinatorID != "" {
		msg := messaging.NewSingleTargetMessage(coordinatorID, messaging.LogCoordinatorTopic, &logservicepb.ChangefeedStates{States: states})
		if err := mc.broker.msgSender.SendEvent(msg); err != nil {
			log.Warn("send changefeed metrics to coordinator failed", zap.Error(err))
		}
	}
}
