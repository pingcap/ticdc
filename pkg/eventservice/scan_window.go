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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// defaultScanInterval is the initial scan interval used when starting up
	// or when the current interval is invalid.
	defaultScanInterval = 5 * time.Second

	// minScanInterval is the minimum allowed scan interval. Even under critical
	// memory pressure, the interval will never go below this value.
	minScanInterval = 1 * time.Second

	// maxScanInterval is the maximum allowed scan interval. Even under very low
	// memory pressure, the interval will never exceed this value.
	maxScanInterval = 30 * time.Minute

	// scanIntervalAdjustCooldown is the minimum time that must pass between
	// scan interval increases. This prevents oscillation by enforcing a waiting
	// period before allowing another increase. Decreases are not affected by
	// this cooldown and are applied immediately.
	scanIntervalAdjustCooldown = 30 * time.Second

	// scanTrendAdjustCooldown is the minimum time between trend-based interval
	// adjustments. This is shorter than the general cooldown because trend
	// adjustments need to be more responsive to rising memory pressure.
	scanTrendAdjustCooldown = 5 * time.Second

	// memoryUsageWindowDuration is the duration of the sliding window for
	// collecting memory usage samples. Samples older than this duration are
	// pruned from the window.
	memoryUsageWindowDuration = 30 * time.Second

	// memoryUsageHighThreshold (70%) triggers a moderate reduction of the scan
	// interval to 1/2 of its current value when memory usage exceeds this level.
	memoryUsageHighThreshold = 0.7

	// memoryUsageCriticalThreshold (90%) triggers an aggressive reduction of
	// the scan interval to 1/4 of its current value when memory usage exceeds
	// this level.
	memoryUsageCriticalThreshold = 0.9

	// memoryUsageLowThreshold (20%) allows the scan interval to be increased
	// by 25% when both max and average memory usage are below this level.
	memoryUsageLowThreshold = 0.2

	// memoryUsageVeryLowThreshold (10%) allows the scan interval to be increased
	// by 50% when both max and average memory usage are below this level. This
	// increase may exceed the normal sync point interval cap.
	memoryUsageVeryLowThreshold = 0.1

	// scanWindowStaleDispatcherHeartbeatThreshold is the duration after which a
	// dispatcher is treated as stale for scan window base ts calculation if it
	// hasn't sent heartbeat updates. This prevents stale dispatchers (for example,
	// after frequent table truncate) from blocking scan window advancement for the
	// whole changefeed.
	//
	// Note: This is intentionally much smaller than heartbeatTimeout, which is
	// used for actual dispatcher removal.
	scanWindowStaleDispatcherHeartbeatThreshold = 1 * time.Minute
)

type memoryUsageSample struct {
	ts    time.Time
	ratio float64
}

type memoryUsageWindow struct {
	window  time.Duration
	mu      sync.Mutex
	samples []memoryUsageSample
}

type memoryUsageStats struct {
	avg   float64
	max   float64
	first float64
	last  float64
	span  time.Duration
	cnt   int
}

func newMemoryUsageWindow(window time.Duration) *memoryUsageWindow {
	return &memoryUsageWindow{
		window: window,
	}
}

func (w *memoryUsageWindow) addSample(now time.Time, ratio float64) {
	if ratio < 0 {
		ratio = 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	w.samples = append(w.samples, memoryUsageSample{ts: now, ratio: ratio})
	w.pruneLocked(now)
}

func (w *memoryUsageWindow) stats(now time.Time) memoryUsageStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.pruneLocked(now)
	if len(w.samples) == 0 {
		return memoryUsageStats{}
	}

	sum := 0.0
	firstRatio := w.samples[0].ratio
	maxRatio := firstRatio
	for _, sample := range w.samples {
		sum += sample.ratio
		if sample.ratio > maxRatio {
			maxRatio = sample.ratio
		}
	}

	return memoryUsageStats{
		avg:   sum / float64(len(w.samples)),
		max:   maxRatio,
		first: firstRatio,
		last:  w.samples[len(w.samples)-1].ratio,
		span:  now.Sub(w.samples[0].ts),
		cnt:   len(w.samples),
	}
}

func (w *memoryUsageWindow) pruneLocked(now time.Time) {
	cutoff := now.Add(-w.window)
	idx := 0
	for idx < len(w.samples) && w.samples[idx].ts.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		w.samples = w.samples[idx:]
	}
}

func (c *changefeedStatus) updateMemoryUsage(now time.Time, used uint64, max uint64, available uint64) {
	if max == 0 || c.usageWindow == nil {
		return
	}

	pressure := float64(used) / float64(max)
	if pressure < 0 {
		pressure = 0
	}
	if pressure > 1 {
		pressure = 1
	}

	availableRatio := float64(available) / float64(max)
	if availableRatio < 0 {
		availableRatio = 0
	}
	if availableRatio > 1 {
		availableRatio = 1
	}
	pressureFromAvailable := 1 - availableRatio
	if pressureFromAvailable > pressure {
		pressure = pressureFromAvailable
	}

	c.usageWindow.addSample(now, pressure)
	stats := c.usageWindow.stats(now)
	c.adjustScanInterval(now, stats)
}

// adjustScanInterval dynamically adjusts the scan interval based on memory pressure.
//
// Algorithm overview:
//   - "Fast brake, slow accelerate": Decreases are applied immediately when memory
//     pressure is high, while increases require cooldown periods and stable conditions.
//   - Tiered response: Different thresholds trigger different adjustment magnitudes.
//   - Trend prediction: Detects rising memory pressure early and proactively reduces
//     the interval before hitting critical thresholds.
//
// Thresholds and actions:
//   - Critical (>90%): Reduce interval to 1/4 (aggressive)
//   - High (>70%): Reduce interval to 1/2
//   - Trend damping (>30% AND rising): Reduce interval by 10%
//   - Low (<30% max AND avg): Increase interval by 25%
//   - Very low (<10% max AND avg): Increase interval by 50%, may exceed normal cap
func (c *changefeedStatus) adjustScanInterval(now time.Time, usage memoryUsageStats) {
	current := time.Duration(c.scanInterval.Load())
	if current <= 0 {
		current = defaultScanInterval
	}
	maxInterval := c.maxScanInterval()
	if maxInterval < minScanInterval {
		maxInterval = minScanInterval
	}

	// Constants for trend detection and increase eligibility.
	const (
		minTrendSamples           = 4    // Minimum samples needed to detect a valid trend
		increasingTrendEpsilon    = 0.02 // Minimum delta to consider as "increasing"
		increasingTrendStartRatio = 0.3  // Threshold (30%) above which trend damping kicks in

		minIncreaseSamples         = 10 // Minimum samples needed before allowing increase
		minIncreaseSpanNumerator   = 4  // Observation span must be at least 4/5 of window
		minIncreaseSpanDenominator = 5
	)

	// Trend detection: check if memory usage is rising over the observation window.
	// This enables proactive intervention before hitting high thresholds.
	trendDelta := usage.last - usage.first
	isIncreasing := usage.cnt >= minTrendSamples && trendDelta > increasingTrendEpsilon
	isAboveTrendStart := usage.last > increasingTrendStartRatio
	canAdjustOnTrend := now.Sub(c.lastTrendAdjustTime.Load()) >= scanTrendAdjustCooldown
	shouldDampOnTrend := isAboveTrendStart && isIncreasing && canAdjustOnTrend

	// Increase eligibility: conservative conditions to prevent oscillation.
	// Requires: cooldown passed, enough samples, sufficient observation span,
	// and NOT in an increasing trend situation (to avoid fighting against pressure).
	minIncreaseSpan := memoryUsageWindowDuration * minIncreaseSpanNumerator / minIncreaseSpanDenominator
	allowedToIncrease := now.Sub(c.lastAdjustTime.Load()) >= scanIntervalAdjustCooldown &&
		usage.cnt >= minIncreaseSamples &&
		usage.span >= minIncreaseSpan &&
		!(isAboveTrendStart && isIncreasing)

	// Determine the new interval based on memory pressure levels.
	// Priority order: critical > high > trend damping > very low > low
	adjustedOnTrend := false
	newInterval := current
	switch {
	case usage.last > memoryUsageCriticalThreshold || usage.max > memoryUsageCriticalThreshold:
		// Critical pressure: aggressive reduction to 1/4
		newInterval = maxDuration(current/4, minScanInterval)
	case usage.last > memoryUsageHighThreshold || usage.max > memoryUsageHighThreshold:
		// High pressure: reduce to 1/2
		newInterval = maxDuration(current/2, minScanInterval)
	case shouldDampOnTrend:
		// Trend damping: pressure is moderate (>30%) but rising. Reduce by 10% to
		// preemptively slow down before downstream gets overwhelmed.
		newInterval = maxDuration(scaleDuration(current, 9, 10), minScanInterval)
		adjustedOnTrend = true
	case allowedToIncrease && usage.max < memoryUsageVeryLowThreshold && usage.avg < memoryUsageVeryLowThreshold:
		// Very low pressure (<20%): increase by 50%, allowed to exceed sync point cap.
		maxInterval = maxScanInterval
		newInterval = minDuration(scaleDuration(current, 3, 2), maxInterval)
	case allowedToIncrease && usage.max < memoryUsageLowThreshold && usage.avg < memoryUsageLowThreshold:
		// Low pressure (<40%): increase by 25%, capped by sync point interval.
		newInterval = minDuration(scaleDuration(current, 5, 4), maxInterval)
	}

	// Anti-oscillation guard: decreases are always applied immediately,
	// but increases are blocked if cooldown conditions aren't met.
	if newInterval > current && !allowedToIncrease {
		return
	}

	if newInterval != current {
		c.scanInterval.Store(int64(newInterval))
		metrics.EventServiceScanWindowIntervalGaugeVec.WithLabelValues(c.changefeedID.String()).Set(newInterval.Seconds())
		c.lastAdjustTime.Store(now)
		if adjustedOnTrend {
			c.lastTrendAdjustTime.Store(now)
		}
		log.Info("scan interval adjusted",
			zap.Stringer("changefeedID", c.changefeedID),
			zap.Duration("oldInterval", current),
			zap.Duration("newInterval", newInterval),
			zap.Duration("maxInterval", maxInterval),
			zap.Float64("avgUsage", usage.avg),
			zap.Float64("maxUsage", usage.max),
			zap.Float64("firstUsage", usage.first),
			zap.Float64("lastUsage", usage.last),
			zap.Float64("trendDelta", trendDelta),
			zap.Int("usageSamples", usage.cnt),
			zap.Bool("syncPointEnabled", c.syncPointEnabled.Load()),
		)
	}
}

func (c *changefeedStatus) maxScanInterval() time.Duration {
	if !c.syncPointEnabled.Load() {
		return maxScanInterval
	}
	interval := time.Duration(c.syncPointInterval.Load())
	if interval <= 0 {
		return maxScanInterval
	}
	if interval < maxScanInterval {
		return interval
	}
	return maxScanInterval
}

func (c *changefeedStatus) refreshMinSentResolvedTs() {
	now := time.Now()
	minSentResolvedTs := ^uint64(0)
	minSentResolvedTsWithStale := ^uint64(0)
	hasEligible := false
	hasNonStale := false
	c.dispatchers.Range(func(_ any, value any) bool {
		dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
		if dispatcher == nil || dispatcher.isRemoved.Load() || dispatcher.seq.Load() == 0 {
			return true
		}

		hasEligible = true
		sentResolvedTs := dispatcher.sentResolvedTs.Load()
		if sentResolvedTs < minSentResolvedTsWithStale {
			minSentResolvedTsWithStale = sentResolvedTs
		}

		lastHeartbeatTime := dispatcher.lastReceivedHeartbeatTime.Load()
		if lastHeartbeatTime > 0 &&
			now.Sub(time.Unix(lastHeartbeatTime, 0)) > scanWindowStaleDispatcherHeartbeatThreshold {
			log.Info("dispatcher is stale, skip it's sent resolved ts", zap.Stringer("changefeedID", c.changefeedID), zap.Stringer("dispatcherID", dispatcher.id))
			return true
		}

		hasNonStale = true
		if sentResolvedTs < minSentResolvedTs {
			minSentResolvedTs = sentResolvedTs
		}
		return true
	})

	if !hasEligible {
		c.storeMinSentTs(0)
		return
	}
	if !hasNonStale {
		c.storeMinSentTs(minSentResolvedTsWithStale)
		return
	}
	c.storeMinSentTs(minSentResolvedTs)
}

func (c *changefeedStatus) getScanMaxTs() uint64 {
	baseTs := c.minSentTs.Load()
	if baseTs == 0 {
		return 0
	}
	interval := time.Duration(c.scanInterval.Load())
	if interval <= 0 {
		interval = defaultScanInterval
	}

	return oracle.GoTimeToTS(oracle.GetTimeFromTS(baseTs).Add(interval))
}

func (c *changefeedStatus) storeMinSentTs(value uint64) {
	prev := c.minSentTs.Load()
	if prev == value {
		return
	}
	c.minSentTs.Store(value)
	metrics.EventServiceScanWindowBaseTsGaugeVec.WithLabelValues(c.changefeedID.String()).Set(float64(value))
}

func (c *changefeedStatus) updateSyncPointConfig(info DispatcherInfo) {
	if !info.SyncPointEnabled() {
		return
	}
	c.syncPointEnabled.Store(true)
	interval := info.GetSyncPointInterval()
	if interval <= 0 {
		return
	}
	for {
		current := time.Duration(c.syncPointInterval.Load())
		if current != 0 && interval >= current {
			return
		}
		if c.syncPointInterval.CompareAndSwap(int64(current), int64(interval)) {
			return
		}
	}
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func scaleDuration(d time.Duration, numerator int64, denominator int64) time.Duration {
	if numerator <= 0 || denominator <= 0 {
		return d
	}
	return time.Duration(int64(d) * numerator / denominator)
}
