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
	defaultScanInterval          = 5 * time.Second
	minScanInterval              = 1 * time.Second
	maxScanInterval              = 30 * time.Minute
	scanIntervalAdjustCooldown   = 30 * time.Second
	scanTrendAdjustCooldown      = 5 * time.Second
	memoryUsageWindowDuration    = 30 * time.Second
	memoryUsageHighThreshold     = 0.7
	memoryUsageCriticalThreshold = 0.9
	memoryUsageLowThreshold      = 0.2
	memoryUsageVeryLowThreshold  = 0.1
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

func (w *memoryUsageWindow) average(now time.Time) float64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.pruneLocked(now)
	if len(w.samples) == 0 {
		return 0
	}
	var sum float64
	for _, sample := range w.samples {
		sum += sample.ratio
	}
	return sum / float64(len(w.samples))
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

func (w *memoryUsageWindow) span(now time.Time) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneLocked(now)
	if len(w.samples) == 0 {
		return 0
	}
	return now.Sub(w.samples[0].ts)
}

func (w *memoryUsageWindow) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.samples)
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

func (c *changefeedStatus) adjustScanInterval(now time.Time, usage memoryUsageStats) {
	current := time.Duration(c.scanInterval.Load())
	if current <= 0 {
		current = defaultScanInterval
	}
	maxInterval := c.maxScanInterval()
	if maxInterval < minScanInterval {
		maxInterval = minScanInterval
	}

	const (
		minTrendSamples           = 4
		increasingTrendEpsilon    = 0.02
		increasingTrendStartRatio = 0.3
	)

	trendDelta := usage.last - usage.first
	isIncreasing := usage.cnt >= minTrendSamples && trendDelta > increasingTrendEpsilon
	isAboveTrendStart := usage.last > increasingTrendStartRatio
	canAdjustOnTrend := now.Sub(c.lastTrendAdjustTime.Load()) >= scanTrendAdjustCooldown
	shouldDampOnTrend := isAboveTrendStart && isIncreasing && canAdjustOnTrend

	allowedToIncrease := now.Sub(c.lastAdjustTime.Load()) >= scanIntervalAdjustCooldown &&
		usage.cnt > 0 &&
		usage.span >= memoryUsageWindowDuration &&
		!(isAboveTrendStart && isIncreasing)

	adjustedOnTrend := false
	newInterval := current
	switch {
	case usage.last > memoryUsageCriticalThreshold || usage.max > memoryUsageCriticalThreshold:
		newInterval = maxDuration(current/4, minScanInterval)
	case usage.last > memoryUsageHighThreshold || usage.max > memoryUsageHighThreshold:
		newInterval = maxDuration(current/2, minScanInterval)
	case shouldDampOnTrend:
		// When pressure is above a safe level and still increasing, it usually indicates
		// downstream can't keep up. Decrease scan interval gradually to avoid quota exhaustion.
		newInterval = maxDuration(scaleDuration(current, 9, 10), minScanInterval)
		adjustedOnTrend = true
	case allowedToIncrease && usage.max < memoryUsageVeryLowThreshold && usage.avg < memoryUsageVeryLowThreshold:
		// When memory pressure stays very low for a full window, allow the scan interval
		// to grow beyond the sync point interval cap, but increase slowly to avoid burst.
		maxInterval = maxScanInterval
		newInterval = minDuration(scaleDuration(current, 3, 2), maxInterval)
	case allowedToIncrease && usage.max < memoryUsageLowThreshold && usage.avg < memoryUsageLowThreshold:
		newInterval = minDuration(scaleDuration(current, 5, 4), maxInterval)
	}

	// Prevent rapid oscillation: always apply decreases immediately, but throttle increases.
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
	minSentResolvedTs := ^uint64(0)
	c.dispatchers.Range(func(_ any, value any) bool {
		dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
		if dispatcher == nil || dispatcher.isRemoved.Load() || dispatcher.seq.Load() == 0 {
			return true
		}
		sentResolvedTs := dispatcher.sentResolvedTs.Load()
		if sentResolvedTs < minSentResolvedTs {
			minSentResolvedTs = sentResolvedTs
		}
		return true
	})

	if minSentResolvedTs == ^uint64(0) {
		c.storeMinSentTs(0)
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
	log.Info("scan window base updated",
		zap.Stringer("changefeedID", c.changefeedID),
		zap.Uint64("oldBaseTs", prev),
		zap.Uint64("newBaseTs", value),
	)
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
