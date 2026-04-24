// Copyright 2026 PingCAP, Inc.
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

	// memoryUsageWindowDuration is the duration of the sliding window for
	// collecting memory usage samples. Samples older than this duration are
	// pruned from the window.
	memoryUsageWindowDuration = 30 * time.Second

	// memoryUsageHighThreshold (70%) triggers a moderate reduction of the scan
	// interval to 1/2 of its current value when memory usage exceeds this level.
	memoryUsageHighThreshold = 0.7

	// memoryUsageCriticalThreshold (90%) triggers an aggressive reduction of
	// the scan interval once memory usage exceeds this level.
	memoryUsageCriticalThreshold = 0.9

	// memoryUsageEmergencyThreshold (98%) triggers the strongest emergency brake.
	memoryUsageEmergencyThreshold = 0.98

	// memoryUsageLowThreshold (20%) allows the scan interval to be increased
	// by 25% when both max and average memory usage are below this level.
	memoryUsageLowThreshold = 0.2

	// memoryUsageVeryLowThreshold (10%) allows the scan interval to be increased
	// by 50% when both max and average memory usage are below this level. This
	// increase may exceed the normal sync point interval cap.
	memoryUsageVeryLowThreshold = 0.1

	// scanWindowModeratePressureThreshold is the smoothed usage threshold that
	// starts accumulating pressure score for gradual interval reductions.
	scanWindowModeratePressureThreshold = 0.55

	// scanWindowHighPressureThreshold triggers a stronger but still bounded
	// interval reduction when sustained high pressure is observed.
	scanWindowHighPressureThreshold = 0.75

	// scanWindowPressureAdjustCooldown is the minimum time between non-critical
	// downward adjustments. It prevents the controller from overreacting before
	// previous interval changes have time to take effect.
	scanWindowPressureAdjustCooldown = 10 * time.Second

	// scanWindowReleaseRecoveryCooldown is the minimum time after a downward
	// adjustment before the controller is allowed to recover upward again.
	scanWindowReleaseRecoveryCooldown = 15 * time.Second

	// scanWindowFastUsageAlpha controls the responsiveness of the short-term EMA.
	scanWindowFastUsageAlpha = 0.4

	// scanWindowSlowUsageAlpha controls the responsiveness of the long-term EMA.
	scanWindowSlowUsageAlpha = 0.2

	// scanWindowPressureTriggerScore is the score required to trigger a gradual
	// downward adjustment under sustained but non-critical pressure.
	scanWindowPressureTriggerScore = 3.0

	// scanWindowPressureScoreCeiling bounds the pressure accumulator.
	scanWindowPressureScoreCeiling = 8.0

	// scanWindowPressureReliefPerRelease is the amount of accumulated pressure
	// cleared by one downstream release pulse.
	scanWindowPressureReliefPerRelease = 2.0

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

type scanWindowReport struct {
	usageRatio         float64
	memoryReleaseCount uint32
}

type scanWindowDecisionReason string

const (
	scanWindowDecisionNone              scanWindowDecisionReason = "none"
	scanWindowDecisionCriticalBrake     scanWindowDecisionReason = "critical_brake"
	scanWindowDecisionHighPressure      scanWindowDecisionReason = "high_pressure"
	scanWindowDecisionSustainedPressure scanWindowDecisionReason = "sustained_pressure"
	scanWindowDecisionLowRecovery       scanWindowDecisionReason = "low_recovery"
	scanWindowDecisionVeryLowRecovery   scanWindowDecisionReason = "very_low_recovery"
)

type scanWindowDecision struct {
	newInterval   time.Duration
	maxInterval   time.Duration
	reason        scanWindowDecisionReason
	usage         memoryUsageStats
	fastUsageEMA  float64
	slowUsageEMA  float64
	pressureScore float64
}

type scanWindowController interface {
	OnCongestionReport(now time.Time, currentInterval time.Duration, maxInterval time.Duration, report scanWindowReport) scanWindowDecision
}

type adaptiveScanWindowController struct {
	mu sync.Mutex

	usageWindow *memoryUsageWindow

	lastAdjustTime     time.Time
	lastDownAdjustTime time.Time

	fastUsageEMA   float64
	slowUsageEMA   float64
	emaInitialized bool

	pressureScore float64
}

func newMemoryUsageWindow(window time.Duration) *memoryUsageWindow {
	return &memoryUsageWindow{
		window: window,
	}
}

func newAdaptiveScanWindowController(now time.Time) *adaptiveScanWindowController {
	return &adaptiveScanWindowController{
		usageWindow:        newMemoryUsageWindow(memoryUsageWindowDuration),
		lastAdjustTime:     now,
		lastDownAdjustTime: now,
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

func (w *memoryUsageWindow) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.samples = nil
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

func (c *changefeedStatus) updateMemoryUsage(now time.Time, usageRatio float64, memoryReleaseCount uint32) {
	if c.scanWindowController == nil {
		return
	}

	current := time.Duration(c.scanInterval.Load())
	decision := c.scanWindowController.OnCongestionReport(now, current, c.maxScanInterval(), scanWindowReport{
		usageRatio:         normalizeUsageRatio(usageRatio),
		memoryReleaseCount: memoryReleaseCount,
	})
	if decision.newInterval == current {
		return
	}

	c.scanInterval.Store(int64(decision.newInterval))
	metrics.EventServiceScanWindowIntervalGaugeVec.WithLabelValues(c.changefeedID.String()).Set(decision.newInterval.Seconds())

	log.Info("scan interval adjusted",
		zap.Stringer("changefeedID", c.changefeedID),
		zap.String("reason", string(decision.reason)),
		zap.Duration("oldInterval", current),
		zap.Duration("newInterval", decision.newInterval),
		zap.Duration("maxInterval", decision.maxInterval),
		zap.Float64("avgUsage", decision.usage.avg),
		zap.Float64("maxUsage", decision.usage.max),
		zap.Float64("firstUsage", decision.usage.first),
		zap.Float64("lastUsage", decision.usage.last),
		zap.Float64("fastUsageEMA", decision.fastUsageEMA),
		zap.Float64("slowUsageEMA", decision.slowUsageEMA),
		zap.Float64("pressureScore", decision.pressureScore),
		zap.Uint32("memoryReleaseCount", memoryReleaseCount),
		zap.Bool("syncPointEnabled", c.isSyncpointEnabled()),
		zap.Duration("syncPointInterval", c.syncPointInterval))
}

const (
	minIncreaseSamples         = 10 // Minimum samples needed before allowing increase
	minIncreaseSpanNumerator   = 4  // Observation span must be at least 4/5 of window
	minIncreaseSpanDenominator = 5
)

func (c *adaptiveScanWindowController) OnCongestionReport(now time.Time, current time.Duration, maxInterval time.Duration, report scanWindowReport) scanWindowDecision {
	c.mu.Lock()
	defer c.mu.Unlock()

	if current <= 0 {
		current = defaultScanInterval
	}
	if maxInterval < minScanInterval {
		maxInterval = minScanInterval
	}

	c.usageWindow.addSample(now, report.usageRatio)
	usage := c.usageWindow.stats(now)
	c.updateUsageEMALocked(report.usageRatio)

	if usage.last > memoryUsageEmergencyThreshold || usage.max > memoryUsageEmergencyThreshold {
		newInterval := max(current/4, minScanInterval)
		c.noteAdjustmentLocked(now, true)
		return scanWindowDecision{
			newInterval:   newInterval,
			maxInterval:   maxInterval,
			reason:        scanWindowDecisionCriticalBrake,
			usage:         usage,
			fastUsageEMA:  c.fastUsageEMA,
			slowUsageEMA:  c.slowUsageEMA,
			pressureScore: c.pressureScore,
		}
	}

	if usage.last > memoryUsageCriticalThreshold || usage.max > memoryUsageCriticalThreshold {
		newInterval := max(current/2, minScanInterval)
		c.noteAdjustmentLocked(now, true)
		return scanWindowDecision{
			newInterval:   newInterval,
			maxInterval:   maxInterval,
			reason:        scanWindowDecisionCriticalBrake,
			usage:         usage,
			fastUsageEMA:  c.fastUsageEMA,
			slowUsageEMA:  c.slowUsageEMA,
			pressureScore: c.pressureScore,
		}
	}

	c.updatePressureScoreLocked(usage)
	if report.memoryReleaseCount > 0 {
		c.relievePressureLocked(report.memoryReleaseCount)
	}

	if c.shouldReduceForHighPressureLocked(now, usage) {
		newInterval := max(scaleDuration(current, 3, 4), defaultScanInterval)
		c.noteAdjustmentLocked(now, true)
		return scanWindowDecision{
			newInterval:   newInterval,
			maxInterval:   maxInterval,
			reason:        scanWindowDecisionHighPressure,
			usage:         usage,
			fastUsageEMA:  c.fastUsageEMA,
			slowUsageEMA:  c.slowUsageEMA,
			pressureScore: c.pressureScore,
		}
	}

	if c.shouldReduceForSustainedPressureLocked(now, usage) {
		newInterval := max(scaleDuration(current, 9, 10), defaultScanInterval)
		c.noteAdjustmentLocked(now, true)
		return scanWindowDecision{
			newInterval:   newInterval,
			maxInterval:   maxInterval,
			reason:        scanWindowDecisionSustainedPressure,
			usage:         usage,
			fastUsageEMA:  c.fastUsageEMA,
			slowUsageEMA:  c.slowUsageEMA,
			pressureScore: c.pressureScore,
		}
	}

	if !c.allowedToIncreaseLocked(now, usage) {
		return scanWindowDecision{
			newInterval:   current,
			maxInterval:   maxInterval,
			reason:        scanWindowDecisionNone,
			usage:         usage,
			fastUsageEMA:  c.fastUsageEMA,
			slowUsageEMA:  c.slowUsageEMA,
			pressureScore: c.pressureScore,
		}
	}

	if c.isVeryLowPressureLocked(usage) {
		effectiveMaxInterval := maxScanInterval
		newInterval := min(scaleDuration(current, 3, 2), effectiveMaxInterval)
		if newInterval > current {
			c.noteAdjustmentLocked(now, false)
			return scanWindowDecision{
				newInterval:   newInterval,
				maxInterval:   effectiveMaxInterval,
				reason:        scanWindowDecisionVeryLowRecovery,
				usage:         usage,
				fastUsageEMA:  c.fastUsageEMA,
				slowUsageEMA:  c.slowUsageEMA,
				pressureScore: c.pressureScore,
			}
		}
	}

	if current < maxInterval && c.isLowPressureLocked(usage) {
		newInterval := min(scaleDuration(current, 5, 4), maxInterval)
		if newInterval > current {
			c.noteAdjustmentLocked(now, false)
			return scanWindowDecision{
				newInterval:   newInterval,
				maxInterval:   maxInterval,
				reason:        scanWindowDecisionLowRecovery,
				usage:         usage,
				fastUsageEMA:  c.fastUsageEMA,
				slowUsageEMA:  c.slowUsageEMA,
				pressureScore: c.pressureScore,
			}
		}
	}

	return scanWindowDecision{
		newInterval:   current,
		maxInterval:   maxInterval,
		reason:        scanWindowDecisionNone,
		usage:         usage,
		fastUsageEMA:  c.fastUsageEMA,
		slowUsageEMA:  c.slowUsageEMA,
		pressureScore: c.pressureScore,
	}
}

func (c *adaptiveScanWindowController) updateUsageEMALocked(value float64) {
	if !c.emaInitialized {
		c.fastUsageEMA = value
		c.slowUsageEMA = value
		c.emaInitialized = true
		return
	}
	c.fastUsageEMA = ema(c.fastUsageEMA, value, scanWindowFastUsageAlpha)
	c.slowUsageEMA = ema(c.slowUsageEMA, value, scanWindowSlowUsageAlpha)
}

func (c *adaptiveScanWindowController) updatePressureScoreLocked(usage memoryUsageStats) {
	switch {
	case c.fastUsageEMA >= scanWindowHighPressureThreshold ||
		c.slowUsageEMA >= scanWindowHighPressureThreshold ||
		usage.max >= memoryUsageHighThreshold:
		c.pressureScore = min(c.pressureScore+2, scanWindowPressureScoreCeiling)
	case c.fastUsageEMA >= scanWindowModeratePressureThreshold ||
		c.slowUsageEMA >= scanWindowModeratePressureThreshold ||
		usage.avg >= scanWindowModeratePressureThreshold:
		c.pressureScore = min(c.pressureScore+1, scanWindowPressureScoreCeiling)
	case c.fastUsageEMA < 0.30 && c.slowUsageEMA < 0.25 && usage.last < 0.30:
		c.pressureScore = maxFloat64(0, c.pressureScore-1.5)
	default:
		c.pressureScore = maxFloat64(0, c.pressureScore-0.5)
	}
}

func (c *adaptiveScanWindowController) relievePressureLocked(memoryReleaseCount uint32) {
	relief := min(float64(memoryReleaseCount)*scanWindowPressureReliefPerRelease, scanWindowPressureScoreCeiling)
	c.pressureScore = maxFloat64(0, c.pressureScore-relief)
}

func (c *adaptiveScanWindowController) shouldReduceForHighPressureLocked(now time.Time, usage memoryUsageStats) bool {
	if now.Sub(c.lastDownAdjustTime) < scanWindowPressureAdjustCooldown {
		return false
	}

	return c.fastUsageEMA >= scanWindowHighPressureThreshold ||
		c.slowUsageEMA >= scanWindowHighPressureThreshold ||
		usage.max >= memoryUsageHighThreshold
}

func (c *adaptiveScanWindowController) shouldReduceForSustainedPressureLocked(now time.Time, usage memoryUsageStats) bool {
	if now.Sub(c.lastDownAdjustTime) < scanWindowPressureAdjustCooldown {
		return false
	}
	if c.pressureScore < scanWindowPressureTriggerScore {
		return false
	}
	return c.fastUsageEMA >= scanWindowModeratePressureThreshold ||
		c.slowUsageEMA >= scanWindowModeratePressureThreshold ||
		usage.avg >= scanWindowModeratePressureThreshold
}

func (c *adaptiveScanWindowController) allowedToIncreaseLocked(now time.Time, usage memoryUsageStats) bool {
	minIncreaseSpan := memoryUsageWindowDuration * minIncreaseSpanNumerator / minIncreaseSpanDenominator
	return now.Sub(c.lastAdjustTime) >= scanIntervalAdjustCooldown &&
		now.Sub(c.lastDownAdjustTime) >= scanWindowReleaseRecoveryCooldown &&
		usage.cnt >= minIncreaseSamples &&
		usage.span >= minIncreaseSpan &&
		c.pressureScore < 1
}

func (c *adaptiveScanWindowController) isVeryLowPressureLocked(usage memoryUsageStats) bool {
	return usage.max < memoryUsageVeryLowThreshold &&
		usage.avg < memoryUsageVeryLowThreshold &&
		c.fastUsageEMA < memoryUsageVeryLowThreshold &&
		c.slowUsageEMA < memoryUsageVeryLowThreshold
}

func (c *adaptiveScanWindowController) isLowPressureLocked(usage memoryUsageStats) bool {
	return usage.max < memoryUsageLowThreshold &&
		usage.avg < memoryUsageLowThreshold &&
		c.fastUsageEMA < memoryUsageLowThreshold+0.03 &&
		c.slowUsageEMA < memoryUsageLowThreshold+0.02
}

func (c *adaptiveScanWindowController) noteAdjustmentLocked(now time.Time, downward bool) {
	c.lastAdjustTime = now
	if downward {
		c.lastDownAdjustTime = now
	}
}

func (c *adaptiveScanWindowController) setLastAdjustTimeForTest(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastAdjustTime = now
}

func (c *adaptiveScanWindowController) setLastDownAdjustTimeForTest(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastDownAdjustTime = now
}

func (c *adaptiveScanWindowController) setPressureScoreForTest(score float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pressureScore = score
}

func (c *adaptiveScanWindowController) resetForTest(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.usageWindow.reset()
	c.lastAdjustTime = now
	c.lastDownAdjustTime = now
	c.fastUsageEMA = 0
	c.slowUsageEMA = 0
	c.emaInitialized = false
	c.pressureScore = 0
}

func normalizeUsageRatio(usageRatio float64) float64 {
	if usageRatio != usageRatio || usageRatio < 0 {
		return 0
	}
	if usageRatio > 1 {
		return 1
	}
	return usageRatio
}

func ema(previous float64, value float64, alpha float64) float64 {
	return previous + alpha*(value-previous)
}

func maxFloat64(a float64, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func (c *changefeedStatus) maxScanInterval() time.Duration {
	if !c.isSyncpointEnabled() {
		return maxScanInterval
	}

	interval := c.syncPointInterval
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

func scaleDuration(d time.Duration, numerator int64, denominator int64) time.Duration {
	if numerator <= 0 || denominator <= 0 {
		return d
	}
	return time.Duration(int64(d) * numerator / denominator)
}
