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
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	legacyScanTrendAdjustCooldown = 5 * time.Second
	legacyMinTrendSamples         = 4
	legacyIncreasingTrendEpsilon  = 0.02
	legacyTrendStartRatio         = 0.3
)

type legacyScanWindowController struct {
	usageWindow         *memoryUsageWindow
	lastAdjustTime      time.Time
	lastTrendAdjustTime time.Time
}

func newLegacyScanWindowController(now time.Time) *legacyScanWindowController {
	return &legacyScanWindowController{
		usageWindow:         newMemoryUsageWindow(memoryUsageWindowDuration),
		lastAdjustTime:      now,
		lastTrendAdjustTime: now,
	}
}

func (c *legacyScanWindowController) OnCongestionReport(now time.Time, current time.Duration, maxInterval time.Duration, report scanWindowReport) scanWindowDecision {
	if current <= 0 {
		current = defaultScanInterval
	}
	if maxInterval < minScanInterval {
		maxInterval = minScanInterval
	}

	c.usageWindow.addSample(now, normalizeUsageRatio(report.usageRatio))
	usage := c.usageWindow.stats(now)

	if report.memoryReleaseCount > 0 {
		c.usageWindow.reset()
		c.usageWindow.addSample(now, normalizeUsageRatio(report.usageRatio))
		c.lastAdjustTime = now
		c.lastTrendAdjustTime = now
		return scanWindowDecision{
			newInterval: defaultScanInterval,
			maxInterval: maxInterval,
			reason:      scanWindowDecisionVeryLowRecovery,
			usage:       usage,
		}
	}

	trendDelta := usage.last - usage.first
	isIncreasing := usage.cnt >= legacyMinTrendSamples && trendDelta > legacyIncreasingTrendEpsilon
	isAboveTrendStart := usage.last > legacyTrendStartRatio
	canAdjustOnTrend := now.Sub(c.lastTrendAdjustTime) >= legacyScanTrendAdjustCooldown
	shouldDampOnTrend := isAboveTrendStart && isIncreasing && canAdjustOnTrend

	minIncreaseSpan := memoryUsageWindowDuration * minIncreaseSpanNumerator / minIncreaseSpanDenominator
	allowedToIncrease := now.Sub(c.lastAdjustTime) >= scanIntervalAdjustCooldown &&
		usage.cnt >= minIncreaseSamples &&
		usage.span >= minIncreaseSpan &&
		!(isAboveTrendStart && isIncreasing)

	newInterval := current
	effectiveMaxInterval := maxInterval
	reason := scanWindowDecisionNone

	switch {
	case usage.last > memoryUsageCriticalThreshold || usage.max > memoryUsageCriticalThreshold:
		newInterval = max(current/4, minScanInterval)
		reason = scanWindowDecisionCriticalBrake
	case usage.last > memoryUsageHighThreshold || usage.max > memoryUsageHighThreshold:
		newInterval = max(current/2, minScanInterval)
		reason = scanWindowDecisionHighPressure
	case shouldDampOnTrend:
		newInterval = max(scaleDuration(current, 9, 10), minScanInterval)
		reason = scanWindowDecisionSustainedPressure
	case allowedToIncrease && usage.max < memoryUsageVeryLowThreshold && usage.avg < memoryUsageVeryLowThreshold:
		effectiveMaxInterval = maxScanInterval
		newInterval = min(scaleDuration(current, 3, 2), effectiveMaxInterval)
		reason = scanWindowDecisionVeryLowRecovery
	case allowedToIncrease && usage.max < memoryUsageLowThreshold && usage.avg < memoryUsageLowThreshold:
		newInterval = min(scaleDuration(current, 5, 4), effectiveMaxInterval)
		reason = scanWindowDecisionLowRecovery
	}

	if newInterval > current && !allowedToIncrease {
		newInterval = current
		reason = scanWindowDecisionNone
	}

	if newInterval != current {
		c.lastAdjustTime = now
		if shouldDampOnTrend {
			c.lastTrendAdjustTime = now
		}
	}

	return scanWindowDecision{
		newInterval: newInterval,
		maxInterval: effectiveMaxInterval,
		reason:      reason,
		usage:       usage,
	}
}

type simulationRateSegment struct {
	start       time.Duration
	end         time.Duration
	bytesPerSec float64
}

type simulationNodeConfig struct {
	capacityBytes   float64
	drainProfile    []simulationRateSegment
	dispatcherQuota float64
}

type simulationDispatcherConfig struct {
	nodeIndex    int
	inputProfile []simulationRateSegment
}

type scanWindowSimulationScenario struct {
	duration             time.Duration
	tick                 time.Duration
	reportInterval       time.Duration
	reportLag            time.Duration
	startInterval        time.Duration
	normalMaxInterval    time.Duration
	scanRateBytesPerSec  float64
	intervalPenaltyPivot time.Duration
	minScanEfficiency    float64
	overshootPenaltyAt   time.Duration
	overshootPenaltyMax  time.Duration
	overshootPenaltyMin  float64
	releasePulseBytes    float64
	releaseArmUsage      float64
	nodes                []simulationNodeConfig
	dispatchers          []simulationDispatcherConfig
}

type simulationReportedState struct {
	usageRatio   float64
	releaseCount uint32
}

type simulationNodeState struct {
	config                simulationNodeConfig
	pendingBytes          float64
	releaseArmed          bool
	releasedBytesBuffered float64
	releaseCount          uint32
	reportHistory         []simulationReportedState
}

type simulationDispatcherState struct {
	config               simulationDispatcherConfig
	upstreamPendingBytes float64
}

type scanWindowSimulationMetrics struct {
	meanThroughputBytesPerSec float64
	throughputCV              float64
	minThroughputBytesPerSec  float64
	maxThroughputBytesPerSec  float64
	minInterval               time.Duration
	maxInterval               time.Duration
	adjustCount               int
	skippedByChangefeedQuota  int
	skippedByDispatcherQuota  int
}

type scanWindowSimulationTracePoint struct {
	elapsed               time.Duration
	intervalBeforeReport  time.Duration
	interval              time.Duration
	throughputBytesPerSec float64
	maxUsageRatio         float64
	reportedUsageRatio    float64
	totalReleaseCount     uint32
	actualReleaseCount    uint32
	adjusted              bool
	decisionReason        scanWindowDecisionReason
	fastUsageEMA          float64
	slowUsageEMA          float64
	pressureScore         float64
}

type scanWindowSimulationResult struct {
	metrics scanWindowSimulationMetrics
	trace   []scanWindowSimulationTracePoint
}

func TestScanWindowSimulatorAdaptiveControllerStabilizesRecovery(t *testing.T) {
	t.Parallel()

	scenario := scanWindowSimulationScenario{
		duration:             150 * time.Second,
		tick:                 200 * time.Millisecond,
		reportInterval:       time.Second,
		startInterval:        defaultScanInterval,
		normalMaxInterval:    time.Minute,
		scanRateBytesPerSec:  10 * 1024 * 1024,
		intervalPenaltyPivot: defaultScanInterval,
		minScanEfficiency:    0.15,
		releasePulseBytes:    8 * 1024 * 1024,
		releaseArmUsage:      0.45,
		nodes: []simulationNodeConfig{
			{
				capacityBytes:   96 * 1024 * 1024,
				dispatcherQuota: 16 * 1024 * 1024,
				drainProfile: []simulationRateSegment{
					{start: 0, end: 40 * time.Second, bytesPerSec: 24 * 1024 * 1024},
					{start: 40 * time.Second, end: 60 * time.Second, bytesPerSec: 12 * 1024 * 1024},
					{start: 60 * time.Second, end: 80 * time.Second, bytesPerSec: 24 * 1024 * 1024},
					{start: 80 * time.Second, end: 100 * time.Second, bytesPerSec: 12 * 1024 * 1024},
					{start: 100 * time.Second, end: 120 * time.Second, bytesPerSec: 24 * 1024 * 1024},
					{start: 120 * time.Second, end: 150 * time.Second, bytesPerSec: 24 * 1024 * 1024},
				},
			},
		},
		dispatchers: []simulationDispatcherConfig{
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 150 * time.Second, bytesPerSec: 8 * 1024 * 1024}}},
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 150 * time.Second, bytesPerSec: 8 * 1024 * 1024}}},
		},
	}

	start := time.Unix(0, 0)
	legacyMetrics := runScanWindowSimulation(newLegacyScanWindowController(start), scenario)
	adaptiveMetrics := runScanWindowSimulation(newAdaptiveScanWindowController(start), scenario)

	t.Logf("legacy metrics: %+v", legacyMetrics)
	t.Logf("adaptive metrics: %+v", adaptiveMetrics)

	require.Greater(t, legacyMetrics.adjustCount, 0)
	require.LessOrEqual(t, adaptiveMetrics.throughputCV, legacyMetrics.throughputCV+1e-9)
	require.LessOrEqual(t, adaptiveMetrics.adjustCount, legacyMetrics.adjustCount)
	require.Greater(t, adaptiveMetrics.minInterval, legacyMetrics.minInterval)
	require.GreaterOrEqual(t, adaptiveMetrics.meanThroughputBytesPerSec, legacyMetrics.meanThroughputBytesPerSec*0.99)
}

func TestScanWindowSimulatorObservedWindowScenarios(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                         string
		scenario                     scanWindowSimulationScenario
		minLegacyMaxInterval         time.Duration
		minLegacyFloorCollapses      int
		maxAdaptiveFloorCollapses    int
		minLegacyTinyWindowReports   int
		maxAdaptiveTinyWindowReports int
		minLegacyReleaseBurst        uint32
	}{
		{
			name:                         "w3_like_big_peak_release_collapse",
			scenario:                     newBigPeakReleaseCollapseScenario(),
			minLegacyMaxInterval:         4 * time.Minute,
			minLegacyFloorCollapses:      1,
			maxAdaptiveFloorCollapses:    0,
			minLegacyTinyWindowReports:   1,
			maxAdaptiveTinyWindowReports: 0,
			minLegacyReleaseBurst:        1,
		},
		{
			name:                         "w4_like_plateau_then_collapse",
			scenario:                     newPlateauCollapseScenario(),
			minLegacyMaxInterval:         4 * time.Minute,
			minLegacyFloorCollapses:      1,
			maxAdaptiveFloorCollapses:    0,
			minLegacyTinyWindowReports:   1,
			maxAdaptiveTinyWindowReports: 0,
			minLegacyReleaseBurst:        1,
		},
		{
			name:                         "w5_like_reset_dominant_collapse",
			scenario:                     newResetDominantCollapseScenario(),
			minLegacyMaxInterval:         90 * time.Second,
			minLegacyFloorCollapses:      1,
			maxAdaptiveFloorCollapses:    0,
			minLegacyTinyWindowReports:   2,
			maxAdaptiveTinyWindowReports: 0,
			minLegacyReleaseBurst:        2,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start := time.Unix(0, 0)
			legacy := runScanWindowSimulationWithTrace(newLegacyScanWindowController(start), tc.scenario)
			adaptive := runScanWindowSimulationWithTrace(newAdaptiveScanWindowController(start), tc.scenario)

			legacyCollapses := countSimulationFloorCollapses(legacy.trace, 30*time.Second, 2*time.Minute, defaultScanInterval, 0.8)
			adaptiveCollapses := countSimulationFloorCollapses(adaptive.trace, 30*time.Second, 2*time.Minute, defaultScanInterval, 0.8)
			legacyTinyWindowReports := countSimulationIntervalsAtMost(legacy.trace, defaultScanInterval)
			adaptiveTinyWindowReports := countSimulationIntervalsAtMost(adaptive.trace, defaultScanInterval)
			legacyReleaseBurst := maxSimulationReleaseBurst(legacy.trace)

			t.Logf("legacy metrics: %+v", legacy.metrics)
			t.Logf("adaptive metrics: %+v", adaptive.metrics)
			t.Logf("legacy collapses=%d adaptive collapses=%d legacy tiny=%d adaptive tiny=%d legacy release=%d",
				legacyCollapses, adaptiveCollapses, legacyTinyWindowReports, adaptiveTinyWindowReports, legacyReleaseBurst)

			require.GreaterOrEqual(t, legacy.metrics.maxInterval, tc.minLegacyMaxInterval)
			require.GreaterOrEqual(t, legacyCollapses, tc.minLegacyFloorCollapses)
			require.LessOrEqual(t, adaptiveCollapses, tc.maxAdaptiveFloorCollapses)
			require.GreaterOrEqual(t, legacyTinyWindowReports, tc.minLegacyTinyWindowReports)
			require.LessOrEqual(t, adaptiveTinyWindowReports, tc.maxAdaptiveTinyWindowReports)
			require.GreaterOrEqual(t, legacyReleaseBurst, tc.minLegacyReleaseBurst)
			require.LessOrEqual(t, adaptive.metrics.throughputCV, legacy.metrics.throughputCV+1e-9)
			require.GreaterOrEqual(t, adaptive.metrics.meanThroughputBytesPerSec, legacy.metrics.meanThroughputBytesPerSec*0.95)
		})
	}
}

func TestScanWindowSimulatorFeedbackLagStepChangeScenario(t *testing.T) {
	t.Parallel()

	scenario := newFeedbackLagStepChangeScenario()
	start := time.Unix(0, 0)
	legacy := runScanWindowSimulationWithTrace(newLegacyScanWindowController(start), scenario)
	adaptive := runScanWindowSimulationWithTrace(newAdaptiveScanWindowController(start), scenario)

	legacyCollapses := countSimulationFloorCollapses(legacy.trace, 30*time.Second, 2*time.Minute, defaultScanInterval, 0.8)
	adaptiveCollapses := countSimulationFloorCollapses(adaptive.trace, 30*time.Second, 2*time.Minute, defaultScanInterval, 0.8)
	legacyTinyWindowReports := countSimulationIntervalsAtMost(legacy.trace, defaultScanInterval)
	adaptiveTinyWindowReports := countSimulationIntervalsAtMost(adaptive.trace, defaultScanInterval)
	legacyLargeWindowShare := shareSimulationIntervalsGreaterThan(legacy.trace, 120*time.Second)
	adaptiveLargeWindowShare := shareSimulationIntervalsGreaterThan(adaptive.trace, 120*time.Second)
	legacyVeryLargeWindowShare := shareSimulationIntervalsGreaterThan(legacy.trace, 300*time.Second)
	adaptiveVeryLargeWindowShare := shareSimulationIntervalsGreaterThan(adaptive.trace, 300*time.Second)
	legacyMidBucket := summarizeSimulationThroughputBucket(legacy.trace, 30*time.Second, 120*time.Second)
	legacyLargeBucket := summarizeSimulationThroughputBucket(legacy.trace, 120*time.Second, 0)
	adaptiveMidBucket := summarizeSimulationThroughputBucket(adaptive.trace, 30*time.Second, 120*time.Second)
	adaptiveLargeBucket := summarizeSimulationThroughputBucket(adaptive.trace, 120*time.Second, 0)
	legacyActionRate := simulationActionRatePerMinute(legacy.metrics, scenario)
	adaptiveActionRate := simulationActionRatePerMinute(adaptive.metrics, scenario)

	t.Logf("legacy metrics: %+v", legacy.metrics)
	t.Logf("adaptive metrics: %+v", adaptive.metrics)
	t.Logf("legacy collapse=%d tiny=%d large=%.3f veryLarge=%.3f actionRate=%.2f/min midBucket=%+v largeBucket=%+v",
		legacyCollapses, legacyTinyWindowReports, legacyLargeWindowShare, legacyVeryLargeWindowShare, legacyActionRate, legacyMidBucket, legacyLargeBucket)
	t.Logf("adaptive collapse=%d tiny=%d large=%.3f veryLarge=%.3f actionRate=%.2f/min midBucket=%+v largeBucket=%+v",
		adaptiveCollapses, adaptiveTinyWindowReports, adaptiveLargeWindowShare, adaptiveVeryLargeWindowShare, adaptiveActionRate, adaptiveMidBucket, adaptiveLargeBucket)

	require.GreaterOrEqual(t, legacyCollapses, 1)
	require.GreaterOrEqual(t, legacyLargeBucket.count, 10)
	require.GreaterOrEqual(t, legacyLargeWindowShare, 0.10)
	require.Less(t, legacyLargeBucket.medianBytesPerSec, legacyMidBucket.medianBytesPerSec)

	require.LessOrEqual(t, adaptiveCollapses, legacyCollapses)
	require.LessOrEqual(t, adaptiveTinyWindowReports, legacyTinyWindowReports)
	require.Less(t, adaptiveLargeWindowShare, legacyLargeWindowShare)
	require.Less(t, adaptive.metrics.maxInterval, 120*time.Second)
	require.GreaterOrEqual(t, adaptive.metrics.meanThroughputBytesPerSec, legacy.metrics.meanThroughputBytesPerSec*0.95)
	require.LessOrEqual(t, adaptive.metrics.throughputCV, legacy.metrics.throughputCV*1.05+1e-9)
}

func TestScanWindowSimulatorTargetBandFineTuneScenario(t *testing.T) {
	t.Parallel()

	scenario := newTargetBandFineTuneScenario()
	start := time.Unix(0, 0)
	legacy := runScanWindowSimulationWithTrace(newLegacyScanWindowController(start), scenario)
	adaptive := runScanWindowSimulationWithTrace(newAdaptiveScanWindowController(start), scenario)

	legacyReportedBandShare := shareSimulationReportedUsageInRange(legacy.trace, 0.30, 0.50)
	adaptiveReportedBandShare := shareSimulationReportedUsageInRange(adaptive.trace, 0.30, 0.50)
	legacyFastBandShare := shareSimulationFastEMAInRange(legacy.trace, 0.30, 0.50)
	adaptiveFastBandShare := shareSimulationFastEMAInRange(adaptive.trace, 0.30, 0.50)
	legacyFastBandCrosses := countSimulationFastEMABandCrossings(legacy.trace, 0.30, 0.50)
	adaptiveFastBandCrosses := countSimulationFastEMABandCrossings(adaptive.trace, 0.30, 0.50)
	legacyLargeWindowShare := shareSimulationIntervalsGreaterThan(legacy.trace, 120*time.Second)
	adaptiveLargeWindowShare := shareSimulationIntervalsGreaterThan(adaptive.trace, 120*time.Second)
	legacyActionRate := simulationActionRatePerMinute(legacy.metrics, scenario)
	adaptiveActionRate := simulationActionRatePerMinute(adaptive.metrics, scenario)

	t.Logf("legacy metrics: %+v", legacy.metrics)
	t.Logf("adaptive metrics: %+v", adaptive.metrics)
	t.Logf("legacy reportedBand=%.3f fastBand=%.3f fastCross=%d large=%.3f actionRate=%.2f/min",
		legacyReportedBandShare, legacyFastBandShare, legacyFastBandCrosses, legacyLargeWindowShare, legacyActionRate)
	t.Logf("adaptive reportedBand=%.3f fastBand=%.3f fastCross=%d large=%.3f actionRate=%.2f/min",
		adaptiveReportedBandShare, adaptiveFastBandShare, adaptiveFastBandCrosses, adaptiveLargeWindowShare, adaptiveActionRate)

	require.Greater(t, len(adaptive.trace), 0)
	require.GreaterOrEqual(t, adaptiveReportedBandShare, legacyReportedBandShare)
	require.GreaterOrEqual(t, adaptiveFastBandShare, legacyFastBandShare)
	require.Greater(t, adaptiveFastBandShare, 0.10)
	require.LessOrEqual(t, adaptiveFastBandCrosses, 12)
	require.LessOrEqual(t, adaptiveLargeWindowShare, legacyLargeWindowShare+1e-9)
	require.GreaterOrEqual(t, adaptive.metrics.meanThroughputBytesPerSec, legacy.metrics.meanThroughputBytesPerSec*0.98)
	require.LessOrEqual(t, adaptive.metrics.throughputCV, legacy.metrics.throughputCV*1.08+1e-9)
	require.LessOrEqual(t, adaptiveActionRate, legacyActionRate*1.20+1e-9)
}

func TestScanWindowSimulatorFeedbackLagSweep(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	for _, lag := range []time.Duration{0, 4 * time.Second, 8 * time.Second, 12 * time.Second} {
		lag := lag
		t.Run(lag.String(), func(t *testing.T) {
			t.Parallel()

			scenario := newFeedbackLagStepChangeScenario()
			scenario.reportLag = lag

			legacy := runScanWindowSimulationWithTrace(newLegacyScanWindowController(start), scenario)
			adaptive := runScanWindowSimulationWithTrace(newAdaptiveScanWindowController(start), scenario)

			legacyLargeWindowShare := shareSimulationIntervalsGreaterThan(legacy.trace, 120*time.Second)
			adaptiveLargeWindowShare := shareSimulationIntervalsGreaterThan(adaptive.trace, 120*time.Second)
			legacyCollapses := countSimulationFloorCollapses(legacy.trace, 30*time.Second, 2*time.Minute, defaultScanInterval, 0.8)
			adaptiveCollapses := countSimulationFloorCollapses(adaptive.trace, 30*time.Second, 2*time.Minute, defaultScanInterval, 0.8)

			t.Logf("lag=%s legacy=%+v adaptive=%+v legacyLarge=%.3f adaptiveLarge=%.3f legacyCollapse=%d adaptiveCollapse=%d",
				lag, legacy.metrics, adaptive.metrics, legacyLargeWindowShare, adaptiveLargeWindowShare, legacyCollapses, adaptiveCollapses)

			require.LessOrEqual(t, adaptiveCollapses, legacyCollapses)
			require.LessOrEqual(t, adaptiveLargeWindowShare, legacyLargeWindowShare+1e-9)
			require.Less(t, adaptive.metrics.maxInterval, 120*time.Second)
			require.GreaterOrEqual(t, adaptive.metrics.meanThroughputBytesPerSec, legacy.metrics.meanThroughputBytesPerSec*0.94)
			require.LessOrEqual(t, adaptive.metrics.throughputCV, legacy.metrics.throughputCV*1.08+1e-9)
		})
	}
}

func runScanWindowSimulation(controller scanWindowController, scenario scanWindowSimulationScenario) scanWindowSimulationMetrics {
	return runScanWindowSimulationWithTrace(controller, scenario).metrics
}

func runScanWindowSimulationWithTrace(controller scanWindowController, scenario scanWindowSimulationScenario) scanWindowSimulationResult {
	nodes := make([]simulationNodeState, len(scenario.nodes))
	for i, cfg := range scenario.nodes {
		nodes[i] = simulationNodeState{config: cfg}
	}

	dispatchers := make([]simulationDispatcherState, len(scenario.dispatchers))
	for i, cfg := range scenario.dispatchers {
		dispatchers[i] = simulationDispatcherState{config: cfg}
	}

	currentInterval := scenario.startInterval
	if currentInterval <= 0 {
		currentInterval = defaultScanInterval
	}

	start := time.Unix(0, 0)
	nextReport := scenario.reportInterval
	totalDrained := 0.0
	lastReportedDrained := 0.0
	throughputSamples := make([]float64, 0, int(scenario.duration/scenario.reportInterval)+1)

	result := scanWindowSimulationResult{
		metrics: scanWindowSimulationMetrics{
			minInterval: currentInterval,
			maxInterval: currentInterval,
		},
		trace: make([]scanWindowSimulationTracePoint, 0, int(scenario.duration/scenario.reportInterval)+1),
	}
	laggedReports := simulationLaggedReportCount(scenario)

	for elapsed := time.Duration(0); elapsed < scenario.duration; elapsed += scenario.tick {
		for i := range dispatchers {
			dispatchers[i].upstreamPendingBytes += rateAt(dispatchers[i].config.inputProfile, elapsed) * scenario.tick.Seconds()
		}

		scanEfficiency := simulationScanEfficiencyForInterval(currentInterval, scenario)
		perDispatcherBudget := scenario.scanRateBytesPerSec * scanEfficiency * scenario.tick.Seconds()

		for i := range dispatchers {
			dispatcher := &dispatchers[i]
			if dispatcher.upstreamPendingBytes <= 0 {
				continue
			}

			node := &nodes[dispatcher.config.nodeIndex]
			available := maxFloat64(0, node.config.capacityBytes-node.pendingBytes)
			if available <= 0 {
				result.metrics.skippedByChangefeedQuota++
				continue
			}
			if perDispatcherBudget > node.config.dispatcherQuota {
				result.metrics.skippedByDispatcherQuota++
				continue
			}

			scanned := min(
				perDispatcherBudget,
				dispatcher.upstreamPendingBytes,
				node.config.dispatcherQuota,
				available,
			)
			if scanned <= 0 {
				continue
			}

			dispatcher.upstreamPendingBytes -= scanned
			node.pendingBytes += scanned
		}

		for i := range nodes {
			node := &nodes[i]
			usageBeforeDrain := 0.0
			if node.config.capacityBytes > 0 {
				usageBeforeDrain = node.pendingBytes / node.config.capacityBytes
			}
			if usageBeforeDrain >= scenario.releaseArmUsage {
				node.releaseArmed = true
			}

			drain := min(node.pendingBytes, rateAt(node.config.drainProfile, elapsed)*scenario.tick.Seconds())
			node.pendingBytes -= drain
			totalDrained += drain

			if node.releaseArmed && drain > 0 {
				node.releasedBytesBuffered += drain
				for node.releasedBytesBuffered >= scenario.releasePulseBytes {
					node.releaseCount++
					node.releasedBytesBuffered -= scenario.releasePulseBytes
				}
			}

			if node.releaseArmed && node.pendingBytes/node.config.capacityBytes < scenario.releaseArmUsage/2 {
				node.releaseArmed = false
				node.releasedBytesBuffered = 0
			}
		}

		if elapsed+scenario.tick < nextReport {
			continue
		}

		throughput := (totalDrained - lastReportedDrained) / scenario.reportInterval.Seconds()
		throughputSamples = append(throughputSamples, throughput)
		lastReportedDrained = totalDrained

		intervalBeforeReport := currentInterval
		reportMaxUsage := 0.0
		reportedMaxUsage := 0.0
		var reportReleaseCount uint32
		var actualReleaseCount uint32
		adjusted := false
		decisionReason := scanWindowDecisionNone
		fastUsageEMA := 0.0
		slowUsageEMA := 0.0
		pressureScore := 0.0
		for i := range nodes {
			node := &nodes[i]
			usageRatio := 0.0
			if node.config.capacityBytes > 0 {
				usageRatio = clampFloat64(0, 1, node.pendingBytes/node.config.capacityBytes)
			}
			reportMaxUsage = maxFloat64(reportMaxUsage, usageRatio)
			actualReleaseCount += node.releaseCount

			reportedState := simulationDelayedReportedState(node, simulationReportedState{
				usageRatio:   usageRatio,
				releaseCount: node.releaseCount,
			}, laggedReports)
			reportedMaxUsage = maxFloat64(reportedMaxUsage, reportedState.usageRatio)
			reportReleaseCount += reportedState.releaseCount

			decision := controller.OnCongestionReport(start.Add(nextReport), currentInterval, scenario.normalMaxInterval, scanWindowReport{
				usageRatio:         reportedState.usageRatio,
				memoryReleaseCount: reportedState.releaseCount,
			})
			if decision.newInterval != currentInterval {
				result.metrics.adjustCount++
				currentInterval = decision.newInterval
				adjusted = true
				if currentInterval < result.metrics.minInterval {
					result.metrics.minInterval = currentInterval
				}
				if currentInterval > result.metrics.maxInterval {
					result.metrics.maxInterval = currentInterval
				}
			}
			decisionReason = decision.reason
			fastUsageEMA = decision.fastUsageEMA
			slowUsageEMA = decision.slowUsageEMA
			pressureScore = decision.pressureScore
			node.releaseCount = 0
		}

		result.trace = append(result.trace, scanWindowSimulationTracePoint{
			elapsed:               nextReport,
			intervalBeforeReport:  intervalBeforeReport,
			interval:              currentInterval,
			throughputBytesPerSec: throughput,
			maxUsageRatio:         reportMaxUsage,
			reportedUsageRatio:    reportedMaxUsage,
			totalReleaseCount:     reportReleaseCount,
			actualReleaseCount:    actualReleaseCount,
			adjusted:              adjusted,
			decisionReason:        decisionReason,
			fastUsageEMA:          fastUsageEMA,
			slowUsageEMA:          slowUsageEMA,
			pressureScore:         pressureScore,
		})
		nextReport += scenario.reportInterval
	}

	result.metrics.meanThroughputBytesPerSec, result.metrics.throughputCV, result.metrics.minThroughputBytesPerSec, result.metrics.maxThroughputBytesPerSec = summarizeThroughput(throughputSamples)
	return result
}

func summarizeThroughput(samples []float64) (mean float64, cv float64, minValue float64, maxValue float64) {
	if len(samples) == 0 {
		return 0, 0, 0, 0
	}

	minValue = samples[0]
	maxValue = samples[0]
	sum := 0.0
	for _, sample := range samples {
		sum += sample
		if sample < minValue {
			minValue = sample
		}
		if sample > maxValue {
			maxValue = sample
		}
	}
	mean = sum / float64(len(samples))
	if mean == 0 {
		return mean, 0, minValue, maxValue
	}

	variance := 0.0
	for _, sample := range samples {
		diff := sample - mean
		variance += diff * diff
	}
	variance /= float64(len(samples))
	cv = math.Sqrt(variance) / mean
	return mean, cv, minValue, maxValue
}

func simulationLaggedReportCount(scenario scanWindowSimulationScenario) int {
	if scenario.reportLag <= 0 || scenario.reportInterval <= 0 {
		return 0
	}
	return int(scenario.reportLag / scenario.reportInterval)
}

func simulationDelayedReportedState(
	node *simulationNodeState,
	current simulationReportedState,
	laggedReports int,
) simulationReportedState {
	node.reportHistory = append(node.reportHistory, current)
	if laggedReports <= 0 || len(node.reportHistory) <= laggedReports {
		return current
	}

	idx := len(node.reportHistory) - laggedReports - 1
	if idx < 0 {
		idx = 0
	}
	return node.reportHistory[idx]
}

func simulationScanEfficiencyForInterval(interval time.Duration, scenario scanWindowSimulationScenario) float64 {
	if scenario.intervalPenaltyPivot <= 0 {
		return 1
	}

	efficiency := clampFloat64(
		scenario.minScanEfficiency,
		1,
		float64(interval)/float64(scenario.intervalPenaltyPivot),
	)

	if scenario.overshootPenaltyAt <= 0 || interval <= scenario.overshootPenaltyAt {
		return efficiency
	}

	penaltyMax := scenario.overshootPenaltyMax
	if penaltyMax <= scenario.overshootPenaltyAt {
		penaltyMax = scenario.normalMaxInterval
	}
	if penaltyMax <= scenario.overshootPenaltyAt {
		penaltyMax = scenario.overshootPenaltyAt + time.Second
	}

	penaltyRatio := clampFloat64(
		0,
		1,
		float64(interval-scenario.overshootPenaltyAt)/float64(penaltyMax-scenario.overshootPenaltyAt),
	)
	penaltyFloor := scenario.overshootPenaltyMin
	if penaltyFloor <= 0 {
		penaltyFloor = scenario.minScanEfficiency
	}
	decayed := 1 - penaltyRatio*(1-penaltyFloor)
	return clampFloat64(scenario.minScanEfficiency, 1, min(efficiency, decayed))
}

func rateAt(segments []simulationRateSegment, elapsed time.Duration) float64 {
	for _, segment := range segments {
		if elapsed >= segment.start && elapsed < segment.end {
			return segment.bytesPerSec
		}
	}
	return 0
}

func clampFloat64(minValue float64, maxValue float64, value float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

type simulationThroughputBucket struct {
	count             int
	meanBytesPerSec   float64
	medianBytesPerSec float64
	minBytesPerSec    float64
	maxBytesPerSec    float64
}

func summarizeSimulationThroughputBucket(
	trace []scanWindowSimulationTracePoint,
	minIntervalInclusive time.Duration,
	maxIntervalExclusive time.Duration,
) simulationThroughputBucket {
	samples := make([]float64, 0, len(trace))
	for _, point := range trace {
		if point.interval < minIntervalInclusive {
			continue
		}
		if maxIntervalExclusive > 0 && point.interval >= maxIntervalExclusive {
			continue
		}
		samples = append(samples, point.throughputBytesPerSec)
	}
	if len(samples) == 0 {
		return simulationThroughputBucket{}
	}

	minValue := samples[0]
	maxValue := samples[0]
	sum := 0.0
	for _, sample := range samples {
		sum += sample
		if sample < minValue {
			minValue = sample
		}
		if sample > maxValue {
			maxValue = sample
		}
	}

	sorted := append([]float64(nil), samples...)
	slices.Sort(sorted)
	median := sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		median = (sorted[len(sorted)/2-1] + sorted[len(sorted)/2]) / 2
	}

	return simulationThroughputBucket{
		count:             len(samples),
		meanBytesPerSec:   sum / float64(len(samples)),
		medianBytesPerSec: median,
		minBytesPerSec:    minValue,
		maxBytesPerSec:    maxValue,
	}
}

func countSimulationIntervalsGreaterThan(trace []scanWindowSimulationTracePoint, threshold time.Duration) int {
	count := 0
	for _, point := range trace {
		if point.interval > threshold {
			count++
		}
	}
	return count
}

func shareSimulationIntervalsGreaterThan(trace []scanWindowSimulationTracePoint, threshold time.Duration) float64 {
	if len(trace) == 0 {
		return 0
	}
	return float64(countSimulationIntervalsGreaterThan(trace, threshold)) / float64(len(trace))
}

func simulationActionRatePerMinute(metrics scanWindowSimulationMetrics, scenario scanWindowSimulationScenario) float64 {
	if scenario.duration <= 0 {
		return 0
	}
	return float64(metrics.adjustCount) / scenario.duration.Minutes()
}

func shareSimulationReportedUsageInRange(trace []scanWindowSimulationTracePoint, minValue float64, maxValue float64) float64 {
	return shareSimulationTraceValueInRange(trace, minValue, maxValue, func(point scanWindowSimulationTracePoint) float64 {
		return point.reportedUsageRatio
	})
}

func shareSimulationFastEMAInRange(trace []scanWindowSimulationTracePoint, minValue float64, maxValue float64) float64 {
	return shareSimulationTraceValueInRange(trace, minValue, maxValue, func(point scanWindowSimulationTracePoint) float64 {
		return point.fastUsageEMA
	})
}

func shareSimulationTraceValueInRange(
	trace []scanWindowSimulationTracePoint,
	minValue float64,
	maxValue float64,
	selector func(scanWindowSimulationTracePoint) float64,
) float64 {
	if len(trace) == 0 {
		return 0
	}

	count := 0
	for _, point := range trace {
		value := selector(point)
		if value >= minValue && value <= maxValue {
			count++
		}
	}
	return float64(count) / float64(len(trace))
}

func countSimulationFastEMABandCrossings(trace []scanWindowSimulationTracePoint, minValue float64, maxValue float64) int {
	return countSimulationTraceBandCrossings(trace, minValue, maxValue, func(point scanWindowSimulationTracePoint) float64 {
		return point.fastUsageEMA
	})
}

func countSimulationTraceBandCrossings(
	trace []scanWindowSimulationTracePoint,
	minValue float64,
	maxValue float64,
	selector func(scanWindowSimulationTracePoint) float64,
) int {
	if len(trace) == 0 {
		return 0
	}

	classify := func(value float64) int {
		switch {
		case value < minValue:
			return -1
		case value > maxValue:
			return 1
		default:
			return 0
		}
	}

	lastClass := classify(selector(trace[0]))
	crosses := 0
	for i := 1; i < len(trace); i++ {
		currentClass := classify(selector(trace[i]))
		if currentClass != lastClass {
			crosses++
			lastClass = currentClass
		}
	}
	return crosses
}

func newBigPeakReleaseCollapseScenario() scanWindowSimulationScenario {
	const mib = 1024 * 1024

	return scanWindowSimulationScenario{
		duration:             6 * time.Minute,
		tick:                 200 * time.Millisecond,
		reportInterval:       time.Second,
		startInterval:        2 * time.Minute,
		normalMaxInterval:    15 * time.Minute,
		scanRateBytesPerSec:  160 * mib,
		intervalPenaltyPivot: 2 * time.Minute,
		minScanEfficiency:    0.05,
		releasePulseBytes:    24 * mib,
		releaseArmUsage:      0.45,
		nodes: []simulationNodeConfig{
			{
				capacityBytes:   512 * mib,
				dispatcherQuota: 32 * mib,
				drainProfile: []simulationRateSegment{
					{start: 0, end: 2 * time.Minute, bytesPerSec: 12 * mib},
					{start: 2 * time.Minute, end: 210 * time.Second, bytesPerSec: 7 * mib},
					{start: 210 * time.Second, end: 4 * time.Minute, bytesPerSec: 10 * mib},
					{start: 4 * time.Minute, end: 5 * time.Minute, bytesPerSec: 18 * mib},
					{start: 5 * time.Minute, end: 6 * time.Minute, bytesPerSec: 10 * mib},
				},
			},
		},
		dispatchers: []simulationDispatcherConfig{
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 6 * time.Minute, bytesPerSec: 5 * mib}}},
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 6 * time.Minute, bytesPerSec: 5 * mib}}},
		},
	}
}

func newTargetBandFineTuneScenario() scanWindowSimulationScenario {
	const mib = 1024 * 1024

	return scanWindowSimulationScenario{
		duration:             7 * time.Minute,
		tick:                 200 * time.Millisecond,
		reportInterval:       time.Second,
		startInterval:        35 * time.Second,
		normalMaxInterval:    4 * time.Minute,
		scanRateBytesPerSec:  12 * mib,
		intervalPenaltyPivot: 25 * time.Second,
		minScanEfficiency:    0.25,
		overshootPenaltyAt:   100 * time.Second,
		overshootPenaltyMax:  4 * time.Minute,
		overshootPenaltyMin:  0.45,
		releasePulseBytes:    24 * mib,
		releaseArmUsage:      0.50,
		nodes: []simulationNodeConfig{
			{
				capacityBytes:   64 * mib,
				dispatcherQuota: 16 * mib,
				drainProfile: []simulationRateSegment{
					{start: 0, end: 40 * time.Second, bytesPerSec: 8.8 * mib},
					{start: 40 * time.Second, end: 100 * time.Second, bytesPerSec: 10.9 * mib},
					{start: 100 * time.Second, end: 160 * time.Second, bytesPerSec: 9.4 * mib},
					{start: 160 * time.Second, end: 230 * time.Second, bytesPerSec: 10.8 * mib},
					{start: 230 * time.Second, end: 300 * time.Second, bytesPerSec: 9.7 * mib},
					{start: 300 * time.Second, end: 7 * time.Minute, bytesPerSec: 10.3 * mib},
				},
			},
		},
		dispatchers: []simulationDispatcherConfig{
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 7 * time.Minute, bytesPerSec: 5.0 * mib}}},
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 7 * time.Minute, bytesPerSec: 5.0 * mib}}},
		},
	}
}

func newFeedbackLagStepChangeScenario() scanWindowSimulationScenario {
	const mib = 1024 * 1024

	return scanWindowSimulationScenario{
		duration:             8 * time.Minute,
		tick:                 200 * time.Millisecond,
		reportInterval:       time.Second,
		reportLag:            8 * time.Second,
		startInterval:        45 * time.Second,
		normalMaxInterval:    10 * time.Minute,
		scanRateBytesPerSec:  10 * mib,
		intervalPenaltyPivot: 30 * time.Second,
		minScanEfficiency:    0.20,
		overshootPenaltyAt:   90 * time.Second,
		overshootPenaltyMax:  5 * time.Minute,
		overshootPenaltyMin:  0.35,
		releasePulseBytes:    16 * mib,
		releaseArmUsage:      0.35,
		nodes: []simulationNodeConfig{
			{
				capacityBytes:   320 * mib,
				dispatcherQuota: 16 * mib,
				drainProfile: []simulationRateSegment{
					{start: 0, end: 100 * time.Second, bytesPerSec: 16 * mib},
					{start: 100 * time.Second, end: 220 * time.Second, bytesPerSec: 10 * mib},
					{start: 220 * time.Second, end: 260 * time.Second, bytesPerSec: 18 * mib},
					{start: 260 * time.Second, end: 360 * time.Second, bytesPerSec: 10 * mib},
					{start: 360 * time.Second, end: 8 * time.Minute, bytesPerSec: 16 * mib},
				},
			},
		},
		dispatchers: []simulationDispatcherConfig{
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 8 * time.Minute, bytesPerSec: 7 * mib}}},
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 8 * time.Minute, bytesPerSec: 7 * mib}}},
		},
	}
}

func newPlateauCollapseScenario() scanWindowSimulationScenario {
	const mib = 1024 * 1024

	return scanWindowSimulationScenario{
		duration:             6 * time.Minute,
		tick:                 200 * time.Millisecond,
		reportInterval:       time.Second,
		startInterval:        4 * time.Minute,
		normalMaxInterval:    15 * time.Minute,
		scanRateBytesPerSec:  160 * mib,
		intervalPenaltyPivot: 2 * time.Minute,
		minScanEfficiency:    0.05,
		releasePulseBytes:    20 * mib,
		releaseArmUsage:      0.40,
		nodes: []simulationNodeConfig{
			{
				capacityBytes:   512 * mib,
				dispatcherQuota: 32 * mib,
				drainProfile: []simulationRateSegment{
					{start: 0, end: 90 * time.Second, bytesPerSec: 12 * mib},
					{start: 90 * time.Second, end: 210 * time.Second, bytesPerSec: 8.5 * mib},
					{start: 210 * time.Second, end: 240 * time.Second, bytesPerSec: 6 * mib},
					{start: 240 * time.Second, end: 300 * time.Second, bytesPerSec: 18 * mib},
					{start: 300 * time.Second, end: 6 * time.Minute, bytesPerSec: 11 * mib},
				},
			},
		},
		dispatchers: []simulationDispatcherConfig{
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 6 * time.Minute, bytesPerSec: 5 * mib}}},
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 6 * time.Minute, bytesPerSec: 5 * mib}}},
		},
	}
}

func newResetDominantCollapseScenario() scanWindowSimulationScenario {
	const mib = 1024 * 1024

	return scanWindowSimulationScenario{
		duration:             4 * time.Minute,
		tick:                 200 * time.Millisecond,
		reportInterval:       time.Second,
		startInterval:        90 * time.Second,
		normalMaxInterval:    10 * time.Minute,
		scanRateBytesPerSec:  128 * mib,
		intervalPenaltyPivot: time.Minute,
		minScanEfficiency:    0.05,
		releasePulseBytes:    8 * mib,
		releaseArmUsage:      0.25,
		nodes: []simulationNodeConfig{
			{
				capacityBytes:   256 * mib,
				dispatcherQuota: 32 * mib,
				drainProfile: []simulationRateSegment{
					{start: 0, end: 30 * time.Second, bytesPerSec: 10 * mib},
					{start: 30 * time.Second, end: 75 * time.Second, bytesPerSec: 5 * mib},
					{start: 75 * time.Second, end: 105 * time.Second, bytesPerSec: 14 * mib},
					{start: 105 * time.Second, end: 150 * time.Second, bytesPerSec: 5 * mib},
					{start: 150 * time.Second, end: 180 * time.Second, bytesPerSec: 14 * mib},
					{start: 180 * time.Second, end: 240 * time.Second, bytesPerSec: 10 * mib},
				},
			},
		},
		dispatchers: []simulationDispatcherConfig{
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 4 * time.Minute, bytesPerSec: 4 * mib}}},
			{nodeIndex: 0, inputProfile: []simulationRateSegment{{start: 0, end: 4 * time.Minute, bytesPerSec: 4 * mib}}},
		},
	}
}

func countSimulationFloorCollapses(
	trace []scanWindowSimulationTracePoint,
	minPeak time.Duration,
	lookback time.Duration,
	floorInterval time.Duration,
	minDropRatio float64,
) int {
	if len(trace) == 0 {
		return 0
	}

	collapseCount := 0
	for i := 1; i < len(trace); i++ {
		if trace[i].interval > floorInterval || trace[i-1].interval <= floorInterval {
			continue
		}

		peakInterval := time.Duration(0)
		for j := i - 1; j >= 0; j-- {
			if trace[i].elapsed-trace[j].elapsed > lookback {
				break
			}
			if trace[j].interval > peakInterval {
				peakInterval = trace[j].interval
			}
		}
		if peakInterval < minPeak {
			continue
		}

		dropRatio := 1 - float64(trace[i].interval)/float64(peakInterval)
		if dropRatio >= minDropRatio {
			collapseCount++
		}
	}
	return collapseCount
}

func countSimulationIntervalsAtMost(trace []scanWindowSimulationTracePoint, threshold time.Duration) int {
	count := 0
	for _, point := range trace {
		if point.interval <= threshold {
			count++
		}
	}
	return count
}

func maxSimulationReleaseBurst(trace []scanWindowSimulationTracePoint) uint32 {
	var maxBurst uint32
	for _, point := range trace {
		if point.totalReleaseCount > maxBurst {
			maxBurst = point.totalReleaseCount
		}
	}
	return maxBurst
}
