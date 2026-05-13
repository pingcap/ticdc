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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scanWindowControllerTracePoint struct {
	offset             time.Duration
	usageRatio         float64
	memoryReleaseCount uint32
}

type scanWindowControllerTraceResult struct {
	finalInterval time.Duration
	reasons       []scanWindowDecisionReason
	intervals     []time.Duration
}

func runScanWindowControllerTrace(
	controller scanWindowController,
	start time.Time,
	startInterval time.Duration,
	maxInterval time.Duration,
	trace []scanWindowControllerTracePoint,
) scanWindowControllerTraceResult {
	currentInterval := startInterval
	result := scanWindowControllerTraceResult{
		finalInterval: currentInterval,
		reasons:       make([]scanWindowDecisionReason, 0, len(trace)),
		intervals:     make([]time.Duration, 0, len(trace)),
	}

	for _, point := range trace {
		decision := controller.OnCongestionReport(
			start.Add(point.offset),
			currentInterval,
			maxInterval,
			scanWindowReport{
				usageRatio:         point.usageRatio,
				memoryReleaseCount: point.memoryReleaseCount,
			},
		)
		currentInterval = decision.newInterval
		result.reasons = append(result.reasons, decision.reason)
		result.intervals = append(result.intervals, currentInterval)
	}

	result.finalInterval = currentInterval
	return result
}

func countScanWindowDecisionReasons(reasons []scanWindowDecisionReason, target scanWindowDecisionReason) int {
	count := 0
	for _, reason := range reasons {
		if reason == target {
			count++
		}
	}
	return count
}

func TestAdaptiveScanWindowControllerLogDerivedBurstDoesNotCascadeCriticalBrake(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	controller := newAdaptiveScanWindowController(start)

	// Derived from the no-reset burst window observed in cdc.log around
	// 2026/04/24 07:41:15 ~ 07:41:17 UTC:
	// - first sample crosses the critical threshold
	// - subsequent samples fall quickly
	// - legacy/current behavior keeps re-entering critical_brake only because
	//   usage.max stays latched in the 30s window
	trace := []scanWindowControllerTracePoint{
		{offset: 0, usageRatio: 0.9749945681542158},
		{offset: 1 * time.Second, usageRatio: 0.800051974132657},
		{offset: 2 * time.Second, usageRatio: 0.6189540326595306},
		{offset: 3 * time.Second, usageRatio: 0.4300000000000000},
	}

	result := runScanWindowControllerTrace(controller, start, 8*time.Second, maxScanInterval, trace)

	require.LessOrEqual(t, countScanWindowDecisionReasons(result.reasons, scanWindowDecisionCriticalBrake), 1)
	require.Greater(t, result.finalInterval, 2*time.Second)
	require.NotEqual(t, minScanInterval, result.finalInterval)
}

func TestAdaptiveScanWindowControllerBlocksVeryLowRecoveryAfterRecentCriticalBrake(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	controller := newAdaptiveScanWindowController(start)

	trace := make([]scanWindowControllerTracePoint, 0, 96)
	trace = append(trace, scanWindowControllerTracePoint{
		offset:     0,
		usageRatio: 1,
	})
	for second := 1; second <= 70; second++ {
		trace = append(trace, scanWindowControllerTracePoint{
			offset:     time.Duration(second) * time.Second,
			usageRatio: 0,
		})
	}

	result := runScanWindowControllerTrace(controller, start, 4*time.Second, maxScanInterval, trace)

	require.Zero(t, countScanWindowDecisionReasons(result.reasons, scanWindowDecisionVeryLowRecovery))
	require.LessOrEqual(t, result.finalInterval, scaleDuration(defaultScanInterval, 5, 4))
}
