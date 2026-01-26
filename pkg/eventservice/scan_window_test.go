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
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestAdjustScanIntervalVeryLowBypassesSyncPointCap(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"))
	status.syncPointEnabled.Store(true)
	status.syncPointInterval.Store(int64(1 * time.Minute))

	now := time.Now()
	status.lastAdjustTime.Store(now.Add(-scanIntervalAdjustCooldown - time.Second))

	// Start from the sync point capped max interval, then allow it to grow slowly.
	status.scanInterval.Store(int64(1 * time.Minute))

	// Maintain a very low pressure for a full window to allow bypassing the sync point cap.
	for i := 0; i <= int(memoryUsageWindowDuration/time.Second); i++ {
		status.updateMemoryUsage(now.Add(time.Duration(i)*time.Second), 0, 100, 100)
	}
	require.Equal(t, int64(90*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalLowRespectsSyncPointCap(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"))
	status.syncPointEnabled.Store(true)
	status.syncPointInterval.Store(int64(1 * time.Minute))

	now := time.Now()
	status.lastAdjustTime.Store(now.Add(-scanIntervalAdjustCooldown - time.Second))

	status.scanInterval.Store(int64(40 * time.Second))

	for i := 0; i <= int(memoryUsageWindowDuration/time.Second); i++ {
		status.updateMemoryUsage(now.Add(time.Duration(i)*time.Second), 15, 100, 100)
	}
	require.Equal(t, int64(50*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalDecreaseIgnoresCooldown(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"))
	now := time.Now()
	status.lastAdjustTime.Store(now)

	status.scanInterval.Store(int64(40 * time.Second))
	status.updateMemoryUsage(now.Add(memoryUsageWindowDuration), 80, 100, 100)
	require.Equal(t, int64(20*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalUsesAvailableAsPressureSignal(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"))
	now := time.Now()
	status.lastAdjustTime.Store(now)

	status.scanInterval.Store(int64(40 * time.Second))

	// used/max suggests low pressure, but available indicates full.
	status.updateMemoryUsage(now.Add(memoryUsageWindowDuration), 10, 100, 0)
	require.Equal(t, int64(10*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalDecreasesWhenUsageIncreasing(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"))
	now := time.Now()
	status.lastAdjustTime.Store(now)

	status.scanInterval.Store(int64(40 * time.Second))

	status.updateMemoryUsage(now, 10, 100, 100)
	status.updateMemoryUsage(now.Add(1*time.Second), 11, 100, 100)
	status.updateMemoryUsage(now.Add(2*time.Second), 12, 100, 100)
	status.updateMemoryUsage(now.Add(3*time.Second), 13, 100, 100)
	require.Equal(t, int64(40*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalDecreasesWhenUsageIncreasingAboveThirtyPercent(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"))
	now := time.Now()
	status.lastAdjustTime.Store(now)
	status.lastTrendAdjustTime.Store(now.Add(-scanTrendAdjustCooldown - time.Second))

	status.scanInterval.Store(int64(40 * time.Second))

	status.updateMemoryUsage(now, 31, 100, 100)
	status.updateMemoryUsage(now.Add(1*time.Second), 32, 100, 100)
	status.updateMemoryUsage(now.Add(2*time.Second), 33, 100, 100)
	status.updateMemoryUsage(now.Add(3*time.Second), 34, 100, 100)
	require.Equal(t, int64(36*time.Second), status.scanInterval.Load())
}
