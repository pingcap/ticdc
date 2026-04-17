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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
)

func TestAdjustScanIntervalVeryLowBypassesSyncPointCap(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)

	now := time.Now()
	status.lastAdjustTime.Store(now.Add(-scanIntervalAdjustCooldown - time.Second))

	// Start from the sync point capped max interval, then allow it to grow slowly.
	status.scanInterval.Store(int64(1 * time.Minute))

	// Maintain a very low pressure for a full window to allow bypassing the sync point cap.
	for i := 0; i <= int(memoryUsageWindowDuration/time.Second); i++ {
		status.updateMemoryUsage(now.Add(time.Duration(i)*time.Second), 0, 0)
	}
	require.Equal(t, int64(90*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalLowRespectsSyncPointCap(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)

	now := time.Now()
	status.lastAdjustTime.Store(now.Add(-scanIntervalAdjustCooldown - time.Second))

	status.scanInterval.Store(int64(40 * time.Second))

	for i := 0; i <= int(memoryUsageWindowDuration/time.Second); i++ {
		status.updateMemoryUsage(now.Add(time.Duration(i)*time.Second), 0.15, 0)
	}
	require.Equal(t, int64(50*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalDecreaseIgnoresCooldown(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)
	now := time.Now()
	status.lastAdjustTime.Store(now)

	status.scanInterval.Store(int64(40 * time.Second))
	status.updateMemoryUsage(now.Add(memoryUsageWindowDuration), 0.8, 0)
	require.Equal(t, int64(20*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalCriticalPressure(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)
	now := time.Now()
	status.lastAdjustTime.Store(now)

	status.scanInterval.Store(int64(40 * time.Second))

	status.updateMemoryUsage(now.Add(memoryUsageWindowDuration), 1, 0)
	require.Equal(t, int64(10*time.Second), status.scanInterval.Load())
}

func TestUpdateMemoryUsageResetsScanIntervalOnMemoryRelease(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)
	now := time.Now()
	status.scanInterval.Store(int64(40 * time.Second))

	status.updateMemoryUsage(now, 0.5, 1)
	require.Equal(t, int64(defaultScanInterval), status.scanInterval.Load())
}

func TestAdjustScanIntervalIncreaseWithJitteredSamples(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)

	start := time.Now()
	status.lastAdjustTime.Store(start.Add(-scanIntervalAdjustCooldown - time.Second))

	status.scanInterval.Store(int64(40 * time.Second))

	// Use a >1s interval to simulate heartbeat jitter, so the window span will be
	// slightly less than memoryUsageWindowDuration.
	step := 1100 * time.Millisecond
	for i := 0; i < 28; i++ {
		status.updateMemoryUsage(start.Add(time.Duration(i)*step), 0.15, 0)
	}
	require.Equal(t, int64(50*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalDecreasesWhenUsageIncreasing(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)
	now := time.Now()
	status.lastAdjustTime.Store(now)

	status.scanInterval.Store(int64(40 * time.Second))

	status.updateMemoryUsage(now, 0.10, 0)
	status.updateMemoryUsage(now.Add(1*time.Second), 0.11, 0)
	status.updateMemoryUsage(now.Add(2*time.Second), 0.12, 0)
	status.updateMemoryUsage(now.Add(3*time.Second), 0.13, 0)
	require.Equal(t, int64(40*time.Second), status.scanInterval.Load())
}

func TestAdjustScanIntervalDecreasesWhenUsageIncreasingAboveThirtyPercent(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)
	now := time.Now()
	status.lastAdjustTime.Store(now)
	status.lastTrendAdjustTime.Store(now.Add(-scanTrendAdjustCooldown - time.Second))

	status.scanInterval.Store(int64(40 * time.Second))

	status.updateMemoryUsage(now, 0.31, 0)
	status.updateMemoryUsage(now.Add(1*time.Second), 0.32, 0)
	status.updateMemoryUsage(now.Add(2*time.Second), 0.33, 0)
	status.updateMemoryUsage(now.Add(3*time.Second), 0.34, 0)
	require.Equal(t, int64(36*time.Second), status.scanInterval.Load())
}

func TestRefreshMinSentResolvedTsMinAndSkipRules(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)

	stale := &dispatcherStat{}
	stale.seq.Store(1)
	stale.sentResolvedTs.Store(10)
	stale.checkpointTs.Store(10)
	stale.lastReceivedHeartbeatTime.Store(time.Now().Add(-scanWindowStaleDispatcherHeartbeatThreshold - time.Second).Unix())

	removed := &dispatcherStat{}
	removed.seq.Store(1)
	removed.sentResolvedTs.Store(150)
	removed.isRemoved.Store(true)

	uninitialized := &dispatcherStat{}
	uninitialized.seq.Store(0)
	uninitialized.sentResolvedTs.Store(10)

	first := &dispatcherStat{}
	first.seq.Store(1)
	first.sentResolvedTs.Store(200)
	first.checkpointTs.Store(200)

	second := &dispatcherStat{}
	second.seq.Store(1)
	second.sentResolvedTs.Store(50)
	second.checkpointTs.Store(50)

	stalePtr := &atomic.Pointer[dispatcherStat]{}
	stalePtr.Store(stale)
	status.addDispatcher(common.NewDispatcherID(), stalePtr)

	removedPtr := &atomic.Pointer[dispatcherStat]{}
	removedPtr.Store(removed)
	status.addDispatcher(common.NewDispatcherID(), removedPtr)

	uninitializedPtr := &atomic.Pointer[dispatcherStat]{}
	uninitializedPtr.Store(uninitialized)
	status.addDispatcher(common.NewDispatcherID(), uninitializedPtr)

	firstPtr := &atomic.Pointer[dispatcherStat]{}
	firstPtr.Store(first)
	status.addDispatcher(common.NewDispatcherID(), firstPtr)

	secondPtr := &atomic.Pointer[dispatcherStat]{}
	secondPtr.Store(second)
	status.addDispatcher(common.NewDispatcherID(), secondPtr)

	status.refreshMinSentResolvedTs()
	require.Equal(t, uint64(50), status.minSentTs.Load())
	minCheckpointTs, ok := status.getMinCheckpointTs()
	require.True(t, ok)
	require.Equal(t, uint64(50), minCheckpointTs)

	second.isRemoved.Store(true)
	status.refreshMinSentResolvedTs()
	require.Equal(t, uint64(200), status.minSentTs.Load())
	minCheckpointTs, ok = status.getMinCheckpointTs()
	require.True(t, ok)
	require.Equal(t, uint64(200), minCheckpointTs)

	stale.isRemoved.Store(true)
	first.seq.Store(0)
	status.refreshMinSentResolvedTs()
	require.Equal(t, uint64(0), status.minSentTs.Load())
	_, ok = status.getMinCheckpointTs()
	require.False(t, ok)
}

func TestRefreshMinSentResolvedTsStaleFallback(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)

	stale := &dispatcherStat{}
	stale.seq.Store(1)
	stale.sentResolvedTs.Store(123)
	stale.checkpointTs.Store(123)
	stale.lastReceivedHeartbeatTime.Store(time.Now().Add(-scanWindowStaleDispatcherHeartbeatThreshold - time.Second).Unix())

	stalePtr := &atomic.Pointer[dispatcherStat]{}
	stalePtr.Store(stale)
	status.addDispatcher(common.NewDispatcherID(), stalePtr)

	status.refreshMinSentResolvedTs()
	require.Equal(t, uint64(123), status.minSentTs.Load())
	_, ok := status.getMinCheckpointTs()
	require.False(t, ok)
}

func TestGetScanMaxTsFallbackInterval(t *testing.T) {
	t.Parallel()

	status := newChangefeedStatus(common.NewChangefeedID4Test("default", "test"), 1*time.Minute)

	baseTime := time.Unix(1234, 0)
	baseTs := oracle.GoTimeToTS(baseTime)
	status.minSentTs.Store(baseTs)

	status.scanInterval.Store(0)
	require.Equal(t, oracle.GoTimeToTS(baseTime.Add(defaultScanInterval)), status.getScanMaxTs())

	status.scanInterval.Store(int64(10 * time.Second))
	require.Equal(t, oracle.GoTimeToTS(baseTime.Add(10*time.Second)), status.getScanMaxTs())

	status.minSentTs.Store(0)
	require.Equal(t, uint64(0), status.getScanMaxTs())
}
