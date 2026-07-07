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

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func newTestQuotaSpan(subID SubscriptionID, changefeedID common.ChangeFeedID) *subscribedSpan {
	span := &subscribedSpan{
		subID: subID,
		meta:  NewChangefeedSubscriptionMeta(changefeedID),
	}
	span.resolvedTs.Store(oracle.GoTimeToTS(time.Now()))
	return span
}

func newTestQuotaRegion(span *subscribedSpan) regionInfo {
	state := &regionlock.LockedRangeState{}
	if span != nil {
		state.ResolvedTs.Store(span.resolvedTs.Load())
	}
	return regionInfo{
		subscribedSpan:   span,
		lockedRangeState: state,
	}
}

func setTestQuotaSpanLag(span *subscribedSpan, lag time.Duration) uint64 {
	currentTime := time.Now()
	span.resolvedTs.Store(oracle.GoTimeToTS(currentTime.Add(-lag)))
	return oracle.GoTimeToTS(currentTime)
}

func TestMemoryQuotaAdmissionLevels(t *testing.T) {
	controller := newMemoryQuotaController(100, 0)
	controller.scanEstimate = 10

	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("warming", common.DefaultKeyspaceName))
	normalSpan := newTestQuotaSpan(2, common.NewChangeFeedIDWithName("normal", common.DefaultKeyspaceName))
	normalSpan.initialized.Store(true)
	currentTs := setTestQuotaSpanLag(warmingSpan, defaultWarmingScanLagThreshold+time.Minute)
	normalCurrentTs := setTestQuotaSpanLag(normalSpan, defaultWarmingScanLagThreshold+time.Minute)

	controller.addSubscription(warmingSpan)
	controller.addSubscription(normalSpan)
	controller.markSubscriptionInitialized(normalSpan)

	lease, ok, reason := controller.acquireScan(newTestQuotaRegion(warmingSpan), currentTs)
	require.True(t, ok)
	require.Empty(t, reason)
	lease.Release()

	softLease := controller.trackEvent(context.Background(), normalSpan, 20)
	t.Cleanup(softLease.Release)
	lease, ok, reason = controller.acquireScan(newTestQuotaRegion(warmingSpan), currentTs)
	require.False(t, ok)
	require.Nil(t, lease)
	require.Equal(t, deferReasonMemoryWarming, reason)
	lease, ok, reason = controller.acquireScan(newTestQuotaRegion(normalSpan), normalCurrentTs)
	require.True(t, ok)
	require.Empty(t, reason)
	lease.Release()

	hardLease := controller.trackEvent(context.Background(), normalSpan, 70)
	t.Cleanup(hardLease.Release)
	lease, ok, reason = controller.acquireScan(newTestQuotaRegion(normalSpan), normalCurrentTs)
	require.False(t, ok)
	require.Nil(t, lease)
	require.Equal(t, deferReasonMemoryFreeze, reason)

	hardLease.Release()
	_, _, level := controller.Snapshot()
	require.Equal(t, admissionPauseWarming, level)

	softLease.Release()
	_, _, level = controller.Snapshot()
	require.Equal(t, admissionNormal, level)
}

func TestHighLagUninitializedScanBlockedByMemoryGate(t *testing.T) {
	controller := newMemoryQuotaController(100, 0)
	controller.scanEstimate = 10
	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("warming", common.DefaultKeyspaceName))
	controller.addSubscription(warmingSpan)
	currentTs := setTestQuotaSpanLag(warmingSpan, defaultWarmingScanLagThreshold+time.Minute)
	lease := controller.trackEvent(context.Background(), warmingSpan, 20)
	t.Cleanup(lease.Release)

	region := newTestQuotaRegion(warmingSpan)
	scanLease, ok, reason := controller.acquireScan(region, currentTs)
	require.False(t, ok)
	require.Nil(t, scanLease)
	require.Equal(t, deferReasonMemoryWarming, reason)
}

func TestRemoveSubscriptionReleasesOnlyItsOutstandingMemory(t *testing.T) {
	controller := newMemoryQuotaController(100, 0)
	changefeedID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)
	span1 := newTestQuotaSpan(1, changefeedID)
	span2 := newTestQuotaSpan(2, changefeedID)
	controller.addSubscription(span1)
	controller.addSubscription(span2)

	lease1 := controller.trackEvent(context.Background(), span1, 30)
	lease2 := controller.trackEvent(context.Background(), span2, 40)
	t.Cleanup(lease2.Release)

	used, _, _ := controller.Snapshot()
	require.Equal(t, uint64(70), used)

	controller.removeSubscription(span1)
	used, _, _ = controller.Snapshot()
	require.Equal(t, uint64(40), used)

	lease1.Release()
	used, _, _ = controller.Snapshot()
	require.Equal(t, uint64(40), used)

	lease2.Release()
	used, _, _ = controller.Snapshot()
	require.Equal(t, uint64(0), used)
}

func TestTrackEventBlocksAtHardLimit(t *testing.T) {
	controller := newMemoryQuotaController(100, 0)
	controller.hardLimitRatio = 2
	span := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName))
	controller.addSubscription(span)

	lease := controller.trackEvent(context.Background(), span, 200)
	t.Cleanup(lease.Release)

	acquired := make(chan struct{})
	go func() {
		blockedLease := controller.trackEvent(context.Background(), span, 1)
		blockedLease.Release()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("trackEvent should block at hard limit")
	case <-time.After(100 * time.Millisecond):
	}

	lease.Release()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("trackEvent should resume after memory is released")
	}
}

func TestWarmingScanBudgetLimitsOutstandingScans(t *testing.T) {
	controller := newMemoryQuotaController(150, 0)
	controller.scanEstimate = 10
	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("warming", common.DefaultKeyspaceName))
	controller.addSubscription(warmingSpan)
	currentTs := setTestQuotaSpanLag(warmingSpan, defaultWarmingScanLagThreshold+time.Minute)

	lease1, ok, reason := controller.acquireScan(newTestQuotaRegion(warmingSpan), currentTs)
	require.True(t, ok)
	require.Empty(t, reason)
	t.Cleanup(lease1.Release)
	lease2, ok, reason := controller.acquireScan(newTestQuotaRegion(warmingSpan), currentTs)
	require.True(t, ok)
	require.Empty(t, reason)
	t.Cleanup(lease2.Release)

	lease3, ok, reason := controller.acquireScan(newTestQuotaRegion(warmingSpan), currentTs)
	require.False(t, ok)
	require.Nil(t, lease3)
	require.Equal(t, deferReasonMemoryWarming, reason)

	lease1.Release()
	lease3, ok, reason = controller.acquireScan(newTestQuotaRegion(warmingSpan), currentTs)
	require.True(t, ok)
	require.Empty(t, reason)
	require.NotNil(t, lease3)
	lease3.Release()
}

func TestInitializedSubscriptionBypassesWarmingScanBudget(t *testing.T) {
	controller := newMemoryQuotaController(100, 0)
	controller.scanEstimate = 10
	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName))
	normalSpan := newTestQuotaSpan(2, common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName))
	normalSpan.initialized.Store(true)
	warmingCurrentTs := setTestQuotaSpanLag(warmingSpan, defaultWarmingScanLagThreshold+time.Minute)
	normalCurrentTs := setTestQuotaSpanLag(normalSpan, defaultWarmingScanLagThreshold+time.Minute)
	controller.addSubscription(warmingSpan)
	controller.addSubscription(normalSpan)

	lease := controller.trackEvent(context.Background(), normalSpan, 20)
	t.Cleanup(lease.Release)

	scanLease, ok, reason := controller.acquireScan(newTestQuotaRegion(warmingSpan), warmingCurrentTs)
	require.False(t, ok)
	require.Nil(t, scanLease)
	require.Equal(t, deferReasonMemoryWarming, reason)

	scanLease, ok, reason = controller.acquireScan(newTestQuotaRegion(normalSpan), normalCurrentTs)
	require.True(t, ok)
	require.Empty(t, reason)
	require.NotNil(t, scanLease)
	scanUsed, warmingScanUsed, _, _, _ := controller.ScanSnapshot()
	require.Equal(t, controller.estimateScanSizeLocked(newTestQuotaRegion(normalSpan), normalCurrentTs), scanUsed)
	require.Equal(t, uint64(0), warmingScanUsed)
	scanLease.Release()
}

func TestLowLagUninitializedSubscriptionBypassesWarmingGate(t *testing.T) {
	controller := newMemoryQuotaController(100, 0)
	controller.scanEstimate = 10
	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("warming", common.DefaultKeyspaceName))
	controller.addSubscription(warmingSpan)
	currentTs := setTestQuotaSpanLag(warmingSpan, defaultWarmingScanLagThreshold-time.Second)

	lease := controller.trackEvent(context.Background(), warmingSpan, 20)
	t.Cleanup(lease.Release)

	scanLease, ok, reason := controller.acquireScan(newTestQuotaRegion(warmingSpan), currentTs)
	require.True(t, ok)
	require.Empty(t, reason)
	require.NotNil(t, scanLease)
	scanUsed, warmingScanUsed, _, _, _ := controller.ScanSnapshot()
	require.Greater(t, scanUsed, uint64(0))
	require.Equal(t, uint64(0), warmingScanUsed)
	scanLease.Release()
}

func TestInitializedSubscriptionBlockedByMemoryFreeze(t *testing.T) {
	controller := newMemoryQuotaController(100, 0)
	normalSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName))
	normalSpan.initialized.Store(true)
	controller.addSubscription(normalSpan)

	lease := controller.trackEvent(context.Background(), normalSpan, 90)
	t.Cleanup(lease.Release)

	scanLease, ok, reason := controller.acquireScan(newTestQuotaRegion(normalSpan), 0)
	require.False(t, ok)
	require.Nil(t, scanLease)
	require.Equal(t, deferReasonMemoryFreeze, reason)
}

func TestWarmingScanBudgetKeepsAdmissionWideEnough(t *testing.T) {
	controller := newMemoryQuotaController(defaultLogPullerMemoryQuota, 0)
	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("warming", common.DefaultKeyspaceName))
	controller.addSubscription(warmingSpan)
	currentTs := setTestQuotaSpanLag(warmingSpan, defaultWarmingScanLagThreshold+time.Minute)
	region := newTestQuotaRegion(warmingSpan)
	_, _, warmingScanBudget, scanEstimate, _ := controller.ScanSnapshot()
	require.Equal(t, defaultScanBaseSize, scanEstimate)
	scanSize := controller.estimateScanSizeLocked(region, currentTs)
	allowedScans := int(warmingScanBudget / scanSize)
	require.GreaterOrEqual(t, allowedScans, 17)

	var leases []*memoryQuotaLease
	for range allowedScans {
		lease, ok, reason := controller.acquireScan(region, currentTs)
		require.True(t, ok)
		require.Empty(t, reason)
		leases = append(leases, lease)
	}
	lease, ok, reason := controller.acquireScan(region, currentTs)
	require.False(t, ok)
	require.Nil(t, lease)
	require.Equal(t, deferReasonMemoryWarming, reason)

	for _, lease := range leases {
		lease.Release()
	}
}
