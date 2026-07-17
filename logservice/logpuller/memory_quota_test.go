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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func newTestQuotaSpan(subID SubscriptionID) *subscribedSpan {
	span := &subscribedSpan{subID: subID}
	span.resolvedTs.Store(oracle.GoTimeToTS(time.Now()))
	return span
}

func newTestQuotaRegion(span *subscribedSpan) regionInfo {
	state := &regionlock.LockedRangeState{}
	state.ResolvedTs.Store(span.resolvedTs.Load())
	return regionInfo{
		subscribedSpan:   span,
		lockedRangeState: state,
	}
}

func setTestQuotaSpanLag(span *subscribedSpan, lag time.Duration) uint64 {
	now := time.Now()
	span.resolvedTs.Store(oracle.GoTimeToTS(now.Add(-lag)))
	return oracle.GoTimeToTS(now)
}

func TestMemoryQuotaAdmissionLevels(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	warmingSpan := newTestQuotaSpan(1)
	initializedSpan := newTestQuotaSpan(2)
	initializedSpan.initialized.Store(true)
	warmingTs := setTestQuotaSpanLag(warmingSpan, lowLagRegionThreshold+time.Minute)
	initializedTs := setTestQuotaSpanLag(initializedSpan, lowLagRegionThreshold+time.Minute)
	quota.addSubscription(warmingSpan)
	quota.addSubscription(initializedSpan)

	lowLease := quota.trackEvent(context.Background(), initializedSpan, 5)
	pauseLease := quota.trackEvent(context.Background(), initializedSpan, 10)
	require.NotNil(t, lowLease)
	require.NotNil(t, pauseLease)
	scanLease, admitted := quota.acquireScan(newTestQuotaRegion(warmingSpan), warmingTs)
	require.False(t, admitted)
	require.Nil(t, scanLease)

	scanLease, admitted = quota.acquireScan(
		newTestQuotaRegion(initializedSpan), initializedTs)
	require.True(t, admitted)
	scanLease.Release()

	middleLease := quota.trackEvent(context.Background(), initializedSpan, 45)
	freezeLease := quota.trackEvent(context.Background(), initializedSpan, 20)
	require.NotNil(t, middleLease)
	require.NotNil(t, freezeLease)
	scanLease, admitted = quota.acquireScan(
		newTestQuotaRegion(initializedSpan), initializedTs)
	require.False(t, admitted)
	require.Nil(t, scanLease)

	freezeLease.Release()
	_, _, level := quota.snapshot()
	require.Equal(t, admissionPauseWarming, level)
	middleLease.Release()
	_, _, level = quota.snapshot()
	require.Equal(t, admissionPauseWarming, level)
	pauseLease.Release()
	_, _, level = quota.snapshot()
	require.Equal(t, admissionNormal, level)
	lowLease.Release()
}

func TestMemoryQuotaRemoveSubscriptionReleasesOwnedMemory(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span1 := newTestQuotaSpan(1)
	span2 := newTestQuotaSpan(2)
	quota.addSubscription(span1)
	quota.addSubscription(span2)

	lease1 := quota.trackEvent(context.Background(), span1, 30)
	lease2 := quota.trackEvent(context.Background(), span2, 40)
	require.NotNil(t, lease1)
	require.NotNil(t, lease2)
	scanLease, admitted := quota.acquireScan(newTestQuotaRegion(span1), span1.resolvedTs.Load())
	require.True(t, admitted)
	require.NotNil(t, scanLease)

	quota.removeSubscription(span1)
	used, _, _ := quota.snapshot()
	require.Equal(t, uint64(40), used)
	scanUsed, _, _, _, _ := quota.scanSnapshot()
	require.Zero(t, scanUsed)
	require.NotContains(t, quota.subscriptions, span1.subID)
	quota.removeSubscription(span1)

	lease1.Release()
	scanLease.Release()
	used, _, _ = quota.snapshot()
	require.Equal(t, uint64(40), used)

	// Late region tasks are allowed to reach the stopped-subscription cleanup
	// path without recreating quota state.
	scanLease, admitted = quota.acquireScan(newTestQuotaRegion(span1), span1.resolvedTs.Load())
	require.True(t, admitted)
	require.Nil(t, scanLease)
	require.NotContains(t, quota.subscriptions, span1.subID)

	lease2.Release()
	used, _, _ = quota.snapshot()
	require.Zero(t, used)
}

func TestMemoryQuotaBlockedEventStopsWhenSubscriptionIsRemoved(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	quota.hardLimitRatio = 1
	span := newTestQuotaSpan(1)
	quota.addSubscription(span)

	lease := quota.trackEvent(context.Background(), span, 100)
	require.NotNil(t, lease)
	acquired := make(chan *memoryQuotaLease, 1)
	go func() {
		acquired <- quota.trackEvent(context.Background(), span, 1)
	}()

	select {
	case <-acquired:
		t.Fatal("event memory should wait at the hard limit")
	case <-time.After(100 * time.Millisecond):
	}

	quota.removeSubscription(span)
	select {
	case blockedLease := <-acquired:
		require.Nil(t, blockedLease)
	case <-time.After(time.Second):
		t.Fatal("removing the subscription did not wake the blocked event")
	}
	require.NotContains(t, quota.subscriptions, span.subID)
	lease.Release()
}

func TestMemoryQuotaBlockedEventResumesAfterRelease(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)
	quota.addSubscription(span)

	lease := quota.trackEvent(context.Background(), span, 200)
	require.NotNil(t, lease)
	acquired := make(chan *memoryQuotaLease, 1)
	go func() {
		acquired <- quota.trackEvent(context.Background(), span, 1)
	}()

	select {
	case <-acquired:
		t.Fatal("event memory should wait at the hard limit")
	case <-time.After(100 * time.Millisecond):
	}

	lease.Release()
	select {
	case nextLease := <-acquired:
		require.NotNil(t, nextLease)
		nextLease.Release()
	case <-time.After(time.Second):
		t.Fatal("event memory did not resume after memory was released")
	}
}

func TestMemoryQuotaWarmingScanBudget(t *testing.T) {
	quota := newMemoryQuotaController(200, 10)
	span := newTestQuotaSpan(1)
	currentTs := setTestQuotaSpanLag(span, lowLagRegionThreshold+time.Minute)
	quota.addSubscription(span)
	region := newTestQuotaRegion(span)

	lease1, admitted := quota.acquireScan(region, currentTs)
	require.True(t, admitted)
	lease2, admitted := quota.acquireScan(region, currentTs)
	require.True(t, admitted)

	lease3, admitted := quota.acquireScan(region, currentTs)
	require.False(t, admitted)
	require.Nil(t, lease3)

	lease1.Release()
	lease3, admitted = quota.acquireScan(region, currentTs)
	require.True(t, admitted)
	lease2.Release()
	lease3.Release()
}

func TestMemoryQuotaLowLagScanBypassesWarmingGate(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)
	currentTs := setTestQuotaSpanLag(span, lowLagRegionThreshold-time.Second)
	quota.addSubscription(span)

	pressureLease := quota.trackEvent(context.Background(), span, 20)
	require.NotNil(t, pressureLease)
	scanLease, admitted := quota.acquireScan(newTestQuotaRegion(span), currentTs)
	require.True(t, admitted)
	require.NotNil(t, scanLease)
	_, warmingScanUsed, _, _, _ := quota.scanSnapshot()
	require.Zero(t, warmingScanUsed)

	scanLease.Release()
	pressureLease.Release()
}

func TestMemoryQuotaRemovalNotifiesAdmissionWithoutLeases(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)
	quota.addSubscription(span)

	notified := make(chan struct{}, 1)
	quota.setOnAvailable(func() {
		select {
		case notified <- struct{}{}:
		default:
		}
	})
	quota.removeSubscription(span)
	select {
	case <-notified:
	case <-time.After(time.Second):
		t.Fatal("subscription removal did not notify admission")
	}
}

func TestAdmissionWaitsForMemoryAndReleasesScanLease(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)
	currentTs := setTestQuotaSpanLag(span, lowLagRegionThreshold+time.Minute)
	quota.addSubscription(span)
	controller := newRegionAdmissionController(1, 1, quota, func() uint64 {
		return currentTs
	})
	quota.setOnAvailable(controller.notifyAvailable)

	pressureLease := quota.trackEvent(context.Background(), span, 20)
	require.NotNil(t, pressureLease)
	region := newTestQuotaRegion(span)
	require.True(t, controller.submit(newRegionPriorityTask(region, currentTs, 1)))

	type popResult struct {
		req *regionReq
		err error
	}
	result := make(chan popResult, 1)
	go func() {
		req, err := controller.pop(context.Background(), nil)
		result <- popResult{req: req, err: err}
	}()
	select {
	case <-result:
		t.Fatal("warming scan should wait while memory is under pressure")
	case <-time.After(100 * time.Millisecond):
	}

	pressureLease.Release()
	var resultValue popResult
	select {
	case resultValue = <-result:
	case <-time.After(time.Second):
		t.Fatal("scan admission was not notified after memory became available")
	}
	require.NoError(t, resultValue.err)
	req := resultValue.req
	scanUsed, _, _, _, _ := quota.scanSnapshot()
	require.NotZero(t, scanUsed)
	require.True(t, req.abort())
	scanUsed, _, _, _, _ = quota.scanSnapshot()
	require.Zero(t, scanUsed)
}
