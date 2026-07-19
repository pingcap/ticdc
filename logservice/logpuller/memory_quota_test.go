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

	require.True(t, quota.acquireEvent(context.Background(), initializedSpan, 5))
	require.True(t, quota.acquireEvent(context.Background(), initializedSpan, 10))
	_, _, admitted := quota.acquireScan(newTestQuotaRegion(warmingSpan), warmingTs)
	require.False(t, admitted)

	scanBytes, _, admitted := quota.acquireScan(
		newTestQuotaRegion(initializedSpan), initializedTs)
	require.True(t, admitted)
	quota.releaseScan(scanBytes)

	require.True(t, quota.acquireEvent(context.Background(), initializedSpan, 45))
	require.True(t, quota.acquireEvent(context.Background(), initializedSpan, 20))
	_, _, admitted = quota.acquireScan(
		newTestQuotaRegion(initializedSpan), initializedTs)
	require.False(t, admitted)

	quota.releaseEvent(20)
	_, _, level := quota.snapshot()
	require.Equal(t, admissionPauseWarming, level)
	quota.releaseEvent(45)
	_, _, level = quota.snapshot()
	require.Equal(t, admissionPauseWarming, level)
	quota.releaseEvent(10)
	_, _, level = quota.snapshot()
	require.Equal(t, admissionNormal, level)
	quota.releaseEvent(5)
}

func TestMemoryQuotaSpanStopKeepsOwnedMemoryUntilRelease(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span1 := newTestQuotaSpan(1)
	span2 := newTestQuotaSpan(2)

	require.True(t, quota.acquireEvent(context.Background(), span1, 30))
	require.True(t, quota.acquireEvent(context.Background(), span2, 40))
	scanBytes, _, admitted := quota.acquireScan(
		newTestQuotaRegion(span1), span1.resolvedTs.Load())
	require.True(t, admitted)
	require.NotZero(t, scanBytes)

	span1.stopped.Store(true)
	quota.wakeAll()
	used, _, _ := quota.snapshot()
	require.Equal(t, uint64(70), used)
	scanUsed, _, _ := quota.scanSnapshot()
	require.Equal(t, scanBytes, scanUsed)

	quota.releaseEvent(30)
	quota.releaseScan(scanBytes)
	used, _, _ = quota.snapshot()
	require.Equal(t, uint64(40), used)

	// Late tasks reach the stopped-subscription cleanup path without consuming
	// scan quota.
	scanBytes, _, admitted = quota.acquireScan(
		newTestQuotaRegion(span1), span1.resolvedTs.Load())
	require.True(t, admitted)
	require.Zero(t, scanBytes)

	quota.releaseEvent(40)
	used, _, _ = quota.snapshot()
	require.Zero(t, used)
}

func TestMemoryQuotaBlockedEventStopsWhenSpanStops(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	quota.hardLimit = 100
	span := newTestQuotaSpan(1)

	require.True(t, quota.acquireEvent(context.Background(), span, 100))
	acquired := make(chan bool, 1)
	go func() {
		acquired <- quota.acquireEvent(context.Background(), span, 1)
	}()

	select {
	case <-acquired:
		t.Fatal("event memory should wait at the hard limit")
	case <-time.After(100 * time.Millisecond):
	}

	span.stopped.Store(true)
	quota.wakeAll()
	select {
	case ok := <-acquired:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("stopping the subscription did not wake the blocked event")
	}
	quota.releaseEvent(100)
}

func TestMemoryQuotaBlockedEventResumesAfterRelease(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)

	require.True(t, quota.acquireEvent(context.Background(), span, 200))
	acquired := make(chan bool, 1)
	go func() {
		acquired <- quota.acquireEvent(context.Background(), span, 1)
	}()

	select {
	case <-acquired:
		t.Fatal("event memory should wait at the hard limit")
	case <-time.After(100 * time.Millisecond):
	}

	quota.releaseEvent(200)
	select {
	case ok := <-acquired:
		require.True(t, ok)
		quota.releaseEvent(1)
	case <-time.After(time.Second):
		t.Fatal("event memory did not resume after memory was released")
	}
}

func TestMemoryQuotaBlockedEventStopsOnContextCancellation(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	quota.hardLimit = 100
	span := newTestQuotaSpan(1)
	ctx, cancel := context.WithCancel(context.Background())

	require.True(t, quota.acquireEvent(context.Background(), span, 100))
	acquired := make(chan bool, 1)
	go func() {
		acquired <- quota.acquireEvent(ctx, span, 1)
	}()
	require.Eventually(t, func() bool {
		return quota.eventNotifier.waiters.Load() == 1
	}, time.Second, time.Millisecond)

	// Cancellation must stop the waiter without a memory release or an explicit
	// quota notification.
	cancel()
	select {
	case ok := <-acquired:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not stop the blocked event")
	}
	quota.releaseEvent(100)
}

func TestMemoryQuotaConcurrentWaitersDoNotLoseWakeups(t *testing.T) {
	const waiterCount = 32
	quota := newMemoryQuotaController(100, 10)
	quota.hardLimit = 1
	span := newTestQuotaSpan(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Hold the only available byte until every goroutine is waiting. Releasing
	// it wakes all waiters; each successful waiter then releases it for the next.
	require.True(t, quota.acquireEvent(ctx, span, 1))
	results := make(chan bool, waiterCount)
	for range waiterCount {
		go func() {
			acquired := quota.acquireEvent(ctx, span, 1)
			if acquired {
				quota.releaseEvent(1)
			}
			results <- acquired
		}()
	}
	require.Eventually(t, func() bool {
		return quota.eventNotifier.waiters.Load() == waiterCount
	}, time.Second, time.Millisecond)

	quota.releaseEvent(1)
	for range waiterCount {
		select {
		case acquired := <-results:
			require.True(t, acquired)
		case <-ctx.Done():
			t.Fatal("event waiter did not make progress")
		}
	}
	used, _, _ := quota.snapshot()
	require.Zero(t, used)
}

func TestMemoryQuotaWarmingScanUsesCurrentPressure(t *testing.T) {
	quota := newMemoryQuotaController(100, 20)
	span := newTestQuotaSpan(1)
	currentTs := setTestQuotaSpanLag(span, lowLagRegionThreshold+time.Minute)
	region := newTestQuotaRegion(span)

	bytes1, _, admitted := quota.acquireScan(region, currentTs)
	require.True(t, admitted)
	require.NotZero(t, bytes1)
	scanUsed, _, _ := quota.scanSnapshot()
	require.Greater(t, scanUsed, quota.pauseWarmingLimit)

	_, _, admitted = quota.acquireScan(region, currentTs)
	require.False(t, admitted)

	quota.releaseScan(bytes1)
	bytes2, _, admitted := quota.acquireScan(region, currentTs)
	require.True(t, admitted)
	quota.releaseScan(bytes2)
}

func TestMemoryQuotaLowLagScanBypassesWarmingGate(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)
	currentTs := setTestQuotaSpanLag(span, lowLagRegionThreshold-time.Second)

	require.True(t, quota.acquireEvent(context.Background(), span, 20))
	scanBytes, _, admitted := quota.acquireScan(newTestQuotaRegion(span), currentTs)
	require.True(t, admitted)
	require.NotZero(t, scanBytes)
	scanUsed, _, _ := quota.scanSnapshot()
	require.NotZero(t, scanUsed)

	quota.releaseScan(scanBytes)
	quota.releaseEvent(20)
}

func TestAdmissionWaitsForMemoryAndReleasesScanMemory(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)
	currentTs := setTestQuotaSpanLag(span, lowLagRegionThreshold+time.Minute)
	controller := newRegionAdmissionController(1, 1, quota, func() uint64 {
		return currentTs
	})

	require.True(t, quota.acquireEvent(context.Background(), span, 20))
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

	quota.releaseEvent(20)
	var resultValue popResult
	select {
	case resultValue = <-result:
	case <-time.After(time.Second):
		t.Fatal("scan admission was not notified after memory became available")
	}
	require.NoError(t, resultValue.err)
	req := resultValue.req
	scanUsed, _, _ := quota.scanSnapshot()
	require.NotZero(t, scanUsed)
	require.True(t, req.abort())
	scanUsed, _, _ = quota.scanSnapshot()
	require.Zero(t, scanUsed)
}

func TestAdmissionWakesWhenBlockedSpanStops(t *testing.T) {
	quota := newMemoryQuotaController(100, 10)
	span := newTestQuotaSpan(1)
	currentTs := setTestQuotaSpanLag(span, lowLagRegionThreshold+time.Minute)
	controller := newRegionAdmissionController(1, 1, quota, func() uint64 {
		return currentTs
	})

	require.True(t, quota.acquireEvent(context.Background(), span, 20))
	require.True(t, controller.submit(newRegionPriorityTask(
		newTestQuotaRegion(span), currentTs, 1)))

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

	span.stopped.Store(true)
	quota.wakeAll()
	select {
	case result := <-result:
		require.NoError(t, result.err)
		require.Zero(t, result.req.scanBytes)
		require.True(t, result.req.abort())
	case <-time.After(time.Second):
		t.Fatal("stopping the span did not wake scan admission")
	}
	quota.releaseEvent(20)
}

func BenchmarkMemoryQuotaEventAccounting(b *testing.B) {
	quota := newMemoryQuotaController(defaultLogPullerMemoryQuota, defaultScanBaseSize)
	span := newTestQuotaSpan(1)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if !quota.acquireEvent(ctx, span, 1) {
			b.Fatal("failed to acquire event memory")
		}
		quota.releaseEvent(1)
	}
}
