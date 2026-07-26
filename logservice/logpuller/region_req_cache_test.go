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

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func createTestRegionInfo(subID SubscriptionID, regionID uint64) regionInfo {
	verID := tikv.NewRegionVerID(regionID, 1, 1)

	span := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
	}

	subscribedSpan := &subscribedSpan{
		subID:   subID,
		startTs: 100,
		span:    span,
	}

	region := newRegionInfo(verID, span, nil, subscribedSpan, false)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	return region
}

func TestRequestCacheAdd_NormalCase(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, cache.getPendingCount())

	// Verify the request was added to the queue
	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, region.verID.GetID(), req.regionInfo.verID.GetID())
	require.Equal(t, region.subscribedSpan.subID, req.regionInfo.subscribedSpan.subID)
}

func TestRequestCacheAdd_ForceFlag(t *testing.T) {
	cache := newRequestCache(1)
	ctx := context.Background()

	// Fill up the cache
	region1 := createTestRegionInfo(1, 1)
	ok, err := cache.add(ctx, region1, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, cache.getPendingCount())

	// Try to add another request without force - should fail due to retry limit
	region2 := createTestRegionInfo(1, 2)
	ok, err = cache.add(ctx, region2, false)
	require.False(t, ok)
	require.NoError(t, err)

	// Move the normal request to sentRequests so the pending queue has room.
	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, region1.verID.GetID(), req.regionInfo.verID.GetID())
	require.Equal(t, region1.subscribedSpan.subID, req.regionInfo.subscribedSpan.subID)
	cache.markSent(req)
	require.Equal(t, 1, cache.getPendingCount())

	// A forced data request can use one extra slot.
	region3 := createTestRegionInfo(1, 3)
	ok, err = cache.add(ctx, region3, true)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())

	// No additional forced data request can exceed the N+1 ceiling.
	req, err = cache.pop(ctx)
	require.NoError(t, err)
	cache.markSent(req)
	region4 := createTestRegionInfo(1, 4)
	ok, err = cache.add(ctx, region4, true)
	require.False(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())

	// Stop/control requests keep their existing bypass and remain accounted.
	stopRegion := createTestRegionInfo(2, 5)
	stopRegion.lockedRangeState = nil
	ok, err = cache.add(ctx, stopRegion, true)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 3, cache.getPendingCount())

	stopReq, err := cache.pop(ctx)
	require.NoError(t, err)
	cache.markSent(stopReq)
	cache.markStopped(stopReq.regionInfo.subscribedSpan.subID, stopReq.regionInfo.verID.GetID())
	require.Equal(t, 2, cache.getPendingCount())

	require.True(t, cache.resolve(region1.subscribedSpan.subID, region1.verID.GetID()))
	require.Equal(t, 1, cache.getPendingCount())
	ok, err = cache.add(ctx, region4, true)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())
}

func TestRequestCacheAddRollsBackReservedSlot(t *testing.T) {
	cache := newRequestCache(1)
	cache.pendingQueue <- newRegionReq(createTestRegionInfo(1, 1))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err := cache.add(ctx, createTestRegionInfo(1, 2), false)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheConcurrentForcedAddsStayWithinCeiling(t *testing.T) {
	const normalLimit = 10
	cache := newRequestCache(normalLimit)
	ctx := context.Background()

	for i := range normalLimit {
		ok, err := cache.add(ctx, createTestRegionInfo(1, uint64(i+1)), false)
		require.True(t, ok)
		require.NoError(t, err)
	}
	for range normalLimit {
		req, err := cache.pop(ctx)
		require.NoError(t, err)
		cache.markSent(req)
	}

	const addCount = 20
	results := make(chan bool, addCount)
	for i := range addCount {
		go func(regionID uint64) {
			ok, err := cache.add(ctx, createTestRegionInfo(1, regionID), true)
			require.NoError(t, err)
			results <- ok
		}(uint64(normalLimit + i + 1))
	}

	successes := 0
	for range addCount {
		if <-results {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, normalLimit+1, cache.getPendingCount())
}

func TestRequestCacheAdd_ContextCancellation(t *testing.T) {
	cache := newRequestCache(1)

	// Fill up the cache
	region1 := createTestRegionInfo(1, 1)
	ctx1 := context.Background()
	ok, err := cache.add(ctx1, region1, false)
	require.True(t, ok)
	require.NoError(t, err)

	// Try to add another request with a cancelled context
	ctx2, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	region2 := createTestRegionInfo(1, 2)
	ok, err = cache.add(ctx2, region2, false)
	require.False(t, ok)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestRequestCacheAdd_RetryLimitExceeded(t *testing.T) {
	cache := newRequestCache(1)
	ctx := context.Background()

	// Fill up the cache
	region1 := createTestRegionInfo(1, 1)
	ok, err := cache.add(ctx, region1, false)
	require.True(t, ok)
	require.NoError(t, err)

	// Try to add another request - should eventually hit retry limit
	region2 := createTestRegionInfo(1, 2)
	ok, err = cache.add(ctx, region2, false)
	require.False(t, ok)
	require.NoError(t, err)
}

func TestRequestCacheAdd_SpaceAvailableNotification(t *testing.T) {
	cache := newRequestCache(2)
	ctx := context.Background()

	// Fill up the cache
	region1 := createTestRegionInfo(1, 1)
	ok, err := cache.add(ctx, region1, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, cache.getPendingCount())

	region2 := createTestRegionInfo(1, 2)
	ok, err = cache.add(ctx, region2, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())

	// Pop a request and mark it as sent, then resolve it to free up space
	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, 2, cache.getPendingCount()) // pop doesn't change pendingCount
	// Mark as sent
	cache.markSent(req)
	require.Equal(t, 2, cache.getPendingCount())

	// Resolve the request to free up space
	success := cache.resolve(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
	require.True(t, success)
	require.Equal(t, 1, cache.getPendingCount())

	// Now we should be able to add another request
	region3 := createTestRegionInfo(1, 3)
	ok, err = cache.add(ctx, region3, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())
}

func TestRequestCacheAdd_ConcurrentAdds(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	const numGoroutines = 5
	done := make(chan error, numGoroutines)

	// Start multiple goroutines adding requests concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			region := createTestRegionInfo(SubscriptionID(id%3), uint64(id))
			ok, err := cache.add(ctx, region, false)
			require.True(t, ok)
			require.NoError(t, err)
			done <- err
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for concurrent adds to complete")
		}
	}

	require.Equal(t, numGoroutines, cache.getPendingCount())
}

func TestRequestCacheAdd_StaleRequestCleanup(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	// Add a request and mark it as sent
	region := createTestRegionInfo(1, 1)
	ok, err := cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, req)

	// Mark as sent
	cache.markSent(req)
	require.Equal(t, 1, cache.getPendingCount())

	// Manually set the request as stale by modifying createTime
	cache.sentRequests.Lock()
	regionReqs := cache.sentRequests.regionReqs[req.regionInfo.subscribedSpan.subID]
	regionReqs[req.regionInfo.verID.GetID()] = regionReq{
		regionInfo: req.regionInfo,
		createTime: time.Now().Add(-requestGCLifeTime - time.Second), // Make it stale
	}
	cache.sentRequests.Unlock()

	// Manually set lastCheckStaleRequestTime to bypass the time interval check
	cache.lastCheckStaleRequestTime.Store(time.Now().Add(-checkStaleRequestInterval - time.Second))

	// Manually trigger stale cleanup by calling clearStaleRequest
	cache.clearStaleRequest()

	// The stale request should be cleaned up
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheAdd_WithStoppedRegion(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	// Create a region info with stopped state (lockedRangeState = nil)
	region := createTestRegionInfo(1, 1)
	region.lockedRangeState = nil // This makes it stopped

	ok, err := cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, cache.getPendingCount())

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, req)

	// Mark as sent
	cache.markSent(req)
	require.Equal(t, 1, cache.getPendingCount())

	// Manually set lastCheckStaleRequestTime to bypass the time interval check
	cache.lastCheckStaleRequestTime.Store(time.Now().Add(-checkStaleRequestInterval - time.Second))

	// Manually trigger cleanup of stopped region
	cache.clearStaleRequest()

	// The stopped region should be cleaned up
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheMarkSent_DuplicateReleaseSlot(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)

	// Add a duplicate request for the same region. It should not leak pendingCount even if
	// markSent overwrites the existing entry.
	ok, err = cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())

	req1, err := cache.pop(ctx)
	require.NoError(t, err)
	cache.markSent(req1)
	require.Equal(t, 2, cache.getPendingCount())

	req2, err := cache.pop(ctx)
	require.NoError(t, err)
	cache.markSent(req2)
	require.Equal(t, 1, cache.getPendingCount())

	// Finish the remaining tracked request.
	require.True(t, cache.resolve(region.subscribedSpan.subID, region.verID.GetID()))
	require.Equal(t, 0, cache.getPendingCount())
}

func TestRequestCacheMarkStopped_ReleasesSlot(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, cache.getPendingCount())

	req, err := cache.pop(ctx)
	require.NoError(t, err)

	cache.markSent(req)
	require.Equal(t, 1, cache.getPendingCount())
	require.Contains(t, cache.sentRequests.regionReqs, req.regionInfo.subscribedSpan.subID)

	cache.markStopped(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
	require.Equal(t, 0, cache.getPendingCount())
	require.NotContains(t, cache.sentRequests.regionReqs, req.regionInfo.subscribedSpan.subID)
}
