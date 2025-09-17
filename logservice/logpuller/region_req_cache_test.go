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

	return newRegionInfo(verID, span, nil, subscribedSpan, false)
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

	// With force=true, it should still fail because the channel is full
	// The force flag only bypasses the pendingCount check, not the channel capacity
	region3 := createTestRegionInfo(1, 3)
	ok, err = cache.add(ctx, region3, true)
	require.False(t, ok)
	require.NoError(t, err)

	// consume the pending queue ann add with force
	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, region1.verID.GetID(), req.regionInfo.verID.GetID())
	require.Equal(t, region1.subscribedSpan.subID, req.regionInfo.subscribedSpan.subID)
	cache.markSent(req)
	require.Equal(t, 1, cache.getPendingCount())

	ok, err = cache.add(ctx, region3, true)
	require.True(t, ok)
	require.NoError(t, err)
	// It is 2 since region1 is unresolved
	require.Equal(t, 2, cache.getPendingCount())

	// resolve region1
	cache.resolve(region1.subscribedSpan.subID, region1.verID.GetID())
	require.Equal(t, 1, cache.getPendingCount())
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
