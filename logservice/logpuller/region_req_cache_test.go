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

	// With force=true, the request bypasses the live request limit.
	region3 := createTestRegionInfo(1, 3)
	ok, err = cache.add(ctx, region3, true)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())

	req, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, region1.verID.GetID(), req.regionInfo.verID.GetID())
	require.Equal(t, region1.subscribedSpan.subID, req.regionInfo.subscribedSpan.subID)
	require.Equal(t, 2, cache.getPendingCount())
	req.markSent()

	// resolve region1
	req.resolve()
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
	req.markSent()
	require.Equal(t, 2, cache.getPendingCount())

	// Resolve the request to free up space
	success := req.resolve()
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

func TestRequestCacheAdd_DuplicateQueuedRequestsAreTrackedIndependently(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)

	ok, err = cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 2, cache.getPendingCount())

	req1, err := cache.pop(ctx)
	require.NoError(t, err)
	req2, err := cache.pop(ctx)
	require.NoError(t, err)
	require.NotSame(t, req1, req2)
}

func TestRequestCacheFinish_ReleasesSlot(t *testing.T) {
	cache := newRequestCache(10)
	ctx := context.Background()

	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(ctx, region, false)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, cache.getPendingCount())

	req, err := cache.pop(ctx)
	require.NoError(t, err)

	req.markSent()
	require.Equal(t, 1, cache.getPendingCount())

	req.finish()
	require.Equal(t, 0, cache.getPendingCount())
}
