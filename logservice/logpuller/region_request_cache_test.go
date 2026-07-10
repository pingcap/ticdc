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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func createTestRegionInfo(subID SubscriptionID, regionID uint64) regionInfo {
	span := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
	}
	return newRegionInfo(
		tikv.NewRegionVerID(regionID, 1, 1),
		span,
		nil,
		&subscribedSpan{subID: subID, startTs: 100, span: span},
		false,
	)
}

func TestRequestCacheLifecycle(t *testing.T) {
	cache := newRequestCache(1)
	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(t.Context(), region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, cache.pendingCount())

	req, err := cache.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, regionReqStageProcessing, req.stage)
	require.Equal(t, 1, cache.pendingCount())

	cache.markSent(req)
	require.Equal(t, regionReqStageSent, req.stage)
	require.True(t, cache.finishScan(req))
	require.False(t, cache.finishScan(req))
	require.Equal(t, 0, cache.pendingCount())
}

func TestRequestCacheCapacityAndForceAdd(t *testing.T) {
	cache := newRequestCache(1)

	ok, err := cache.add(t.Context(), createTestRegionInfo(1, 1), false)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cache.add(t.Context(), createTestRegionInfo(1, 2), false)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = cache.add(t.Context(), createTestRegionInfo(1, 3), true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, cache.pendingCount())
}

func TestRequestCacheAddHonorsCancellationWhenFull(t *testing.T) {
	cache := newRequestCache(1)
	ok, err := cache.add(t.Context(), createTestRegionInfo(1, 1), false)
	require.NoError(t, err)
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err = cache.add(ctx, createTestRegionInfo(1, 2), false)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRequestCacheTracksDuplicateRegionsIndependently(t *testing.T) {
	cache := newRequestCache(2)
	region := createTestRegionInfo(1, 1)

	ok, err := cache.add(t.Context(), region, false)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.add(t.Context(), region, false)
	require.NoError(t, err)
	require.True(t, ok)

	req1, err := cache.pop(t.Context())
	require.NoError(t, err)
	req2, err := cache.pop(t.Context())
	require.NoError(t, err)
	require.NotSame(t, req1, req2)
	require.Equal(t, 2, cache.pendingCount())
}

func TestRequestCacheDrainOnlyUnsentRegions(t *testing.T) {
	cache := newRequestCache(3)
	for regionID := uint64(1); regionID <= 3; regionID++ {
		ok, err := cache.add(t.Context(), createTestRegionInfo(1, regionID), false)
		require.NoError(t, err)
		require.True(t, ok)
	}

	sentReq, err := cache.pop(t.Context())
	require.NoError(t, err)
	cache.markSent(sentReq)
	processingReq, err := cache.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, regionReqStageProcessing, processingReq.stage)

	regions := cache.drainUnsentRegions()
	require.Len(t, regions, 2)
	require.Equal(t, 1, cache.pendingCount())
	require.True(t, cache.abortScan(sentReq))
	require.Equal(t, 0, cache.pendingCount())
}

func TestRequestCacheCloseRemovesAllRequests(t *testing.T) {
	cache := newRequestCache(2)
	for regionID := uint64(1); regionID <= 2; regionID++ {
		ok, err := cache.add(t.Context(), createTestRegionInfo(1, regionID), false)
		require.NoError(t, err)
		require.True(t, ok)
	}

	req, err := cache.pop(t.Context())
	require.NoError(t, err)
	cache.markSent(req)
	cache.close()

	require.Equal(t, 0, cache.pendingCount())
	require.Nil(t, cache.tryPop())
	require.False(t, cache.abortScan(req))
}
