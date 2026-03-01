// Copyright 2024 PingCAP, Inc.
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

	"github.com/stretchr/testify/require"
)

func TestRegionStatesOperation(t *testing.T) {
	worker := &regionRequestWorker{}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	require.Nil(t, worker.getRegionState(1, 2))
	require.Nil(t, worker.takeRegionState(1, 2))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))
}

func TestClearPendingRegionsReleaseSlotForPreFetchedRegion(t *testing.T) {
	worker := &regionRequestWorker{
		requestCache: newRequestCache(10),
	}

	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := worker.requestCache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := worker.requestCache.pop(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, worker.requestCache.getPendingCount())

	worker.preFetchForConnecting = new(regionInfo)
	*worker.preFetchForConnecting = req.regionInfo

	regions := worker.clearPendingRegions()
	require.Len(t, regions, 1)
	require.Nil(t, worker.preFetchForConnecting)
	require.Equal(t, 0, worker.requestCache.getPendingCount())
}
