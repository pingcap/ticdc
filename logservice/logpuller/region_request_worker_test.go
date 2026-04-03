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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type mockEventFeedV2Client struct {
	sendErr error
}

func (m *mockEventFeedV2Client) Send(*cdcpb.ChangeDataRequest) error   { return m.sendErr }
func (m *mockEventFeedV2Client) Recv() (*cdcpb.ChangeDataEvent, error) { return nil, nil }
func (m *mockEventFeedV2Client) Header() (metadata.MD, error)          { return metadata.MD{}, nil }
func (m *mockEventFeedV2Client) Trailer() metadata.MD                  { return metadata.MD{} }
func (m *mockEventFeedV2Client) CloseSend() error                      { return nil }
func (m *mockEventFeedV2Client) Context() context.Context              { return context.Background() }
func (m *mockEventFeedV2Client) SendMsg(any) error                     { return nil }
func (m *mockEventFeedV2Client) RecvMsg(any) error                     { return nil }

func prepareRegionForSendTest(region regionInfo) regionInfo {
	region.rpcCtx = &tikv.RPCContext{
		Meta: &metapb.Region{
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		},
	}
	region.lockedRangeState = &regionlock.LockedRangeState{}
	region.lockedRangeState.ResolvedTs.Store(100)
	return region
}

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

func TestClearPendingRegionsDoesNotReturnStoppedSentRegion(t *testing.T) {
	worker := &regionRequestWorker{
		requestCache: newRequestCache(10),
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	ctx := context.Background()
	region := createTestRegionInfo(1, 1)

	ok, err := worker.requestCache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)

	req, err := worker.requestCache.pop(ctx)
	require.NoError(t, err)

	state := newRegionFeedState(req.regionInfo, uint64(req.regionInfo.subscribedSpan.subID), worker)
	state.start()
	worker.addRegionState(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID(), state)

	// Simulate the race we are fixing in processRegionSendTask:
	// once a request is visible in sentRequests, a fast region error may mark the
	// region stopped before worker cleanup runs. In that case, markStopped should
	// remove the sent request immediately, so clearPendingRegions must not return
	// the stale region again during worker shutdown.
	worker.requestCache.markSent(req)
	state.markStopped(errors.New("send request to store error"))
	worker.takeRegionState(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())

	require.Equal(t, 0, worker.requestCache.getPendingCount())
	require.Empty(t, worker.clearPendingRegions())
}

func TestProcessRegionSendTaskSendFailureCleansSentRequest(t *testing.T) {
	worker := &regionRequestWorker{
		requestCache: newRequestCache(10),
		store:        &requestedStore{storeAddr: "store-1"},
		client:       &subscriptionClient{},
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	ctx := context.Background()
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

	ok, err := worker.requestCache.add(ctx, region, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, worker.requestCache.getPendingCount())

	req, err := worker.requestCache.pop(ctx)
	require.NoError(t, err)
	worker.preFetchForConnecting = new(regionInfo)
	*worker.preFetchForConnecting = req.regionInfo

	sendErr := errors.New("send failed")
	conn := &ConnAndClient{
		Client: &mockEventFeedV2Client{sendErr: sendErr},
		Conn:   &grpc.ClientConn{},
	}

	err = worker.processRegionSendTask(ctx, conn)
	require.ErrorIs(t, err, sendErr)
	require.Equal(t, 0, worker.requestCache.getPendingCount())
	require.Empty(t, worker.requestCache.sentRequests.regionReqs)
}
