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
	"io"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
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

type pushedResolvedEvent struct {
	subscriptionID SubscriptionID
	resolvedTs     uint64
	statesCount    int
}

type mockRegionEventDynamicStream struct {
	pushCount   int
	totalStates int
	pushed      []pushedResolvedEvent
}

func (m *mockRegionEventDynamicStream) Start() {}

func (m *mockRegionEventDynamicStream) Close() {}

func (m *mockRegionEventDynamicStream) Push(path SubscriptionID, event regionEvent) {
	m.pushCount++
	m.totalStates += len(event.states)
	m.pushed = append(m.pushed, pushedResolvedEvent{
		subscriptionID: path,
		resolvedTs:     event.resolvedTs,
		statesCount:    len(event.states),
	})
}

func (m *mockRegionEventDynamicStream) Wake(SubscriptionID) {}

func (m *mockRegionEventDynamicStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return nil
}

func (m *mockRegionEventDynamicStream) AddPath(SubscriptionID, *subscribedSpan, ...dynstream.AreaSettings) error {
	return nil
}

func (m *mockRegionEventDynamicStream) RemovePath(SubscriptionID) error {
	return nil
}

func (m *mockRegionEventDynamicStream) Release(SubscriptionID) {}

func (m *mockRegionEventDynamicStream) SetAreaSettings(int, dynstream.AreaSettings) {}

func (m *mockRegionEventDynamicStream) GetMetrics() dynstream.Metrics[int, SubscriptionID] {
	return dynstream.Metrics[int, SubscriptionID]{}
}

func newDispatchResolvedTsTestWorker(regionCount int) (*regionRequestWorker, *mockRegionEventDynamicStream, *cdcpb.ResolvedTs) {
	ds := &mockRegionEventDynamicStream{}
	worker := &regionRequestWorker{
		client: &subscriptionClient{
			metrics: sharedClientMetrics{
				batchResolvedSize: prometheus.ObserverFunc(func(float64) {}),
			},
			ds: ds,
		},
	}
	worker.requestedRegions.subscriptions = map[SubscriptionID]regionFeedStates{
		1: make(regionFeedStates, regionCount),
	}
	regions := make([]uint64, regionCount)
	for i := 0; i < regionCount; i++ {
		regionID := uint64(i + 1)
		regions[i] = regionID
		worker.requestedRegions.subscriptions[1][regionID] = &regionFeedState{
			requestID: 1,
		}
	}

	return worker, ds, &cdcpb.ResolvedTs{
		RequestId: 1,
		Ts:        100,
		Regions:   regions,
	}
}

func dispatchResolvedTsEventLegacyForBenchmark(s *regionRequestWorker, resolvedTsEvent *cdcpb.ResolvedTs) {
	subscriptionID := SubscriptionID(resolvedTsEvent.RequestId)
	const resolvedTsStateBatchSize = 1024
	resolvedStates := make([]*regionFeedState, 0, resolvedTsStateBatchSize)
	flush := func() {
		if len(resolvedStates) == 0 {
			return
		}
		states := resolvedStates
		s.client.pushRegionEventToDS(subscriptionID, regionEvent{
			resolvedTs: resolvedTsEvent.Ts,
			states:     states,
		})
		resolvedStates = make([]*regionFeedState, 0, resolvedTsStateBatchSize)
	}
	for _, regionID := range resolvedTsEvent.Regions {
		if state := s.getRegionState(subscriptionID, regionID); state != nil {
			resolvedStates = append(resolvedStates, state)
			if len(resolvedStates) >= resolvedTsStateBatchSize {
				flush()
			}
		}
	}
	flush()
}

func benchmarkDispatchResolvedTsEvent(b *testing.B, regionCount int, useLegacy bool) {
	worker, ds, event := newDispatchResolvedTsTestWorker(regionCount)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if useLegacy {
			dispatchResolvedTsEventLegacyForBenchmark(worker, event)
		} else {
			worker.dispatchResolvedTsEvent(event)
		}
	}
	b.StopTimer()
	if ds.pushCount == 0 {
		b.Fatal("expected at least one push")
	}
}

func TestDispatchResolvedTsEventSingleRegion(t *testing.T) {
	worker, ds, event := newDispatchResolvedTsTestWorker(1)
	worker.dispatchResolvedTsEvent(event)

	require.Equal(t, 1, ds.pushCount)
	require.Equal(t, 1, ds.totalStates)
	require.Len(t, ds.pushed, 1)
	require.Equal(t, 1, ds.pushed[0].statesCount)
	require.Equal(t, uint64(100), ds.pushed[0].resolvedTs)
	require.Equal(t, SubscriptionID(1), ds.pushed[0].subscriptionID)
}

func TestDispatchResolvedTsEventBatchSplitForLargeRegions(t *testing.T) {
	worker, ds, event := newDispatchResolvedTsTestWorker(2050)
	worker.dispatchResolvedTsEvent(event)

	require.Equal(t, 2050, ds.totalStates)
	require.Len(t, ds.pushed, 3)
	require.Equal(t, 1024, ds.pushed[0].statesCount)
	require.Equal(t, 1024, ds.pushed[1].statesCount)
	require.Equal(t, 2, ds.pushed[2].statesCount)
}

func BenchmarkDispatchResolvedTsEventSingleRegionLegacy(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 1, true)
}

func BenchmarkDispatchResolvedTsEventSingleRegionCurrent(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 1, false)
}

func BenchmarkDispatchResolvedTsEventLargeBatchLegacy(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 4096, true)
}

func BenchmarkDispatchResolvedTsEventLargeBatchCurrent(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 4096, false)
}

func BenchmarkDispatchResolvedTsEventSmallBatchLegacy(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 16, true)
}

func BenchmarkDispatchResolvedTsEventSmallBatchCurrent(b *testing.B) {
	benchmarkDispatchResolvedTsEvent(b, 16, false)
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
	state := worker.getRegionState(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
	require.True(t, state == nil || state.isStale(), "region state should be removed or marked stale after send failure")
}

func TestProcessRegionSendTaskSendEOFIsRetriable(t *testing.T) {
	testCases := []struct {
		name    string
		sendErr error
	}{
		{
			name:    "io EOF",
			sendErr: io.EOF,
		},
		{
			name:    "grpc canceled",
			sendErr: grpcstatus.Error(codes.Canceled, context.Canceled.Error()),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			req, err := worker.requestCache.pop(ctx)
			require.NoError(t, err)
			worker.preFetchForConnecting = new(regionInfo)
			*worker.preFetchForConnecting = req.regionInfo

			conn := &ConnAndClient{
				Client: &mockEventFeedV2Client{sendErr: tc.sendErr},
				Conn:   &grpc.ClientConn{},
			}

			err = worker.processRegionSendTask(ctx, conn)
			var sendRequestErr *sendRequestToStoreErr
			require.ErrorAs(t, err, &sendRequestErr)
			require.Equal(t, 0, worker.requestCache.getPendingCount())
			require.Empty(t, worker.requestCache.sentRequests.regionReqs)

			state := worker.getRegionState(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
			require.NotNil(t, state)
			require.True(t, state.isStale())

			var storedErr *sendRequestToStoreErr
			require.ErrorAs(t, state.takeError(), &storedErr)
		})
	}
}
