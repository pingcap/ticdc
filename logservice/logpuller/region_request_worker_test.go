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
	recvErr error
}

func (m *mockEventFeedV2Client) Send(*cdcpb.ChangeDataRequest) error   { return m.sendErr }
func (m *mockEventFeedV2Client) Recv() (*cdcpb.ChangeDataEvent, error) { return nil, m.recvErr }
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

<<<<<<< HEAD
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
=======
func TestCreateRegionRequestScanPriority(t *testing.T) {
	for _, tc := range []struct {
		name     string
		priority cdcpb.ScanPriority
		expected cdcpb.ScanPriority
	}{
		{
			name:     "high",
			priority: cdcpb.ScanPriority_SCAN_PRIORITY_HIGH,
			expected: cdcpb.ScanPriority_SCAN_PRIORITY_HIGH,
		},
		{
			name:     "low",
			priority: cdcpb.ScanPriority_SCAN_PRIORITY_LOW,
			expected: cdcpb.ScanPriority_SCAN_PRIORITY_LOW,
		},
		{
			name:     "unknown defaults to low",
			priority: cdcpb.ScanPriority_SCAN_PRIORITY_UNKNOWN,
			expected: cdcpb.ScanPriority_SCAN_PRIORITY_LOW,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
			region.scanPriority = tc.priority

			req := createRegionRequest(1, region)
			require.Equal(t, tc.expected, req.GetScanPriority())
		})
	}
}

func admitRegionRequest(
	t *testing.T,
	controller *regionAdmissionController,
	region regionInfo,
) *regionReq {
	t.Helper()
	currentTs := oracle.GoTimeToTS(time.Now())
	submitRegionForAdmission(t, controller, region, currentTs)
	req, err := controller.pop(t.Context(), nil)
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
	require.NoError(t, err)
	require.True(t, ok)

<<<<<<< HEAD
	req, err := worker.requestCache.pop(ctx)
=======
func TestRunStreamCancelsBlockingReceiveWhenSenderExits(t *testing.T) {
	ctx := t.Context()

	serverImpl := &blockingEventFeedServer{requestReceived: make(chan struct{})}
	var serverWG sync.WaitGroup
	server, storeAddr := newMockService(ctx, t, serverImpl, &serverWG)
	defer func() {
		server.Stop()
		serverWG.Wait()
	}()

	_, cluster, pdClient, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	defer pdClient.Close()
	cluster.AddStore(1, storeAddr)

	admission := newRegionAdmissionController(1, 1)
	worker := &regionRequestWorker{
		admission:    admission,
		controlQueue: newControlQueue(),
		storeAddr:    storeAddr,
		upstream: &upstreamHandle{
			pd:         &mockPDClient{Client: pdClient, versionGen: defaultVersionGen},
			credential: &security.Credential{},
		},
		tracker: newRegionTracker(),
	}
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	req := admitRegionRequest(t, admission, region)

	done := make(chan error, 1)
	go func() {
		done <- worker.runStream(ctx, req)
	}()

	select {
	case <-serverImpl.requestReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first region request")
	}
	admission.close()

	select {
	case err := <-done:
		var streamErr *storeStreamErr
		require.ErrorAs(t, err, &streamErr)
	case <-time.After(time.Second):
		t.Fatal("runStream did not cancel the blocking receive")
	}
}

type pushedRegionEvent struct {
	subscriptionID SubscriptionID
	event          regionEvent
}

type recordingRegionEventDynamicStream struct {
	events chan pushedRegionEvent
}

func (m *recordingRegionEventDynamicStream) Start() {}

func (m *recordingRegionEventDynamicStream) Close() {}

func (m *recordingRegionEventDynamicStream) Push(path SubscriptionID, event regionEvent) {
	m.events <- pushedRegionEvent{subscriptionID: path, event: event}
}

func (m *recordingRegionEventDynamicStream) Wake(SubscriptionID) {}

func (m *recordingRegionEventDynamicStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return nil
}

func (m *recordingRegionEventDynamicStream) AddPath(SubscriptionID, *subscribedSpan, ...dynstream.AreaSettings) error {
	return nil
}

func (m *recordingRegionEventDynamicStream) RemovePath(SubscriptionID) error {
	return nil
}

func (m *recordingRegionEventDynamicStream) Release(SubscriptionID) {}

func (m *recordingRegionEventDynamicStream) SetAreaSettings(int, dynstream.AreaSettings) {}

func (m *recordingRegionEventDynamicStream) GetMetrics() dynstream.Metrics[int, SubscriptionID] {
	return dynstream.Metrics[int, SubscriptionID]{}
}

func createFailureRecoveryTestRegion(t *testing.T, subID SubscriptionID, regionID uint64) regionInfo {
	t.Helper()

	fullSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}
	subSpan := &subscribedSpan{
		subID:     subID,
		startTs:   100,
		span:      fullSpan,
		rangeLock: regionlock.NewRangeLock(1, fullSpan.StartKey, fullSpan.EndKey, 100),
	}
	regionSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
	}
	locked := subSpan.rangeLock.LockRange(context.Background(), regionSpan.StartKey, regionSpan.EndKey, regionID, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, locked.Status)
	other := subSpan.rangeLock.LockRange(context.Background(), []byte("m"), []byte("z"), regionID+1000, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, other.Status)

	region := newRegionInfo(tikv.NewRegionVerID(regionID, 1, 1), regionSpan, nil, subSpan, false)
	region.lockedRangeState = locked.LockedRangeState
	region.lockedRangeState.ResolvedTs.Store(100)
	return region
}

func newFailureRecoveryTestPDClient(t *testing.T) *mockPDClient {
	t.Helper()

	_, _, pdClient, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	return &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
}

func snapshotErrCacheRegionIDs(handler *regionFailureHandler) []uint64 {
	handler.cache.Lock()
	defer handler.cache.Unlock()

	regionIDs := make([]uint64, 0, len(handler.cache.cache))
	for _, errInfo := range handler.cache.cache {
		regionIDs = append(regionIDs, errInfo.verID.GetID())
	}
	return regionIDs
}

func errCacheLen(handler *regionFailureHandler) int {
	handler.cache.Lock()
	defer handler.cache.Unlock()
	return len(handler.cache.cache)
}

func TestRegionRequestWorkerIgnoresDuplicateActiveRegion(t *testing.T) {
	admission := newRegionAdmissionController(10, 1)
	worker := &regionRequestWorker{
		admission: admission,
		storeAddr: "store-1",
		upstream:  &upstreamHandle{},
		tracker:   newRegionTracker(),
	}
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

	req1 := admitRegionRequest(t, admission, region)
	state1 := newRegionFeedState(region, uint64(region.subscribedSpan.subID), worker, req1)
	require.True(t, worker.tracker.Add(region.subscribedSpan.subID, region.verID.GetID(), state1))

	req2 := admitRegionRequest(t, admission, region)
	sendCh := make(chan *cdcpb.ChangeDataRequest, 1)
	err := worker.sendRegionRequest(&ConnAndClient{
		Client: &mockEventFeedV2Client{sendCh: sendCh},
		Conn:   &grpc.ClientConn{},
	}, req2)
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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
<<<<<<< HEAD
		client: &subscriptionClient{
			metrics: sharedClientMetrics{
				batchResolvedSize: prometheus.ObserverFunc(func(float64) {}),
			},
			eventSink: newTestRegionEventSink(ds),
		},
	}
	worker.requestedRegions.subscriptions = map[SubscriptionID]regionFeedStates{
		1: make(regionFeedStates, regionCount),
=======
		eventSink: &regionEventSink{ds: ds},
		tracker:   newRegionTracker(),
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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
		s.eventSink.Push(subscriptionID, regionEvent{
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
<<<<<<< HEAD
	worker, ds, event := newDispatchResolvedTsTestWorker(regionCount)
=======
	worker, _, event := newDispatchResolvedTsTestWorker(regionCount)
	ds := &countingRegionEventDynamicStream{}
	worker.eventSink.ds = ds
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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

<<<<<<< HEAD
	require.Equal(t, 0, worker.requestCache.getPendingCount())
	require.Empty(t, worker.clearPendingRegions())
=======
	require.Equal(t, 0, admission.stats().inflight)
}

func TestRunStreamFailurePushesTrackedRegionToEventSink(t *testing.T) {
	pdClient := newFailureRecoveryTestPDClient(t)
	defer pdClient.Close()

	ds := &recordingRegionEventDynamicStream{events: make(chan pushedRegionEvent, 4)}
	handler := newRegionFailureHandler(nil, func(*subscribedSpan) {}, nil, nil)
	worker := &regionRequestWorker{
		upstream:       &upstreamHandle{pd: pdClient, credential: &security.Credential{}},
		eventSink:      &regionEventSink{ds: ds},
		failureHandler: handler,
		admission:      newRegionAdmissionController(10, 1),
		controlQueue:   newControlQueue(),
		tracker:        newRegionTracker(),
		storeAddr:      "127.0.0.1:1",
	}

	sentRegion := createFailureRecoveryTestRegion(t, 1, 1)
	sentReq := admitRegionRequest(t, worker.admission, sentRegion)
	sentState := newRegionFeedState(sentRegion, uint64(sentRegion.subscribedSpan.subID), worker, sentReq)
	require.True(t, worker.tracker.Add(sentRegion.subscribedSpan.subID, sentRegion.verID.GetID(), sentState))

	firstRegion := createFailureRecoveryTestRegion(t, 2, 2)
	submitRegionForAdmission(t, worker.admission, firstRegion, 100)

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- worker.Run(ctx)
	}()

	var pushed pushedRegionEvent
	select {
	case pushed = <-ds.events:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not push tracked region after stream failure")
	}

	require.Equal(t, SubscriptionID(1), pushed.subscriptionID)
	require.Len(t, pushed.event.states, 1)
	require.Same(t, sentState, pushed.event.states[0])
	require.Eventually(t, func() bool {
		return worker.admission.stats().inflight == 0
	}, time.Second, 10*time.Millisecond)

	var streamErr *storeStreamErr
	require.ErrorAs(t, sentState.takeError(), &streamErr)

	cancel()
	require.ErrorIs(t, <-runErrCh, context.Canceled)
}

func TestRunStreamFailureReportsPendingRegionsToFailureHandler(t *testing.T) {
	pdClient := newFailureRecoveryTestPDClient(t)
	defer pdClient.Close()

	handler := newRegionFailureHandler(nil, func(*subscribedSpan) {}, nil, nil)
	worker := &regionRequestWorker{
		upstream:       &upstreamHandle{pd: pdClient, credential: &security.Credential{}},
		eventSink:      &regionEventSink{ds: &mockDynamicStream{}},
		failureHandler: handler,
		admission:      newRegionAdmissionController(10, 1),
		controlQueue:   newControlQueue(),
		tracker:        newRegionTracker(),
		storeAddr:      "127.0.0.1:1",
	}

	firstRegion := createFailureRecoveryTestRegion(t, 1, 1)
	pendingRegion := createFailureRecoveryTestRegion(t, 2, 2)
	submitRegionForAdmission(t, worker.admission, firstRegion, 100)
	submitRegionForAdmission(t, worker.admission, pendingRegion, 100)

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- worker.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return errCacheLen(handler) == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.ElementsMatch(t, []uint64{1, 2}, snapshotErrCacheRegionIDs(handler))
	require.Equal(t, 0, worker.admission.stats().pending)
	require.Equal(t, 0, worker.admission.stats().inflight)

	cancel()
	require.ErrorIs(t, <-runErrCh, context.Canceled)
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
}

func TestProcessRegionSendTaskSendFailureCleansSentRequest(t *testing.T) {
	worker := &regionRequestWorker{
<<<<<<< HEAD
		requestCache: newRequestCache(10),
		store:        &requestedStore{storeAddr: "store-1"},
		client:       &subscriptionClient{},
=======
		admission:    admission,
		controlQueue: newControlQueue(),
		storeAddr:    "store-1",
		upstream:     &upstreamHandle{},
		tracker:      newRegionTracker(),
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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
<<<<<<< HEAD
	require.Equal(t, 0, worker.requestCache.getPendingCount())
	require.Empty(t, worker.requestCache.sentRequests.regionReqs)
	state := worker.getRegionState(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
	require.True(t, state == nil || state.isStale(), "region state should be removed or marked stale after send failure")
=======
	require.Equal(t, 0, admission.stats().inflight)
	state := worker.tracker.Get(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
	require.NotNil(t, state)
	require.True(t, state.isStale())
	var streamErr *storeStreamErr
	require.ErrorAs(t, state.takeError(), &streamErr)
}

func TestProcessRegionSendTaskDoesNotSendRemovedRequest(t *testing.T) {
	admission := newRegionAdmissionController(1, 1)
	worker := &regionRequestWorker{
		admission:    admission,
		controlQueue: newControlQueue(),
		storeAddr:    "store-1",
		upstream:     &upstreamHandle{},
		tracker:      newRegionTracker(),
	}
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	req := admitRegionRequest(t, admission, region)
	require.True(t, req.abort())

	ctx, cancel := context.WithCancel(context.Background())
	sendCh := make(chan *cdcpb.ChangeDataRequest, 1)
	done := make(chan error, 1)
	go func() {
		done <- worker.processRegionSendTask(ctx, &ConnAndClient{
			Client: &mockEventFeedV2Client{sendCh: sendCh},
			Conn:   &grpc.ClientConn{},
		}, req)
	}()

	select {
	case sentReq := <-sendCh:
		t.Fatalf("removed request was sent: %+v", sentReq)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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
<<<<<<< HEAD
				requestCache: newRequestCache(10),
				store:        &requestedStore{storeAddr: "store-1"},
				client:       &subscriptionClient{},
=======
				admission:    admission,
				controlQueue: newControlQueue(),
				storeAddr:    "store-1",
				upstream:     &upstreamHandle{},
				tracker:      newRegionTracker(),
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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
			var streamErr *storeStreamErr
			require.ErrorAs(t, err, &streamErr)
			require.Equal(t, 0, worker.requestCache.getPendingCount())
			require.Empty(t, worker.requestCache.sentRequests.regionReqs)

			state := worker.getRegionState(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
			require.NotNil(t, state)
			require.True(t, state.isStale())

			var storedErr *storeStreamErr
			require.ErrorAs(t, state.takeError(), &storedErr)
		})
	}
}

<<<<<<< HEAD
=======
func TestProcessRegionSendTaskHandlesDeregisterFromControlQueue(t *testing.T) {
	ds := &mockRegionEventDynamicStream{}
	worker := &regionRequestWorker{
		admission:    newRegionAdmissionController(1, 1),
		controlQueue: newControlQueue(),
		storeAddr:    "store-1",
		upstream:     &upstreamHandle{clusterID: 42},
		eventSink:    &regionEventSink{ds: ds},
		tracker:      newRegionTracker(),
	}
	state := &regionFeedState{worker: worker}
	require.True(t, worker.tracker.Add(1, 1, state))
	worker.controlQueue.push(deregisterRequest{subID: 1, filterLoop: true})

	ctx, cancel := context.WithCancel(context.Background())
	sendCh := make(chan *cdcpb.ChangeDataRequest, 1)
	done := make(chan error, 1)
	go func() {
		done <- worker.processRegionSendTask(ctx, &ConnAndClient{
			Client: &mockEventFeedV2Client{sendCh: sendCh},
			Conn:   &grpc.ClientConn{},
		}, nil)
	}()

	req := <-sendCh
	require.Equal(t, uint64(42), req.Header.ClusterId)
	require.Equal(t, uint64(1), req.RequestId)
	require.True(t, req.FilterLoop)
	require.NotNil(t, req.GetDeregister())
	require.Eventually(t, func() bool {
		return worker.tracker.Get(1, 1) == nil
	}, time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
func TestReceiveAndDispatchChangeEventsEOFIsRetriable(t *testing.T) {
	testCases := []struct {
		name    string
		recvErr error
	}{
		{
			name:    "io EOF",
			recvErr: io.EOF,
		},
		{
			name:    "grpc canceled",
			recvErr: grpcstatus.Error(codes.Canceled, context.Canceled.Error()),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			worker := &regionRequestWorker{storeAddr: "store-1"}
			conn := &ConnAndClient{
				Client: &mockEventFeedV2Client{recvErr: tc.recvErr},
				Conn:   &grpc.ClientConn{},
			}

			err := worker.receiveAndDispatchChangeEvents(conn)
			var streamErr *storeStreamErr
			require.ErrorAs(t, err, &streamErr)
		})
	}
}
