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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

type mockEventFeedV2Client struct {
	sendErr error
	recvErr error
	sendCh  chan *cdcpb.ChangeDataRequest
}

func (m *mockEventFeedV2Client) Send(req *cdcpb.ChangeDataRequest) error {
	if m.sendCh != nil {
		m.sendCh <- req
	}
	return m.sendErr
}
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

func admitRegionRequest(
	t *testing.T,
	controller *regionAdmissionController,
	region regionInfo,
) *regionReq {
	t.Helper()
	currentTs := oracle.GoTimeToTS(time.Now())
	submitRegionForAdmission(t, controller, region, TaskLowPrior, currentTs)
	req, err := controller.pop(t.Context())
	require.NoError(t, err)
	return req
}

func TestRegionRequestWorkerIgnoresDuplicateActiveRegion(t *testing.T) {
	admission := newRegionAdmissionController(10, 1)
	worker := &regionRequestWorker{
		admission: admission,
		store:     &requestedStore{storeAddr: "store-1"},
		client:    &subscriptionClient{},
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
	require.NoError(t, err)

	require.Equal(t, 1, admission.inflightCount())
	require.Same(t, state1, worker.tracker.Get(region.subscribedSpan.subID, region.verID.GetID()))
	require.False(t, state1.isStale())
	select {
	case <-sendCh:
		t.Fatal("duplicate region request must not be sent")
	default:
	}

	state1.abortScanIfNeeded()
	state1.matcher.clear()
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

type countingRegionEventDynamicStream struct {
	mockRegionEventDynamicStream
	pushCount int
}

func (m *countingRegionEventDynamicStream) Push(_ SubscriptionID, _ regionEvent) {
	m.pushCount++
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
			eventSink: &regionEventSink{ds: ds},
		},
		tracker: newRegionTracker(),
	}
	regions := make([]uint64, regionCount)
	for i := 0; i < regionCount; i++ {
		regionID := uint64(i + 1)
		regions[i] = regionID
		worker.tracker.Add(1, regionID, &regionFeedState{
			requestID: 1,
		})
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
		if state := s.tracker.Get(subscriptionID, regionID); state != nil {
			resolvedStates = append(resolvedStates, state)
			if len(resolvedStates) >= resolvedTsStateBatchSize {
				flush()
			}
		}
	}
	flush()
}

func benchmarkDispatchResolvedTsEvent(b *testing.B, regionCount int, useLegacy bool) {
	worker, _, event := newDispatchResolvedTsTestWorker(regionCount)
	ds := &countingRegionEventDynamicStream{}
	worker.client.eventSink.ds = ds
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

func TestWaitForRegionRequestDrainsIdleControlQueue(t *testing.T) {
	admission := newRegionAdmissionController(1, 1)
	worker := &regionRequestWorker{
		admission:    admission,
		controlQueue: newControlQueue(),
	}

	type waitResult struct {
		req *regionReq
		err error
	}
	resultCh := make(chan waitResult, 1)
	go func() {
		req, err := worker.waitForRegionRequest(t.Context())
		resultCh <- waitResult{req: req, err: err}
	}()

	for subID := SubscriptionID(1); subID <= 100; subID++ {
		worker.controlQueue.push(deregisterRequest{subID: subID})
	}

	region := prepareRegionForAdmission(createTestRegionInfo(1, 1), 1)
	submitRegionForAdmission(t, admission, region, TaskLowPrior, 1)

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.NotNil(t, result.req)
		require.Equal(t, uint64(1), result.req.regionInfo.verID.GetID())
	case <-time.After(time.Second):
		t.Fatal("worker did not receive the first region request")
	}
	require.Zero(t, worker.controlQueue.len())
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

func TestStoppedStateRemovesSentRequest(t *testing.T) {
	admission := newRegionAdmissionController(10, 1)
	worker := &regionRequestWorker{
		admission: admission,
		tracker:   newRegionTracker(),
	}
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	req := admitRegionRequest(t, admission, region)

	state := newRegionFeedState(req.regionInfo, uint64(req.regionInfo.subscribedSpan.subID), worker, req)
	require.True(t, worker.tracker.Add(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID(), state))
	state.markStopped(errors.New("send request to store error"))
	worker.tracker.RemoveIf(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID(), state)

	require.Equal(t, 0, admission.inflightCount())
}

func TestFailStreamRegionsReleasesSentAdmission(t *testing.T) {
	admission := newRegionAdmissionController(1, 1)
	ds := &mockRegionEventDynamicStream{}
	worker := &regionRequestWorker{
		admission:    admission,
		controlQueue: newControlQueue(),
		client: &subscriptionClient{
			eventSink: &regionEventSink{ds: ds},
		},
		tracker: newRegionTracker(),
	}
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	req := admitRegionRequest(t, admission, region)
	state := newRegionFeedState(region, uint64(region.subscribedSpan.subID), worker, req)
	require.True(t, worker.tracker.Add(region.subscribedSpan.subID, region.verID.GetID(), state))

	worker.failStreamRegions(&storeStreamErr{})

	require.Zero(t, admission.inflightCount())
	require.False(t, req.abort())
	require.Equal(t, 1, ds.pushCount)
}

func TestProcessRegionSendTaskSendFailureCleansSentRequest(t *testing.T) {
	admission := newRegionAdmissionController(10, 1)
	worker := &regionRequestWorker{
		admission:    admission,
		controlQueue: newControlQueue(),
		store:        &requestedStore{storeAddr: "store-1"},
		client:       &subscriptionClient{},
		tracker:      newRegionTracker(),
	}

	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

	req := admitRegionRequest(t, admission, region)
	require.Equal(t, 1, admission.inflightCount())

	sendErr := errors.New("send failed")
	conn := &ConnAndClient{
		Client: &mockEventFeedV2Client{sendErr: sendErr},
		Conn:   &grpc.ClientConn{},
	}

	err := worker.processRegionSendTask(t.Context(), conn, req)
	require.ErrorIs(t, err, sendErr)
	require.Equal(t, 0, admission.inflightCount())
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
		store:        &requestedStore{storeAddr: "store-1"},
		client:       &subscriptionClient{},
		tracker:      newRegionTracker(),
	}
	region := prepareRegionForSendTest(createTestRegionInfo(1, 1))
	req := admitRegionRequest(t, admission, region)
	require.True(t, req.abort())

	sendCh := make(chan *cdcpb.ChangeDataRequest, 1)
	err := worker.processRegionSendTask(t.Context(), &ConnAndClient{
		Client: &mockEventFeedV2Client{sendCh: sendCh},
		Conn:   &grpc.ClientConn{},
	}, req)
	var streamErr *storeStreamErr
	require.ErrorAs(t, err, &streamErr)
	select {
	case sentReq := <-sendCh:
		t.Fatalf("removed request was sent: %+v", sentReq)
	default:
	}
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
			admission := newRegionAdmissionController(10, 1)
			worker := &regionRequestWorker{
				admission:    admission,
				controlQueue: newControlQueue(),
				store:        &requestedStore{storeAddr: "store-1"},
				client:       &subscriptionClient{},
				tracker:      newRegionTracker(),
			}
			region := prepareRegionForSendTest(createTestRegionInfo(1, 1))

			req := admitRegionRequest(t, admission, region)

			conn := &ConnAndClient{
				Client: &mockEventFeedV2Client{sendErr: tc.sendErr},
				Conn:   &grpc.ClientConn{},
			}

			err := worker.processRegionSendTask(t.Context(), conn, req)
			var streamErr *storeStreamErr
			require.ErrorAs(t, err, &streamErr)
			require.Equal(t, 0, admission.inflightCount())

			state := worker.tracker.Get(req.regionInfo.subscribedSpan.subID, req.regionInfo.verID.GetID())
			require.NotNil(t, state)
			require.True(t, state.isStale())

			var storedErr *storeStreamErr
			require.ErrorAs(t, state.takeError(), &storedErr)
		})
	}
}

func TestProcessRegionSendTaskHandlesDeregisterFromControlQueue(t *testing.T) {
	ds := &mockRegionEventDynamicStream{}
	worker := &regionRequestWorker{
		admission:    newRegionAdmissionController(1, 1),
		controlQueue: newControlQueue(),
		store:        &requestedStore{storeAddr: "store-1"},
		client: &subscriptionClient{
			eventSink: &regionEventSink{ds: ds},
		},
		tracker: newRegionTracker(),
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
	require.Equal(t, uint64(1), req.RequestId)
	require.True(t, req.FilterLoop)
	require.NotNil(t, req.GetDeregister())
	require.Eventually(t, func() bool {
		return worker.tracker.Get(1, 1) == nil
	}, time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

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
			worker := &regionRequestWorker{
				store: &requestedStore{storeAddr: "store-1"},
			}
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
