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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockcopr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

type mockLockResolver struct {
	calls atomic.Int32
}

func (r *mockLockResolver) Resolve(
	_ context.Context,
	_ uint32,
	_ uint64,
	_ uint64,
) error {
	r.calls.Add(1)
	return nil
}

func TestGenerateResolveLockTask(t *testing.T) {
	client := &subscriptionClient{
		resolveLockTaskCh:      make(chan resolveLockTask, 10),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(ts uint64) {}
	client.upstream = &upstreamHandle{pdClock: pdutil.NewClock4Test()}
	client.spanRegistry = newSpanRegistry(nil, client.upstream.pdClock)
	span := newSubscribedSpan(
		client.ctx,
		client.resolveLockRateLimiter,
		client.resolveLockTaskCh,
		SubscriptionID(1),
		rawSpan,
		100,
		consumeKVEvents,
		advanceResolvedTs,
		0,
		false,
	)
	client.spanRegistry.Add(span)

	// Lock a range, and then ResolveLock will trigger a task for it.
	res := span.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.Initialized.Store(true)
	span.resolveStaleLocks(200)
	select {
	case task := <-client.resolveLockTaskCh:
		require.Equal(t, uint64(1), task.regionID)
		require.Equal(t, uint64(200), task.targetTs)
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}

	// The same region should not be enqueued repeatedly within resolveLockMinInterval.
	span.resolveStaleLocks(200)
	select {
	case <-client.resolveLockTaskCh:
		require.True(t, false, "shouldn't get a duplicate resolve lock task")
	case <-time.After(100 * time.Millisecond):
	}

	worker := &regionRequestWorker{
		tracker: newRegionTracker(),
	}
	// Lock another range, no task will be triggered before initialized.
	res = span.rangeLock.LockRange(context.Background(), []byte{'c'}, []byte{'d'}, 2, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	state := newRegionFeedState(regionInfo{lockedRangeState: res.LockedRangeState, subscribedSpan: span}, 1, worker, nil)
	span.resolveStaleLocks(200)
	select {
	case <-client.resolveLockTaskCh:
		require.True(t, false, "shouldn't get a resolve lock task")
	case <-time.After(100 * time.Millisecond):
	}

	// Task will be triggered after initialized.
	state.setInitialized()
	span.resolveStaleLocks(200)
	select {
	case task := <-client.resolveLockTaskCh:
		require.Equal(t, uint64(2), task.regionID)
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}
	span.resolveStaleLocks(200)
	select {
	case <-client.resolveLockTaskCh:
		require.True(t, false, "shouldn't get a duplicate resolve lock task")
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, 0, len(client.resolveLockTaskCh))

	close(client.resolveLockTaskCh)
}

func TestResolveLockTaskDeduplicatedAcrossSubscribedSpans(t *testing.T) {
	client := &subscriptionClient{
		resolveLockTaskCh:      make(chan resolveLockTask, 10),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	defer client.cancel()

	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(ts uint64) {}
	span1 := newSubscribedSpan(client.ctx, client.resolveLockRateLimiter, client.resolveLockTaskCh, SubscriptionID(1), heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}, 100, consumeKVEvents, advanceResolvedTs, 0, false)
	span2 := newSubscribedSpan(client.ctx, client.resolveLockRateLimiter, client.resolveLockTaskCh, SubscriptionID(2), heartbeatpb.TableSpan{
		TableID:  2,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	res := span1.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.Initialized.Store(true)
	res = span2.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.Initialized.Store(true)

	span1.resolveStaleLocks(200)
	select {
	case task := <-client.resolveLockTaskCh:
		require.Equal(t, uint64(1), task.regionID)
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}

	span2.resolveStaleLocks(200)
	select {
	case <-client.resolveLockTaskCh:
		require.True(t, false, "shouldn't get a duplicate resolve lock task")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHandleResolveLockTasksMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := &mockLockResolver{}
	client := &subscriptionClient{
		lockResolver:           resolver,
		resolveLockTaskCh:      make(chan resolveLockTask, 4),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.handleResolveLockTasks(ctx)
	}()

	state := &regionlock.LockedRangeState{}
	state.Initialized.Store(true)
	state.ResolvedTs.Store(100)

	successBefore := testutil.ToFloat64(
		metricResolveLockSuccessCounter)

	key := resolveLockKey{keyspaceID: 1, regionID: 1}
	require.True(t, client.resolveLockRateLimiter.trySchedule(key, time.Now()))
	client.resolveLockTaskCh <- resolveLockTask{
		keyspaceID: 1,
		regionID:   1,
		targetTs:   200,
		state:      state,
	}
	require.Eventually(t, func() bool {
		return resolver.calls.Load() == 1 &&
			testutil.ToFloat64(metricResolveLockSuccessCounter) >= successBefore+1
	}, time.Second, 10*time.Millisecond)
	require.False(t, client.resolveLockRateLimiter.trySchedule(key, time.Now()))

	state.ResolvedTs.Store(300)
	key = resolveLockKey{keyspaceID: 1, regionID: 2}
	require.True(t, client.resolveLockRateLimiter.trySchedule(key, time.Now()))
	client.resolveLockTaskCh <- resolveLockTask{
		keyspaceID: 1,
		regionID:   2,
		targetTs:   400,
		state:      state,
	}
	require.Eventually(t, func() bool {
		return resolver.calls.Load() == 2
	}, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-errCh:
		require.Equal(t, context.Canceled, errors.Cause(err))
	case <-time.After(time.Second):
		t.Fatal("resolve lock task handler did not exit")
	}
}

func TestResolveLockTaskDroppedWhenChannelFull(t *testing.T) {
	client := &subscriptionClient{
		resolveLockTaskCh:      make(chan resolveLockTask, 1),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	defer client.cancel()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(ts uint64) {}
	span := newSubscribedSpan(
		client.ctx,
		client.resolveLockRateLimiter,
		client.resolveLockTaskCh,
		SubscriptionID(1),
		rawSpan,
		100,
		consumeKVEvents,
		advanceResolvedTs,
		0,
		false,
	)

	res := span.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.Initialized.Store(true)

	// Fill the channel to simulate the resolver goroutine being blocked.
	client.resolveLockTaskCh <- resolveLockTask{}

	before := testutil.ToFloat64(metrics.SubscriptionClientResolveLockTaskDropCounter)
	done := make(chan struct{})
	go func() {
		span.resolveStaleLocks(200)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("resolveStaleLocks should not block even if resolveLockTaskCh is full")
	}

	// No new task is added because the channel is still full.
	require.Equal(t, 1, len(client.resolveLockTaskCh))

	after := testutil.ToFloat64(metrics.SubscriptionClientResolveLockTaskDropCounter)
	require.Equal(t, before+1, after)

	<-client.resolveLockTaskCh
	close(client.resolveLockTaskCh)
}

func TestStopTaskUsesSubscribedSpanFilterLoop(t *testing.T) {
	client := &subscriptionClient{
		resolveLockTaskCh: make(chan resolveLockTask, 1),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	defer client.cancel()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(ts uint64) {}
	span := newSubscribedSpan(
		client.ctx,
		client.resolveLockRateLimiter,
		client.resolveLockTaskCh,
		SubscriptionID(1),
		rawSpan,
		100,
		consumeKVEvents,
		advanceResolvedTs,
		0,
		true,
	)

	res := span.rangeLock.LockRange(context.Background(), rawSpan.StartKey, rawSpan.EndKey, 1, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	worker := &regionRequestWorker{controlQueue: newControlQueue()}
	store := &requestedStore{storeAddr: "store-1", workers: []*regionRequestWorker{worker}}
	client.regionScheduler = &regionRequestScheduler{}
	client.regionScheduler.stores.Store(store.storeAddr, store)

	client.setTableStopped(span)

	req, ok := worker.controlQueue.tryPop()
	require.True(t, ok)
	require.Equal(t, SubscriptionID(1), req.subID)
	require.True(t, req.filterLoop)
}

func TestRegionFailureHandlerQueuesCanceledError(t *testing.T) {
	client := &subscriptionClient{
		eventSink: &regionEventSink{ds: &mockDynamicStream{}},
	}
	client.spanRegistry = newSpanRegistry(nil, nil)
	client.failureHandler = newRegionFailureHandler(nil, client.onTableDrained, nil, nil)
	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}
	span := &subscribedSpan{
		subID:     SubscriptionID(1),
		span:      rawSpan,
		rangeLock: regionlock.NewRangeLock(1, rawSpan.StartKey, rawSpan.EndKey, 100),
	}
	client.spanRegistry.Add(span)

	res1 := span.rangeLock.LockRange(context.Background(), []byte("a"), []byte("m"), 1, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res1.Status)
	res2 := span.rangeLock.LockRange(context.Background(), []byte("m"), []byte("z"), 2, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res2.Status)
	require.False(t, span.rangeLock.Stop())

	client.failureHandler.Report(newRegionErrorInfo(regionInfo{
		verID:            tikv.NewRegionVerID(1, 1, 1),
		span:             heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("m")},
		subscribedSpan:   span,
		lockedRangeState: res1.LockedRangeState,
	}, &requestCancelledErr{}))

	require.Len(t, client.failureHandler.cache.cache, 1)
	require.Len(t, span.rangeLock.IterAll(nil).UnLockedRanges, 1)

	client.failureHandler.Report(newRegionErrorInfo(regionInfo{
		verID:            tikv.NewRegionVerID(2, 1, 1),
		span:             heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("m"), EndKey: []byte("z")},
		subscribedSpan:   span,
		lockedRangeState: res2.LockedRangeState,
	}, &requestCancelledErr{}))

	require.Len(t, client.failureHandler.cache.cache, 1)
	require.Nil(t, client.spanRegistry.Get(span.subID))
}

type mockDynamicStream struct{}

func (s *mockDynamicStream) Start() {}

func (s *mockDynamicStream) Close() {}

func (s *mockDynamicStream) Push(_ SubscriptionID, _ regionEvent) {}

func (s *mockDynamicStream) Wake(_ SubscriptionID) {}

func (s *mockDynamicStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return nil
}

func (s *mockDynamicStream) AddPath(_ SubscriptionID, _ *subscribedSpan, _ ...dynstream.AreaSettings) error {
	return nil
}

func (s *mockDynamicStream) RemovePath(_ SubscriptionID) error {
	return nil
}

func (s *mockDynamicStream) Release(_ SubscriptionID) {}

func (s *mockDynamicStream) SetAreaSettings(_ int, _ dynstream.AreaSettings) {}

func (s *mockDynamicStream) GetMetrics() dynstream.Metrics[int, SubscriptionID] {
	return dynstream.Metrics[int, SubscriptionID]{}
}

func TestRegionEventSinkPushUnblocksOnClientClose(t *testing.T) {
	sink := &regionEventSink{
		ctx: context.Background(),
		ds:  &mockDynamicStream{},
	}
	sink.cond = sync.NewCond(&sink.mu)
	client := &subscriptionClient{eventSink: sink}
	client.regionScheduler = &regionRequestScheduler{
		taskQueue: priorityqueue.New[*regionPriorityTask](),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())

	sink.paused.Store(true)

	done := make(chan struct{})
	go func() {
		sink.Push(SubscriptionID(1), regionEvent{})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("pushRegionEventToDS should block when paused")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, client.Close(context.Background()))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pushRegionEventToDS should be unblocked by Close")
	}
}

func TestBroadcastDeregisterUsesWorkerControlQueue(t *testing.T) {
	scheduler := &regionRequestScheduler{}
	admission := newRegionAdmissionController(1, 1)

	worker := &regionRequestWorker{
		admission:    admission,
		controlQueue: newControlQueue(),
	}
	store := &requestedStore{storeAddr: "store-1", workers: []*regionRequestWorker{worker}}
	scheduler.stores.Store(store.storeAddr, store)

	dummyRegion := regionInfo{
		subscribedSpan:   &subscribedSpan{subID: SubscriptionID(2)},
		lockedRangeState: &regionlock.LockedRangeState{},
	}
	require.True(t, admission.submit(NewRegionPriorityTask(dummyRegion, 1, 1)))

	scheduler.broadcastDeregister(SubscriptionID(1), true)
	require.Equal(t, 1, worker.controlQueue.len())
	req, ok := worker.controlQueue.tryPop()
	require.True(t, ok)
	require.Equal(t, SubscriptionID(1), req.subID)
	require.True(t, req.filterLoop)
	require.Equal(t, 1, admission.stats().pending)
}

func TestRequestedStoreDistributesRegionsAcrossWorkerBuffers(t *testing.T) {
	worker1 := &regionRequestWorker{admission: newRegionAdmissionController(1, 1)}
	worker2 := &regionRequestWorker{admission: newRegionAdmissionController(1, 1)}
	store := &requestedStore{
		storeAddr: "store-1",
		workers:   []*regionRequestWorker{worker1, worker2},
	}

	for i := uint64(1); i <= 4; i++ {
		region := regionInfo{
			verID:            tikv.NewRegionVerID(i, 1, 1),
			subscribedSpan:   &subscribedSpan{subID: 1},
			lockedRangeState: &regionlock.LockedRangeState{},
		}
		require.True(t, store.submit(NewRegionPriorityTask(region, 1, i)))
	}

	require.Equal(t, 2, worker1.admission.stats().pending)
	require.Equal(t, 2, worker2.admission.stats().pending)
}

func TestSubscriptionWithFailedTiKV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	wg := &sync.WaitGroup{}

	eventsCh1 := make(chan *cdcpb.ChangeDataEvent, 10)
	eventsCh2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataServer(eventsCh1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	srv2 := newMockChangeDataServer(eventsCh2)
	server2, addr2 := newMockService(ctx, t, srv2, wg)

	rpcClient, cluster, pdClient, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())

	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	regionCache := tikv.NewRegionCache(pdClient)
	appcontext.SetService(appcontext.RegionCache, regionCache)
	pdClock := pdutil.NewClock4Test()
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)

	invalidStore := "localhost:1"
	cluster.AddStore(1, addr1)
	cluster.AddStore(2, addr2)
	cluster.AddStore(3, invalidStore)
	// bootstrap cluster with a region which leader is in invalid store.
	cluster.Bootstrap(11, []uint64{1, 2, 3}, []uint64{4, 5, 6}, 6)

	clientConfig := &SubscriptionClientConfig{
		RegionRequestWorkerPerStore: 2,
	}
	client := NewSubscriptionClient(
		clientConfig,
		pdClient,
		nil, // we don't need it in this unittest, so we can pass nil
		&security.Credential{},
	)

	defer func() {
		cancel()
		client.Close(ctx)
		_ = kvStorage.Close()
		regionCache.Close()
		pdClient.Close()
		srv1.wg.Wait()
		srv2.wg.Wait()
		server1.Stop()
		server2.Stop()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(ctx)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	subID := client.AllocSubscriptionID()
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool {
		// should not reach here
		require.True(t, false)
		return false
	}
	tsCh := make(chan uint64, 10)
	advanceResolvedTs := func(ts uint64) {
		select {
		case <-ctx.Done():
		case tsCh <- ts:
		}
	}
	client.Subscribe(subID, span, 1, consumeKVEvents, advanceResolvedTs, 0, false)

	eventsCh1 <- mockInitializedEvent(11, uint64(subID))
	targetTs := oracle.GoTimeToTS(pdClock.CurrentTime())
	eventsCh1 <- mockTsEventBatch(11, targetTs, uint64(subID))
	// After trying to receive something from the invalid store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case resolvedTs := <-tsCh:
		require.Equal(t, targetTs, resolvedTs)
	case <-time.After(30 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}

	// Stop server1 and the client needs to handle it.
	server1.Stop()

	eventsCh2 <- mockInitializedEvent(11, uint64(subID))
	targetTs = oracle.GoTimeToTS(pdClock.CurrentTime())
	eventsCh2 <- mockTsEvent(11, targetTs, uint64(subID))
	// After trying to receive something from a failed store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case resolvedTs := <-tsCh:
		require.Equal(t, targetTs, resolvedTs)
	case <-time.After(30 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}
}

// TestErrCacheDispatchWithFullChannelAndCanceledContext tests that when errCh is full
// and context is canceled, the dispatch method doesn't get stuck.
func TestErrCacheDispatchWithFullChannelAndCanceledContext(t *testing.T) {
	// Create errCache with a small errCh to easily fill it up
	errCache := &errCache{
		cache:  make([]regionErrorInfo, 0, 10),
		errCh:  make(chan regionErrorInfo, 2), // Small buffer to easily fill
		notify: make(chan struct{}, 10),
	}

	// Create a mock regionErrorInfo
	mockErrInfo := regionErrorInfo{
		regionInfo: regionInfo{
			verID: tikv.NewRegionVerID(1, 1, 1),
			span:  heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")},
		},
		err: errors.New("test error"),
	}

	// Fill up the errCh channel to make it full
	errCache.errCh <- mockErrInfo
	errCache.errCh <- mockErrInfo

	// Add some errors to the cache
	for i := 0; i < 5; i++ {
		errCache.add(mockErrInfo)
	}

	// Create a context that will be canceled
	ctx, cancel := context.WithCancel(context.Background())

	// Channel to signal when dispatch returns
	dispatchDone := make(chan error, 1)

	// Start dispatch in a goroutine
	go func() {
		err := errCache.dispatch(ctx)
		dispatchDone <- err
	}()

	// Give dispatch some time to start and potentially get stuck
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for dispatch to return with a timeout
	select {
	case err := <-dispatchDone:
		// Verify that dispatch returned with context.Canceled error
		require.Equal(t, context.Canceled, err)
	case <-time.After(5 * time.Second):
		// If we timeout here, it means dispatch is stuck
		t.Fatal("dispatch method is stuck and didn't return after context cancellation")
	}
}

func TestErrCacheDispatchBatch(t *testing.T) {
	mockErrInfo := regionErrorInfo{
		regionInfo: regionInfo{
			verID: tikv.NewRegionVerID(1, 1, 1),
			span:  heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")},
		},
		err: errors.New("test error"),
	}

	tests := []struct {
		name          string
		cacheLen      int
		limit         int
		expectedN     int
		expectedCache int
		expectedErrCh int
	}{
		{
			name:          "dispatch all when limit equals cache length",
			cacheLen:      5,
			limit:         5,
			expectedN:     5,
			expectedCache: 0,
			expectedErrCh: 5,
		},
		{
			name:          "keep remaining cache when limit is smaller",
			cacheLen:      5,
			limit:         2,
			expectedN:     2,
			expectedCache: 3,
			expectedErrCh: 2,
		},
		{
			name:          "dispatch all when limit is larger",
			cacheLen:      5,
			limit:         10,
			expectedN:     5,
			expectedCache: 0,
			expectedErrCh: 5,
		},
		{
			name:          "dispatch all when limit is zero",
			cacheLen:      5,
			limit:         0,
			expectedN:     5,
			expectedCache: 0,
			expectedErrCh: 5,
		},
		{
			name:          "dispatch all when limit is negative",
			cacheLen:      5,
			limit:         -1,
			expectedN:     5,
			expectedCache: 0,
			expectedErrCh: 5,
		},
		{
			name:          "empty cache",
			cacheLen:      0,
			limit:         5,
			expectedN:     0,
			expectedCache: 0,
			expectedErrCh: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errCache := &errCache{
				cache:  make([]regionErrorInfo, 0, 10),
				errCh:  make(chan regionErrorInfo, 10),
				notify: make(chan struct{}, 1),
			}
			for i := 0; i < tc.cacheLen; i++ {
				errCache.add(mockErrInfo)
			}

			n, err := errCache.dispatchBatch(context.Background(), tc.limit)
			require.NoError(t, err)
			require.Equal(t, tc.expectedN, n)
			require.Len(t, errCache.cache, tc.expectedCache)
			require.Len(t, errCache.errCh, tc.expectedErrCh)
		})
	}
}

func TestGetResolvedTargetTs(t *testing.T) {
	client := &subscriptionClient{
		resolveLockTaskCh:      make(chan resolveLockTask, 10),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(ts uint64) {}

	span := newSubscribedSpan(client.ctx, client.resolveLockRateLimiter, client.resolveLockTaskCh, SubscriptionID(1), heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}, 100, consumeKVEvents, advanceResolvedTs, 0, false)
	span.initialized.Store(true)

	// Replicate the getResolvedTargetTs closure from runResolveLockChecker
	getResolvedTargetTs := func(subSpan *subscribedSpan, currentTime time.Time, currentTs uint64) uint64 {
		resolvedTsUpdated := time.Unix(subSpan.resolvedTsUpdated.Load(), 0)
		if !subSpan.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
			return 0
		}
		resolvedTs := subSpan.resolvedTs.Load()
		resolvedTime := oracle.GetTimeFromTS(resolvedTs)
		if currentTime.Sub(resolvedTime) < resolveLockFence {
			return 0
		}
		return min(currentTs, oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence)))
	}

	// Simulate clock skew: local pdClock is 30s ahead of PD.
	// In the real scenario:
	//   - currentTs comes from pd.GetTS (PD time)
	//   - currentTime comes from pdClock.CurrentTime() (local clock, could be ahead)
	//   - resolvedTs is a TiKV/PD timestamp
	pdNow := time.Now()
	localClockNow := pdNow.Add(30 * time.Second) // local clock 30s ahead
	currentTs := oracle.GoTimeToTS(pdNow)

	// resolvedTime is 2 seconds ago in PD time, so:
	//   resolvedTime + resolveLockFence = pdNow - 2s + 4s = pdNow + 2s > pdNow
	//   => oracle.GoTimeToTS(resolvedTime + resolveLockFence) > currentTs
	// But currentTime (local) - resolvedTime = 32s > 4s (resolveLockFence), so the check passes
	resolvedTime := pdNow.Add(-2 * time.Second)
	resolvedTs := oracle.GoTimeToTS(resolvedTime)
	span.resolvedTs.Store(resolvedTs)
	span.resolvedTsUpdated.Store(pdNow.Add(-10 * time.Second).Unix())

	// Verify the setup: resolvedTime + resolveLockFence should exceed currentTs
	tsIfUncapped := oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence))
	require.True(t, tsIfUncapped > currentTs,
		"setup: resolvedTime+resolveLockFence TS (%d) should exceed currentTs (%d)", tsIfUncapped, currentTs)

	// With the fix (min), targetTs should be capped at currentTs
	targetTs := getResolvedTargetTs(span, localClockNow, currentTs)
	require.Equal(t, currentTs, targetTs,
		"targetTs should be capped at currentTs when resolvedTime+resolveLockFence exceeds it")

	// Test case 2: resolvedTime + resolveLockFence is in the past (< currentTs)
	// resolvedTime = 20s ago, +4s = 16s ago < pdNow, so tsIfUncapped < currentTs
	resolvedTime2 := pdNow.Add(-20 * time.Second)
	span.resolvedTs.Store(oracle.GoTimeToTS(resolvedTime2))
	tsIfUncapped2 := oracle.GoTimeToTS(resolvedTime2.Add(resolveLockFence))
	require.True(t, tsIfUncapped2 < currentTs,
		"setup: resolvedTime+resolveLockFence TS (%d) should be less than currentTs (%d)", tsIfUncapped2, currentTs)

	targetTs2 := getResolvedTargetTs(span, localClockNow, currentTs)
	require.Equal(t, tsIfUncapped2, targetTs2,
		"targetTs should be resolvedTime+resolveLockFence when it's less than currentTs")

	// Test case 3: span not initialized
	span.initialized.Store(false)
	targetTs3 := getResolvedTargetTs(span, localClockNow, currentTs)
	require.Equal(t, uint64(0), targetTs3, "targetTs should be 0 when span is not initialized")

	// Test case 4: resolvedTsUpdated is recent (within resolveLockFence)
	span.initialized.Store(true)
	span.resolvedTsUpdated.Store(time.Now().Unix())
	targetTs4 := getResolvedTargetTs(span, localClockNow, currentTs)
	require.Equal(t, uint64(0), targetTs4, "targetTs should be 0 when resolvedTsUpdated is recent")

	// Test case 5: currentTime - resolvedTime < resolveLockFence (should return 0)
	span.resolvedTsUpdated.Store(pdNow.Add(-10 * time.Second).Unix())
	recentResolvedTime := localClockNow.Add(-2 * time.Second) // 2s ago in local time
	span.resolvedTs.Store(oracle.GoTimeToTS(recentResolvedTime))
	targetTs5 := getResolvedTargetTs(span, localClockNow, currentTs)
	require.Equal(t, uint64(0), targetTs5,
		"targetTs should be 0 when currentTime - resolvedTime < resolveLockFence")
}
