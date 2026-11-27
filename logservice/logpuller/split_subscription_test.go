// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockcopr"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestSpanSplitPlan(t *testing.T) {
	for _, test := range []struct {
		regions int
		spans   int
	}{
		{1, 1}, {4, 1}, {5, 1}, {6, 3}, {7, 4},
	} {
		t.Run(fmt.Sprint(test.regions), func(t *testing.T) {
			original := heartbeatpb.TableSpan{KeyspaceID: 9, TableID: 10, StartKey: []byte("b"), EndKey: []byte("y")}
			plan := spanSplitPlan{
				span: original, startKey: original.StartKey,
				config: config.PullerSplitConfig{RegionThreshold: 5, RegionCountPerSpan: 2},
			}
			for i := 0; i < test.regions; i++ {
				end := []byte{byte('c' + i)}
				if i == test.regions-1 {
					end = nil // The final Region extends beyond the subscribed range.
				}
				plan.addRegion(&metapb.Region{EndKey: end})
			}
			spans := plan.finish()
			require.Len(t, spans, test.spans)
			require.Equal(t, original.StartKey, spans[0].StartKey)
			require.Equal(t, original.EndKey, spans[len(spans)-1].EndKey)
			for i, span := range spans {
				require.Equal(t, original.TableID, span.TableID)
				require.Equal(t, original.KeyspaceID, span.KeyspaceID)
				require.False(t, common.IsEmptySpan(span))
				if i != 0 {
					require.Equal(t, spans[i-1].EndKey, span.StartKey)
				}
			}
		})
	}
}

func newSplitSubscriptionForTest(
	t *testing.T,
	consume func([]common.RawKVEntry, func()) bool,
	advance func(uint64),
) (*subscriptionClient, *splitSubscription) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clock := pdutil.NewClock4Test()
	client := &subscriptionClient{
		ctx: ctx, cancel: cancel,
		upstream:               &upstreamHandle{pdClock: clock},
		spanRegistry:           newSpanRegistry(nil, clock),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
		resolveLockTaskCh:      make(chan resolveLockTask, 10),
		memoryQuota:            newMemoryQuotaController(1<<20, 1024),
		regionScheduler:        &regionRequestScheduler{taskQueue: priorityqueue.New[*regionPriorityTask]()},
		rangeTaskCh:            make(chan rangeTask, 10),
		splitConfig:            config.PullerSplitConfig{EnableSpanSplit: true, RegionThreshold: 2, RegionCountPerSpan: 1},
	}
	client.eventSink = &regionEventSink{ctx: ctx, ds: &mockDynamicStream{}, memoryQuota: client.memoryQuota}
	client.failureHandler = newRegionFailureHandler(nil, client.onTableDrained, client.scheduleRegionRequest, client.scheduleRangeRequest)
	parent := newSubscribedSpan(ctx, client.resolveLockRateLimiter, client.resolveLockTaskCh,
		client.AllocSubscriptionID(), heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")},
		10, consume, advance, 0, true, clock, time.Minute)
	parent.baseScanPriority = ScanPriorityHigh
	group := newSplitSubscription(client, parent)
	client.splitSubscriptions.Store(parent.subID, group)
	return client, group
}

func TestSplitSubscriptionDiscovery(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			client, group := newSplitSubscriptionForTest(t, func([]common.RawKVEntry, func()) bool { return false }, func(uint64) {})
			_, cluster, pd, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
			t.Cleanup(pd.Close)
			cluster.AddStore(1, "store-1")
			cluster.Bootstrap(11, []uint64{1}, []uint64{2}, 2)
			cluster.Split(11, 12, []byte("m"), []uint64{3}, 3)
			cluster.Split(12, 13, []byte("t"), []uint64{4}, 4)
			cache := tikv.NewRegionCache(pd)
			t.Cleanup(cache.Close)
			client.upstream.regionCache = cache
			client.splitConfig.EnableSpanSplit = enabled
			client.splitSubscriptions.Delete(group.parent.subID)
			parent := group.parent
			client.Subscribe(parent.subID, parent.span, parent.startTs, parent.consumeKVEvents,
				parent.advanceResolvedTs, 0, true, ScanPriorityHigh)
			task := <-client.rangeTaskCh
			if enabled {
				require.NotNil(t, task.splitSubscription)
				require.NoError(t, task.splitSubscription.run(client.ctx))
			} else {
				require.Nil(t, task.splitSubscription)
				require.NoError(t, client.divideSpanAndScheduleRegionRequests(client.ctx, task))
			}
			regions := make([]regionInfo, 0, 3)
			for {
				task, ok := client.regionScheduler.taskQueue.TryPop()
				if !ok {
					break
				}
				regions = append(regions, task.regionInfo)
			}
			require.Len(t, regions, 3)
			for i, region := range regions {
				require.True(t, region.filterLoop)
				require.Equal(t, ScanPriorityHigh, region.scanPriority)
				require.Equal(t, uint64(10), region.resolvedTs())
				if enabled {
					require.Equal(t, region.span.StartKey, region.subscribedSpan.span.StartKey)
					require.Equal(t, region.span.EndKey, region.subscribedSpan.span.EndKey)
					if i > 0 {
						require.NotEqual(t, regions[i-1].subscribedSpan.subID, region.subscribedSpan.subID)
					}
				} else {
					require.Equal(t, parent.subID, region.subscribedSpan.subID)
				}
			}
			client.Unsubscribe(parent.subID)
			for _, region := range regions {
				client.failureHandler.Report(newRegionErrorInfo(region, &requestCancelledErr{}))
			}
			for _, span := range client.failureHandler.cache.popDrainedSpans(10) {
				client.onTableDrained(span)
			}
			require.Empty(t, client.spanRegistry.spans)
			_, exists := client.splitSubscriptions.Load(parent.subID)
			require.False(t, exists)
		})
	}
}

func TestSplitSubscriptionAcknowledgedProgress(t *testing.T) {
	var callbacks []func()
	var published []uint64
	client, group := newSplitSubscriptionForTest(t, func(events []common.RawKVEntry, done func()) bool {
		require.Len(t, events, 1)
		callbacks = append(callbacks, done)
		return true
	}, func(ts uint64) { published = append(published, ts) })
	spans := []heartbeatpb.TableSpan{
		{TableID: 1, StartKey: []byte("a"), EndKey: []byte("m")},
		{TableID: 1, StartKey: []byte("m"), EndKey: []byte("z")},
	}
	children := group.prepare(spans)
	group.finishPlanning()
	handler := &regionEventHandler{eventSink: client.eventSink}
	states := make([]*regionFeedState, 2)
	for i, child := range children {
		lock := child.rangeLock.LockRange(client.ctx, child.span.StartKey, child.span.EndKey, uint64(i+1), 1)
		states[i] = newRegionFeedState(regionInfo{
			subscribedSpan: child, span: child.span, lockedRangeState: lock.LockedRangeState,
			verID: tikv.NewRegionVerID(uint64(i+1), 1, 1), rpcCtx: &tikv.RPCContext{},
		}, uint64(child.subID), nil, nil, nil)
		states[i].setInitialized()
	}
	send := func(i int, ts uint64) {
		state := states[i]
		blocked := handler.Handle(children[i],
			regionEvent{states: []*regionFeedState{state}, entries: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{Entries: []*cdcpb.Event_Row{{
					Type: cdcpb.Event_COMMITTED, OpType: cdcpb.Event_Row_PUT,
					Key: []byte("key"), CommitTs: ts,
				}}},
			}},
			regionEvent{states: []*regionFeedState{state}, resolvedTs: ts})
		require.True(t, blocked)
	}
	send(0, 20)
	send(1, 30)
	callbacks[1]()
	require.Empty(t, published)
	// The fast child can consume another batch while the slow child's first
	// write is outstanding. Public progress still waits for the slow child.
	send(1, 40)
	callbacks[2]()
	require.Empty(t, published)
	callbacks[0]()
	require.Equal(t, []uint64{20}, published)

	// A retry hole must retain the child's old watermark until its replacement
	// regions are initialized and advance again.
	first := children[0]
	first.rangeLock.UnlockRange(first.span.StartKey, first.span.EndKey, 1, 1)
	left := first.rangeLock.LockRange(client.ctx, []byte("a"), []byte("g"), 3, 2)
	right := first.rangeLock.LockRange(client.ctx, []byte("g"), []byte("m"), 4, 2)
	for i, lock := range []*regionlock.LockedRangeState{left.LockedRangeState, right.LockedRangeState} {
		state := newRegionFeedState(regionInfo{subscribedSpan: first, lockedRangeState: lock}, uint64(first.subID), nil, nil, nil)
		if i == 0 {
			state.setInitialized()
		}
		handler.Handle(first, regionEvent{states: []*regionFeedState{state}, resolvedTs: 35})
		require.Equal(t, []uint64{20}, published)
		state.setInitialized()
		handler.Handle(first, regionEvent{states: []*regionFeedState{state}, resolvedTs: 35})
	}
	require.Equal(t, []uint64{20, 35}, published)

	// Delayed completion after unsubscribe cannot publish a new watermark.
	send(1, 50)
	client.Unsubscribe(group.parent.subID)
	callbacks[3]()
	require.Equal(t, []uint64{20, 35}, published)
}

func TestSplitSubscriptionConcurrentProgressAndCancelPlanning(t *testing.T) {
	var published []uint64
	client, group := newSplitSubscriptionForTest(t, func(_ []common.RawKVEntry, done func()) bool {
		done() // Inline acknowledgment must not deadlock with progress reporting.
		return true
	}, func(ts uint64) { published = append(published, ts) })
	children := group.prepare([]heartbeatpb.TableSpan{
		{TableID: 1, StartKey: []byte("a"), EndKey: []byte("m")},
		{TableID: 1, StartKey: []byte("m"), EndKey: []byte("z")},
	})
	var wg sync.WaitGroup
	for _, child := range children {
		wg.Go(func() {
			group.consume(nil, func() { child.advanceResolvedTs(10) })
			for ts := uint64(10); ts <= 100; ts++ {
				child.advanceResolvedTs(ts)
			}
		})
	}
	wg.Wait()
	require.NotEmpty(t, published)
	require.Equal(t, uint64(100), published[len(published)-1])
	for i := 1; i < len(published); i++ {
		require.Greater(t, published[i], published[i-1])
	}
	client.Unsubscribe(group.parent.subID)
	_, exists := client.splitSubscriptions.Load(group.parent.subID)
	require.True(t, exists, "planning still owns the public subscription")
	require.Empty(t, group.prepare([]heartbeatpb.TableSpan{group.parent.span}))
	group.finishPlanning()
	_, exists = client.splitSubscriptions.Load(group.parent.subID)
	require.False(t, exists)
}

func TestSplitSubscriptionIndependentPaths(t *testing.T) {
	type batch struct {
		regionID uint64
		done     func()
	}
	batches := make(chan batch, 4)
	watermarks := make(chan uint64, 4)
	client, group := newSplitSubscriptionForTest(t, func(events []common.RawKVEntry, done func()) bool {
		batches <- batch{regionID: events[0].RegionID, done: done}
		return true
	}, func(ts uint64) { watermarks <- ts })
	client.eventSink = newRegionEventSink(client.ctx, client.failureHandler, client.memoryQuota)
	t.Cleanup(client.eventSink.Close)
	children := group.prepare([]heartbeatpb.TableSpan{
		{TableID: 1, StartKey: []byte("a"), EndKey: []byte("m")},
		{TableID: 1, StartKey: []byte("m"), EndKey: []byte("z")},
	})
	group.finishPlanning()
	states := make([]*regionFeedState, 2)
	for i, child := range children {
		id := uint64(i + 1)
		lock := child.rangeLock.LockRange(client.ctx, child.span.StartKey, child.span.EndKey, id, 1)
		states[i] = newRegionFeedState(regionInfo{
			subscribedSpan: child, span: child.span, lockedRangeState: lock.LockedRangeState,
			verID: tikv.NewRegionVerID(id, 1, 1), rpcCtx: &tikv.RPCContext{},
		}, uint64(child.subID), nil, nil, nil)
		child.markRegionInitialized(states[i])
	}
	push := func(i int, ts uint64) {
		state := states[i]
		client.eventSink.Push(children[i].subID, regionEvent{
			states: []*regionFeedState{state},
			entries: &cdcpb.Event_Entries_{Entries: &cdcpb.Event_Entries{Entries: []*cdcpb.Event_Row{{
				Type: cdcpb.Event_COMMITTED, OpType: cdcpb.Event_Row_PUT,
				Key: []byte("key"), CommitTs: ts,
			}}}},
		})
		client.eventSink.Push(children[i].subID, regionEvent{states: []*regionFeedState{state}, resolvedTs: ts})
	}
	receive := func() batch {
		select {
		case result := <-batches:
			return result
		case <-time.After(5 * time.Second):
			t.Fatal("child path did not deliver its next batch")
			return batch{}
		}
	}
	push(0, 20)
	slow := receive()
	push(1, 30)
	fast := receive()
	require.Equal(t, uint64(1), slow.regionID)
	require.Equal(t, uint64(2), fast.regionID)
	fast.done()
	push(1, 40)
	fast = receive() // No acknowledgment has been given to the slow child.
	require.Equal(t, uint64(2), fast.regionID)
	fast.done()
	select {
	case ts := <-watermarks:
		t.Fatalf("watermark %d passed an unacknowledged child", ts)
	default:
	}
	slow.done()
	select {
	case ts := <-watermarks:
		require.Equal(t, uint64(20), ts)
	case <-time.After(5 * time.Second):
		t.Fatal("public watermark did not advance after acknowledgment")
	}
	client.Unsubscribe(group.parent.subID)
	for _, state := range states {
		client.failureHandler.Report(newRegionErrorInfo(state.region, &requestCancelledErr{}))
	}
	for _, span := range client.failureHandler.cache.popDrainedSpans(10) {
		client.onTableDrained(span)
	}
	require.Empty(t, client.spanRegistry.spans)
}
