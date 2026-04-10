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
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestScheduleRegionRequestUpdatesRuntimeRegistry(t *testing.T) {
	client := &subscriptionClient{
		regionTaskQueue:       NewPriorityQueue(),
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
		pdClock:               pdutil.NewClock4Test(),
	}

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	regionSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'b'},
		EndKey:   []byte{'c'},
	}
	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), regionSpan, nil, subSpan, false)

	client.scheduleRegionRequest(context.Background(), region, TaskLowPrior)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := client.regionTaskQueue.Pop(ctx)
	require.NoError(t, err)
	queued := task.GetRegionInfo()
	require.True(t, queued.runtimeKey.isValid())

	state, ok := client.regionRuntimeRegistry.get(queued.runtimeKey)
	require.True(t, ok)
	require.Equal(t, regionPhaseQueued, state.phase)
	require.Equal(t, uint64(10), state.verID.GetID())
	require.False(t, state.rangeLockAcquiredTime.IsZero())
}

func TestOnRegionFailUpdatesRuntimeRegistry(t *testing.T) {
	client := &subscriptionClient{
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
		errCache:              newErrCache(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	defer client.cancel()

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	lockRes := subSpan.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 10, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes.Status)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'b'},
		EndKey:   []byte{'c'},
	}, nil, subSpan, false)
	region.lockedRangeState = lockRes.LockedRangeState

	client.ensureRegionRuntime(&region, time.Now())
	require.True(t, region.runtimeKey.isValid())

	client.onRegionFail(newRegionErrorInfo(region, &sendRequestToStoreErr{}))

	state, ok := client.regionRuntimeRegistry.get(region.runtimeKey)
	require.True(t, ok)
	require.Equal(t, regionPhaseDiscovered, state.phase)
	require.Equal(t, "send request to store error", state.lastError)
	require.Equal(t, 0, state.retryCount)
}

func TestHandleResolvedTsUpdatesRuntimeRegistry(t *testing.T) {
	client := &subscriptionClient{
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
	}
	worker := &regionRequestWorker{client: client}

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	subSpan := &subscribedSpan{
		subID:      SubscriptionID(1),
		startTs:    100,
		span:       rawSpan,
		rangeLock:  regionlock.NewRangeLock(1, rawSpan.StartKey, rawSpan.EndKey, 100),
		filterLoop: false,
	}
	subSpan.resolvedTs.Store(100)
	subSpan.resolvedTsUpdated.Store(time.Now().Unix())
	subSpan.advanceInterval = 0

	lockRes := subSpan.rangeLock.LockRange(context.Background(), rawSpan.StartKey, rawSpan.EndKey, 10, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes.Status)
	lockRes.LockedRangeState.Initialized.Store(true)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	region.lockedRangeState = lockRes.LockedRangeState
	region.runtimeKey = client.regionRuntimeRegistry.allocKey(subSpan.subID, region.verID.GetID())
	client.regionRuntimeRegistry.updateRegionInfo(region.runtimeKey, region)

	state := newRegionFeedState(region, uint64(subSpan.subID), worker)
	state.start()

	resolvedTs := uint64(200)
	handleResolvedTs(subSpan, state, resolvedTs)

	stored, ok := client.regionRuntimeRegistry.get(region.runtimeKey)
	require.True(t, ok)
	require.Equal(t, resolvedTs, stored.lastResolvedTs)
	require.False(t, stored.lastEventTime.IsZero())
}

func TestDoHandleErrorMarksRetryPendingForRetryableRegionError(t *testing.T) {
	client := &subscriptionClient{
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
		regionTaskQueue:       NewPriorityQueue(),
		pdClock:               pdutil.NewClock4Test(),
	}

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	client.ensureRegionRuntime(&region, time.Now())

	err := client.doHandleError(context.Background(), newRegionErrorInfo(region, &eventError{
		err: &cdcpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{Reason: "busy"}},
	}))
	require.NoError(t, err)

	state, ok := client.regionRuntimeRegistry.get(region.runtimeKey)
	require.True(t, ok)
	require.Equal(t, regionPhaseQueued, state.phase)
	require.Equal(t, 1, state.retryCount)
	require.Contains(t, state.lastError, "server_is_busy")
}

func TestDoHandleErrorRemovesRuntimeForRangeReload(t *testing.T) {
	client := &subscriptionClient{
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
		rangeTaskCh:           make(chan rangeTask, 1),
	}

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	client.ensureRegionRuntime(&region, time.Now())

	err := client.doHandleError(context.Background(), newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
	require.NoError(t, err)

	_, ok := client.regionRuntimeRegistry.get(region.runtimeKey)
	require.False(t, ok)

	select {
	case task := <-client.rangeTaskCh:
		require.Equal(t, rawSpan, task.span)
		require.Equal(t, subSpan, task.subscribedSpan)
	case <-time.After(time.Second):
		t.Fatal("expected range task to be scheduled")
	}
}

func TestDoHandleErrorRemovesRuntimeForCancelledRequest(t *testing.T) {
	client := &subscriptionClient{
		regionRuntimeRegistry: newRegionRuntimeRegistry(),
	}

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{'a'},
		EndKey:   []byte{'z'},
	}
	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(uint64) {}
	subSpan := client.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	region := newRegionInfo(tikv.NewRegionVerID(10, 1, 1), rawSpan, nil, subSpan, false)
	client.ensureRegionRuntime(&region, time.Now())

	err := client.doHandleError(context.Background(), newRegionErrorInfo(region, &requestCancelledErr{}))
	require.NoError(t, err)

	_, ok := client.regionRuntimeRegistry.get(region.runtimeKey)
	require.False(t, ok)
}
