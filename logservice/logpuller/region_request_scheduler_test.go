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
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockcopr"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"golang.org/x/sync/errgroup"
)

func TestRegionRequestSchedulerBroadcastDeregisterUsesWorkerControlQueue(t *testing.T) {
	scheduler := &regionRequestScheduler{}

	worker1 := &regionRequestWorker{
		storeAddr:    "store-1",
		admission:    newTestRegionAdmissionController(1, 1),
		controlQueue: newControlQueue(),
	}
	worker2 := &regionRequestWorker{
		storeAddr:    "store-2",
		admission:    newTestRegionAdmissionController(1, 1),
		controlQueue: newControlQueue(),
	}
	store1 := &regionRequestStore{workers: []*regionRequestWorker{worker1}}
	store2 := &regionRequestStore{workers: []*regionRequestWorker{worker2}}
	scheduler.stores.Store("store-1", store1)
	scheduler.stores.Store("store-2", store2)

	dummyRegion := regionInfo{
		subscribedSpan:   &subscribedSpan{subID: SubscriptionID(2)},
		lockedRangeState: &regionlock.LockedRangeState{},
	}
	require.True(t, worker1.admission.submit(newRegionPriorityTask(dummyRegion, 1)))

	scheduler.BroadcastDeregister(SubscriptionID(1), true)

	req1, ok := worker1.controlQueue.tryPop()
	require.True(t, ok)
	require.Equal(t, SubscriptionID(1), req1.subID)
	require.True(t, req1.filterLoop)

	req2, ok := worker2.controlQueue.tryPop()
	require.True(t, ok)
	require.Equal(t, SubscriptionID(1), req2.subID)
	require.True(t, req2.filterLoop)

	require.Equal(t, 1, worker1.admission.stats().pending)
}

func TestRegionRequestSchedulerRequestedRegionCountAggregatesStores(t *testing.T) {
	scheduler := &regionRequestScheduler{}

	worker1 := &regionRequestWorker{admission: newTestRegionAdmissionController(2, 1)}
	worker2 := &regionRequestWorker{admission: newTestRegionAdmissionController(2, 1)}
	scheduler.stores.Store("store-1", &regionRequestStore{workers: []*regionRequestWorker{worker1}})
	scheduler.stores.Store("store-2", &regionRequestStore{workers: []*regionRequestWorker{worker2}})

	req1 := admitRegionRequest(t, worker1.admission, prepareRegionForAdmission(createTestRegionInfo(1, 1), 100))
	req2 := admitRegionRequest(t, worker2.admission, prepareRegionForAdmission(createTestRegionInfo(1, 2), 100))
	require.True(t, worker1.admission.submit(newRegionPriorityTask(
		prepareRegionForAdmission(createTestRegionInfo(1, 3), 100), 3)))
	require.True(t, worker2.admission.submit(newRegionPriorityTask(
		prepareRegionForAdmission(createTestRegionInfo(1, 4), 100), 4)))

	require.Equal(t, 4, scheduler.requestedRegionCount())

	require.True(t, req1.abort())
	require.True(t, req2.abort())
}

func TestRegionRequestSchedulerReschedulesRegionWhenStoreSubmitFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, cluster, pdClient, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	defer pdClient.Close()

	const storeAddr = "store-1"
	cluster.AddStore(1, storeAddr)
	cluster.Bootstrap(11, []uint64{1}, []uint64{2}, 2)

	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()

	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	location, err := regionCache.LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}
	span := &subscribedSpan{
		subID:          SubscriptionID(1),
		startTs:        100,
		span:           rawSpan,
		rangeLock:      regionlock.NewRangeLock(1, rawSpan.StartKey, rawSpan.EndKey, 100),
		priorityPolicy: newScanPriorityPolicy(pdutil.NewClock4Test(), 30*time.Minute),
	}
	lockRes := span.rangeLock.LockRange(
		context.Background(), rawSpan.StartKey, rawSpan.EndKey, location.Region.GetID(), location.Region.GetVer())
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes.Status)

	admission := newTestRegionAdmissionController(1, 1)
	admission.close()
	store := &regionRequestStore{workers: []*regionRequestWorker{{admission: admission}}}

	handler := newRegionFailureHandler(nil, func(*subscribedSpan) {}, nil, nil)
	scheduler := &regionRequestScheduler{
		upstream: &upstreamHandle{
			pd:          pdClient,
			regionCache: regionCache,
		},
		failureHandler: handler,
		taskQueue:      priorityqueue.New[*regionPriorityTask](),
	}
	scheduler.stores.Store(storeAddr, store)

	region := newRegionInfo(location.Region, rawSpan, nil, span, false)
	region.lockedRangeState = lockRes.LockedRangeState
	scheduler.taskQueue.Push(newRegionPriorityTask(region, 1))

	var workerGroup errgroup.Group
	errCh := make(chan error, 1)
	go func() {
		errCh <- scheduler.Run(ctx, &workerGroup)
	}()

	require.Eventually(t, func() bool {
		return errCacheLen(handler) == 1
	}, time.Second, 20*time.Millisecond)

	select {
	case err := <-errCh:
		t.Fatalf("scheduler exited unexpectedly: %v", err)
	default:
	}

	batch := handler.cache.popBatch(1)
	require.Len(t, batch, 1)
	require.Equal(t, region.verID, batch[0].verID)
	var streamErr *storeStreamErr
	require.ErrorAs(t, batch[0].err, &streamErr)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestRegionRequestSchedulerSkipsStoppedSubscriptionBeforeCreatingStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, cluster, pdClient, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	defer pdClient.Close()

	const storeAddr = "store-1"
	cluster.AddStore(1, storeAddr)
	cluster.Bootstrap(11, []uint64{1}, []uint64{2}, 2)

	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()

	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	location, err := regionCache.LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}
	span := &subscribedSpan{
		subID:          SubscriptionID(1),
		startTs:        100,
		span:           rawSpan,
		rangeLock:      regionlock.NewRangeLock(1, rawSpan.StartKey, rawSpan.EndKey, 100),
		priorityPolicy: newScanPriorityPolicy(pdutil.NewClock4Test(), 30*time.Minute),
	}
	lockRes := span.rangeLock.LockRange(
		context.Background(), rawSpan.StartKey, rawSpan.EndKey, location.Region.GetID(), location.Region.GetVer())
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes.Status)
	span.stopped.Store(true)
	require.False(t, span.rangeLock.Stop())

	drainedCh := make(chan *subscribedSpan, 1)
	handler := newRegionFailureHandler(nil, func(rt *subscribedSpan) {
		drainedCh <- rt
	}, nil, nil)
	scheduler := &regionRequestScheduler{
		upstream: &upstreamHandle{
			pd:          pdClient,
			regionCache: regionCache,
		},
		failureHandler: handler,
		taskQueue:      priorityqueue.New[*regionPriorityTask](),
	}

	region := newRegionInfo(location.Region, rawSpan, nil, span, false)
	region.lockedRangeState = lockRes.LockedRangeState
	scheduler.taskQueue.Push(newRegionPriorityTask(region, 1))

	var workerGroup errgroup.Group
	errCh := make(chan error, 1)
	go func() {
		errCh <- scheduler.Run(ctx, &workerGroup)
	}()

	select {
	case drained := <-drainedCh:
		require.Same(t, span, drained)
	case <-time.After(time.Second):
		t.Fatal("stopped subscription was not drained")
	}

	_, ok := scheduler.stores.Load(storeAddr)
	require.False(t, ok)

	select {
	case err := <-errCh:
		t.Fatalf("scheduler exited unexpectedly: %v", err)
	default:
	}

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}
