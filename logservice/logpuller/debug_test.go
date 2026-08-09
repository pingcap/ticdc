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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func TestPullerDebugSnapshots(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := pdutil.NewClockWithValue4Test(now)
	quota := newMemoryQuotaController(1000, 10)
	quota.used.Store(120)
	quota.scanMu.Lock()
	quota.scanUsed = 80
	quota.scanMu.Unlock()

	spanRange := heartbeatpb.TableSpan{
		TableID:    42,
		StartKey:   []byte("a"),
		EndKey:     []byte("z"),
		KeyspaceID: 7,
	}
	span := &subscribedSpan{
		subID:           101,
		startTs:         oracle.GoTimeToTS(now.Add(-time.Minute)),
		span:            spanRange,
		rangeLock:       regionlock.NewRangeLock(101, spanRange.StartKey, spanRange.EndKey, oracle.GoTimeToTS(now.Add(-time.Minute))),
		advanceInterval: 100,
		priorityPolicy:  newScanPriorityPolicy(clock, 30*time.Minute),
	}
	span.resolvedTs.Store(oracle.GoTimeToTS(now.Add(-5 * time.Second)))
	span.resolvedTsUpdated.Store(now.Add(-3 * time.Second).Unix())
	span.lastAdvanceTime.Store(now.Add(-2 * time.Second).UnixMilli())

	lockRegion := func(start, end string, regionID uint64, lag time.Duration, initialized bool) *regionlock.LockedRangeState {
		result := span.rangeLock.LockRange(
			context.Background(), []byte(start), []byte(end), regionID, 1)
		require.Equal(t, regionlock.LockRangeStatusSuccess, result.Status)
		result.LockedRangeState.ResolvedTs.Store(oracle.GoTimeToTS(now.Add(-lag)))
		result.LockedRangeState.Initialized.Store(initialized)
		return result.LockedRangeState
	}
	state11 := lockRegion("a", "g", 11, 2*time.Second, true)
	state12 := lockRegion("m", "z", 12, 5*time.Second, false)

	registry := newSpanRegistry(nil, clock)
	registry.Add(span)

	worker := &regionRequestWorker{
		workerID:     9,
		admission:    newTestRegionAdmissionController(4, 1),
		controlQueue: newControlQueue(),
		tracker:      newRegionTracker(),
	}
	region11 := regionInfo{
		verID:            tikv.NewRegionVerID(11, 1, 1),
		span:             heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("g")},
		subscribedSpan:   span,
		lockedRangeState: state11,
	}
	region12 := regionInfo{
		verID:            tikv.NewRegionVerID(12, 1, 1),
		span:             heartbeatpb.TableSpan{StartKey: []byte("m"), EndKey: []byte("z")},
		subscribedSpan:   span,
		lockedRangeState: state12,
	}
	require.True(t, worker.tracker.Add(span.subID, 11,
		newRegionFeedState(region11, uint64(span.subID), worker, nil, nil)))
	require.True(t, worker.tracker.Add(span.subID, 12,
		newRegionFeedState(region12, uint64(span.subID), worker, nil, nil)))
	require.True(t, worker.admission.submit(newRegionPriorityTask(region12, 1)))
	worker.controlQueue.push(deregisterRequest{subID: 999})

	scheduler := &regionRequestScheduler{taskQueue: priorityqueue.New[*regionPriorityTask]()}
	scheduler.stores.Store("tikv-1:20160", &regionRequestStore{
		workers: []*regionRequestWorker{worker},
	})
	scheduler.taskQueue.Push(newRegionPriorityTask(region11, 2))

	failureHandler := newRegionFailureHandler(nil, nil, nil, nil)
	failureHandler.cache.cache = append(failureHandler.cache.cache, newRegionErrorInfo(region12, &storeStreamErr{}))
	failureHandler.recoveries[newRegionRecoveryKey(span.subID, region12.span)] = &regionRecoveryState{attempt: 3}

	ds := newMockRegionEventSinkStream()
	ds.metrics.EventChanSize = 4
	ds.metrics.PendingQueueLen = 5
	ds.metrics.AddPath = 2
	ds.metrics.RemovePath = 1

	client := &subscriptionClient{
		ctx:               context.Background(),
		spanRegistry:      registry,
		regionScheduler:   scheduler,
		failureHandler:    failureHandler,
		eventSink:         &regionEventSink{ds: ds, memoryQuota: quota},
		memoryQuota:       quota,
		rangeTaskCh:       make(chan rangeTask, 2),
		resolveLockTaskCh: make(chan resolveLockTask, 2),
	}
	client.runState.Store(pullerDebugStateRunning)
	client.rangeTaskCh <- rangeTask{}
	client.resolveLockTaskCh <- resolveLockTask{}

	overview := client.GetPullerDebugInfo()
	require.Equal(t, "running", overview.State)
	require.Equal(t, PullerQueueDebugInfo{Length: 1, Capacity: 2}, overview.Channels.RangeTask)
	require.Equal(t, 1, overview.Subscriptions.Total)
	require.Equal(t, 1, overview.Subscriptions.Uninitialized)
	require.Equal(t, 2, overview.Subscriptions.LockedRegions)
	require.Equal(t, 1, overview.Scheduler.GlobalPending)
	require.Equal(t, 1, overview.Scheduler.WorkerPending)
	require.Equal(t, 2, overview.Scheduler.TrackedRegions)
	require.Equal(t, 1, overview.Failure.PendingErrors)
	require.Equal(t, 1, overview.Failure.RecoveringRanges)
	require.Equal(t, uint32(3), overview.Failure.MaxRecoveryAttempt)
	require.Equal(t, 5, overview.EventSink.PendingQueue)
	require.Equal(t, 1, overview.EventSink.Paths)
	require.Equal(t, uint64(120), overview.Memory.EventUsedBytes)
	require.Equal(t, uint64(80), overview.Memory.ScanUsedBytes)

	detail, found := client.GetPullerDebugSubscription(span.subID, PullerSubscriptionDebugOptions{
		RegionMode:  "slow",
		RegionLimit: 1,
		IncludeKeys: true,
	})
	require.True(t, found)
	require.Equal(t, 2, detail.Ranges.LockedRegions)
	require.Equal(t, 1, detail.Ranges.InitializedRegions)
	require.Equal(t, 1, detail.Ranges.UninitializedRegions)
	require.Equal(t, 1, detail.Ranges.UncoveredRanges)
	require.Equal(t, 1, detail.Pipeline.StreamingRegions)
	require.Equal(t, 1, detail.Pipeline.InitialScanning)
	require.Equal(t, 1, detail.Pipeline.RecoveringRanges)
	require.True(t, detail.RegionsTruncated)
	require.Len(t, detail.Regions, 1)
	require.Equal(t, uint64(12), detail.Regions[0].RegionID)
	require.Equal(t, "initial_scan", detail.Regions[0].Phase)
	require.Equal(t, "tikv-1:20160", detail.Regions[0].StoreAddress)
	require.Equal(t, "67", detail.UncoveredRanges[0].StartKey)
	require.Equal(t, "6d", detail.UncoveredRanges[0].EndKey)

	stores := client.GetPullerDebugStores()
	require.Len(t, stores, 1)
	require.Equal(t, 2, stores[0].TrackedRegions)
	require.Empty(t, stores[0].Workers)
	store, found := client.GetPullerDebugStore("tikv-1:20160")
	require.True(t, found)
	require.Len(t, store.Workers, 1)
	require.Equal(t, 2, store.Workers[0].TrackedRegions)
	_, found = client.GetPullerDebugSubscription(999, PullerSubscriptionDebugOptions{})
	require.False(t, found)
}

func TestPullerDebugSnapshotsConcurrent(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := pdutil.NewClockWithValue4Test(now)
	spanRange := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	span := &subscribedSpan{
		subID:          1,
		span:           spanRange,
		rangeLock:      regionlock.NewRangeLock(1, spanRange.StartKey, spanRange.EndKey, oracle.GoTimeToTS(now)),
		priorityPolicy: newScanPriorityPolicy(clock, time.Minute),
	}
	lockResult := span.rangeLock.LockRange(context.Background(), []byte("a"), []byte("z"), 11, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockResult.Status)

	registry := newSpanRegistry(nil, clock)
	registry.Add(span)
	worker := &regionRequestWorker{
		workerID:     1,
		admission:    newTestRegionAdmissionController(1, 1),
		controlQueue: newControlQueue(),
		tracker:      newRegionTracker(),
	}
	region := regionInfo{
		verID:            tikv.NewRegionVerID(11, 1, 1),
		span:             spanRange,
		subscribedSpan:   span,
		lockedRangeState: lockResult.LockedRangeState,
	}
	state := newRegionFeedState(region, uint64(span.subID), worker, nil, nil)
	scheduler := &regionRequestScheduler{taskQueue: priorityqueue.New[*regionPriorityTask]()}
	scheduler.stores.Store("tikv-1:20160", &regionRequestStore{workers: []*regionRequestWorker{worker}})
	client := &subscriptionClient{
		ctx:             context.Background(),
		spanRegistry:    registry,
		regionScheduler: scheduler,
		memoryQuota:     newMemoryQuotaController(100, 10),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			span.resolvedTs.Store(oracle.GoTimeToTS(now.Add(time.Duration(i) * time.Millisecond)))
			span.resolvedTsUpdated.Store(now.Unix() + int64(i))
			registry.Remove(span.subID)
			registry.Add(span)
			worker.tracker.Add(span.subID, 11, state)
			worker.tracker.RemoveIf(span.subID, 11, state)
		}
	}()
	for i := 0; i < 100; i++ {
		client.GetPullerDebugInfo()
		client.GetPullerDebugSubscription(span.subID, PullerSubscriptionDebugOptions{RegionMode: "all"})
		client.GetPullerDebugStores()
	}
	wg.Wait()
}
