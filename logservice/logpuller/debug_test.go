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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func TestPullerDebugInfo(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := pdutil.NewClockWithValue4Test(now)
	registry := newSpanRegistry(nil, clock)

	slowSpan := newDebugTestSpan(101, 42, now.Add(-10*time.Second), clock)
	slowRegion := lockDebugTestRegion(
		t, slowSpan, "a", "g", 11, now.Add(-8*time.Second), true)
	waitingRegion := lockDebugTestRegion(
		t, slowSpan, "m", "z", 12, now.Add(-2*time.Second), false)
	registry.Add(slowSpan)

	fastSpan := newDebugTestSpan(102, 43, now.Add(-time.Second), clock)
	registry.Add(fastSpan)

	mediumSpan := newDebugTestSpan(104, 45, now.Add(-5*time.Second), clock)
	registry.Add(mediumSpan)

	stoppingSpan := newDebugTestSpan(103, 44, now.Add(-time.Hour), clock)
	stoppingSpan.stopped.Store(true)
	registry.Add(stoppingSpan)

	worker := &regionRequestWorker{
		workerID: 9,
		tracker:  newRegionTracker(),
	}
	region := regionInfo{
		verID:            tikv.NewRegionVerID(11, 1, 1),
		span:             heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("g")},
		subscribedSpan:   slowSpan,
		lockedRangeState: slowRegion,
	}
	require.True(t, worker.tracker.Add(
		slowSpan.subID,
		region.verID.GetID(),
		newRegionFeedState(region, uint64(slowSpan.subID), worker, nil, nil),
	))
	region12 := regionInfo{
		verID:            tikv.NewRegionVerID(12, 1, 1),
		span:             heartbeatpb.TableSpan{StartKey: []byte("m"), EndKey: []byte("z")},
		subscribedSpan:   slowSpan,
		lockedRangeState: waitingRegion,
	}
	require.True(t, worker.tracker.Add(
		slowSpan.subID,
		region12.verID.GetID(),
		newRegionFeedState(region12, uint64(slowSpan.subID), worker, &regionReq{
			createTime: now.Add(-time.Second),
		}, nil),
	))
	scheduler := &regionRequestScheduler{}
	scheduler.stores.Store("tikv-1:20160", &regionRequestStore{
		workers: []*regionRequestWorker{worker},
	})

	client := &subscriptionClient{
		spanRegistry:    registry,
		regionScheduler: scheduler,
	}
	info := client.GetPullerDebugInfo(PullerDebugOptions{
		SubscriptionLimit: 2,
		RegionLimit:       1,
	})

	require.Equal(t, now, info.SnapshotAt)
	require.Len(t, info.SlowSubscriptions, 2)
	slowest := info.SlowSubscriptions[0]
	require.Equal(t, SubscriptionID(101), slowest.SubscriptionID)
	require.Equal(t, int64(42), slowest.TableID)
	require.Equal(t, int64(10*time.Second/time.Millisecond),
		slowest.ResolvedTsLagMillis)
	require.Equal(t, 2, slowest.LockedRegions)
	require.Equal(t, 1, slowest.InitializedRegions)
	require.Equal(t, 1, slowest.UninitializedRegions)
	require.Equal(t, 1, slowest.UncoveredRanges)
	require.Equal(t, SubscriptionID(104), info.SlowSubscriptions[1].SubscriptionID)

	require.Len(t, slowest.SlowRegions, 1)
	require.Equal(t, uint64(11), slowest.SlowRegions[0].RegionID)
	require.Equal(t, int64(8*time.Second/time.Millisecond),
		slowest.SlowRegions[0].ResolvedTsLagMs)
	require.Equal(t, "tikv-1:20160", slowest.SlowRegions[0].StoreAddress)
	require.Equal(t, uint64(9), slowest.SlowRegions[0].WorkerID)
	require.Equal(t, "streaming", slowest.SlowRegions[0].Phase)
	require.False(t, slowest.SlowRegions[0].CreatedAt.IsZero())

	detail, found := client.GetPullerDebugRegion(slowSpan.subID, 12)
	require.True(t, found)
	require.Equal(t, uint64(12), detail.Region.RegionID)
	require.False(t, detail.Region.Initialized)
	require.Equal(t, "waiting_tikv_initial_scan", detail.Region.Phase)
	require.Equal(t, "tikv-1:20160", detail.Region.StoreAddress)
	require.Equal(t, now.Add(-time.Second), *detail.Region.RequestCreatedAt)
	_, found = client.GetPullerDebugRegion(slowSpan.subID, 999)
	require.False(t, found)
}

func TestPullerDebugInfoConcurrent(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := pdutil.NewClockWithValue4Test(now)
	span := newDebugTestSpan(1, 1, now, clock)
	lockDebugTestRegion(t, span, "a", "z", 11, now, false)
	registry := newSpanRegistry(nil, clock)
	registry.Add(span)
	client := &subscriptionClient{spanRegistry: registry}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			span.resolvedTs.Store(oracle.GoTimeToTS(now.Add(time.Duration(i) * time.Millisecond)))
			registry.Remove(span.subID)
			registry.Add(span)
		}
	}()
	for i := 0; i < 100; i++ {
		client.GetPullerDebugInfo(PullerDebugOptions{
			SubscriptionLimit: 1,
			RegionLimit:       1,
		})
		client.GetPullerDebugRegion(span.subID, 11)
	}
	wg.Wait()
}

func newDebugTestSpan(
	subID SubscriptionID,
	tableID int64,
	resolvedAt time.Time,
	clock pdutil.Clock,
) *subscribedSpan {
	span := heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}
	result := &subscribedSpan{
		subID:          subID,
		span:           span,
		rangeLock:      regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, oracle.GoTimeToTS(resolvedAt)),
		priorityPolicy: newScanPriorityPolicy(clock, time.Minute),
	}
	result.resolvedTs.Store(oracle.GoTimeToTS(resolvedAt))
	return result
}

func lockDebugTestRegion(
	t *testing.T,
	span *subscribedSpan,
	startKey string,
	endKey string,
	regionID uint64,
	resolvedAt time.Time,
	initialized bool,
) *regionlock.LockedRangeState {
	t.Helper()
	result := span.rangeLock.LockRange(
		context.Background(), []byte(startKey), []byte(endKey), regionID, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, result.Status)
	result.LockedRangeState.ResolvedTs.Store(oracle.GoTimeToTS(resolvedAt))
	result.LockedRangeState.Initialized.Store(initialized)
	return result.LockedRangeState
}
