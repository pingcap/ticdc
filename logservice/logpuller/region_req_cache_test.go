// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func createTestRegionInfo(subID SubscriptionID, regionID uint64) regionInfo {
	verID := tikv.NewRegionVerID(regionID, 1, 1)
	span := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
	}
	return newRegionInfo(verID, span, nil, &subscribedSpan{
		subID:   subID,
		startTs: 100,
		span:    span,
	}, false)
}

func TestRegionRegisterQueuePriorityAndCapacity(t *testing.T) {
	queue := newRegionRegisterQueue(2)
	ctx := context.Background()

	ok, err := queue.add(ctx, createTestRegionInfo(1, 1), 10)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = queue.add(ctx, createTestRegionInfo(1, 2), 1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = queue.add(ctx, createTestRegionInfo(1, 3), 0)
	require.NoError(t, err)
	require.False(t, ok)

	req, ok, _ := queue.tryPopOrNotify()
	require.True(t, ok)
	require.Equal(t, uint64(2), req.regionInfo.verID.GetID())

	ok, err = queue.add(ctx, createTestRegionInfo(1, 3), 0)
	require.NoError(t, err)
	require.True(t, ok)
	req, ok, _ = queue.tryPopOrNotify()
	require.True(t, ok)
	require.Equal(t, uint64(3), req.regionInfo.verID.GetID())
	require.Equal(t, 1, queue.len())
}

func TestRegionRegisterQueueContextCancellation(t *testing.T) {
	queue := newRegionRegisterQueue(1)
	ok, err := queue.add(context.Background(), createTestRegionInfo(1, 1), 0)
	require.NoError(t, err)
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, err = queue.add(ctx, createTestRegionInfo(1, 2), 0)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
}

func TestRegionScanLimiterExactLimitAndIdempotentRelease(t *testing.T) {
	limiter := newRegionScanLimiter(2)
	slot1, _ := limiter.tryAcquireOrNotify()
	slot2, _ := limiter.tryAcquireOrNotify()
	slot3, notify := limiter.tryAcquireOrNotify()
	require.NotNil(t, slot1)
	require.NotNil(t, slot2)
	require.Nil(t, slot3)
	require.NotNil(t, notify)
	require.Equal(t, 2, activeScanCount(limiter))

	slot1.release()
	slot1.release()
	require.Equal(t, 1, activeScanCount(limiter))
	select {
	case <-notify:
	default:
		t.Fatal("slot release must notify waiters")
	}

	slot3, _ = limiter.tryAcquireOrNotify()
	require.NotNil(t, slot3)
	require.Equal(t, 2, activeScanCount(limiter))
	slot2.release()
	slot3.release()
	require.Equal(t, 0, activeScanCount(limiter))
}

func TestRegionScanLimiterNeverExceedsLimit(t *testing.T) {
	const (
		limit      = 4
		goroutines = 32
	)
	limiter := newRegionScanLimiter(limit)
	var wg sync.WaitGroup
	start := make(chan struct{})
	maxActive := 0
	var maxMu sync.Mutex

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				slot, notify := limiter.tryAcquireOrNotify()
				if slot == nil {
					<-notify
					continue
				}
				active := activeScanCount(limiter)
				maxMu.Lock()
				if active > maxActive {
					maxActive = active
				}
				maxMu.Unlock()
				slot.release()
				return
			}
		}()
	}
	close(start)
	wg.Wait()
	require.LessOrEqual(t, maxActive, limit)
	require.Equal(t, 0, activeScanCount(limiter))
}

func TestRegionRequestTrackerTerminalReleasePaths(t *testing.T) {
	testCases := []struct {
		name    string
		release func(*testing.T, *regionRequestTracker, SubscriptionID, uint64)
	}{
		{
			name: "initialized",
			release: func(t *testing.T, tracker *regionRequestTracker, subID SubscriptionID, regionID uint64) {
				require.True(t, tracker.resolve(subID, regionID))
			},
		},
		{
			name: "region error or send failure",
			release: func(t *testing.T, tracker *regionRequestTracker, subID SubscriptionID, regionID uint64) {
				require.True(t, tracker.stop(subID, regionID))
			},
		},
		{
			name: "deregister",
			release: func(_ *testing.T, tracker *regionRequestTracker, subID SubscriptionID, _ uint64) {
				tracker.removeSubscription(subID)
			},
		},
		{
			name: "reconnect or shutdown",
			release: func(t *testing.T, tracker *regionRequestTracker, _ SubscriptionID, _ uint64) {
				require.Len(t, tracker.clear(), 1)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			limiter := newRegionScanLimiter(1)
			slot, _ := limiter.tryAcquireOrNotify()
			tracker := newRegionRequestTracker()
			req := newRegionReq(createTestRegionInfo(1, 1), 0)
			require.True(t, tracker.track(req, slot))
			require.Equal(t, 1, activeScanCount(limiter))

			tc.release(t, tracker, 1, 1)
			require.Equal(t, 0, activeScanCount(limiter))
			require.Equal(t, 0, tracker.len())
			require.False(t, tracker.stop(1, 1))
		})
	}
}

func TestRegionRequestTrackerRejectsDuplicate(t *testing.T) {
	limiter := newRegionScanLimiter(2)
	tracker := newRegionRequestTracker()
	req := newRegionReq(createTestRegionInfo(1, 1), 0)
	slot1, _ := limiter.tryAcquireOrNotify()
	slot2, _ := limiter.tryAcquireOrNotify()
	require.True(t, tracker.track(req, slot1))
	require.False(t, tracker.track(newRegionReq(req.regionInfo, 0), slot2))
	slot2.release()
	tracker.clear()
	require.Equal(t, 0, activeScanCount(limiter))
}

func activeScanCount(limiter *regionScanLimiter) int {
	active, _ := limiter.usage()
	return active
}
