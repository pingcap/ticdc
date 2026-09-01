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
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func newTestRegionErrorInfo(err error) regionErrorInfo {
	return regionErrorInfo{
		regionInfo: regionInfo{
			verID: tikv.NewRegionVerID(1, 1, 1),
			span:  heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")},
		},
		err: err,
	}
}

func TestErrCachePopBatch(t *testing.T) {
	mockErrInfo := newTestRegionErrorInfo(errors.New("test error"))

	tests := []struct {
		name          string
		cacheLen      int
		limit         int
		expectedN     int
		expectedCache int
		expectedCalls int
	}{
		{
			name:          "handle all when limit equals cache length",
			cacheLen:      5,
			limit:         5,
			expectedN:     5,
			expectedCache: 0,
			expectedCalls: 5,
		},
		{
			name:          "keep remaining cache when limit is smaller",
			cacheLen:      5,
			limit:         2,
			expectedN:     2,
			expectedCache: 3,
			expectedCalls: 2,
		},
		{
			name:          "handle all when limit is larger",
			cacheLen:      5,
			limit:         10,
			expectedN:     5,
			expectedCache: 0,
			expectedCalls: 5,
		},
		{
			name:          "handle all when limit is zero",
			cacheLen:      5,
			limit:         0,
			expectedN:     5,
			expectedCache: 0,
			expectedCalls: 5,
		},
		{
			name:          "handle all when limit is negative",
			cacheLen:      5,
			limit:         -1,
			expectedN:     5,
			expectedCache: 0,
			expectedCalls: 5,
		},
		{
			name:          "empty cache",
			cacheLen:      0,
			limit:         5,
			expectedN:     0,
			expectedCache: 0,
			expectedCalls: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errCache := &errCache{
				cache:  make([]regionErrorInfo, 0, 10),
				notify: make(chan struct{}, 1),
			}
			for i := 0; i < tc.cacheLen; i++ {
				errCache.add(mockErrInfo)
			}

			batch := errCache.popBatch(tc.limit)
			n := len(batch)
			require.Equal(t, tc.expectedN, n)
			require.Len(t, batch, tc.expectedCalls)
			require.Len(t, errCache.cache, tc.expectedCache)
		})
	}
}

func TestRegionFailureHandlerRunDrainsErrCacheWithoutDispatcher(t *testing.T) {
	handler := newRegionFailureHandler(nil, func(*subscribedSpan) {}, func(context.Context, regionInfo) {}, func(context.Context, rangeTask) {})
	for i := 0; i < errCacheBatchSize+5; i++ {
		handler.cache.add(newTestRegionErrorInfo(&requestCancelledErr{}))
	}

	ctx, cancel := context.WithCancel(t.Context())

	runDone := make(chan error, 1)
	go func() {
		runDone <- handler.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		handler.cache.Lock()
		defer handler.cache.Unlock()
		return len(handler.cache.cache) == 0
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-runDone:
		require.Equal(t, context.Canceled, errors.Cause(err))
	case <-time.After(time.Second):
		t.Fatal("failure handler did not exit after context cancellation")
	}
}

func TestRegionFailureHandlerSchedulesNotLeaderRangeRetry(t *testing.T) {
	pdClient := newFailureRecoveryTestPDClient(t)
	defer pdClient.Close()

	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()

	region := createFailureRecoveryTestRegion(t, SubscriptionID(1), 1)
	region.subscribedSpan.priorityPolicy = newScanPriorityPolicy(pdutil.NewClock4Test(), 30*time.Minute)

	rangeRetryCh := make(chan rangeTask, 2)
	handler := newRegionFailureHandler(
		regionCache,
		func(*subscribedSpan) {},
		func(context.Context, regionInfo) {
			t.Fatal("unexpected region retry")
		},
		func(_ context.Context, task rangeTask) {
			rangeRetryCh <- task
		},
	)
	errInfo := newRegionErrorInfo(region, &eventError{
		err: &cdcpb.Error{NotLeader: &errorpb.NotLeader{}},
	})

	ctx := t.Context()

	require.NoError(t, handler.handleError(ctx, errInfo))

	select {
	case task := <-rangeRetryCh:
		require.Equal(t, region.span, task.span)
		require.Same(t, region.subscribedSpan, task.subscribedSpan)
	case <-time.After(time.Second):
		t.Fatal("not leader retry was not scheduled")
	}
}

func TestRegionRecoveryBackoffFollowsRangeAcrossRegionChanges(t *testing.T) {
	regionRetryCh := make(chan regionInfo, 2)
	handler := newRegionFailureHandler(
		nil,
		func(*subscribedSpan) {},
		func(_ context.Context, region regionInfo) {
			regionRetryCh <- region
		},
		func(context.Context, rangeTask) {},
	)
	t.Cleanup(handler.cancelRecoveries)

	region := createFailureRecoveryTestRegion(t, SubscriptionID(1), 1)
	errInfo := newRegionErrorInfo(region, &eventError{
		err: &cdcpb.Error{Congested: &cdcpb.Congested{}},
	})
	require.NoError(t, handler.handleError(context.Background(), errInfo))
	select {
	case retried := <-regionRetryCh:
		require.Equal(t, uint64(1), retried.verID.GetID())
	case <-time.After(time.Second):
		t.Fatal("first region recovery was not scheduled")
	}

	region.verID = tikv.NewRegionVerID(2, 1, 1)
	errInfo = newRegionErrorInfo(region, &eventError{
		err: &cdcpb.Error{Congested: &cdcpb.Congested{}},
	})
	require.NoError(t, handler.handleError(context.Background(), errInfo))
	select {
	case retried := <-regionRetryCh:
		require.Equal(t, uint64(2), retried.verID.GetID())
	case <-time.After(time.Second):
		t.Fatal("second region recovery was not scheduled")
	}

	key := newRegionRecoveryKey(region.subscribedSpan.subID, region.span)
	handler.recovery.Lock()
	attempt := handler.recovery.states[key].attempt
	handler.recovery.Unlock()
	require.Equal(t, uint32(2), attempt)
}

func TestScheduleRecoveryCoalescesPendingRetries(t *testing.T) {
	handler := newRegionFailureHandler(nil, func(*subscribedSpan) {}, func(context.Context, regionInfo) {}, func(context.Context, rangeTask) {})
	t.Cleanup(handler.cancelRecoveries)

	ctx := t.Context()

	subSpan := &subscribedSpan{subID: SubscriptionID(1)}
	span := heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("b")}
	retryCh := make(chan struct{}, 2)
	retry := func() {
		retryCh <- struct{}{}
	}

	handler.scheduleRecovery(ctx, subSpan, span, 100*time.Millisecond, retry)
	handler.scheduleRecovery(ctx, subSpan, span, 100*time.Millisecond, retry)

	select {
	case <-retryCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retry was not scheduled")
	}

	select {
	case <-retryCh:
		t.Fatal("expected coalesced retries for the same recovery key")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRegionFailureHandlerRequestCancelledResetsRecoveryState(t *testing.T) {
	handler := newRegionFailureHandler(nil, func(*subscribedSpan) {}, func(context.Context, regionInfo) {}, func(context.Context, rangeTask) {})
	region := createFailureRecoveryTestRegion(t, SubscriptionID(1), 1)
	key := newRegionRecoveryKey(region.subscribedSpan.subID, region.span)
	handler.recovery.states[key] = &regionRecoveryState{}

	err := handler.handleError(context.Background(), newRegionErrorInfo(region, &requestCancelledErr{}))
	require.NoError(t, err)

	handler.recovery.Lock()
	_, ok := handler.recovery.states[key]
	handler.recovery.Unlock()
	assert.False(t, ok)
}

func TestRegionFailureHandlerExpiresRecoveryStates(t *testing.T) {
	handler := newRegionFailureHandler(nil, func(*subscribedSpan) {}, func(context.Context, regionInfo) {}, func(context.Context, rangeTask) {})
	now := time.Now()
	expiredKey := newRegionRecoveryKey(1, heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("b")})
	activeKey := newRegionRecoveryKey(1, heartbeatpb.TableSpan{StartKey: []byte("b"), EndKey: []byte("c")})
	handler.recovery.states[expiredKey] = &regionRecoveryState{expiresAt: now.Add(-time.Second)}
	handler.recovery.states[activeKey] = &regionRecoveryState{expiresAt: now.Add(time.Second)}

	handler.expireRecoveries(now)

	handler.recovery.Lock()
	_, expiredExists := handler.recovery.states[expiredKey]
	_, activeExists := handler.recovery.states[activeKey]
	handler.recovery.Unlock()
	require.False(t, expiredExists)
	require.True(t, activeExists)
}

func TestSafeBackoffDuration(t *testing.T) {
	require.Equal(t, time.Duration(0), safeBackoffDuration(0))
	require.Equal(t, 123*time.Millisecond, safeBackoffDuration(123))
	require.Equal(t, 10*time.Second, safeBackoffDuration(10_000))
	require.Equal(t, 10*time.Minute, safeBackoffDuration(10*60*1000))
	require.Equal(t, 10*time.Minute, safeBackoffDuration(11*60*1000))
	require.Equal(t, 10*time.Minute, safeBackoffDuration(math.MaxUint64))
}
