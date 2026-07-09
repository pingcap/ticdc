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

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/heartbeatpb"
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
	handler := newRegionFailureHandler(&subscriptionClient{})
	for i := 0; i < errCacheBatchSize+5; i++ {
		handler.cache.add(newTestRegionErrorInfo(&requestCancelledErr{}))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- handler.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		handler.cache.Lock()
		defer handler.cache.Unlock()
		return len(handler.cache.cache) == 0
	}, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-runDone:
		require.Equal(t, context.Canceled, errors.Cause(err))
	case <-time.After(time.Second):
		t.Fatal("failure handler did not exit after context cancellation")
	}
}
