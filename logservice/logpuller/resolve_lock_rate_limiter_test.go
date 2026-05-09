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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveLockRateLimiterDedupAndThrottle(t *testing.T) {
	now := time.Now()
	limiter := newResolveLockRateLimiter()
	key := resolveLockKey{keyspaceID: 1, regionID: 1}

	require.True(t, limiter.tryEnqueue(key, now))
	require.False(t, limiter.tryEnqueue(key, now.Add(2*resolveLockMinInterval)))

	limiter.finish(key, now.Add(time.Second))
	require.False(t, limiter.tryEnqueue(key, now.Add(time.Second+resolveLockMinInterval/2)))
	require.True(t, limiter.tryEnqueue(key, now.Add(time.Second+resolveLockMinInterval)))
}

func TestResolveLockRateLimiterKeyedByKeyspaceAndRegion(t *testing.T) {
	now := time.Now()
	limiter := newResolveLockRateLimiter()

	require.True(t, limiter.tryEnqueue(resolveLockKey{keyspaceID: 1, regionID: 1}, now))
	require.True(t, limiter.tryEnqueue(resolveLockKey{keyspaceID: 2, regionID: 1}, now))
	require.True(t, limiter.tryEnqueue(resolveLockKey{keyspaceID: 1, regionID: 2}, now))
}

func TestResolveLockRateLimiterGC(t *testing.T) {
	now := time.Now()
	limiter := newResolveLockRateLimiter()
	const keep = 10
	for i := 0; i < resolveLastRunGCThreshold+1; i++ {
		lastRunTime := now.Add(-2 * resolveLockMinInterval)
		if i < keep {
			lastRunTime = now.Add(-resolveLockMinInterval / 2)
		}
		limiter.lastRun[resolveLockKey{keyspaceID: 1, regionID: uint64(i)}] = lastRunTime
	}

	limiter.gc(now)
	require.Len(t, limiter.lastRun, keep)
	for i := 0; i < keep; i++ {
		_, ok := limiter.lastRun[resolveLockKey{keyspaceID: 1, regionID: uint64(i)}]
		require.True(t, ok)
	}
}
