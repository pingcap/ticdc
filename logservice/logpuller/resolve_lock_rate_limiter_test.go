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

func TestResolveLockRateLimiterTryMark(t *testing.T) {
	now := time.Now()
	limiter := newResolveLockRateLimiter()

	require.True(t, limiter.tryMark(1, now))
	require.False(t, limiter.tryMark(1, now.Add(resolveLockMinInterval/2)))
	require.True(t, limiter.tryMark(1, now.Add(resolveLockMinInterval)))
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
		limiter.lastRun[uint64(i)] = lastRunTime
	}

	limiter.gc(now)
	require.Len(t, limiter.lastRun, keep)
	for i := 0; i < keep; i++ {
		_, ok := limiter.lastRun[uint64(i)]
		require.True(t, ok)
	}
}
