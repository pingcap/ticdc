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
	"sync"
	"time"
)

// resolveLastRunGCThreshold is the size threshold to GC resolveLastRun and drop stale entries.
const resolveLastRunGCThreshold = 1024

// resolveLockRateLimiter limits how frequently resolve-lock can be attempted
// for the same region. minInterval is the minimum time between two attempts for
// one region.
type resolveLockRateLimiter struct {
	mu          sync.Mutex
	lastRun     map[uint64]time.Time
	minInterval time.Duration
}

func newResolveLockRateLimiter() *resolveLockRateLimiter {
	return &resolveLockRateLimiter{
		lastRun:     make(map[uint64]time.Time),
		minInterval: resolveLockMinInterval,
	}
}

func (l *resolveLockRateLimiter) allow(regionID uint64, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	lastRun, ok := l.lastRun[regionID]
	if ok && now.Sub(lastRun) < l.minInterval {
		return false
	}
	return true
}

func (l *resolveLockRateLimiter) mark(regionID uint64, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lastRun[regionID] = now
	l.gcLocked(now)
}

func (l *resolveLockRateLimiter) tryMark(regionID uint64, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	lastRun, ok := l.lastRun[regionID]
	if ok && now.Sub(lastRun) < l.minInterval {
		return false
	}
	l.lastRun[regionID] = now
	l.gcLocked(now)
	return true
}

func (l *resolveLockRateLimiter) unmark(regionID uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.lastRun, regionID)
}

func (l *resolveLockRateLimiter) gc(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.gcLocked(now)
}

func (l *resolveLockRateLimiter) gcLocked(now time.Time) {
	if len(l.lastRun) <= resolveLastRunGCThreshold {
		return
	}
	for regionID, lastRun := range l.lastRun {
		if now.Sub(lastRun) >= l.minInterval {
			delete(l.lastRun, regionID)
		}
	}
}
