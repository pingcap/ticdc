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

type resolveLockKey struct {
	keyspaceID uint32
	regionID   uint64
}

// resolveLockRateLimiter limits how frequently resolve-lock can be attempted
// for the same keyspace and region. Pending tasks are tracked separately from
// actual execution time, so queued tasks are deduplicated without moving the
// execution throttle window forward.
type resolveLockRateLimiter struct {
	mu          sync.Mutex
	pending     map[resolveLockKey]struct{}
	lastRun     map[resolveLockKey]time.Time
	minInterval time.Duration
}

func newResolveLockRateLimiter() *resolveLockRateLimiter {
	return &resolveLockRateLimiter{
		pending:     make(map[resolveLockKey]struct{}),
		lastRun:     make(map[resolveLockKey]time.Time),
		minInterval: resolveLockMinInterval,
	}
}

func (l *resolveLockRateLimiter) tryEnqueue(key resolveLockKey, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.pending[key]; ok {
		return false
	}
	if !l.canRunLocked(key, now) {
		return false
	}

	l.pending[key] = struct{}{}
	l.gcLocked(now)
	return true
}

func (l *resolveLockRateLimiter) cancel(key resolveLockKey) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.pending, key)
}

func (l *resolveLockRateLimiter) tryStart(key resolveLockKey, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.canRunLocked(key, now) {
		delete(l.pending, key)
		l.gcLocked(now)
		return false
	}

	l.pending[key] = struct{}{}
	l.gcLocked(now)
	return true
}

func (l *resolveLockRateLimiter) finish(key resolveLockKey, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.pending, key)
	l.lastRun[key] = now
	l.gcLocked(now)
}

func (l *resolveLockRateLimiter) gc(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.gcLocked(now)
}

func (l *resolveLockRateLimiter) canRunLocked(key resolveLockKey, now time.Time) bool {
	lastRun, ok := l.lastRun[key]
	return !ok || now.Sub(lastRun) >= l.minInterval
}

func (l *resolveLockRateLimiter) gcLocked(now time.Time) {
	if len(l.lastRun) <= resolveLastRunGCThreshold {
		return
	}
	for key, lastRun := range l.lastRun {
		if _, ok := l.pending[key]; ok {
			continue
		}
		if now.Sub(lastRun) >= l.minInterval {
			delete(l.lastRun, key)
		}
	}
}
