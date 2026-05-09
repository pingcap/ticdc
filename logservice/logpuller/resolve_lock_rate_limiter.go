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

// resolveLockRateLimiter deduplicates and throttles resolve-lock tasks by
// (keyspaceID, regionID). A key in pending means a task is already queued or
// running. lastRun records the last actual resolve completion time, so new
// tasks are blocked until minInterval has passed after the previous execution.
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

// tryEnqueue is called before sending a resolve-lock task to the queue.
// It rejects duplicate pending tasks and tasks still inside the cooldown window.
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

// cancel clears the pending mark when a task cannot be queued or no longer needs to run.
func (l *resolveLockRateLimiter) cancel(key resolveLockKey) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.pending, key)
}

// tryStart is called by the resolve-lock worker right before executing Resolve.
// It rechecks the cooldown window in case the task waited in the queue.
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

// finish is called after Resolve returns to clear pending and start the cooldown window.
func (l *resolveLockRateLimiter) finish(key resolveLockKey, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.pending, key)
	l.lastRun[key] = now
	l.gcLocked(now)
}

// gc removes old cooldown records after the map grows beyond the threshold.
func (l *resolveLockRateLimiter) gc(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.gcLocked(now)
}

// canRunLocked reports whether a key is outside its cooldown window.
func (l *resolveLockRateLimiter) canRunLocked(key resolveLockKey, now time.Time) bool {
	lastRun, ok := l.lastRun[key]
	return !ok || now.Sub(lastRun) >= l.minInterval
}

// gcLocked drops expired last-run entries while keeping pending tasks intact.
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
