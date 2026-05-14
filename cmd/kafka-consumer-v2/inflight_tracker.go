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

package main

import (
	"context"
	"sync"
	"time"
)

type inflightTracker struct {
	mu       sync.Mutex
	inflight map[uint64]int
}

func newInflightTracker() *inflightTracker {
	return &inflightTracker{inflight: make(map[uint64]int)}
}

func (t *inflightTracker) Add(commitTs uint64) {
	t.mu.Lock()
	t.inflight[commitTs]++
	t.mu.Unlock()
}

func (t *inflightTracker) Done(commitTs uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	count := t.inflight[commitTs]
	if count <= 1 {
		delete(t.inflight, commitTs)
		return
	}
	t.inflight[commitTs] = count - 1
}

func (t *inflightTracker) HasLTE(commitTs uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for ts, count := range t.inflight {
		if ts <= commitTs && count > 0 {
			return true
		}
	}
	return false
}

func (t *inflightTracker) WaitLTE(ctx context.Context, commitTs uint64) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !t.HasLTE(commitTs) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
