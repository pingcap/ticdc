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

package util

import "sync"

// GuardedWaitGroup wraps a WaitGroup with a mutex to prevent the Go runtime rule violation
// where calling Add while another goroutine is waiting causes a data race or panic.
type GuardedWaitGroup struct {
	mu sync.Mutex
	wg sync.WaitGroup
}

// AddIf increments the wait group counter when predicate returns true.
// Predicate executes while the mutex is held to ensure no Wait can progress concurrently.
// When predicate is nil it is treated as true.
func (g *GuardedWaitGroup) AddIf(predicate func() bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if predicate != nil && !predicate() {
		return false
	}
	g.wg.Add(1)
	return true
}

// Done decrements the wait group counter.
func (g *GuardedWaitGroup) Done() {
	g.wg.Done()
}

// Wait waits for the wait group while holding the guard mutex so no further AddIf calls can progress.
func (g *GuardedWaitGroup) Wait() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.wg.Wait()
}
