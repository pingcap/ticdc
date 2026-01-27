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
	"sync/atomic"
)

// pauseGate provides a low-contention gate for pausing many concurrent producers.
//
// When paused, waiters block on a channel which is closed on resume.
// It avoids a global mutex/cond that every event would contend on.
type pauseGate struct {
	pausedCh atomic.Value // stores chan struct{} (typed nil when not paused)
}

func newPauseGate() *pauseGate {
	g := &pauseGate{}
	g.pausedCh.Store((chan struct{})(nil))
	return g
}

func (g *pauseGate) pause() {
	ch := g.pausedCh.Load().(chan struct{})
	if ch != nil {
		return
	}
	g.pausedCh.Store(make(chan struct{}))
}

func (g *pauseGate) resume() {
	ch := g.pausedCh.Load().(chan struct{})
	if ch == nil {
		return
	}
	close(ch)
	g.pausedCh.Store((chan struct{})(nil))
}

// wait blocks while paused. It returns false if ctx is done.
func (g *pauseGate) wait(ctx context.Context) bool {
	for {
		ch := g.pausedCh.Load().(chan struct{})
		if ch == nil {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ch:
		}
	}
}
