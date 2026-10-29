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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lake

import (
	"sync"
	"sync/atomic"
)

// eventCompletion owns the source flush callback independently of spool entries.
// Spool release/discard/close must never call a fragment's applied method.
type eventCompletion struct {
	mu        sync.Mutex
	pending   int
	sealed    bool
	failed    bool
	completed bool
	callback  func()
}

type fragmentCompletion struct {
	done   atomic.Bool
	parent *eventCompletion
}

func newEventCompletion(callback func()) *eventCompletion {
	return &eventCompletion{callback: callback}
}

// register must happen before publishing a fragment to a worker.
func (c *eventCompletion) register() (*fragmentCompletion, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed || c.failed || c.completed {
		return nil, false
	}
	c.pending++
	return &fragmentCompletion{parent: c}, true
}

func (c *eventCompletion) seal() {
	c.mu.Lock()
	c.sealed = true
	callback := c.finishLocked()
	c.mu.Unlock()
	if callback != nil {
		callback()
	}
}

func (c *eventCompletion) fail() {
	c.mu.Lock()
	c.failed = true
	c.callback = nil
	c.mu.Unlock()
}

func (c *eventCompletion) finishLocked() func() {
	if !c.sealed || c.failed || c.completed || c.pending != 0 {
		return nil
	}
	c.completed = true
	callback := c.callback
	c.callback = nil
	return callback
}

// applied is called only with confirmed target application. Its caller may
// release the fragment payload immediately, even while the event is OPEN.
func (f *fragmentCompletion) applied() {
	if !f.done.CompareAndSwap(false, true) {
		return
	}
	c := f.parent
	c.mu.Lock()
	c.pending--
	callback := c.finishLocked()
	c.mu.Unlock()
	if callback != nil {
		callback()
	}
}
