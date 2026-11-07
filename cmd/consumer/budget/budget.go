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

// Package budget accounts for the memory owned by the consumer conductor.
package budget

import (
	"context"
	"math"
	"runtime/debug"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

const (
	// defaultMemoryBudget applies when the process memory limit is unavailable.
	defaultMemoryBudget = 1 << 30
	// memoryBudgetRatio is the share of the process memory limit admitted as
	// accounted data; the rest covers the Go runtime and unaccounted memory.
	memoryBudgetRatio = 0.6
	// goMemoryLimitRatio keeps the Go runtime below the process memory limit.
	goMemoryLimitRatio = 0.9
)

// Budget is a soft budget for raw input, captured data, decoded rows and
// pending transport positions. Each owner releases its own allocation.
// The limit scales with the process memory limit; it does not model process or
// object overhead.
type Budget struct {
	limit     int64
	bytes     atomic.Int64
	waiting   atomic.Bool
	requested atomic.Int64
	mu        sync.Mutex
	changed   chan struct{}
}

// New builds a budget from the process memory limit.
func New() *Budget {
	limit, err := util.GetMemoryLimit()
	if err != nil || limit == 0 {
		log.Info("consumer memory quota", zap.Int64("quotaBytes", defaultMemoryBudget))
		return &Budget{
			limit: defaultMemoryBudget,
		}
	}
	goLimit := debug.SetMemoryLimit(-1)
	if goLimit == math.MaxInt64 {
		goLimit = int64(float64(limit) * goMemoryLimitRatio)
		debug.SetMemoryLimit(goLimit)
	}
	quota := int64(float64(min(limit, uint64(goLimit))) * memoryBudgetRatio)
	log.Info("consumer memory quota",
		zap.Uint64("processLimitBytes", limit),
		zap.Int64("goMemoryLimitBytes", goLimit),
		zap.Int64("quotaBytes", quota))
	return &Budget{
		limit: quota,
	}
}

// Limit is the maximum number of accounted bytes.
func (b *Budget) Limit() int64 {
	return b.limit
}

// Used is the number of currently accounted bytes.
func (b *Budget) Used() int64 {
	return b.bytes.Load()
}

// Acquire blocks until the budget can admit n bytes. Read callers use it after
// reading an input so a full budget stops the read loop without blocking owners
// that already hold data.
func (b *Budget) Acquire(ctx context.Context, n int64) error {
	if n < 0 {
		log.Panic("consumer allocation has a negative size", zap.Int64("bytes", n))
	}
	for {
		used := b.bytes.Load()
		if used+n <= b.limit {
			if b.bytes.CAS(used, used+n) {
				return nil
			}
			continue
		}
		if err := b.wait(ctx, n); err != nil {
			return err
		}
	}
}

func (b *Budget) wait(ctx context.Context, n int64) error {
	// There is one source read loop. Store n+1 so a blocked Acquire(0)
	// can be distinguished from having no pending admission.
	b.requested.Store(n + 1)
	defer b.requested.Store(0)
	b.notify()
	for {
		changed := b.changes()
		if b.bytes.Load()+n <= b.limit {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

// ForceAcquire records memory that is already allocated or must be admitted for
// a dependency. It never blocks.
func (b *Budget) ForceAcquire(bytes int64) {
	if bytes < 0 {
		log.Panic("consumer allocation has a negative size", zap.Int64("bytes", bytes))
	}
	b.bytes.Add(bytes)
}

// changes subscribes before checking an admission condition to avoid missing a
// release. A release wakes every subscriber; callers decide whether to wait
// again.
func (b *Budget) changes() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.changed == nil {
		b.changed = make(chan struct{})
	}
	b.waiting.Store(true)
	return b.changed
}

// Release returns previously acquired bytes to the budget.
func (b *Budget) Release(bytes int64) {
	if bytes == 0 {
		return
	}
	b.bytes.Add(-bytes)
	b.notify()
}

func (b *Budget) notify() {
	if !b.waiting.Load() {
		return
	}
	b.mu.Lock()
	if b.changed != nil {
		close(b.changed)
		b.changed = nil
	}
	b.waiting.Store(false)
	b.mu.Unlock()
}
