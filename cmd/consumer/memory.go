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

package main

import (
	"math"
	"runtime/debug"
	"sync"

	"github.com/pingcap/log"
	putil "github.com/pingcap/ticdc/pkg/util"
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

// memoryBudget is a soft budget for raw input, captured data, decoded rows and
// pending transport positions. Each owner releases its own allocation.
// The limit scales with the process memory limit; it does not model process or
// object overhead.
type memoryBudget struct {
	limit   int64
	bytes   atomic.Int64
	waiting atomic.Bool
	mu      sync.Mutex
	changed chan struct{}
}

func newMemoryBudget() *memoryBudget {
	limit, err := putil.GetMemoryLimit()
	if err != nil || limit == 0 {
		log.Info("consumer memory quota", zap.Int64("quotaBytes", defaultMemoryBudget))
		return &memoryBudget{
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
	return &memoryBudget{
		limit: quota,
	}
}

func (m *memoryBudget) used() int64 {
	return m.bytes.Load()
}

// Already admitted work can reserve space without waiting so it can finish.
func (m *memoryBudget) reserve(bytes int64) {
	if bytes < 0 {
		log.Panic("consumer allocation has a negative size", zap.Int64("bytes", bytes))
	}
	m.bytes.Add(bytes)
}

// Subscribe before checking an admission condition to avoid missing a release.
// A release wakes every subscriber; callers decide whether to wait again.
func (m *memoryBudget) changes() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.changed == nil {
		m.changed = make(chan struct{})
	}
	m.waiting.Store(true)
	return m.changed
}

func (m *memoryBudget) release(bytes int64) {
	if bytes == 0 {
		return
	}
	m.bytes.Add(-bytes)
	if !m.waiting.Load() {
		return
	}
	m.mu.Lock()
	if m.changed != nil {
		close(m.changed)
		m.changed = nil
	}
	m.waiting.Store(false)
	m.mu.Unlock()
}
