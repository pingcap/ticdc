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
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

const maxMemoryBytes = 1 << 30

type memoryUsage struct {
	limit   int64
	bytes   atomic.Int64
	waiting atomic.Bool
	mu      sync.Mutex
	changed chan struct{}
}

func newMemoryUsage() *memoryUsage {
	return &memoryUsage{
		limit: maxMemoryBytes,
	}
}

func (m *memoryUsage) used() int64 {
	return m.bytes.Load()
}

// Wait before admitting new input. Already admitted work reserves without
// waiting so decoding and downstream writes can drain a full soft budget.
func (m *memoryUsage) wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if m.used() < m.limit {
			return nil
		}
		changed := m.changes()
		if m.used() < m.limit {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (m *memoryUsage) reserve(bytes int64) {
	if bytes < 0 {
		log.Panic("consumer allocation has a negative size", zap.Int64("bytes", bytes))
	}
	m.bytes.Add(bytes)
}

// Subscribe before checking an admission condition to avoid missing a release.
// A release wakes every subscriber; callers decide whether to wait again.
func (m *memoryUsage) changes() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.changed == nil {
		m.changed = make(chan struct{})
	}
	m.waiting.Store(true)
	return m.changed
}

func (m *memoryUsage) release(bytes int64) {
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
