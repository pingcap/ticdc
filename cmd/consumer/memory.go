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

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/uber-go/atomic"
)

type memoryUsage struct {
	bytes         atomic.Int64
	waiting       atomic.Bool
	externalBytes func() int64
	mu            sync.Mutex
	changed       chan struct{}
}

func (m *memoryUsage) used() int64 {
	bytes := m.bytes.Load()
	if m.externalBytes != nil {
		bytes += m.externalBytes()
	}
	return bytes
}

func (m *memoryUsage) reserve(ctx context.Context, bytes int64) error {
	if bytes < 0 {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer allocation has a negative size")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	m.bytes.Add(bytes)
	return nil
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
