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
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/errors"
)

const maxMemoryBytes = 1 << 30

type memoryUsage struct {
	bytes         atomic.Int64
	readBytes     atomic.Int64
	records       atomic.Int64
	effects       atomic.Int64
	received      atomic.Int64
	confirmed     atomic.Int64
	externalBytes func() int64
	mu            sync.Mutex
	changed       chan struct{}
	completed     chan struct{}
}

func (m *memoryUsage) reserve(ctx context.Context, bytes int64) error {
	if bytes < 0 || bytes > maxMemoryBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer allocation exceeds its memory budget")
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		m.mu.Lock()
		used := m.bytes.Load()
		external := int64(0)
		if m.externalBytes != nil {
			external = m.externalBytes()
		}
		if used+bytes+external <= maxMemoryBytes {
			m.bytes.Add(bytes)
			m.mu.Unlock()
			return nil
		}
		if m.changed == nil {
			m.changed = make(chan struct{})
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

func (m *memoryUsage) release(bytes int64) {
	if bytes == 0 {
		return
	}
	m.mu.Lock()
	m.bytes.Add(-bytes)
	if m.changed != nil {
		close(m.changed)
		m.changed = nil
	}
	m.mu.Unlock()
}
