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

const maxMemoryBytes = 1 << 30

type memoryUsage struct {
	bytes         atomic.Int64
	received      atomic.Int64
	confirmed     atomic.Int64
	externalBytes func() int64
	mu            sync.Mutex
	changed       chan struct{}
	completed     chan struct{}
}

// An input can be confirmed after decoding and downstream writes release all refs.
type ack struct {
	refs   atomic.Int64
	memory atomic.Int64
}

func (m *memoryUsage) newAck(ctx context.Context, bytes int64) (*ack, error) {
	if err := m.reserve(ctx, bytes); err != nil {
		return nil, err
	}
	record := &ack{}
	record.memory.Store(bytes)
	record.refs.Store(1)
	m.received.Inc()
	return record, nil
}

func (m *memoryUsage) confirm(record *ack) {
	m.confirmed.Inc()
	m.release(record.memory.Load())
}

func (m *memoryUsage) decoded(record *ack, retainedBytes int64) {
	m.release(record.memory.Swap(retainedBytes) - retainedBytes)
	record.refs.Add(-1)
	select {
	case m.completed <- struct{}{}:
	default:
	}
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

// Wait before starting another independent input. The current input and any
// unfinished ordering window must be able to finish even if they exceed the budget.
func (m *memoryUsage) wait(ctx context.Context) error {
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		m.mu.Lock()
		if m.used() < maxMemoryBytes || m.received.Load() == m.confirmed.Load() {
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
