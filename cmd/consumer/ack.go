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

	"github.com/uber-go/atomic"
)

type inputProgress struct {
	memory    *memoryUsage
	received  atomic.Int64
	confirmed atomic.Int64
}

// An input can be confirmed after decoding and downstream writes release all refs.
type ack struct {
	refs     atomic.Int64
	decodes  atomic.Int64
	memory   atomic.Int64
	progress *inputProgress
	done     chan<- struct{}
}

func (p *inputProgress) newAck(ctx context.Context, bytes int64, done chan<- struct{}) (*ack, error) {
	if err := p.memory.reserve(ctx, bytes); err != nil {
		return nil, err
	}
	record := &ack{
		progress: p,
		done:     done,
	}
	record.memory.Store(bytes)
	record.refs.Store(1)
	record.decodes.Store(1)
	p.received.Inc()
	return record, nil
}

func (a *ack) release() {
	if a.refs.Add(-1) == 0 {
		select {
		case a.done <- struct{}{}:
		default:
		}
	}
}

// A decoding operation keeps both the input and its parsed contents alive.
func (a *ack) retainDecode() {
	a.decodes.Inc()
	a.refs.Inc()
}

func (a *ack) retain(ctx context.Context, bytes int64) error {
	if err := a.progress.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	a.memory.Add(bytes)
	return nil
}

func (a *ack) decoded(retainedBytes int64) {
	if a.decodes.Dec() == 0 {
		a.progress.memory.release(a.memory.Swap(retainedBytes) - retainedBytes)
	}
	a.release()
}

func (a *ack) confirm() {
	a.progress.confirmed.Inc()
	a.progress.memory.release(a.memory.Load())
}
