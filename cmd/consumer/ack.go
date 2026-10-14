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
	"github.com/pingcap/log"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

// An input can be confirmed after decoding and downstream writes release all refs.
type ack struct {
	refs    atomic.Int64
	decodes atomic.Int64
	bytes   atomic.Int64 // This input's allocation balance, returned to the shared budget.
	memory  *memoryUsage
	done    chan<- struct{}
}

func newAck(memory *memoryUsage, bytes int64, done chan<- struct{}) *ack {
	memory.reserve(bytes)
	record := &ack{
		memory: memory,
		done:   done,
	}
	record.bytes.Store(bytes)
	record.refs.Store(1)
	record.decodes.Store(1)
	return record
}

func (a *ack) release() {
	refs := a.refs.Dec()
	if refs < 0 {
		log.Panic("consumer input completed more than once", zap.Int64("refs", refs))
	}
	if refs == 0 {
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

func (a *ack) retain(bytes int64) {
	a.memory.reserve(bytes)
	a.bytes.Add(bytes)
}

func (a *ack) decoded(retainedBytes int64) {
	if a.decodes.Dec() == 0 {
		a.memory.release(a.bytes.Swap(retainedBytes) - retainedBytes)
	}
	a.release()
}

func (a *ack) confirm() {
	a.memory.release(a.bytes.Load())
}
