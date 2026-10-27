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
	refs   atomic.Int64
	bytes  int64 // Input size, held until parsing and all associated writes finish.
	memory *memoryUsage
	done   chan<- struct{}
}

func newAck(memory *memoryUsage, bytes int64, done chan<- struct{}) *ack {
	memory.reserve(bytes)
	record := &ack{
		memory: memory,
		bytes:  bytes,
		done:   done,
	}
	record.refs.Store(1)
	return record
}

func (a *ack) release() {
	refs := a.refs.Dec()
	if refs < 0 {
		log.Panic("consumer input completed more than once", zap.Int64("refs", refs))
	}
	if refs == 0 {
		a.memory.release(a.bytes)
		select {
		case a.done <- struct{}{}:
		default:
		}
	}
}

// retain registers another parser, conversion or downstream write using this input.
func (a *ack) retain() {
	a.refs.Inc()
}

func (a *ack) ready() bool {
	return a.refs.Load() == 0
}
