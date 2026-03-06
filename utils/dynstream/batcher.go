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
// See the License for the specific language governing permissions and
// limitations under the License.

package dynstream

import (
	"time"
)

type batchConfig struct {
	// count controls event-count based flushing.
	// If bytes == 0, it acts as the hard count limit.
	// If bytes > 0, it acts as the count target.
	count int
	// bytes controls size based flushing. 0 disables byte based limit.
	bytes int
}

const countCapMultiple = 2

func newDefaultBatchConfig() batchConfig {
	// Keep the default behavior consistent with the legacy Option.BatchCount=1:
	// no batching unless explicitly configured by the caller.
	return NewBatchConfig(1, 0)
}

// If bytes is 0, count is the hard limit.
// If bytes is enabled, count is a target:
// bytes is checked before appending the next event, so the current batch can
// exceed bytes by at most one event.
func NewBatchConfig(count, bytes int) batchConfig {
	if count <= 0 {
		count = 1
	}
	if bytes < 0 {
		bytes = 0
	}
	return batchConfig{
		count: count,
		bytes: bytes,
	}
}

type batcher[T Event] struct {
	config batchConfig
	nBytes int
	buf    []T

	start time.Time
}

func newDefaultBatcher[T Event]() *batcher[T] {
	return newBatcher[T](newDefaultBatchConfig())
}

func newBatcher[T Event](cfg batchConfig) *batcher[T] {
	b := &batcher[T]{}
	b.setLimit(cfg)
	return b
}

func (b *batcher[T]) setLimit(cfg batchConfig) {
	if cfg.count <= 0 {
		cfg.count = 1
	}
	b.config = cfg
	if cap(b.buf) < cfg.count {
		b.buf = make([]T, 0, cfg.count)
	} else {
		b.buf = b.buf[:0]
	}
	b.nBytes = 0
}

func (b *batcher[T]) addEvent(event T, size int) {
	if len(b.buf) == 0 {
		b.start = time.Now()
	}
	b.buf = append(b.buf, event)
	b.nBytes += size
}

// isFull returns true if the batcher should flush and stop appending more events.
func (b *batcher[T]) isFull() bool {
	n := len(b.buf)
	if n == 0 {
		return false
	}

	// If bytes is disabled, count is the hard limit.
	if b.config.bytes <= 0 {
		return n >= b.config.count
	}

	// When bytes is enabled, keep batching past count for small events,
	// but cap the total events with countCap to avoid oversized batches.
	countCap := b.config.count * countCapMultiple
	if n >= countCap {
		return true
	}

	// bytes is checked before appending the next event.
	// So with tiny events we can exceed bytes by at most one event.
	return b.nBytes >= b.config.bytes
}

// flush returned events must be processed before adding more events to the batcher.
func (b *batcher[T]) flush() ([]T, int, time.Duration) {
	if len(b.buf) == 0 {
		return nil, 0, 0
	}
	// note: the returned events share the underlying slice with the batcher
	events := b.buf
	b.buf = b.buf[:0]
	nBytes := b.nBytes
	b.nBytes = 0
	return events, nBytes, time.Since(b.start)
}
