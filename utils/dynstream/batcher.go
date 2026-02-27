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
	softCount int
	hardBytes int
}

const hardCountMultiple = 2

func newDefaultBatchConfig() batchConfig {
	// Keep the default behavior consistent with the legacy Option.BatchCount=1:
	// no batching unless explicitly configured by the caller.
	return NewBatchConfig(1, 0)
}

// If hardBytes is 0, softCount is the hard limit.
// If hardBytes is enabled, softCount is a soft target:
// hardBytes is checked before appending the next event, so the current batch can
// exceed hardBytes by at most one event.
func NewBatchConfig(count, nBytes int) batchConfig {
	return batchConfig{
		softCount: count,
		hardBytes: nBytes,
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
	if cfg.softCount <= 0 {
		cfg.softCount = 1
	}
	b.config = cfg
	if cap(b.buf) < cfg.softCount {
		b.buf = make([]T, 0, cfg.softCount)
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

	// If hardBytes is disabled, softCount is the hard limit.
	if b.config.hardBytes <= 0 {
		return n >= b.config.softCount
	}

	// When hardBytes is enabled, keep batching past softCount for small events,
	// but cap the total events with hardCount to avoid oversized batches.
	hardCount := b.config.softCount * hardCountMultiple
	if n >= hardCount {
		return true
	}

	// hardBytes is checked before appending the next event.
	// So with tiny events we can exceed hardBytes by at most one event.
	return b.nBytes >= b.config.hardBytes
}

func (b *batcher[T]) flush() ([]T, int, time.Duration) {
	if len(b.buf) == 0 {
		return nil, 0, 0
	}
	// note: the returned events is shared the underline slice with the batcher
	events := b.buf
	b.buf = b.buf[:0]
	nBytes := b.nBytes
	b.nBytes = 0
	return events, nBytes, time.Since(b.start)
}
