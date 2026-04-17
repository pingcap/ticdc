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

	"github.com/pingcap/ticdc/pkg/config"
)

type batchConfig struct {
	// if the bytes == 0, softCount is the hard count limit.
	// if the bytes > 0, softCount is the soft count limit, means it can be exceed.
	softCount int

	// hardCount take effect only if the bytes > 0, it becomes the hard count limit.
	hardCount int

	// hardBytes controls size based flushing. 0 disables byte based limit.
	// The current batch can exceed bytes by at most one event.
	hardBytes int
}

const countCapMultiple = 4

func newDefaultBatchConfig() batchConfig {
	// Keep the default behavior consistent with the legacy Option.BatchCount=1:
	// no batching unless explicitly configured by the caller.
	return newBatchConfig(1, 0)
}

func newBatchConfig(count, bytes int) batchConfig {
	if count <= 0 {
		count = 1
	}
	if count > config.MaxEventCollectorBatchCount {
		count = config.MaxEventCollectorBatchCount
	}
	if bytes < 0 {
		bytes = 0
	}
	return batchConfig{
		softCount: count,
		hardCount: count * countCapMultiple,
		hardBytes: bytes,
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

// setLimit must be called after the previous flushed data is fully consumed
func (b *batcher[T]) setLimit(cfg batchConfig) {
	b.config = cfg
	if cap(b.buf) < cfg.softCount {
		b.buf = make([]T, 0, cfg.softCount)
	} else {
		b.buf = b.buf[:0]
	}
	b.nBytes = 0
	b.start = time.Now()
}

func (b *batcher[T]) addEvent(event T, size int) {
	b.buf = append(b.buf, event)
	b.nBytes += size
}

// isFull returns true if the batcher should flush and stop appending more events.
func (b *batcher[T]) isFull() bool {
	n := len(b.buf)
	if n == 0 {
		return false
	}

	if b.config.hardBytes <= 0 {
		return n >= b.config.softCount
	}

	if n >= b.config.hardCount {
		return true
	}

	return b.nBytes >= b.config.hardBytes
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
