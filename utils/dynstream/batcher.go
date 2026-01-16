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

import "time"

type batchConfig struct {
	maxCount int
	maxBytes int
}

func newDefaultBatchConfig() batchConfig {
	// Keep the default behavior consistent with the legacy Option.BatchCount=1:
	// no batching unless explicitly configured by the caller.
	return NewBatchConfig(1, 0)
}

func NewBatchConfig(count, nBytes int) batchConfig {
	return batchConfig{
		maxCount: count,
		maxBytes: nBytes,
	}
}

type batcher[T Event] struct {
	config batchConfig
	count  int
	nBytes int
	buf    []T

	start time.Time
}

func newDefaultBatcher[T Event]() *batcher[T] {
	return newBatcher[T](newDefaultBatchConfig())
}

func newBatcher[T Event](cfg batchConfig) *batcher[T] {
	if cfg.maxCount <= 0 {
		cfg.maxCount = 1
	}
	return &batcher[T]{
		config: cfg,
		buf:    make([]T, 0, cfg.maxCount),
	}
}

func (b *batcher[T]) addEvent(event T, size int) {
	if len(b.buf) == 0 {
		b.start = time.Now()
	}
	b.buf = append(b.buf, event)
	b.count++
	b.nBytes += size
}

// todo: revise this condition, aims to make it as full as possible, and also keeps low latency.
func (b *batcher[T]) isFull() bool {
	return b.count >= b.config.maxCount || b.nBytes >= b.config.maxBytes
}

func (b *batcher[T]) flush() ([]T, int, time.Duration) {
	events := b.buf
	nBytes := b.nBytes
	b.buf = b.buf[:0]
	b.count = 0
	b.nBytes = 0

	return events, nBytes, time.Since(b.start)
}
