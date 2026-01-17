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

func newDefaultBatchConfig() batchConfig {
	// Keep the default behavior consistent with the legacy Option.BatchCount=1:
	// no batching unless explicitly configured by the caller.
	return NewBatchConfig(1, 0)
}

// if the hardBytes is 0, the softCount become the hard limit, cannot be exceeded
// else the softCount can be exceed, but the hardBytes cannot be exceeded.
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
	if cfg.softCount <= 0 {
		cfg.softCount = 1
	}
	return &batcher[T]{
		config: cfg,
		buf:    make([]T, 0, cfg.softCount),
	}
}

func (b *batcher[T]) addEvent(event T, size int) {
	if len(b.buf) == 0 {
		b.start = time.Now()
	}
	b.buf = append(b.buf, event)
	b.nBytes += size
}

// the batcher aims to batch messages as much as possible in one time.
// 1. always make sure the hardBytes not exceeded, to avoid high memory pressue.
// 2. the softCount can be exceeded if the event size is very small, but also make sure the total event count not
// too much if the event size is extreamly small, to avoid high CPU usage pressue and high latency.
// isFull return true if the batcher cannot add more events, and the caller should flushed it.
func (b *batcher[T]) isFull() bool {
	n := len(b.buf)
	if n == 0 {
		return false
	}

	hardBytes := b.config.hardBytes

	// 1) Hard bytes limit (if enabled).
	if hardBytes > 0 && b.nBytes >= hardBytes {
		return true
	}

	// 2) Protective hard count to avoid pathological tiny events.
	// Choose a simple, deterministic cap. Tune as needed.
	hardCount := 4 * b.config.softCount
	if n >= hardCount {
		return true
	}

	// 3) If hardBytes is disabled, fall back to count-only to avoid unbounded growth.
	if hardBytes <= 0 {
		return n >= b.config.softCount
	}

	// 4) Otherwise, bytes is the primary driver; softCount does NOT force flush.
	// Caller should use a max-wait (time-based) policy to prevent high latency under low traffic.
	return false
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
