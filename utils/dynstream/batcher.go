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
	"math"
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

	// ---- 1) Hard bytes (must flush) ----
	if b.config.hardBytes > 0 && b.nBytes >= b.config.hardBytes {
		return true
	}

	// If hardBytes is disabled, softCount becomes the only batching limiter.
	if b.config.hardBytes <= 0 {
		return n >= b.config.softCount
	}

	// ---- 2) Protective hardCount (avoid pathological tiny events) ----
	// Rationale: if events are extremely small, bytes-based batching could accumulate huge counts,
	// making flush cost (CPU/serialization/retry) explode. Hard cap the count.
	hardCount := max(1024, 4*b.config.softCount) // heuristic; adjust for your system
	if n >= hardCount {
		return true
	}

	// ---- 3) Soft count + "fill as much as possible" policy ----
	// Once we reach softCount, we *prefer* to keep batching until bytes are "mostly full".
	// Use a watermark to trade latency vs fullness.
	if n < b.config.softCount {
		return false
	}

	// Watermark: flush when bytes reach some portion of hardBytes after we've hit softCount.
	// 0.80 is a reasonable default; tune based on latency budget.
	watermark := int(math.Ceil(0.80 * float64(b.config.hardBytes)))
	if b.nBytes >= watermark {
		return true
	}

	// ---- 4) Dynamic target count derived from average event size ----
	// Assumption: event sizes don't vary too wildly. Use average bytes/event to estimate how many
	// events we need to approach hardBytes.
	avg := b.nBytes / n
	if avg <= 0 {
		// Defensive: if size accounting is broken, fall back to softCount policy.
		return false
	}

	// Target count to fill hardBytes given current avg size.
	// We trigger "soft full" once we reach ~80% of that estimated target.
	targetByBytes := b.config.hardBytes / avg
	if targetByBytes <= 0 {
		targetByBytes = 1
	}
	targetSoft := int(math.Ceil(0.80 * float64(targetByBytes)))

	// Ensure we don't flush *earlier* than softCount due to noisy averages.
	targetSoft = max(targetSoft, b.config.softCount)
	return n >= targetSoft
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
