// Copyright 2025 PingCAP, Inc.
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

type batchConfig struct {
	batchCount int
	batchBytes int
}

func newBatchConfig(batchCount int, batchBytes int) batchConfig {
	if batchCount <= 0 {
		batchCount = 1
	}
	return batchConfig{
		batchCount: batchCount,
		batchBytes: batchBytes,
	}
}

type batcher[T any] struct {
	config   batchConfig
	maxCount int

	buf        []T
	totalBytes int
}

func newBatcher[T any](config batchConfig, initialCap int) batcher[T] {
	if initialCap < 0 {
		initialCap = 0
	}
	return batcher[T]{config: config, buf: make([]T, 0, initialCap)}
}

func (b *batcher[T]) setLimit(config batchConfig, maxCount int) {
	b.config = config
	b.maxCount = maxCount
	b.totalBytes = 0
	b.buf = b.buf[:0]
}

func (b *batcher[T]) addEvent(event T, eventBytes int) {
	b.buf = append(b.buf, event)
	b.totalBytes += eventBytes
}

func (b *batcher[T]) isFull() bool {
	if b.maxCount > 0 && len(b.buf) >= b.maxCount {
		return true
	}
	if b.config.batchBytes > 0 && b.totalBytes >= b.config.batchBytes {
		return true
	}
	return false
}

func (b *batcher[T]) flush() ([]T, int) {
	return b.buf, b.totalBytes
}

func (b *batcher[T]) reset() {
	var zeroT T
	for i := range b.buf {
		b.buf[i] = zeroT
	}
	b.setLimit(b.config, 0)
}
