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

type BatchType int

const (
	BatchTypeCount BatchType = iota
	BatchTypeSize
)

type batcher[T Event] struct {
	capacity  int
	current   int
	batchType BatchType
	buf       []T
}

func newDefaultBatcher[T Event]() *batcher[T] {
	// Keep the default behavior consistent with the legacy Option.BatchCount=1:
	// no batching unless explicitly configured by the caller.
	return newBatcher[T](BatchTypeCount, 1)
}

func newBatcher[T Event](batchType BatchType, capacity int) *batcher[T] {
	if capacity <= 0 {
		capacity = 1
	}
	bufCap := 128
	if batchType == BatchTypeCount {
		bufCap = min(bufCap, capacity)
	}
	return &batcher[T]{
		batchType: batchType,
		capacity:  capacity,
		buf:       make([]T, 0, bufCap),
	}
}

func (b *batcher[T]) clone() *batcher[T] {
	if b == nil {
		return nil
	}
	return newBatcher[T](b.batchType, b.capacity)
}

func (b *batcher[T]) addEvent(event T, size int) {
	b.buf = append(b.buf, event)
	switch b.batchType {
	case BatchTypeCount:
		b.current++
	case BatchTypeSize:
		b.current += size
	}
}

func (b *batcher[T]) isFull() bool {
	return b.current >= b.capacity
}

func (b *batcher[T]) reset() []T {
	events := b.buf
	b.buf = b.buf[:0]
	b.current = 0
	return events
}
