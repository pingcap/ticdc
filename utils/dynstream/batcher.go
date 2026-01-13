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

import "fmt"

type SizePolicy interface {
	fmt.Stringer
	currentSize() int
	observe(event Event)
	isFull() bool
	reset()
}

// The batch count of handling events. <= 1 means no batch. By default 1.
type countPolicy struct {
	capacity int
	current  int
}

func NewCountPolicy(capacity int) countPolicy {
	return countPolicy{
		capacity: capacity,
	}
}

func (c countPolicy) String() string {
	return "count-policy"
}

func (c countPolicy) currentSize() int {
	return c.current
}

func (c countPolicy) observe(_ Event) {
	c.current++
}

func (c countPolicy) isFull() bool {
	return c.current >= c.capacity
}

func (c countPolicy) reset() {
	c.current = 0
}

// The max bytes of the batch. <= 1 means no limit. By default 0.
type bytesPolicy struct {
	capacity int
	current  int
}

func NewBytesPolicy(capacity int) bytesPolicy {
	return bytesPolicy{
		capacity: capacity,
	}
}

func (b bytesPolicy) currentSize() int {
	return b.current
}

func (b bytesPolicy) String() string {
	return "bytes-policy"
}

func (b bytesPolicy) observe(event T) {
	b.current += event.GetByteSize()
}

func (b bytesPolicy) isFull() bool {
	return b.current >= b.capacity
}

func (b bytesPolicy) reset() {
	b.current = 0
}

type batcher[T Event] struct {
	policy SizePolicy
	buf    []T
}

func (b *batcher[T]) addEvent(event T) {
	b.buf = append(b.buf, event)
	b.policy.observe(event)
}

func (b *batcher[T]) isFull() bool {
	return b.policy.isFull()
}

func (b *batcher[T]) reset() []T {
	events := b.buf
	b.buf = b.buf[:0]
	return events
}

func newBatcher[T Event](policy SizePolicy) *batcher[T] {
	return &batcher[T]{
		policy: policy,
		buf:    make([]T, 4096),
	}
}
