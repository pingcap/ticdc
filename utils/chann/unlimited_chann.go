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

package chann

import (
	"math"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/deque"
)

type Group comparable

type Grouper[T any, G Group] func(v T) G

type Sizer[T any] func(v T) int

// UnlimitedChannel is a channel with unlimited buffer.
// It is safe for concurrent use.
// It supports get multiple elements at once, which is suitable for batch processing.
type UnlimitedChannel[T any, G Group] struct {
	grouper Grouper[T, G]
	sizer   Sizer[T]
	queue   deque.Deque[T]

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
}

func NewUnlimitedChannelDefault[T any]() *UnlimitedChannel[T, any] {
	return NewUnlimitedChannel[T, any](nil, nil)
}

// NewUnlimitedChannel creates a new UnlimitedChannel.
// grouper is a function that returns the group of the element belongs to.
// sizer is a function that returns the number of bytes of the element, by default the bytes is 0.
func NewUnlimitedChannel[T any, G Group](grouper Grouper[T, G], sizer Sizer[T]) *UnlimitedChannel[T, G] {
	ch := &UnlimitedChannel[T, G]{
		grouper: grouper,
		sizer:   sizer,
		queue:   *deque.NewDequeDefault[T](),
	}
	ch.cond = sync.NewCond(&ch.mu)
	return ch
}

func (c *UnlimitedChannel[T, G]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.cond.Broadcast()
}

func (c *UnlimitedChannel[T, G]) Push(values ...T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		log.Warn("push to closed ulimited channel")
		return
	}
	for _, v := range values {
		c.queue.PushBack(v)
	}

	c.cond.Signal()
}

// Get retrieves an element from the channel.
// Return the element and a boolean indicating whether the channel is available.
// Return false if the channel is closed.
func (c *UnlimitedChannel[T, G]) Get() (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for !c.closed && c.queue.Length() == 0 {
		c.cond.Wait()
	}
	var zero T
	if c.closed && c.queue.Length() == 0 {
		return zero, false
	}

	v, ok := c.queue.PopFront()
	if !ok {
		panic("unreachable")
	}

	return v, true
}

type getMultType int

const (
	getMultNoGroup getMultType = iota
	getMultMixdGroupCons
	getMultSingleGroup
)

func (c *UnlimitedChannel[T, G]) getMultiple(gmt getMultType, buffer []T, batchBytes ...int) ([]T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for !c.closed && c.queue.Length() == 0 {
		c.cond.Wait()
	}

	if c.closed && c.queue.Length() == 0 {
		return buffer, false
	}

	maxBytes := math.MaxInt
	if len(batchBytes) > 0 {
		maxBytes = batchBytes[0]
	}
	getBytes := func(v T) int {
		if c.sizer == nil {
			return 0
		}
		return c.sizer(v)
	}

	cap := cap(buffer)
	bytes := 0

	switch gmt {
	case getMultNoGroup:
		for {
			if len(buffer) >= cap || bytes >= maxBytes {
				break
			}
			v, ok := c.queue.PopFront()
			if !ok {
				break
			}
			buffer = append(buffer, v)
			bytes += getBytes(v)
		}
	case getMultMixdGroupCons, getMultSingleGroup:
		first, ok := c.queue.FrontRef()
		if !ok {
			panic("unreachable")
		}
		lastGroup := c.grouper(*first)
		for {
			v, ok := c.queue.FrontRef()
			if !ok {
				break
			}

			curGroup := c.grouper(*v)
			if gmt == getMultSingleGroup && (curGroup != lastGroup || (len(buffer) >= cap || bytes >= maxBytes)) {
				break
			} else if curGroup != lastGroup && (len(buffer) >= cap || bytes >= maxBytes) {
				break
			}
			lastGroup = curGroup

			buffer = append(buffer, *v)
			bytes += getBytes(*v)

			c.queue.PopFront()
		}
	}

	return buffer, true
}

// Get multiple elements from the channel.
func (c *UnlimitedChannel[T, G]) GetMultipleNoGroup(buffer []T, batchBytes ...int) ([]T, bool) {
	return c.getMultiple(getMultNoGroup, buffer, batchBytes...)
}

// Get multiple elements from the channel. Grouper must be provided.
//
// It returns ALL consecutive elements that in the same group, even if they already exceeds the cap(buffer) and batchBytes;
// And then it try to fill the buffer and the batch bytes.
//
// Return the original buffer and a boolean indicating whether the channel is available.
// Return false if the channel is closed.
// Note that different groups could be mixed in the result.
func (c *UnlimitedChannel[T, G]) GetMultipleMixdGroupConsecutive(buffer []T, batchBytes ...int) ([]T, bool) {
	if c.grouper == nil {
		panic("grouper is required")
	}
	return c.getMultiple(getMultMixdGroupCons, buffer, batchBytes...)
}

// Get multiple elements from the channel. Grouper must be provided.
//
// Note that it only returns the elements in the same group, and tries to fill the buffer and the batch bytes.
func (c *UnlimitedChannel[T, G]) GetMultipleSingleGroup(buffer []T, batchBytes ...int) ([]T, bool) {
	if c.grouper == nil {
		panic("grouper is required")
	}
	return c.getMultiple(getMultSingleGroup, buffer, batchBytes...)
}

func (c *UnlimitedChannel[T, G]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queue.Length()
}
