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

package main

import (
	"sort"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type offsetKey struct {
	topic     string
	partition int32
}

type partitionOffsetState struct {
	initialized      bool
	nextCommitOffset int64
	done             map[int64]struct{}
}

type offsetTracker struct {
	mu          sync.Mutex
	partitions  map[offsetKey]*partitionOffsetState
	committable map[offsetKey]int64
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{
		partitions:  make(map[offsetKey]*partitionOffsetState),
		committable: make(map[offsetKey]int64),
	}
}

func (t *offsetTracker) NewSource(topic string, partition int32, offset kafka.Offset) *messageSource {
	key := offsetKey{topic: topic, partition: partition}
	offsetValue := int64(offset)

	t.mu.Lock()
	state := t.partitions[key]
	if state == nil {
		state = &partitionOffsetState{done: make(map[int64]struct{})}
		t.partitions[key] = state
	}
	if !state.initialized {
		state.initialized = true
		state.nextCommitOffset = offsetValue
	}
	t.mu.Unlock()

	return &messageSource{
		tracker: t,
		key:     key,
		offset:  offsetValue,
	}
}

func (t *offsetTracker) markDone(key offsetKey, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.partitions[key]
	if state == nil {
		log.Warn("offset source done for unknown partition",
			zap.String("topic", key.topic), zap.Int32("partition", key.partition), zap.Int64("offset", offset))
		return
	}
	if offset < state.nextCommitOffset {
		return
	}
	state.done[offset] = struct{}{}
	for {
		if _, ok := state.done[state.nextCommitOffset]; !ok {
			break
		}
		delete(state.done, state.nextCommitOffset)
		state.nextCommitOffset++
		t.committable[key] = state.nextCommitOffset
	}
}

func (t *offsetTracker) DrainCommittable() []kafka.TopicPartition {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.committable) == 0 {
		return nil
	}
	keys := make([]offsetKey, 0, len(t.committable))
	for key := range t.committable {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic != keys[j].topic {
			return keys[i].topic < keys[j].topic
		}
		return keys[i].partition < keys[j].partition
	})

	result := make([]kafka.TopicPartition, 0, len(keys))
	for _, key := range keys {
		offset := kafka.Offset(t.committable[key])
		topic := key.topic
		result = append(result, kafka.TopicPartition{
			Topic:     &topic,
			Partition: key.partition,
			Offset:    offset,
		})
		delete(t.committable, key)
	}
	return result
}

type messageSource struct {
	mu      sync.Mutex
	tracker *offsetTracker
	key     offsetKey
	offset  int64

	pending int
	closed  bool
	done    bool
}

func (s *messageSource) AddWork() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return
	}
	s.pending++
}

func (s *messageSource) Done() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending > 0 {
		s.pending--
	}
	s.markDoneIfReadyLocked()
}

func (s *messageSource) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.markDoneIfReadyLocked()
}

func (s *messageSource) markDoneIfReadyLocked() {
	if s.done || !s.closed || s.pending != 0 {
		return
	}
	s.done = true
	s.tracker.markDone(s.key, s.offset)
}
