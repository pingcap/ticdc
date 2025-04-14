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

package producer

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

var (
	_ DDLProducer = (*KafkaMockProducer)(nil)
	_ DMLProducer = (*KafkaMockProducer)(nil)
)

func NewMockKafkaDMLProducer() DMLProducer {
	return &KafkaMockProducer{
		events: make(map[string][]*common.Message),
	}
}

func NewMockKafkaDDLProducer() DDLProducer {
	return &KafkaMockProducer{
		events: make(map[string][]*common.Message),
	}
}

// KafkaMockProducer is a mock producer for test.
type KafkaMockProducer struct {
	mu     sync.Mutex
	events map[string][]*common.Message
}

// AsyncSendMessage appends a message to the mock producer.
func (m *KafkaMockProducer) AsyncSendMessage(_ context.Context, topic string,
	partition int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*common.Message, 0)
	}
	m.events[key] = append(m.events[key], message)

	message.Callback()

	return nil
}

func (m *KafkaMockProducer) Run(_ context.Context) error {
	// do nothing
	return nil
}

// Close do nothing.
func (m *KafkaMockProducer) Close() {
}

// GetAllEvents returns the events received by the mock producer.
func (m *KafkaMockProducer) GetAllEvents() []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var events []*common.Message
	for _, v := range m.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (m *KafkaMockProducer) GetEvents(topic string, partition int32) []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return m.events[key]
}

// SyncBroadcastMessage stores a message to all partitions of the topic.
func (m *KafkaMockProducer) SyncBroadcastMessage(_ context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < int(totalPartitionsNum); i++ {
		key := fmt.Sprintf("%s-%d", topic, i)
		if _, ok := m.events[key]; !ok {
			m.events[key] = make([]*common.Message, 0)
		}
		m.events[key] = append(m.events[key], message)
	}

	return nil
}

// SyncSendMessage stores a message to a partition of the topic.
func (m *KafkaMockProducer) SyncSendMessage(_ context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partitionNum)
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*common.Message, 0)
	}
	m.events[key] = append(m.events[key], message)

	return nil
}
