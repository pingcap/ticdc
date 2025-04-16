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

package pulsar

import (
	"context"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

var (
	_ ddlProducer = (*mockProducer)(nil)
	_ dmlProducer = (*mockProducer)(nil)
)

// mockProducer is a mock pulsar producer
type mockProducer struct {
	mu     sync.Mutex
	events map[string][]*pulsar.ProducerMessage
}

func newMockDDLProducer() ddlProducer {
	return &mockProducer{
		events: map[string][]*pulsar.ProducerMessage{},
	}
}

func newMockDMLProducer() dmlProducer {
	return &mockProducer{
		events: map[string][]*pulsar.ProducerMessage{},
	}
}

// SyncBroadcastMessage pulsar consume all partitions
func (p *mockProducer) syncBroadcastMessage(ctx context.Context, topic string, message *common.Message,
) error {
	return p.syncSendMessage(ctx, topic, message)
}

// SyncSendMessage sends a message
// partitionNum is not used,pulsar consume all partitions
func (p *mockProducer) syncSendMessage(ctx context.Context, topic string,
	message *common.Message,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}
	p.events[topic] = append(p.events[topic], data)
	return nil
}

// GetProducerByTopic returns a producer by topic name
func (p *mockProducer) GetProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
	return producer, nil
}

// AsyncSendMessage appends a message to the mock producer.
func (p *mockProducer) asyncSendMessage(_ context.Context, topic string, message *common.Message,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}
	p.events[topic] = append(p.events[topic], data)
	if message.Callback != nil {
		message.Callback()
	}
	return nil
}

func (m *mockProducer) run(_ context.Context) error {
	// do nothing
	return nil
}

// Close close all producers
func (p *mockProducer) close() {
	p.events = make(map[string][]*pulsar.ProducerMessage)
}

// GetAllEvents returns the events received by the mock producer.
func (p *mockProducer) GetAllEvents() []*pulsar.ProducerMessage {
	var events []*pulsar.ProducerMessage
	for _, v := range p.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (p *mockProducer) GetEvents(topic string) []*pulsar.ProducerMessage {
	return p.events[topic]
}
