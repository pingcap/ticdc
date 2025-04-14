// Copyright 2023 PingCAP, Inc.
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
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

var (
	_ DDLProducer = (*PulsarMockProducer)(nil)
	_ DMLProducer = (*PulsarMockProducer)(nil)
)

// PulsarMockProducer is a mock pulsar producer
type PulsarMockProducer struct {
	mu     sync.Mutex
	events map[string][]*pulsar.ProducerMessage
}

// NewMockPulsarDDLProducer creates a pulsar producer for DDLProducer
func NewMockPulsarDDLProducer() DDLProducer {
	return &PulsarMockProducer{
		events: map[string][]*pulsar.ProducerMessage{},
	}
}

// NewMockPulsarDMLProducer creates a pulsar producer for DMLProducer
func NewMockPulsarDMLProducer() DMLProducer {
	return &PulsarMockProducer{
		events: map[string][]*pulsar.ProducerMessage{},
	}
}

// SyncBroadcastMessage pulsar consume all partitions
func (p *PulsarMockProducer) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	return p.SyncSendMessage(ctx, topic, totalPartitionsNum, message)
}

// SyncSendMessage sends a message
// partitionNum is not used,pulsar consume all partitions
func (p *PulsarMockProducer) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message,
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
func (p *PulsarMockProducer) GetProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
	return producer, nil
}

// AsyncSendMessage appends a message to the mock producer.
func (p *PulsarMockProducer) AsyncSendMessage(_ context.Context, topic string,
	partition int32, message *common.Message,
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

func (m *PulsarMockProducer) Run(_ context.Context) error {
	// do nothing
	return nil
}

// Close close all producers
func (p *PulsarMockProducer) Close() {
	p.events = make(map[string][]*pulsar.ProducerMessage)
}

// GetAllEvents returns the events received by the mock producer.
func (p *PulsarMockProducer) GetAllEvents() []*pulsar.ProducerMessage {
	var events []*pulsar.ProducerMessage
	for _, v := range p.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (p *PulsarMockProducer) GetEvents(topic string) []*pulsar.ProducerMessage {
	return p.events[topic]
}
