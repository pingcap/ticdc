// Copyright 2022 PingCAP, Inc.
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

package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// mockFactory is a mock implementation of Factory interface.
type mockFactory struct {
	changefeedID  commonType.ChangeFeedID
	config        *sarama.Config
	errorReporter mocks.ErrorReporter
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(
	_ context.Context,
	o *options, changefeedID commonType.ChangeFeedID,
) (Factory, error) {
	config, err := newSaramaConfig(context.Background(), o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mockFactory{
		changefeedID: changefeedID,
		config:       config,
	}, nil
}

// AdminClient return a mocked admin client
func (f *mockFactory) AdminClient() (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}

// SyncProducer creates a sync producer
func (f *mockFactory) SyncProducer() (SyncProducer, error) {
	return &MockSaramaSyncProducer{
		SyncProducer: mocks.NewSyncProducer(f.errorReporter, f.config),
	}, nil
}

// AsyncProducer creates an async producer
func (f *mockFactory) AsyncProducer() (AsyncProducer, error) {
	return &MockSaramaAsyncProducer{
		AsyncProducer: mocks.NewAsyncProducer(f.errorReporter, f.config),
		failpointCh:   make(chan error, 1),
	}, nil
}

// MetricsCollector returns the metric collector
func (f *mockFactory) MetricsCollector(_ ClusterAdminClient) MetricsCollector {
	return &mockMetricsCollector{}
}

// MockSaramaSyncProducer is a mock implementation of SyncProducer interface.
type MockSaramaSyncProducer struct {
	SyncProducer *mocks.SyncProducer
}

// SendMessage implement the SyncProducer interface.
func (m *MockSaramaSyncProducer) SendMessage(
	_ context.Context,
	topic string, partitionNum int32,
	message *common.Message,
) error {
	_, _, err := m.SyncProducer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Partition: partitionNum,
	})
	return err
}

// SendMessages implement the SyncProducer interface.
func (m *MockSaramaSyncProducer) SendMessages(_ context.Context, topic string, partitionNum int32, message *common.Message) error {
	msgs := make([]*sarama.ProducerMessage, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     sarama.ByteEncoder(message.Value),
			Partition: int32(i),
		}
	}
	return m.SyncProducer.SendMessages(msgs)
}

// Close implement the SyncProducer interface.
func (m *MockSaramaSyncProducer) Close() {
	_ = m.SyncProducer.Close()
}

func (m *MockSaramaSyncProducer) Heartbeat() {
	return
}

// MockSaramaAsyncProducer is a mock implementation of AsyncProducer interface.
type MockSaramaAsyncProducer struct {
	AsyncProducer *mocks.AsyncProducer
	failpointCh   chan error

	closed bool
}

// AsyncRunCallback implement the AsyncProducer interface.
func (p *MockSaramaAsyncProducer) AsyncRunCallback(
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-p.failpointCh:
			return errors.Trace(err)
		case ack := <-p.AsyncProducer.Successes():
			if ack != nil {
				switch meta := ack.Metadata.(type) {
				case *messageMetadata:
					if meta != nil && meta.callback != nil {
						meta.callback()
					}
				default:
					log.Error("unknown message metadata type in mock async producer",
						zap.Any("metadata", ack.Metadata))
				}
			}
		case err := <-p.AsyncProducer.Errors():
			// We should not wrap a nil pointer if the pointer
			// is of a subtype of `error` because Go would store the type info
			// and the resulted `error` variable would not be nil,
			// which will cause the pkg/error library to malfunction.
			// See: https://go.dev/doc/faq#nil_error
			if err == nil {
				return nil
			}
			return errors.WrapError(errors.ErrKafkaAsyncSendMessage, err)
		}
	}
}

// AsyncSend implement the AsyncProducer interface.
func (p *MockSaramaAsyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	meta := &messageMetadata{
		callback: message.Callback,
		logInfo:  message.LogInfo,
	}
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Metadata:  meta,
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case p.AsyncProducer.Input() <- msg:
	}
	return nil
}

func (p *MockSaramaAsyncProducer) Heartbeat() {
	return
}

// Close implement the AsyncProducer interface.
func (p *MockSaramaAsyncProducer) Close() {
	if p.closed {
		return
	}
	_ = p.AsyncProducer.Close()
	p.closed = true
}

type mockMetricsCollector struct{}

// Run implements the MetricsCollector interface.
func (m *mockMetricsCollector) Run(_ context.Context) {}
