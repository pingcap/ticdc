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
	"github.com/pingcap/errors"
	ticommon "github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	changefeedID  ticommon.ChangeFeedID
	config        *sarama.Config
	ErrorReporter mocks.ErrorReporter
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(
	_ context.Context,
	o *Options, changefeedID ticommon.ChangeFeedID,
) (Factory, error) {
	config, err := NewSaramaConfig(context.Background(), o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &MockFactory{
		changefeedID: changefeedID,
		config:       config,
	}, nil
}

// AdminClient return a mocked admin client
func (f *MockFactory) AdminClient() (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}

// SyncProducer creates a sync producer
func (f *MockFactory) SyncProducer() (SyncProducer, error) {
	syncProducer := mocks.NewSyncProducer(f.ErrorReporter, f.config)
	return &MockSaramaSyncProducer{
		Producer: syncProducer,
	}, nil
}

// AsyncProducer creates an async producer
func (f *MockFactory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	asyncProducer := mocks.NewAsyncProducer(f.ErrorReporter, f.config)
	return &MockSaramaAsyncProducer{
		AsyncProducer: asyncProducer,
		failpointCh:   make(chan error, 1),
	}, nil
}

// MetricsCollector returns the metric collector
func (f *MockFactory) MetricsCollector(_ ClusterAdminClient) MetricsCollector {
	return &mockMetricsCollector{}
}

// MockSaramaSyncProducer is a mock implementation of SyncProducer interface.
type MockSaramaSyncProducer struct {
	Producer *mocks.SyncProducer
}

// SendMessage implement the SyncProducer interface.
func (m *MockSaramaSyncProducer) SendMessage(
	_ context.Context,
	topic string, partitionNum int32,
	message *common.Message,
) error {
	_, _, err := m.Producer.SendMessage(&sarama.ProducerMessage{
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
	return m.Producer.SendMessages(msgs)
}

// Close implement the SyncProducer interface.
func (m *MockSaramaSyncProducer) Close() {
	m.Producer.Close()
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
				callback := ack.Metadata.(func())
				if callback != nil {
					callback()
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
			return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
		}
	}
}

// AsyncSend implement the AsyncProducer interface.
func (p *MockSaramaAsyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Metadata:  message.Callback,
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case p.AsyncProducer.Input() <- msg:
	}
	return nil
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
func (m *mockMetricsCollector) Run(ctx context.Context) {
}
