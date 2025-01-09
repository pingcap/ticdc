// Copyright 2024 PingCAP, Inc.
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
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/errors"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// DefaultMockTopicName specifies the default mock topic name.
	DefaultMockTopicName = "mock_topic"
	// DefaultMockPartitionNum is the default partition number of default mock topic.
	DefaultMockPartitionNum = 3
	// defaultMockControllerID specifies the default mock controller ID.
	defaultMockControllerID = 1
	// topic replication factor must be 3 for Confluent Cloud Kafka.
	defaultReplicationFactor = 3
)

const (
	// defaultMaxMessageBytes specifies the default max message bytes,
	// default to 1048576, identical to kafka broker's `message.max.bytes` and topic's `max.message.bytes`
	// see: https://kafka.apache.org/documentation/#brokerconfigs_message.max.bytes
	// see: https://kafka.apache.org/documentation/#topicconfigs_max.message.bytes
	defaultMaxMessageBytes = "1048588"

	// defaultMinInsyncReplicas specifies the default `min.insync.replicas` for broker and topic.
	defaultMinInsyncReplicas = "1"
)

var (
	// BrokerMessageMaxBytes is the broker's `message.max.bytes`
	BrokerMessageMaxBytes = defaultMaxMessageBytes
	// TopicMaxMessageBytes is the topic's `max.message.bytes`
	TopicMaxMessageBytes = defaultMaxMessageBytes
	// MinInSyncReplicas is the `min.insync.replicas`
	MinInSyncReplicas = defaultMinInsyncReplicas
)

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	config       *kafka.ConfigMap
	changefeedID commonType.ChangeFeedID
	mockCluster  *kafka.MockCluster
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(
	o *Options, changefeedID commonType.ChangeFeedID,
) (Factory, error) {
	// The broker ids will start at 1 up to and including brokerCount.
	mockCluster, err := kafka.NewMockCluster(defaultReplicationFactor)
	if err != nil {
		return nil, errors.Trace(err)
	}
	config := NewConfig(o)
	config.SetKey("bootstrap.servers", mockCluster.BootstrapServers())
	return &MockFactory{
		config:       config,
		changefeedID: changefeedID,
		mockCluster:  mockCluster,
	}, nil
}

// AdminClient return a mocked admin client
func (f *MockFactory) AdminClient(_ context.Context) (ClusterAdminClient, error) {
	client, err := kafka.NewAdminClient(f.config)
	if err != nil {
		return nil, err
	}
	// f.mockCluster.CreateTopic()
	return &MockClusterAdmin{
		ClusterAdminClient: newClusterAdminClient(client, f.changefeedID, defaultTimeoutMs),
		topics:             make(map[string]*MockTopicDetail),
	}, nil
}

// SyncProducer creates a sync producer
func (f *MockFactory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	syncProducer, err := kafka.NewProducer(f.config)
	if err != nil {
		return nil, err
	}
	return &MockSyncProducer{
		Producer:     syncProducer,
		deliveryChan: make(chan kafka.Event),
	}, nil
}

// AsyncProducer creates an async producer
func (f *MockFactory) AsyncProducer(
	ctx context.Context,
) (AsyncProducer, error) {
	asyncProducer, err := kafka.NewProducer(f.config)
	if err != nil {
		return nil, err
	}
	return &MockAsyncProducer{
		AsyncProducer: asyncProducer,
	}, nil
}

// MetricsCollector returns the metric collector
func (f *MockFactory) MetricsCollector() MetricsCollector {
	return &mockMetricsCollector{}
}

type MockClusterAdmin struct {
	ClusterAdminClient
	topics map[string]*MockTopicDetail
}
type MockTopicDetail struct {
	TopicDetail
	fetchesRemainingUntilVisible int
}

// SetRemainingFetchesUntilTopicVisible is used to control the visibility of a specific topic.
// It is used to mock the topic creation delay.
func (c *MockClusterAdmin) SetRemainingFetchesUntilTopicVisible(
	topicName string,
	fetchesRemainingUntilVisible int,
) error {
	topic, ok := c.topics[topicName]
	if !ok {
		return fmt.Errorf("no such topic as %s", topicName)
	}
	topic.fetchesRemainingUntilVisible = fetchesRemainingUntilVisible
	return nil
}

// MockSyncProducer is a mock implementation of SyncProducer interface.
type MockSyncProducer struct {
	Producer     *kafka.Producer
	deliveryChan chan kafka.Event
}

func (s *MockSyncProducer) SendMessage(
	ctx context.Context,
	topic string, partitionNum int32,
	message *common.Message,
) error {
	msg := &kafka.Message{}
	s.Producer.Produce(msg, s.deliveryChan)
	event := <-s.deliveryChan
	switch e := event.(type) {
	case *kafka.Error:
		return e
	}
	return nil
}

// SendMessages implement the SyncProducer interface.
func (s *MockSyncProducer) SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error {
	var err error
	for i := 0; i < int(partitionNum); i++ {
		e := s.SendMessage(ctx, topic, int32(i), message)
		if e != nil {
			err = e
		}
	}
	return err
}

// Close implement the SyncProducer interface.
func (s *MockSyncProducer) Close() {
	s.Producer.Close()
	close(s.deliveryChan)
}

// MockAsyncProducer is a mock implementation of AsyncProducer interface.
type MockAsyncProducer struct {
	AsyncProducer *kafka.Producer
	closed        bool
}

// Close implement the AsyncProducer interface.
func (a *MockAsyncProducer) Close() {
	if a.closed {
		return
	}
	a.AsyncProducer.Close()
	a.closed = true
}

// AsyncRunCallback implement the AsyncProducer interface.
func (a *MockAsyncProducer) AsyncRunCallback(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event := <-a.AsyncProducer.Events():
			switch e := event.(type) {
			case *kafka.Message:
			case *kafka.Error:
				return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, e)
			}
		}
	}
}

// AsyncSend implement the AsyncProducer interface.
func (a *MockAsyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	return a.AsyncProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Key:            message.Key,
		Value:          message.Value,
		Opaque:         message.Callback,
	}, nil)
}

type mockMetricsCollector struct{}

// Run implements the MetricsCollector interface.
func (m *mockMetricsCollector) Run(ctx context.Context) {
}
