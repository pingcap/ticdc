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

package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/errors"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

const (
	// DefaultMockTopicName specifies the default mock topic name.
	DefaultMockTopicName = "mock_topic"
	// DefaultMockPartitionNum is the default partition number of default mock topic.
	DefaultMockPartitionNum = 3
	// defaultMockControllerID specifies the default mock controller ID.
	defaultMockControllerID = 1
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

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	option       *Options
	changefeedID commonType.ChangeFeedID
	mockCluster  *kafka.MockCluster
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(
	o *Options, changefeedID commonType.ChangeFeedID,
) (Factory, error) {
	// The broker ids will start at 1 up to and including brokerCount.
	mockCluster, err := kafka.NewMockCluster(defaultMockControllerID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &MockFactory{
		option:       o,
		changefeedID: changefeedID,
		mockCluster:  mockCluster,
	}, nil
}

// AdminClient return a mocked admin client
func (f *MockFactory) AdminClient() (ClusterAdminClient, error) {
	cfg := NewConfig(f.option)
	cfg.SetKey("bootstrap.servers", f.mockCluster.BootstrapServers())
	c, err := kafka.NewAdminClient(cfg)
	if err != nil {
		return nil, err
	}
	return &MockClusterAdmin{mockCluster: f.mockCluster, client: newClusterAdminClient(c, f.changefeedID)}, nil
}

// SyncProducer creates a sync producer to Producer message to kafka
func (f *MockFactory) SyncProducer() (SyncProducer, error) {
	cfg := NewConfig(f.option)
	cfg.SetKey("bootstrap.servers", f.mockCluster.BootstrapServers())
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	producer := &syncProducer{changefeedID: f.changefeedID, p: p, deliveryChan: make(chan kafka.Event)}
	return &MockSyncProducer{
		mockCluster: f.mockCluster,
		producer:    producer,
	}, nil
}

// AsyncProducer creates an async producer to Producer message to kafka
func (f *MockFactory) AsyncProducer() (AsyncProducer, error) {
	cfg := NewConfig(f.option)
	cfg.SetKey("bootstrap.servers", f.mockCluster.BootstrapServers())
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	producer := &asyncProducer{changefeedID: f.changefeedID, p: p}
	return &MockAsyncProducer{
		mockCluster: f.mockCluster,
		producer:    producer,
	}, nil
}

// MetricsCollector returns a mocked metrics collector
func (f *MockFactory) MetricsCollector() MetricsCollector {
	cfg := NewConfig(f.option)
	cfg.SetKey("bootstrap.servers", f.mockCluster.BootstrapServers())
	return &MockMetricsCollector{mockCluster: f.mockCluster, metricsCollector: NewMetricsCollector(f.changefeedID, cfg)}
}

type MockClusterAdmin struct {
	mockCluster *kafka.MockCluster
	client      ClusterAdminClient
}

func (c *MockClusterAdmin) GetAllBrokers(ctx context.Context) ([]Broker, error) {
	return c.client.GetAllBrokers(ctx)
}

func (c *MockClusterAdmin) GetTopicsMeta(ctx context.Context,
	topics []string, ignoreTopicError bool,
) (map[string]TopicDetail, error) {
	return c.client.GetTopicsMeta(ctx, topics, ignoreTopicError)
}

func (c *MockClusterAdmin) GetTopicsPartitionsNum(
	ctx context.Context, topics []string,
) (map[string]int32, error) {
	return c.client.GetTopicsPartitionsNum(ctx, topics)
}

func (c *MockClusterAdmin) CreateTopic(ctx context.Context, detail *TopicDetail, validateOnly bool) error {
	brokers, err := c.GetAllBrokers(ctx)
	if err != nil {
		return err
	}
	if len(brokers) < int(detail.ReplicationFactor) {
		return fmt.Errorf("kafka create topic failed: kafka server: Replication-factor is invalid")
	}
	return c.mockCluster.CreateTopic(detail.Name, int(detail.NumPartitions), int(detail.ReplicationFactor))
}

func (c *MockClusterAdmin) GetTopicConfig(ctx context.Context, topicName string, configName string) (string, error) {
	topicDetail, err := c.GetTopicsMeta(ctx, []string{topicName}, false)
	if err != nil {
		return "", err
	}
	_, ok := topicDetail[topicName]
	if !ok {
		return "", kafka.NewError(kafka.ErrUnknownTopic, topicName, false)
	}
	switch configName {
	case "message.max.bytes", "max.message.bytes":
		return defaultMaxMessageBytes, nil
	case "min.insync.replicas":
		return defaultMinInsyncReplicas, nil
	}
	return "0", nil
}

func (c *MockClusterAdmin) GetBrokerConfig(ctx context.Context, configName string) (string, error) {
	switch configName {
	case "message.max.bytes", "max.message.bytes":
		return defaultMaxMessageBytes, nil
	case "min.insync.replicas":
		return defaultMinInsyncReplicas, nil
	}
	return "0", nil
}

func (c *MockClusterAdmin) Close() {
	c.client.Close()
	c.mockCluster.Close()
}

type MockSyncProducer struct {
	mockCluster *kafka.MockCluster
	producer    SyncProducer
}

// SendMessage produces message
func (s *MockSyncProducer) SendMessage(
	ctx context.Context,
	topic string, partition int32,
	message *common.Message,
) error {
	return s.producer.SendMessage(ctx, topic, partition, message)
}

// SendMessages produces a given set of messages
func (s *MockSyncProducer) SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error {
	return s.producer.SendMessages(ctx, topic, partitionNum, message)

}

// Close shuts down the mock producer
func (s *MockSyncProducer) Close() {
	s.producer.Close()
	s.mockCluster.Close()
}

type MockAsyncProducer struct {
	mockCluster *kafka.MockCluster
	producer    AsyncProducer
}

// Close shuts down the producer
func (a *MockAsyncProducer) Close() {
	a.producer.Close()
	a.mockCluster.Close()
}

// AsyncRunCallback process the messages that has sent to kafka
func (a *MockAsyncProducer) AsyncRunCallback(ctx context.Context) error {
	return a.producer.AsyncRunCallback(ctx)
}

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (a *MockAsyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	return a.producer.AsyncSend(ctx, topic, partition, message)
}

type MockMetricsCollector struct {
	mockCluster      *kafka.MockCluster
	metricsCollector MetricsCollector
}

func (c *MockMetricsCollector) Run(ctx context.Context) {
	c.metricsCollector.Run(ctx)
}
