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
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	// DefaultMockTopicName specifies the default mock topic name.
	DefaultMockTopicName = "mock_topic"
	// DefaultMockPartitionNum is the default partition number of default mock topic.
	DefaultMockPartitionNum = 3
	// defaultMockControllerID specifies the default mock controller ID.
	defaultMockControllerID = 1
	// topic replication factor must be 3 for Confluent Cloud Kafka.
	// defaultReplicationFactor = 3
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
	Factory
	config       *kafka.ConfigMap
	changefeedID commonType.ChangeFeedID
	mockCluster  *kafka.MockCluster
}
type MockClusterAdmin struct {
	ClusterAdminClient
	mockCluster *kafka.MockCluster
	topics      map[string]*MockTopicDetail
}
type MockTopicDetail struct {
	TopicDetail
	fetchesRemainingUntilVisible int
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
		config:       NewConfig(o),
		changefeedID: changefeedID,
		mockCluster:  mockCluster,
	}, nil
}

// AdminClient return a mocked admin client
func (f *MockFactory) AdminClient(_ context.Context) (ClusterAdminClient, error) {
	return &MockClusterAdmin{
		mockCluster: f.mockCluster,
		topics:      make(map[string]*MockTopicDetail),
	}, nil
}

func (c *MockClusterAdmin) GetAllBrokers(ctx context.Context) ([]Broker, error) {
	bootstrapServers := c.mockCluster.BootstrapServers()
	n := len(strings.Split(bootstrapServers, ","))
	brokers := make([]Broker, 0, n)
	for i := 0; i < n; i++ {
		brokers = append(brokers, Broker{ID: int32(i)})
	}
	return brokers, nil
}

func (c *MockClusterAdmin) GetTopicsMeta(ctx context.Context,
	topics []string, ignoreTopicError bool) (map[string]TopicDetail, error) {
	topicsMeta := make(map[string]TopicDetail)
	for key, val := range c.topics {
		topicsMeta[key] = val.TopicDetail
	}
	return topicsMeta, nil
}

func (c *MockClusterAdmin) GetTopicsPartitionsNum(
	ctx context.Context, topics []string,
) (map[string]int32, error) {
	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		msg, ok := c.topics[topic]
		if !ok {
			log.Warn("fetch topic meta failed",
				zap.String("topic", topic), zap.Any("msg", msg))
			continue
		}
		result[topic] = msg.NumPartitions
	}
	return result, nil
}

func (c *MockClusterAdmin) CreateTopic(ctx context.Context, detail *TopicDetail, validateOnly bool) error {
	bootstrapServers := c.mockCluster.BootstrapServers()
	n := len(strings.Split(bootstrapServers, ","))
	if int(detail.ReplicationFactor) > n {
		return kafka.NewError(kafka.ErrInvalidReplicationFactor, "kafka create topic failed: kafka server: Replication-factor is invalid", false)
	}
	c.mockCluster.CreateTopic(detail.Name, int(detail.NumPartitions), int(detail.ReplicationFactor))
	c.topics[detail.Name] = &MockTopicDetail{TopicDetail: *detail}
	return nil
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

func (c *MockClusterAdmin) Close() {
	c.mockCluster.Close()
}
