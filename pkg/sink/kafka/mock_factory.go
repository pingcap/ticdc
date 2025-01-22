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
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
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

// var (
// 	// BrokerMessageMaxBytes is the broker's `message.max.bytes`
// 	BrokerMessageMaxBytes = defaultMaxMessageBytes
// 	// TopicMaxMessageBytes is the topic's `max.message.bytes`
// 	TopicMaxMessageBytes = defaultMaxMessageBytes
// 	// MinInSyncReplicas is the `min.insync.replicas`
// 	MinInSyncReplicas = defaultMinInsyncReplicas
// )

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	option       *Options
	changefeedID commonType.ChangeFeedID
	mockCluster  *kafka.MockCluster
}

type mockTopicDetail struct {
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
		option:       o,
		changefeedID: changefeedID,
		mockCluster:  mockCluster,
	}, nil
}

// AdminClient return a mocked admin client
func (f *MockFactory) AdminClient() (ClusterAdminClient, error) {
	return &MockClusterAdmin{
		mockCluster: f.mockCluster,
		topics:      make(map[string]*mockTopicDetail),
	}, nil
}

// SyncProducer creates a sync producer to Producer message to kafka
func (f *MockFactory) SyncProducer() (SyncProducer, error) {
	return &MockSyncProducer{Producer: &MockProducer{}, changefeedID: f.changefeedID, option: f.option}, nil
}

// AsyncProducer creates an async producer to Producer message to kafka
func (f *MockFactory) AsyncProducer() (AsyncProducer, error) {
	return &MockAsyncProducer{Producer: &MockProducer{}, changefeedID: f.changefeedID, option: f.option, ch: make(chan *kafka.Message)}, nil
}

// MetricsCollector returns a mocked metrics collector
func (f *MockFactory) MetricsCollector() MetricsCollector {
	return &MockMetricsCollector{changefeedID: f.changefeedID}
}

type MockClusterAdmin struct {
	mockCluster *kafka.MockCluster
	topics      map[string]*mockTopicDetail
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
	topics []string, ignoreTopicError bool,
) (map[string]TopicDetail, error) {
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

func (c *MockClusterAdmin) GetTopicConfig(ctx context.Context, topicName string, configName string) (string, error) {
	_, ok := c.topics[topicName]
	if !ok {
		return "", nil
	}
	switch configName {
	case "message.max.bytes", "max.message.bytes":
		return defaultMaxMessageBytes, nil
	case "min.insync.replicas":
		return defaultMinInsyncReplicas, nil
	}
	return "0", nil
}

func (c *MockClusterAdmin) CreateTopic(ctx context.Context, detail *TopicDetail, validateOnly bool) error {
	bootstrapServers := c.mockCluster.BootstrapServers()
	n := len(strings.Split(bootstrapServers, ","))
	if int(detail.ReplicationFactor) > n {
		return kafka.NewError(kafka.ErrInvalidReplicationFactor, "kafka create topic failed: kafka server: Replication-factor is invalid", false)
	}
	err := c.mockCluster.CreateTopic(detail.Name, int(detail.NumPartitions), int(detail.ReplicationFactor))
	if err != nil {
		return err
	}
	c.topics[detail.Name] = &mockTopicDetail{TopicDetail: *detail}
	return nil
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

type MockSyncProducer struct {
	Producer     *MockProducer
	option       *Options
	changefeedID commonType.ChangeFeedID
}

// SendMessage produces message
func (s *MockSyncProducer) SendMessage(
	ctx context.Context,
	topic string, partition int32,
	message *common.Message,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if message.Length() > s.option.MaxMessageBytes {
		return kafka.NewError(kafka.ErrMsgSizeTooLarge, "", false)
	}
	return nil
}

// SendMessages produces a given set of messages
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

// Close shuts down the mock producer
func (s *MockSyncProducer) Close() {
}

// ExpectInputAndFail for test
func (s *MockSyncProducer) ExpectInputAndFail() {
}

type MockAsyncProducer struct {
	Producer     *MockProducer
	option       *Options
	changefeedID commonType.ChangeFeedID
	ch           chan *kafka.Message
}

// Close shuts down the producer
func (a *MockAsyncProducer) Close() {
	a.Producer.err = kafka.ErrUnknown
	close(a.ch)
}

// AsyncRunCallback process the messages that has sent to kafka
func (a *MockAsyncProducer) AsyncRunCallback(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case msg := <-a.ch:
			if msg != nil {
				callback := msg.Opaque.(func())
				if callback != nil {
					callback()
				}
			}
		case <-ticker.C:
			if a.Producer.err != kafka.ErrNoError {
				return kafka.NewError(a.Producer.err, "", false)
			}
		}
	}
}

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (a *MockAsyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Key:            message.Key,
		Value:          message.Value,
		Opaque:         message.Callback,
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case a.ch <- msg:
	}
	if message.Length() > a.option.MaxMessageBytes {
		a.Producer.err = kafka.ErrMsgSizeTooLarge
	}
	return nil
}

type MockMetricsCollector struct {
	changefeedID commonType.ChangeFeedID
}

func (c *MockMetricsCollector) Run(_ context.Context) {
}

type MockProducer struct {
	err kafka.ErrorCode
}

// ExpectInputAndFail for test
func (s *MockProducer) ExpectInputAndFail(err kafka.ErrorCode) {
	if err != s.err {
		panic(fmt.Sprintf("expect input and fail. actual err: %s expected err: %s", s.err, err))
	}
}

// ExpectSendMessageAndFail for test
func (s *MockProducer) ExpectSendMessageAndFail(err kafka.ErrorCode) {
	if err != s.err {
		panic(fmt.Sprintf("expect send message and succeed failed. actual err: %s expected err: %s", s.err, err))
	}
}

// ExpectInputAndSucceed for test
func (s *MockProducer) ExpectInputAndSucceed() {
	if s.err != kafka.ErrNoError {
		panic(fmt.Sprintf("expect input and succeed failed. err: %s", s.err))
	}
}

// ExpectInputAndSucceed for test
func (s *MockProducer) ExpectSendMessageAndSucceed() {
	if s.err != kafka.ErrNoError {
		panic(fmt.Sprintf("expect send message and succeed failed. err: %s", s.err))
	}
}
