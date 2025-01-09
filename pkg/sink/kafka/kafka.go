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

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// TopicDetail represent a topic's detail information.
type TopicDetail struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

// Broker represents a Kafka broker.
type Broker struct {
	ID int32
}

// Factory is used to produce all kafka components.
type Factory interface {
	// AdminClient return a kafka cluster admin client
	AdminClient(ctx context.Context) (ClusterAdminClient, error)
	// SyncProducer creates a sync producer to writer message to kafka
	SyncProducer(ctx context.Context) (SyncProducer, error)
	// AsyncProducer creates an async producer to writer message to kafka
	AsyncProducer(ctx context.Context) (AsyncProducer, error)
	// MetricsCollector returns the kafka metrics collector
	MetricsCollector() MetricsCollector
}

// FactoryCreator defines the type of factory creator.
type FactoryCreator func(*Options, commonType.ChangeFeedID) (Factory, error)

// ClusterAdminClient is the administrative client for Kafka,
// which supports managing and inspecting topics, brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// GetAllBrokers return all brokers among the cluster
	GetAllBrokers(ctx context.Context) ([]Broker, error)

	// GetBrokerConfig return the broker level configuration with the `configName`
	GetBrokerConfig(ctx context.Context, configName string) (string, error)

	// GetTopicConfig return the topic level configuration with the `configName`
	GetTopicConfig(ctx context.Context, topicName string, configName string) (string, error)

	// GetTopicsMeta return all target topics' metadata
	// if `ignoreTopicError` is true, ignore the topic error and return the metadata of valid topics
	GetTopicsMeta(ctx context.Context,
		topics []string, ignoreTopicError bool) (map[string]TopicDetail, error)

	// GetTopicsPartitionsNum return the number of partitions of each topic.
	GetTopicsPartitionsNum(ctx context.Context, topics []string) (map[string]int32, error)

	// CreateTopic creates a new topic.
	CreateTopic(ctx context.Context, detail *TopicDetail, validateOnly bool) error

	// Close shuts down the admin client.
	Close()

	// SetRemainingFetchesUntilTopicVisible for mock
	SetRemainingFetchesUntilTopicVisible(topic string, num int) error
}

// SyncProducer is the kafka sync producer
type SyncProducer interface {
	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessage(ctx context.Context,
		topic string, partitionNum int32,
		message *common.Message) error

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error

	// Close shuts down the producer; you must call this function before a producer
	// object passes out of scope, as it may otherwise leak memory.
	// You must call this before calling Close on the underlying client.
	Close()
}

// AsyncProducer is the kafka async producer
type AsyncProducer interface {
	// Close shuts down the producer and waits for any buffered messages to be
	// flushed. You must call this function before a producer object passes out of
	// scope, as it may otherwise leak memory. You must call this before process
	// shutting down, or you may lose messages. You must call this before calling
	// Close on the underlying client.
	Close()

	// AsyncSend is the input channel for the user to write messages to that they
	// wish to send.
	AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error

	// AsyncRunCallback process the messages that has sent to kafka,
	// and run tha attached callback. the caller should call this
	// method in a background goroutine
	AsyncRunCallback(ctx context.Context) error
}

// MetricsCollector is the interface for kafka metrics collector.
type MetricsCollector interface {
	Run(ctx context.Context)
}
