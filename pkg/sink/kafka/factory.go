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

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// Factory is used to produce all kafka components.
type Factory interface {
	// AdminClient return a kafka cluster admin client
	AdminClient(ctx context.Context) (ClusterAdminClient, error)
	// SyncProducer creates a sync producer to writer message to kafka
	SyncProducer(ctx context.Context) (SyncProducer, error)
	// AsyncProducer creates an async producer to writer message to kafka
	AsyncProducer(ctx context.Context) (AsyncProducer, error)
	// MetricsCollector returns the kafka metrics collector
	MetricsCollector(adminClient ClusterAdminClient) MetricsCollector
}

// FactoryCreator defines the type of factory creator.
type FactoryCreator func(context.Context, *options, commonType.ChangeFeedID) (Factory, error)

// SyncProducer is the kafka sync producer
type SyncProducer interface {
	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessage(topic string, partitionNum int32, message *common.Message) error

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(topic string, partitionNum int32, message *common.Message) error

	Heartbeat()

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

	Heartbeat()

	// AsyncRunCallback process the messages that has sent to kafka,
	// and run tha attached callback. the caller should call this
	// method in a background goroutine
	AsyncRunCallback(ctx context.Context) error
}
