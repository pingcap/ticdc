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

	"github.com/pingcap/ticdc/pkg/common"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// NewFactory selects the Kafka client for one changefeed without automatic fallback.
func NewFactory(ctx context.Context, o *options, changefeedID common.ChangeFeedID) (Factory, error) {
	if o.Client == KafkaClientSarama {
		return newSaramaFactory(ctx, o, changefeedID)
	}
	return newFranzFactory(ctx, o, changefeedID)
}

// Factory is used to produce all kafka components.
type Factory interface {
	// AdminClient return a kafka cluster admin client
	AdminClient(ctx context.Context) (AdminClient, error)
	// SyncProducer creates a sync producer to writer message to kafka
	SyncProducer(ctx context.Context) (SyncProducer, error)
	// AsyncProducer creates an async producer to writer message to kafka
	AsyncProducer(ctx context.Context) (AsyncProducer, error)
	// MetricsCollector returns the kafka metrics collector
	MetricsCollector(adminClient AdminClient) MetricsCollector
	// CleanupMetrics removes metrics owned directly by the factory.
	CleanupMetrics()
}

// SyncProducer is the kafka sync producer
type SyncProducer interface {
	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessage(ctx context.Context, topic string, partitionNum int32, message *codecCommon.Message) error

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(ctx context.Context, topic string, partitionNum int32, message *codecCommon.Message) error

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
	AsyncSend(ctx context.Context, topic string, partition int32, message *codecCommon.Message) error

	// AsyncRunCallback process the messages that has sent to kafka,
	// and run tha attached callback. the caller should call this
	// method in a background goroutine
	AsyncRunCallback(ctx context.Context) error
}
