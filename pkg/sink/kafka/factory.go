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
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Factory is used to produce all kafka components.
type Factory interface {
	// AdminClient return a kafka cluster admin client
	AdminClient() (ClusterAdminClient, error)
	// SyncProducer creates a sync producer to writer message to kafka
	SyncProducer() (SyncProducer, error)
	// AsyncProducer creates an async producer to writer message to kafka
	AsyncProducer() (AsyncProducer, error)
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
	SendMessage(ctx context.Context,
		topic string, partitionNum int32,
		message *common.Message) error

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error

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

type saramaSyncProducer struct {
	id       commonType.ChangeFeedID
	client   sarama.Client
	producer sarama.SyncProducer
	closed   *atomic.Bool
}

func (p *saramaSyncProducer) SendMessage(
	_ context.Context,
	topic string, partitionNum int32,
	message *common.Message,
) error {
	if p.closed.Load() {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Partition: partitionNum,
	})
	failpoint.Inject("KafkaSinkSyncSendMessageError", func() {
		err = cerror.WrapError(cerror.ErrKafkaSendMessage, errors.New("kafka sink sync send message injected error"))
	})
	if err != nil {
		err = AnnotateEventError(
			p.id.Keyspace(),
			p.id.Name(),
			message.LogInfo,
			err,
		)
	}
	return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
}

func (p *saramaSyncProducer) SendMessages(
	_ context.Context, topic string, partitionNum int32, message *common.Message,
) error {
	if p.closed.Load() {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}
	msgs := make([]*sarama.ProducerMessage, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     sarama.ByteEncoder(message.Value),
			Partition: int32(i),
		}
	}
	err := p.producer.SendMessages(msgs)
	failpoint.Inject("KafkaSinkSyncSendMessagesError", func() {
		err = cerror.WrapError(cerror.ErrKafkaSendMessage, errors.New("kafka sink sync send messages injected error"))
	})
	if err != nil {
		err = AnnotateEventError(
			p.id.Keyspace(),
			p.id.Name(),
			message.LogInfo,
			err,
		)
	}
	return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
}

func (p *saramaSyncProducer) Heartbeat() {
	if p.closed.Load() {
		return
	}
	brokers := p.client.Brokers()
	for _, b := range brokers {
		_, _ = b.ApiVersions(&sarama.ApiVersionsRequest{})
	}
}

func (p *saramaSyncProducer) Close() {
	if p.closed.Load() {
		log.Warn("kafka DDL producer already closed",
			zap.String("keyspace", p.id.Keyspace()),
			zap.String("changefeed", p.id.Name()))
		return
	}

	p.closed.Store(true)
	start := time.Now()
	// this also close the client.
	err := p.producer.Close()
	if err != nil {
		log.Error("Close Kafka DDL producer with error",
			zap.String("keyspace", p.id.Keyspace()),
			zap.String("changefeed", p.id.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return
	}
	log.Info("Kafka DDL producer closed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.Duration("duration", time.Since(start)))
}

type saramaAsyncProducer struct {
	client       sarama.Client
	producer     sarama.AsyncProducer
	changefeedID commonType.ChangeFeedID

	closed      *atomic.Bool
	failpointCh chan *sarama.ProducerError
}

type messageMetadata struct {
	callback func()
	logInfo  *common.MessageLogInfo
}

func (p *saramaAsyncProducer) Close() {
	p.closed.Store(true)
	go func() {
		// We need to close it asynchronously. Otherwise, we might get stuck
		// with an unhealthy(i.e. Network jitter, isolation) state of Kafka.
		// Safety:
		// * If the kafka cluster is running well, it will be closed as soon as possible.
		//   Also, we cancel all table pipelines before closed, so it's safe.
		// * If there is a problem with the kafka cluster, it will shut down the client first,
		//   which means no more data will be sent because the connection to the broker is dropped.
		//   Also, we cancel all table pipelines before closed, so it's safe.
		// * For Kafka Sink, duplicate data is acceptable.
		// * There is a risk of goroutine leakage, but it is acceptable and our main
		//   goal is not to get stuck with the processor tick.

		// `client` is mainly used by `asyncProducer` to fetch metadata and perform other related
		// operations. When we close the `kafkaSaramaProducer`,
		// there is no need for TiCDC to make sure that all buffered messages are flushed.
		// Consider the situation where the broker is irresponsive. If the client were not
		// closed, `asyncProducer.Close()` would waste a mount of time to try flush all messages.
		// To prevent the scenario mentioned above, close the client first.
		start := time.Now()
		if err := p.client.Close(); err != nil {
			log.Warn("Close kafka async producer client error",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("Close kafka async producer client success",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()),
				zap.Duration("duration", time.Since(start)))
		}

		start = time.Now()
		if err := p.producer.Close(); err != nil {
			log.Warn("Close kafka async producer error",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("Close kafka async producer success",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()),
				zap.Duration("duration", time.Since(start)))
		}
	}()
}

func (p *saramaAsyncProducer) AsyncRunCallback(
	ctx context.Context,
) error {
	defer p.closed.Store(true)
	for {
		select {
		case <-ctx.Done():
			log.Info("async producer exit since context is done",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()))
			return errors.Trace(ctx.Err())
		case err := <-p.failpointCh:
			log.Warn("Receive from failpoint chan in kafka DML producer",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()),
				zap.Error(err))
			return p.handleProducerError(err)
		case ack := <-p.producer.Successes():
			if ack != nil {
				switch meta := ack.Metadata.(type) {
				case *messageMetadata:
					if meta != nil && meta.callback != nil {
						meta.callback()
					}
				default:
					log.Error("unknown message metadata type in async producer",
						zap.Any("metadata", ack.Metadata))
				}
			}
		case err := <-p.producer.Errors():
			// We should not wrap a nil pointer if the pointer
			// is of a subtype of `error` because Go would store the type info
			// and the resulted `error` variable would not be nil,
			// which will cause the pkg/error library to malfunction.
			// See: https://go.dev/doc/faq#nil_error
			if err == nil {
				return nil
			}
			return p.handleProducerError(err)
		}
	}
}

func (p *saramaAsyncProducer) handleProducerError(err *sarama.ProducerError) error {
	errWithInfo := AnnotateEventError(
		p.changefeedID.Keyspace(),
		p.changefeedID.Name(),
		extractLogInfo(err.Msg),
		err.Err,
	)
	return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, errWithInfo)
}

func (p *saramaAsyncProducer) Heartbeat() {
	brokers := p.client.Brokers()
	for _, b := range brokers {
		_, _ = b.ApiVersions(&sarama.ApiVersionsRequest{})
	}
}

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (p *saramaAsyncProducer) AsyncSend(
	ctx context.Context, topic string, partition int32, message *common.Message,
) error {
	if p.closed.Load() {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}
	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("KafkaSinkAsyncSendError error injected", zap.String("keyspace", p.changefeedID.Keyspace()),
			zap.String("changefeed", p.changefeedID.Name()))
		p.failpointCh <- &sarama.ProducerError{
			Err: errors.New("kafka sink injected error"),
			Msg: &sarama.ProducerMessage{Metadata: &messageMetadata{
				callback: message.Callback,
				logInfo:  message.LogInfo,
			}},
		}
		failpoint.Return(nil)
	})
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
	case p.producer.Input() <- msg:
	}
	return nil
}

func extractLogInfo(msg *sarama.ProducerMessage) *common.MessageLogInfo {
	if msg == nil {
		return nil
	}
	meta, ok := msg.Metadata.(*messageMetadata)
	if !ok || meta == nil {
		return nil
	}
	return meta.logInfo
}
