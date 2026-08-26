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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type saramaAsyncProducer struct {
	producer     sarama.AsyncProducer
	changefeedID common.ChangeFeedID

	closed *atomic.Bool
}

type messageMetadata struct {
	callback func()
	logInfo  *codecCommon.MessageLogInfo
}

func (p *saramaAsyncProducer) Close() {
	p.closed.Store(true)
	go func() {
		// We need to close it asynchronously. Otherwise, we might get stuck
		// with an unhealthy(i.e. Network jitter, isolation) state of Kafka.
		// Safety:
		// * If the kafka cluster is running well, it will be closed as soon as possible.
		//   Also, we cancel all table pipelines before closed, so it's safe.
		// * The shared client is closed by the Factory during sink shutdown, which helps
		//   unblock broker operations before this producer is closed.
		// * For Kafka Sink, duplicate data is acceptable.
		// * There is a risk of goroutine leakage, but it is acceptable and our main
		//   goal is not to get stuck with the processor tick.

		start := time.Now()
		if err := p.producer.Close(); err != nil {
			log.Warn("kafka async producer close failed",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("kafka async producer closed",
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
			return context.Cause(ctx)
		case ack := <-p.producer.Successes():
			if ack != nil {
				switch meta := ack.Metadata.(type) {
				case *messageMetadata:
					if meta != nil && meta.callback != nil {
						meta.callback()
					}
				default:
					log.Error("kafka producer received unknown message metadata type",
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
			return p.handleProducerError(err.Err, extractLogInfo(err.Msg))
		}
	}
}

func (p *saramaAsyncProducer) handleProducerError(err error, logInfo *codecCommon.MessageLogInfo) error {
	log.Error("kafka message send failed",
		zap.String("keyspace", p.changefeedID.Keyspace()),
		zap.String("changefeed", p.changefeedID.Name()),
		zap.String("eventContext", BuildEventLogContext(
			p.changefeedID.Keyspace(), p.changefeedID.Name(), logInfo)),
		zap.Error(err))
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (p *saramaAsyncProducer) AsyncSend(
	ctx context.Context, topic string, partition int32, message *codecCommon.Message,
) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
	}
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
		return context.Cause(ctx)
	case p.producer.Input() <- msg:
	}
	return nil
}

func extractLogInfo(msg *sarama.ProducerMessage) *codecCommon.MessageLogInfo {
	if msg == nil {
		return nil
	}
	meta, ok := msg.Metadata.(*messageMetadata)
	if !ok || meta == nil {
		return nil
	}
	return meta.logInfo
}
