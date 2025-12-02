// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type saramaSyncProducer struct {
	id       commonType.ChangeFeedID
	client   sarama.Client
	producer sarama.SyncProducer
	closed   *atomic.Bool
}

func (p *saramaSyncProducer) SendMessage(topic string, partitionNum int32, message *common.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Partition: partitionNum,
	}
	_, _, err := p.producer.SendMessage(msg)

	failpoint.Inject("KafkaSinkSyncSendMessageError", func() {
		err = errors.WrapError(errors.ErrKafkaSendMessage, errors.New("kafka sink sync send message injected error"))
	})
	if err != nil {
		err = AnnotateEventError(
			p.id.Keyspace(),
			p.id.Name(),
			message.LogInfo,
			err,
		)
	}
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

func (p *saramaSyncProducer) SendMessages(topic string, partitionNum int32, message *common.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaProducerClosed.GenWithStackByArgs()
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
		err = errors.WrapError(errors.ErrKafkaSendMessage, errors.New("kafka sink sync send messages injected error"))
	})
	if err != nil {
		err = AnnotateEventError(
			p.id.Keyspace(),
			p.id.Name(),
			message.LogInfo,
			err,
		)
	}
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
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
