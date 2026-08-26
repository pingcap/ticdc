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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type saramaSyncProducerClient interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	SendMessages(msgs []*sarama.ProducerMessage) error
	Close() error
}

type saramaSyncProducer struct {
	id       common.ChangeFeedID
	producer saramaSyncProducerClient
	closed   *atomic.Bool
}

func (p *saramaSyncProducer) SendMessage(topic string, partitionNum int32, message *codecCommon.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Partition: partitionNum,
	}
	_, _, err := p.producer.SendMessage(msg)
	if err == nil {
		return nil
	}
	log.Error("kafka message send failed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.String("eventContext", BuildEventLogContext(p.id.Keyspace(), p.id.Name(), message.LogInfo)),
		zap.Error(err))
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

func (p *saramaSyncProducer) SendMessages(topic string, partitionNum int32, message *codecCommon.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
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
	if err == nil {
		return nil
	}
	log.Error("kafka message send failed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.String("eventContext", BuildEventLogContext(p.id.Keyspace(), p.id.Name(), message.LogInfo)),
		zap.Error(err))
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

func (p *saramaSyncProducer) Close() {
	if p.closed.Load() {
		log.Warn("kafka ddl producer already closed",
			zap.String("keyspace", p.id.Keyspace()),
			zap.String("changefeed", p.id.Name()))
		return
	}

	p.closed.Store(true)
	start := time.Now()
	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			log.Error("kafka ddl producer close failed",
				zap.String("keyspace", p.id.Keyspace()),
				zap.String("changefeed", p.id.Name()),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
			return
		}
	}
	log.Info("kafka ddl producer closed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.Duration("duration", time.Since(start)))
}
