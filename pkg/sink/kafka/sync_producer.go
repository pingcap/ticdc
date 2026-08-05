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
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

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

	// Close shuts down the producer and releases its Kafka client resources.
	Close()
}

type syncProducer struct {
	id commonType.ChangeFeedID

	client  *kgo.Client
	closed  atomic.Bool
	timeout time.Duration
}

func newSyncProducer(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	o *options,
	hook *metricsHook,
) (*syncProducer, error) {
	opts, err := newOptions(ctx, o, hook)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts = append(opts, newProducerOptions(o)...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &syncProducer{
		id:      changefeedID,
		client:  client,
		timeout: o.requestTimeout(),
	}, nil
}

func (p *syncProducer) SendMessage(topic string, partitionNum int32, message *common.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
	}

	ctx, cancel := context.WithTimeout(p.client.Context(), p.timeout)
	defer cancel()

	record := &kgo.Record{
		Topic:     topic,
		Partition: partitionNum,
		Key:       message.Key,
		Value:     message.Value,
	}
	err := p.client.ProduceSync(ctx, record).FirstErr()
	if err == nil {
		return nil
	}
	log.Error("kafka message send failed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.String("eventContext", BuildEventLogContext(
			p.id.Keyspace(), p.id.Name(), message.LogInfo)),
		zap.Error(err))
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

func (p *syncProducer) SendMessages(topic string, partitionNum int32, message *common.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
	}

	records := make([]*kgo.Record, 0, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		records = append(records, &kgo.Record{
			Topic:     topic,
			Partition: int32(i),
			Key:       message.Key,
			Value:     message.Value,
		})
	}

	ctx, cancel := context.WithTimeout(p.client.Context(), p.timeout)
	defer cancel()

	err := p.client.ProduceSync(ctx, records...).FirstErr()
	if err == nil {
		return nil
	}
	log.Error("kafka message send failed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.String("eventContext", BuildEventLogContext(
			p.id.Keyspace(), p.id.Name(), message.LogInfo)),
		zap.Error(err))
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

func (p *syncProducer) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		log.Warn("kafka ddl producer already closed",
			zap.String("keyspace", p.id.Keyspace()),
			zap.String("changefeed", p.id.Name()))
		return
	}

	start := time.Now()
	p.client.Close()
	log.Info("kafka ddl producer closed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.Duration("duration", time.Since(start)))
}
