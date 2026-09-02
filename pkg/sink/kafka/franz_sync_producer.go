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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type syncProducer struct {
	id common.ChangeFeedID

	client  *kgo.Client
	closed  atomic.Bool
	timeout time.Duration
}

func (p *syncProducer) SendMessage(ctx context.Context, topic string, partitionNum int32, message *codeccommon.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
	}

	record := &kgo.Record{
		Topic:     topic,
		Partition: partitionNum,
		Key:       message.Key,
		Value:     message.Value,
	}
	return p.sendRecords(ctx, message, record)
}

func (p *syncProducer) SendMessages(ctx context.Context, topic string, partitionNum int32, message *codeccommon.Message) error {
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

	return p.sendRecords(ctx, message, records...)
}

func (p *syncProducer) sendRecords(ctx context.Context, message *codeccommon.Message, records ...*kgo.Record) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
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
