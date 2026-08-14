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

package franz

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

type SyncProducer struct {
	id common.ChangeFeedID

	client  *kgo.Client
	closed  atomic.Bool
	timeout time.Duration
}

func NewSyncProducer(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg Config,
	hook *metricsHook,
) (*SyncProducer, error) {
	opts, err := newClientOptions(ctx, changefeedID, "sync-producer", cfg, hook)
	if err != nil {
		return nil, err
	}

	producerOpts, err := producerOptions(cfg)
	if err != nil {
		return nil, err
	}

	opts = append(opts, producerOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	return &SyncProducer{
		id:      changefeedID,
		client:  client,
		timeout: cfg.requestTimeout(),
	}, nil
}

func (p *SyncProducer) SendMessage(topic string, partitionNum int32, message *codeccommon.Message) error {
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
		zap.String("eventContext", buildEventLogContext(
			p.id.Keyspace(), p.id.Name(), message.LogInfo)),
		zap.Error(err))

	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

func (p *SyncProducer) SendMessages(topic string, partitionNum int32, message *codeccommon.Message) error {
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
		zap.String("eventContext", buildEventLogContext(
			p.id.Keyspace(), p.id.Name(), message.LogInfo)),
		zap.Error(err))

	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}

func (p *SyncProducer) Close() {
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
