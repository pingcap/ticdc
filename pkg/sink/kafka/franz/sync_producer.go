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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka/internal/logutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type SyncProducer struct {
	id commonType.ChangeFeedID

	client  *kgo.Client
	closed  *atomic.Bool
	timeout time.Duration
}

func NewSyncProducer(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	o *Options,
	hook kgo.Hook,
) (*SyncProducer, error) {
	if o == nil {
		o = &Options{}
	}

	opts, err := newOptions(ctx, o, hook)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts = append(opts, newProducerOptions(o)...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	timeout := maxTimeoutWithDefault(o.ReadTimeout, 0)

	return &SyncProducer{
		id:      changefeedID,
		client:  client,
		closed:  atomic.NewBool(false),
		timeout: timeout,
	}, nil
}

func (p *SyncProducer) newRequestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(p.client.Context(), p.timeout)
}

func (p *SyncProducer) SendMessage(topic string, partitionNum int32, message *common.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	ctx, cancel := p.newRequestContext()
	defer cancel()

	record := buildRecord(topic, partitionNum, message)
	err := p.client.ProduceSync(ctx, record).FirstErr()

	failpoint.Inject("KafkaSinkSyncSendMessageError", func() {
		err = errors.New("kafka sink sync send message injected error")
	})

	return p.wrapSendError(message, err)
}

func (p *SyncProducer) SendMessages(topic string, partitionNum int32, message *common.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	records := make([]*kgo.Record, 0, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		records = append(records, buildRecord(topic, int32(i), message))
	}

	ctx, cancel := p.newRequestContext()
	defer cancel()

	err := p.client.ProduceSync(ctx, records...).FirstErr()

	failpoint.Inject("KafkaSinkSyncSendMessagesError", func() {
		err = errors.New("kafka sink sync send messages injected error")
	})

	return p.wrapSendError(message, err)
}

func (p *SyncProducer) Heartbeat() {}

func (p *SyncProducer) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		log.Warn("kafka DDL producer already closed",
			zap.String("keyspace", p.id.Keyspace()),
			zap.String("changefeed", p.id.Name()))
		return
	}

	start := time.Now()
	p.client.Close()
	log.Info("Kafka DDL producer closed",
		zap.String("keyspace", p.id.Keyspace()),
		zap.String("changefeed", p.id.Name()),
		zap.Duration("duration", time.Since(start)))
}

func buildRecord(topic string, partition int32, message *common.Message) *kgo.Record {
	return &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       message.Key,
		Value:     message.Value,
	}
}

func (p *SyncProducer) wrapSendError(message *common.Message, err error) error {
	if err != nil {
		err = logutil.AnnotateEventError(
			p.id.Keyspace(),
			p.id.Name(),
			message.LogInfo,
			err,
		)
	}
	return errors.WrapError(errors.ErrKafkaSendMessage, err)
}
