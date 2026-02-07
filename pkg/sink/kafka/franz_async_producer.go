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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const franzAsyncRecordRetries = 3

type franzAsyncProducer struct {
	client       *kgo.Client
	changefeedID commonType.ChangeFeedID

	closed *atomic.Bool
	errCh  chan error
}

func newFranzAsyncProducer(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	o *options,
	hook kgo.Hook,
) (AsyncProducer, error) {
	baseOpts, err := buildFranzBaseOptions(ctx, o, hook)
	if err != nil {
		return nil, errors.Trace(err)
	}
	producerOpts, err := buildFranzProducerOptions(o, franzAsyncRecordRetries)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client, err := kgo.NewClient(append(baseOpts, producerOpts...)...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &franzAsyncProducer{
		client:       client,
		changefeedID: changefeedID,
		closed:       atomic.NewBool(false),
		errCh:        make(chan error, 1),
	}, nil
}

func (p *franzAsyncProducer) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}

	go func() {
		start := time.Now()
		p.client.Close()
		log.Info("Close kafka async producer success",
			zap.String("keyspace", p.changefeedID.Keyspace()),
			zap.String("changefeed", p.changefeedID.Name()),
			zap.Duration("duration", time.Since(start)))
	}()
}

func (p *franzAsyncProducer) AsyncSend(
	ctx context.Context,
	topic string,
	partition int32,
	message *common.Message,
) error {
	if p.closed.Load() {
		return errors.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		log.Info("KafkaSinkAsyncSendError error injected",
			zap.String("keyspace", p.changefeedID.Keyspace()),
			zap.String("changefeed", p.changefeedID.Name()))
		errWithInfo := AnnotateEventError(
			p.changefeedID.Keyspace(),
			p.changefeedID.Name(),
			message.LogInfo,
			errors.New("kafka sink injected error"),
		)
		select {
		case p.errCh <- errors.WrapError(errors.ErrKafkaAsyncSendMessage, errWithInfo):
		default:
		}
		failpoint.Return(nil)
	})

	callback := message.Callback
	logInfo := message.LogInfo

	record := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       message.Key,
		Value:     message.Value,
	}

	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			errWithInfo := AnnotateEventError(
				p.changefeedID.Keyspace(),
				p.changefeedID.Name(),
				logInfo,
				err,
			)
			select {
			case p.errCh <- errors.WrapError(errors.ErrKafkaAsyncSendMessage, errWithInfo):
			default:
			}
			return
		}
		if callback != nil {
			callback()
		}
	})

	return nil
}

func (p *franzAsyncProducer) Heartbeat() {}

func (p *franzAsyncProducer) AsyncRunCallback(ctx context.Context) error {
	defer p.closed.Store(true)
	for {
		select {
		case <-ctx.Done():
			log.Info("async producer exit since context is done",
				zap.String("keyspace", p.changefeedID.Keyspace()),
				zap.String("changefeed", p.changefeedID.Name()))
			return errors.Trace(ctx.Err())
		case err := <-p.errCh:
			if err == nil {
				return nil
			}
			return err
		}
	}
}
