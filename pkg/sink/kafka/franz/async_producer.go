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

type AsyncProducer struct {
	client       *kgo.Client
	changefeedID common.ChangeFeedID

	closeStarted atomic.Bool
	closed       atomic.Bool
	errCh        chan error
}

func NewAsyncProducer(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg Config,
	hook *metricsHook,
) (*AsyncProducer, error) {
	opts, err := newClientOptions(ctx, changefeedID, "async-producer", cfg, hook)
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

	return &AsyncProducer{
		client:       client,
		changefeedID: changefeedID,
		errCh:        make(chan error, 1),
	}, nil
}

func (p *AsyncProducer) Close() {
	if !p.closeStarted.CompareAndSwap(false, true) {
		return
	}
	p.closed.Store(true)

	start := time.Now()
	p.client.Close()

	log.Info("kafka async producer closed",
		zap.String("keyspace", p.changefeedID.Keyspace()),
		zap.String("changefeed", p.changefeedID.Name()),
		zap.Duration("duration", time.Since(start)))
}

func (p *AsyncProducer) AsyncSend(
	ctx context.Context,
	topic string,
	partition int32,
	message *codeccommon.Message,
) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	record := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       message.Key,
		Value:     message.Value,
	}

	callback := message.Callback
	logInfo := message.LogInfo
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			p.enqueueAsyncSendError(logInfo, err)
			return
		}

		if callback != nil {
			callback()
		}
	}

	p.client.Produce(ctx, record, promise)

	return nil
}

func (p *AsyncProducer) enqueueAsyncSendError(
	logInfo *codeccommon.MessageLogInfo,
	err error,
) {
	log.Error("kafka message send failed",
		zap.String("keyspace", p.changefeedID.Keyspace()),
		zap.String("changefeed", p.changefeedID.Name()),
		zap.String("eventContext", buildEventLogContext(
			p.changefeedID.Keyspace(), p.changefeedID.Name(), logInfo)),
		zap.Error(err))

	select {
	case p.errCh <- errors.WrapError(errors.ErrKafkaSendMessage, err):
	// Keep the first error until the dispatcher can recover from multiple errors.
	default:
	}
}

func (p *AsyncProducer) AsyncRunCallback(ctx context.Context) error {
	defer p.closed.Store(true)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case err := <-p.errCh:
			return err
		}
	}
}
