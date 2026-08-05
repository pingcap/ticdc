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

// AsyncProducer is the kafka async producer
type AsyncProducer interface {
	// Close shuts down the producer and releases its Kafka client resources.
	// Buffered messages fail instead of being flushed.
	Close()

	// AsyncSend is the input channel for the user to write messages to that they
	// wish to send.
	AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error

	// AsyncRunCallback process the messages that has sent to kafka,
	// and run tha attached callback. the caller should call this
	// method in a background goroutine
	AsyncRunCallback(ctx context.Context) error
}

type asyncProducer struct {
	client       *kgo.Client
	changefeedID commonType.ChangeFeedID

	closeStarted atomic.Bool
	closed       atomic.Bool
	errCh        chan error
}

func newAsyncProducer(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	o *options,
	hook *metricsHook,
) (*asyncProducer, error) {
	opts, err := newOptions(ctx, o, hook)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts = append(opts, newProducerOptions(o)...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &asyncProducer{
		client:       client,
		changefeedID: changefeedID,
		errCh:        make(chan error, 1),
	}, nil
}

func (p *asyncProducer) Close() {
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

func (p *asyncProducer) AsyncSend(
	ctx context.Context,
	topic string,
	partition int32,
	message *common.Message,
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

func (p *asyncProducer) enqueueAsyncSendError(
	logInfo *common.MessageLogInfo,
	err error,
) {
	log.Error("kafka message send failed",
		zap.String("keyspace", p.changefeedID.Keyspace()),
		zap.String("changefeed", p.changefeedID.Name()),
		zap.String("eventContext", BuildEventLogContext(
			p.changefeedID.Keyspace(), p.changefeedID.Name(), logInfo)),
		zap.Error(err))
	select {
	case p.errCh <- errors.WrapError(errors.ErrKafkaSendMessage, err):
	// todo: remove this default after support dispatcher recover logic.
	default:
	}
}

func (p *asyncProducer) AsyncRunCallback(ctx context.Context) error {
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
