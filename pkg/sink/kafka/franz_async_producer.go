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

type asyncProducer struct {
	client       *kgo.Client
	changefeedID common.ChangeFeedID

	closed   atomic.Bool
	resultCh chan asyncProduceResult
}

type asyncProduceResult struct {
	callback func()
	logInfo  *codeccommon.MessageLogInfo
	err      error
}

func (p *asyncProducer) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}

	start := time.Now()
	log.Info("kafka async producer closed",
		zap.String("keyspace", p.changefeedID.Keyspace()),
		zap.String("changefeed", p.changefeedID.Name()),
		zap.Duration("duration", time.Since(start)))
}

func (p *asyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *codeccommon.Message) error {
	if p.closed.Load() {
		return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
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
		result := asyncProduceResult{
			callback: callback,
			logInfo:  logInfo,
			err:      err,
		}
		select {
		case p.resultCh <- result:
		case <-ctx.Done():
		case <-p.client.Context().Done():
		}
	}

	// Produce can buffer a record without checking ctx, so reject prior cancellation.
	// If it waits for buffer space, canceling ctx makes it return; propagate the cause below.
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	p.client.Produce(ctx, record, promise)
	return context.Cause(ctx)
}

func (p *asyncProducer) AsyncRunCallback(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-p.client.Context().Done():
			return context.Cause(p.client.Context())
		case result := <-p.resultCh:
			if result.err != nil {
				log.Error("kafka message send failed",
					zap.String("keyspace", p.changefeedID.Keyspace()),
					zap.String("changefeed", p.changefeedID.Name()),
					zap.String("eventContext", BuildEventLogContext(
						p.changefeedID.Keyspace(), p.changefeedID.Name(), result.logInfo)),
					zap.Error(result.err))
				return errors.WrapError(errors.ErrKafkaSendMessage, result.err)
			}
			if result.callback != nil {
				result.callback()
			}
		}
	}
}
