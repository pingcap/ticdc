// Copyright 2022 PingCAP, Inc.
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

package codec

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultInputChanSize  = 128
	defaultMetricInterval = 15 * time.Second
)

// EncoderGroup manages a group of encoders
type EncoderGroup interface {
	// Run start the group
	Run(ctx context.Context) error
	// AddEvents add events into the group and encode them by one of the encoders in the group.
	// Note: The caller should make sure all events should belong to the same topic and partition.
	AddEvents(ctx context.Context, key commonEvent.TopicPartitionKey, events ...*commonEvent.RowEvent) error
	// Output returns a channel produce futures
	Output() <-chan *future
}

type encoderGroup struct {
	changefeedID commonType.ChangeFeedID

	// concurrency is the number of encoder pipelines to run
	concurrency int
	// inputCh is the input channel for each encoder pipeline
	inputCh []chan *future
	index   uint64

	rowEventEncoders []common.EventEncoder

	outputCh chan *future

	bootstrapWorker *bootstrapWorker
}

// NewEncoderGroup creates a new EncoderGroup instance
func NewEncoderGroup(
	ctx context.Context,
	cfg *config.SinkConfig,
	encoderConfig *common.Config,
	changefeedID commonType.ChangeFeedID,
) (*encoderGroup, error) {
	concurrency := util.GetOrZero(cfg.EncoderConcurrency)
	if concurrency <= 0 {
		concurrency = config.DefaultEncoderGroupConcurrency
	}
	inputCh := make([]chan *future, concurrency)
	rowEventEncoders := make([]common.EventEncoder, concurrency)
	var err error
	for i := 0; i < concurrency; i++ {
		inputCh[i] = make(chan *future, defaultInputChanSize)
		rowEventEncoders[i], err = NewEventEncoder(ctx, encoderConfig)
		if err != nil {
			log.Error("failed to create row event encoder", zap.Error(err))
			return nil, errors.Trace(err)
		}
	}
	outCh := make(chan *future, defaultInputChanSize*concurrency)

	var bw *bootstrapWorker
	if cfg.ShouldSendBootstrapMsg() {
		encoder, err := NewEventEncoder(ctx, encoderConfig)
		if err != nil {
			log.Error("failed to create row event encoder", zap.Error(err))
			return nil, errors.Trace(err)
		}
		bw = newBootstrapWorker(
			changefeedID,
			outCh,
			encoder,
			util.GetOrZero(cfg.SendBootstrapIntervalInSec),
			util.GetOrZero(cfg.SendBootstrapInMsgCount),
			util.GetOrZero(cfg.SendBootstrapToAllPartition),
			defaultMaxInactiveDuration,
		)
	}

	return &encoderGroup{
		changefeedID:     changefeedID,
		rowEventEncoders: rowEventEncoders,
		concurrency:      concurrency,
		inputCh:          inputCh,
		index:            0,
		outputCh:         outCh,
		bootstrapWorker:  bw,
	}, nil
}

func (g *encoderGroup) Run(ctx context.Context) error {
	defer func() {
		g.cleanMetrics()
		log.Info("encoder group exited",
			zap.String("namespace", g.changefeedID.Namespace()),
			zap.String("changefeed", g.changefeedID.Name()))
	}()
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < g.concurrency; i++ {
		idx := i
		eg.Go(func() error {
			return g.runEncoder(ctx, idx)
		})
	}

	if g.bootstrapWorker != nil {
		eg.Go(func() error {
			return g.bootstrapWorker.run(ctx)
		})
	}

	return eg.Wait()
}

func (g *encoderGroup) runEncoder(ctx context.Context, idx int) error {
	inputCh := g.inputCh[idx]
	metric := encoderGroupInputChanSizeGauge.
		WithLabelValues(g.changefeedID.Namespace(), g.changefeedID.Name(), strconv.Itoa(idx))
	ticker := time.NewTicker(defaultMetricInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			metric.Set(float64(len(inputCh)))
		case future := <-inputCh:
			for _, event := range future.events {
				err := g.rowEventEncoders[idx].AppendRowChangedEvent(ctx, future.Key.Topic, event)
				if err != nil {
					return errors.Trace(err)
				}
			}
			future.Messages = g.rowEventEncoders[idx].Build()
			// TODO: Is it necessary to clear after use?
			close(future.done)
		}
	}
}

func (g *encoderGroup) AddEvents(
	ctx context.Context,
	key commonEvent.TopicPartitionKey,
	events ...*commonEvent.RowEvent,
) error {
	// bootstrapWorker only not nil when the protocol is simple
	if g.bootstrapWorker != nil {
		err := g.bootstrapWorker.addEvent(ctx, key, events[0])
		if err != nil {
			return errors.Trace(err)
		}
	}

	future := newFuture(key, events...)
	index := atomic.AddUint64(&g.index, 1) % uint64(g.concurrency)
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case g.inputCh[index] <- future:
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case g.outputCh <- future:
	}

	return nil
}

func (g *encoderGroup) Output() <-chan *future {
	return g.outputCh
}

func (g *encoderGroup) cleanMetrics() {
	encoderGroupInputChanSizeGauge.DeleteLabelValues(g.changefeedID.Namespace(), g.changefeedID.Name())
	for _, encoder := range g.rowEventEncoders {
		encoder.Clean()
	}
	common.CleanMetrics(g.changefeedID)
}

// future is a wrapper of the result of encoding events
// It's used to notify the caller that the result is ready.
type future struct {
	Key      commonEvent.TopicPartitionKey
	events   []*commonEvent.RowEvent
	Messages []*common.Message
	done     chan struct{}
}

func newFuture(key commonEvent.TopicPartitionKey,
	events ...*commonEvent.RowEvent,
) *future {
	return &future{
		Key:    key,
		events: events,
		done:   make(chan struct{}),
	}
}

// Ready waits until the response is ready, should be called before consuming the future.
func (p *future) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-p.done:
	}
	return nil
}
