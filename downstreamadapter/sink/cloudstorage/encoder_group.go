// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
)

type encoderGroup struct {
	codecConfig *common.Config

	concurrency int
	indexer     *indexer

	// inputCh/outputCh are bounded channels owned by encoderGroup.
	// They are not closed explicitly; all producers and consumers stop on the
	// shared errgroup context.
	inputCh  []chan *future
	outputCh []chan *future
}

// newEncoderGroup creates an internal two-queue model:
//  1. inputCh: consumed by encoder shards.
//  2. outputCh: consumed by downstream writer shards.
//
// Invariant: the same future is inserted into both queues, and output-side
// consumers only observe tasks after encoding completes or the shared ctx is
// canceled by a fatal encoder error.
func newEncoderGroup(
	codecConfig *common.Config,
	concurrency int,
	outputShards int,
) *encoderGroup {
	if concurrency <= 0 {
		concurrency = defaultEncodingConcurrency
	}
	if outputShards <= 0 {
		outputShards = defaultEncodingConcurrency
	}

	const defaultChannelSize = 1024
	inputCh := make([]chan *future, concurrency)
	for idx := range concurrency {
		inputCh[idx] = make(chan *future, defaultChannelSize)
	}

	outputCh := make([]chan *future, outputShards)
	for idx := range outputShards {
		outputCh[idx] = make(chan *future, defaultChannelSize)
	}

	return &encoderGroup{
		codecConfig: codecConfig,
		concurrency: concurrency,
		indexer:     newIndexer(concurrency, outputShards),
		inputCh:     inputCh,
		outputCh:    outputCh,
	}
}

func (eg *encoderGroup) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for idx := range eg.concurrency {
		g.Go(func() error {
			return eg.runEncoder(ctx, idx)
		})
	}
	return g.Wait()
}

// runEncoder is the only place that mutates task.encodedMsgs.
// Invariant: each task is encoded at most once.
func (eg *encoderGroup) runEncoder(ctx context.Context, index int) error {
	encoder, err := codec.NewTxnEventEncoder(eg.codecConfig)
	if err != nil {
		return err
	}

	inputCh := eg.inputCh[index]
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case future := <-inputCh:
			task := future.task
			if task.isFlushTask() {
				future.Done()
				continue
			}

			err = encoder.AppendTxnEvent(task.event)
			if err != nil {
				return err
			}
			task.encodedMsgs = encoder.Build()
			future.Done()
		}
	}
}

func (eg *encoderGroup) add(ctx context.Context, task *task) error {
	future := newFuture(task)
	inputIndex, outputIndex := eg.indexer.next(task.dispatcherID)
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case eg.inputCh[inputIndex] <- future:
	}

	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case eg.outputCh[outputIndex] <- future:
	}
	return nil
}

// Outputs returns read only channels to prevent misusage.
func (eg *encoderGroup) Outputs() []<-chan *future {
	outputs := make([]<-chan *future, 0, len(eg.outputCh))
	for _, ch := range eg.outputCh {
		outputs = append(outputs, ch)
	}
	return outputs
}

type future struct {
	task *task
	// done is closed on successful encode or flush-marker fast path.
	// Fatal encoder errors are propagated by the shared errgroup ctx instead.
	done chan struct{}
}

func newFuture(task *task) *future {
	return &future{
		task: task,
		done: make(chan struct{}),
	}
}

func (f *future) Done() {
	close(f.done)
}

func (f *future) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case <-f.done:
		return nil
	}
}
