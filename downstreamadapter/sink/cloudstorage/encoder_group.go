// Copyright 2025 PingCAP, Inc.
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

type encoderGroup struct {
	codecConfig *common.Config

	concurrency int
	indexer     *indexer

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
		concurrency = 1
	}
	if outputShards <= 0 {
		outputShards = 1
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

	err := g.Wait()
	// outputCh is owned by encoderGroup.run. It closes the fan-out only after all
	// encoder goroutines have exited and no more futures can be published.
	for _, outCh := range eg.outputCh {
		close(outCh)
	}
	return err
}

// closeInput is owned by the upstream submit stage.
// It closes each encoder input once no more tasks can be added.
func (eg *encoderGroup) closeInput() {
	for _, inputCh := range eg.inputCh {
		close(inputCh)
	}
}

// runEncoder is the only place that mutates task.encodedMsgs.
// Invariant: each task is encoded at most once.
func (eg *encoderGroup) runEncoder(ctx context.Context, index int) error {
	encoder, err := codec.NewTxnEventEncoder(eg.codecConfig)
	if err != nil {
		return errors.Trace(err)
	}

	inputCh := eg.inputCh[index]
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case future, ok := <-inputCh:
			if !ok {
				return nil
			}
			task := future.task
			if task.isDrainTask() {
				close(future.done)
				continue
			}

			err = encoder.AppendTxnEvent(task.event)
			if err != nil {
				return errors.Trace(err)
			}
			task.encodedMsgs = encoder.Build()
			close(future.done)
		}
	}
}

func (eg *encoderGroup) add(ctx context.Context, task *task) error {
	future := newFuture(task)
	inputIndex, outputIndex := eg.indexer.next(task.dispatcherID)
	// Principle: encoder parallelism and writer ordering are decoupled.
	// Input shard can be round-robin; output shard must be dispatcher-stable.
	if err := context.Cause(ctx); err != nil {
		return errors.Trace(err)
	}
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case eg.inputCh[inputIndex] <- future:
	}

	if err := context.Cause(ctx); err != nil {
		return errors.Trace(err)
	}
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case eg.outputCh[outputIndex] <- future:
	}
	return nil
}

func (eg *encoderGroup) consumeOutputShard(
	ctx context.Context,
	index int,
	handle func(*task) error,
) error {
	if index < 0 || index >= len(eg.outputCh) {
		return errors.Errorf("output index out of range: %d", index)
	}
	outputCh := eg.outputCh[index]
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case future, ok := <-outputCh:
			if !ok {
				return nil
			}
			if err := future.ready(ctx); err != nil {
				return err
			}
			if err := handle(future.task); err != nil {
				return err
			}
		}
	}
}

type future struct {
	task *task
	// done is closed on successful encode or drain fast path.
	// Fatal encoder errors are propagated by the shared errgroup ctx instead.
	done chan struct{}
}

func newFuture(task *task) *future {
	return &future{
		task: task,
		done: make(chan struct{}),
	}
}

func (f *future) ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case <-f.done:
		return nil
	}
}
