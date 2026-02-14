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

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
	defaultChannelSize         = 1024
)

type encodingGroup struct {
	changeFeedID commonType.ChangeFeedID
	codecConfig  *common.Config

	concurrency  int
	outputShards int

	indexer *taskIndexer

	inputCh  []chan *taskFuture
	outputCh []chan *taskFuture
}

// newEncodingGroup creates an internal two-queue model:
//  1. inputCh: consumed by encoder shards.
//  2. outputCh: consumed by downstream writer shards.
//
// Invariant: the same taskFuture is inserted into both queues, and writer side
// must call future.Ready() before using encoded payload.
func newEncodingGroup(
	changefeedID commonType.ChangeFeedID,
	codecConfig *common.Config,
	concurrency int,
	outputShards int,
) *encodingGroup {
	if concurrency <= 0 {
		concurrency = 1
	}
	if outputShards <= 0 {
		outputShards = 1
	}

	inputCh := make([]chan *taskFuture, concurrency)
	for i := 0; i < concurrency; i++ {
		inputCh[i] = make(chan *taskFuture, defaultChannelSize)
	}

	outputCh := make([]chan *taskFuture, outputShards)
	for i := 0; i < outputShards; i++ {
		outputCh[i] = make(chan *taskFuture, defaultChannelSize)
	}

	return &encodingGroup{
		changeFeedID: changefeedID,
		codecConfig:  codecConfig,
		concurrency:  concurrency,
		outputShards: outputShards,
		indexer:      newTaskIndexer(concurrency, outputShards),
		inputCh:      inputCh,
		outputCh:     outputCh,
	}
}

func (eg *encodingGroup) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < eg.concurrency; i++ {
		idx := i
		g.Go(func() error {
			return eg.runEncoder(ctx, idx)
		})
	}

	err := g.Wait()
	for _, outCh := range eg.outputCh {
		close(outCh)
	}
	return err
}

// runEncoder is the only place that mutates task.encodedMsgs.
// Invariant: each task is encoded at most once.
func (eg *encodingGroup) runEncoder(ctx context.Context, index int) error {
	encoder, err := codec.NewTxnEventEncoder(eg.codecConfig)
	if err != nil {
		return errors.Trace(err)
	}

	inputCh := eg.inputCh[index]
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-inputCh:
			if !ok {
				return nil
			}
			task := future.task
			if task.isDrainTask() {
				future.finish(nil)
				continue
			}

			err = encoder.AppendTxnEvent(task.event)
			if err != nil {
				wrappedErr := errors.Trace(err)
				future.finish(wrappedErr)
				return wrappedErr
			}
			task.encodedMsgs = encoder.Build()
			future.finish(nil)
		}
	}
}

func (eg *encodingGroup) Add(ctx context.Context, task *task) error {
	if task == nil {
		return errors.New("nil task")
	}

	future := newTaskFuture(task)
	inputIndex, outputIndex := eg.indexer.next(task.dispatcherID)
	// Principle: encoder parallelism and writer ordering are decoupled.
	// Input shard can be round-robin; output shard must be dispatcher-stable.
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case eg.inputCh[inputIndex] <- future:
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case eg.outputCh[outputIndex] <- future:
	}
	return nil
}

func (eg *encodingGroup) ConsumeOutputShard(
	ctx context.Context,
	index int,
	handle func(*taskFuture) error,
) error {
	if index < 0 || index >= len(eg.outputCh) {
		return errors.Errorf("output index out of range: %d", index)
	}
	outputCh := eg.outputCh[index]
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-outputCh:
			if !ok {
				return nil
			}
			if err := handle(future); err != nil {
				return err
			}
		}
	}
}

type taskFuture struct {
	task *task
	done chan struct{}
	err  error
}

func newTaskFuture(task *task) *taskFuture {
	return &taskFuture{
		task: task,
		done: make(chan struct{}),
	}
}

func (f *taskFuture) finish(err error) {
	f.err = err
	close(f.done)
}

func (f *taskFuture) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-f.done:
	}
	// f.err is already wrapped at the origin.
	return f.err
}
