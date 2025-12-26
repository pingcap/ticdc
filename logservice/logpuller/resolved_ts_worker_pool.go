// Copyright 2024 PingCAP, Inc.
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

package logpuller

import (
	"context"
)

type resolvedTsTask struct {
	span       *subscribedSpan
	states     []*regionFeedState
	resolvedTs uint64
}

type resolvedTsWorkerPool struct {
	ctx     context.Context
	client  *subscriptionClient
	workers []*resolvedTsWorker
}

type resolvedTsWorker struct {
	client *subscriptionClient
	input  chan resolvedTsTask
}

func newResolvedTsWorkerPool(
	ctx context.Context,
	client *subscriptionClient,
	workerCount int,
) *resolvedTsWorkerPool {
	if workerCount <= 0 {
		workerCount = 1
	}
	pool := &resolvedTsWorkerPool{
		ctx:     ctx,
		client:  client,
		workers: make([]*resolvedTsWorker, workerCount),
	}
	for i := range pool.workers {
		worker := &resolvedTsWorker{
			client: client,
			input:  make(chan resolvedTsTask, 64),
		}
		pool.workers[i] = worker
		go worker.run(ctx)
	}
	return pool
}

func (p *resolvedTsWorkerPool) submit(span *subscribedSpan, states []*regionFeedState, resolvedTs uint64) {
	if len(states) == 0 || resolvedTs == 0 {
		return
	}
	workerCount := len(p.workers)
	buckets := make([][]*regionFeedState, workerCount)
	for i, state := range states {
		if state == nil {
			continue
		}
		idx := i % workerCount
		buckets[idx] = append(buckets[idx], state)
	}
	for i, bucket := range buckets {
		if len(bucket) == 0 {
			continue
		}
		task := resolvedTsTask{
			span:       span,
			states:     bucket,
			resolvedTs: resolvedTs,
		}
		worker := p.workers[i]
		select {
		case <-p.ctx.Done():
			return
		case worker.input <- task:
		}
	}
}

func (w *resolvedTsWorker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-w.input:
			w.handleTask(task)
		}
	}
}

func (w *resolvedTsWorker) handleTask(task resolvedTsTask) {
	if task.span == nil {
		return
	}
	maxSpanResolvedTs := uint64(0)
	for _, state := range task.states {
		if state == nil {
			continue
		}
		if ts := handleResolvedTs(task.span, state, task.resolvedTs); ts > maxSpanResolvedTs {
			maxSpanResolvedTs = ts
		}
	}
	if maxSpanResolvedTs != 0 {
		w.client.pushSpanResolvedTsEvent(task.span, maxSpanResolvedTs)
	}
}
