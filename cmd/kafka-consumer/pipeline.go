// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// batchChannelSize bounds how many restored batches may wait for the submitter.
// It is the backpressure of the resolve path: once it is full the resolver
// stops restoring and the read loop keeps appending to the spill store.
const batchChannelSize = 64

// sinkBatchMessages caps how many events the submitter hands to the sink before
// waiting for the flush callbacks, mirroring the single goroutine batching.
const sinkBatchMessages = 10000

// preparedBatch is a restored group prefix on its way to the downstream.
//
// A barrier batch carries no events: the submitter drains everything submitted
// before it and then acknowledges the barrier, which lets the read loop run a
// DDL flush while the resolve path is quiesced.
//
// A batch with appliedUpTo set carries no events either: it means every batch
// submitted before it is applied, so the read loop may commit the resolved
// messages whose watermark is at or below appliedUpTo.
type preparedBatch struct {
	batch  *util.ResolveBatch
	events []*event.DMLEvent

	barrier     chan struct{}
	appliedUpTo uint64
}

// pipeline runs the resolve path, restore plus downstream apply, off the read
// loop so that reading and applying overlap.
//
// Ordering rules it must keep:
//   - the read loop owns appends and the published global watermark;
//   - one group has at most one batch in flight (EventsGroup.batchPending), so a
//     group is always applied in commit-ts order;
//   - only the submitter talks to the sink, because AddDMLEvent is not safe for
//     concurrent callers;
//   - a resolve never applies an event above the global watermark.
type pipeline struct {
	w *writer

	requests chan struct{}
	batches  chan *preparedBatch
	barriers chan chan struct{}
	resume   chan struct{}

	// appliedWatermarkValue is the highest watermark whose events reached the
	// downstream. The read loop commits a resolved message only after this
	// reaches that message's watermark, because the spill store is temporary: an
	// offset committed before its events are applied would be skipped on restart.
	appliedWatermarkValue atomic.Uint64

	wg sync.WaitGroup

	errMu   sync.Mutex
	failure error

	failed atomic.Bool
}

func newPipeline(w *writer) *pipeline {
	return &pipeline{
		w:        w,
		requests: make(chan struct{}, 1),
		batches:  make(chan *preparedBatch, batchChannelSize),
		barriers: make(chan chan struct{}, 1),
		resume:   make(chan struct{}, 1),
	}
}

// request schedules a resolve pass. It never blocks the read loop, and repeated
// requests coalesce into one pass.
func (p *pipeline) request() {
	select {
	case p.requests <- struct{}{}:
	default:
	}
}

// fail records the first resolve failure so the read loop can stop the consumer.
func (p *pipeline) fail(err error) {
	p.errMu.Lock()
	if p.failure == nil {
		p.failure = err
		log.Error("resolve pipeline failed, stop the consumer", zap.Error(err))
	}
	p.errMu.Unlock()
	p.failed.Store(true)
	p.request()
}

// err returns the first resolve failure, if any.
func (p *pipeline) err() error {
	if !p.failed.Load() {
		return nil
	}
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.failure
}

// appliedWatermark is the watermark whose events are known to be applied.
func (p *pipeline) appliedWatermark() uint64 {
	return p.appliedWatermarkValue.Load()
}

func (p *pipeline) run(ctx context.Context) {
	p.wg.Add(2)
	go func() { defer p.wg.Done(); p.resolveLoop(ctx) }()
	go func() { defer p.wg.Done(); p.submitLoop(ctx) }()
}

// stop waits until both pipeline goroutines returned. It must only be called
// after the context they were started with is cancelled, and it lets the caller
// release the spill store without racing an in-flight resolve.
func (p *pipeline) stop() {
	p.wg.Wait()
}

// pause quiesces the resolve path: the resolver stops submitting, the submitter
// applies and acknowledges everything already submitted, and neither of them
// touches the sink until the returned function is called. The caller may then
// use the sink itself.
func (p *pipeline) pause(ctx context.Context) func() {
	barrier := make(chan struct{})
	select {
	case p.barriers <- barrier:
	case <-ctx.Done():
		return func() {}
	}
	select {
	case <-barrier:
	case <-ctx.Done():
		return func() {}
	}
	return func() {
		select {
		case p.resume <- struct{}{}:
		default:
		}
	}
}

// resolveLoop restores the commit-ts ordered prefix of every group that is at or
// below the current global watermark.
func (p *pipeline) resolveLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case barrier := <-p.barriers:
			// The barrier travels on the batch channel, so the submitter
			// acknowledges it after everything submitted before it.
			select {
			case p.batches <- &preparedBatch{barrier: barrier}:
			case <-ctx.Done():
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-p.resume:
			}
			continue
		case <-p.requests:
		}
		if !p.resolveOnce(ctx) {
			return
		}
	}
}

// resolveOnce runs one resolve pass and reports whether it made progress, in
// which case another pass is scheduled immediately.
func (p *pipeline) resolveOnce(ctx context.Context) bool {
	watermark := p.w.publishedWatermark()
	limit := p.w.getSpillStore().ResolveLimit()
	more := false
	for _, group := range p.w.snapshotEventsGroups() {
		if ctx.Err() != nil {
			return false
		}
		if group.HasPendingBatch() {
			continue
		}
		restoreStart := time.Now()
		batch, hasMore, err := group.PrepareResolve(watermark, limit)
		p.w.stats.restoreNanos.Add(int64(time.Since(restoreStart)))
		if err != nil {
			p.fail(err)
			return false
		}
		if batch == nil {
			continue
		}
		more = more || hasMore
		select {
		case p.batches <- &preparedBatch{
			batch:  batch,
			events: util.DMLMessagesToEvents(batch.Messages),
		}:
		case <-ctx.Done():
			return false
		}
	}
	if more {
		p.request()
		return true
	}
	// Every group is drained up to watermark, and every batch of this pass was
	// submitted before this item, so once the submitter applied it the read loop
	// may commit the resolved messages up to that watermark.
	if watermark != 0 {
		select {
		case p.batches <- &preparedBatch{appliedUpTo: watermark}:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// submitLoop is the only goroutine that feeds the sink, so downstream apply
// order per group is the order in which batches are submitted.
func (p *pipeline) submitLoop(ctx context.Context) {
	var (
		prepared []*preparedBatch
		events   []*event.DMLEvent
	)
	flush := func() error {
		if len(events) == 0 {
			return nil
		}
		applyStart := time.Now()
		err := p.w.flushDMLBatch(ctx, events)
		p.w.stats.applyWaitNanos.Add(int64(time.Since(applyStart)))
		if err != nil {
			return err
		}
		for _, item := range prepared {
			if err := item.batch.Ack(); err != nil {
				return err
			}
		}
		for _, item := range prepared {
			p.w.stats.flushedMessages.Add(int64(len(item.batch.Messages)))
		}
		prepared = prepared[:0]
		events = events[:0]
		// A group whose batch was just acknowledged may hold events that arrived
		// while that batch was in flight, so ask for another resolve pass instead
		// of waiting for the next resolved message of the topic.
		p.request()
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-p.batches:
			if item.barrier != nil {
				if err := flush(); err != nil {
					p.fail(err)
					return
				}
				close(item.barrier)
				select {
				case <-ctx.Done():
					return
				case <-p.resume:
				}
				continue
			}
			if item.appliedUpTo != 0 {
				if err := flush(); err != nil {
					p.fail(err)
					return
				}
				p.appliedWatermarkValue.Store(item.appliedUpTo)
				continue
			}
			prepared = append(prepared, item)
			events = append(events, item.events...)
			if len(events) >= sinkBatchMessages {
				if err := flush(); err != nil {
					p.fail(err)
					return
				}
			}
		}
	}
}
