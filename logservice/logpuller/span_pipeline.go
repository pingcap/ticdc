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

package logpuller

import (
	"context"
	"runtime"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

const (
	defaultSpanPipelineQuotaBytes = int64(1 << 30) // 1GiB
	defaultSpanPipelineQueueSize  = 4096
)

// spanPipelineManager is a delivery layer between log puller and EventStore.
// For each subscription span (subID), it enforces a strict resolved-ts barrier:
// a resolved-ts is advanced only after all earlier data batches (in pipeline receive order)
// have been persisted.
//
// Concurrency model:
// - Events are routed by subID to a single worker goroutine.
// - Each worker maintains per-subID state. No locks are required for per-subID state.
// - A global semaphore quota bounds in-flight data bytes.
//
// See docs/design/2026-02-23-logpuller-span-pipeline.md for details.
type spanPipelineManager struct {
	ctx   context.Context
	quota *semaphore.Weighted

	workers []*spanPipelineWorker
}

type spanPipelineWorker struct {
	ch     chan spanPipelineEvent
	states map[SubscriptionID]*spanPipelineState
	mgr    *spanPipelineManager
}

type spanPipelineState struct {
	span        *subscribedSpan
	nextDataSeq uint64
	ackedSeq    uint64

	doneSet map[uint64]struct{}

	pendingResolved     []resolvedBarrier
	pendingResolvedHead int
}

type resolvedBarrier struct {
	waitSeq uint64
	ts      uint64
}

type spanPipelineEventType uint8

const (
	spanPipelineEventRegister spanPipelineEventType = iota
	spanPipelineEventUnregister
	spanPipelineEventData
	spanPipelineEventResolved
	spanPipelineEventPersisted
)

type spanPipelineEvent struct {
	subID SubscriptionID
	typ   spanPipelineEventType

	// register
	span   *subscribedSpan
	doneCh chan struct{}

	// data
	kvs    []common.RawKVEntry
	weight int64

	// resolved
	resolvedTs uint64

	// persisted
	seq uint64
}

func newSpanPipelineManager(
	ctx context.Context, workerCount int, queueSize int, quotaBytes int64,
) *spanPipelineManager {
	if workerCount <= 0 {
		workerCount = runtime.GOMAXPROCS(0)
		if workerCount <= 0 {
			workerCount = 1
		}
	}
	if queueSize <= 0 {
		queueSize = defaultSpanPipelineQueueSize
	}
	if quotaBytes <= 0 {
		quotaBytes = defaultSpanPipelineQuotaBytes
	}

	mgr := &spanPipelineManager{
		ctx:     ctx,
		quota:   semaphore.NewWeighted(quotaBytes),
		workers: make([]*spanPipelineWorker, 0, workerCount),
	}
	for i := 0; i < workerCount; i++ {
		w := &spanPipelineWorker{
			ch:     make(chan spanPipelineEvent, queueSize),
			states: make(map[SubscriptionID]*spanPipelineState),
			mgr:    mgr,
		}
		mgr.workers = append(mgr.workers, w)
		go w.run()
	}
	return mgr
}

func (m *spanPipelineManager) Register(span *subscribedSpan) {
	if span == nil {
		return
	}
	doneCh := make(chan struct{})
	ev := spanPipelineEvent{
		subID:  span.subID,
		typ:    spanPipelineEventRegister,
		span:   span,
		doneCh: doneCh,
	}
	if !m.enqueue(span.subID, ev) {
		return
	}
	select {
	case <-doneCh:
	case <-m.ctx.Done():
	}
}

func (m *spanPipelineManager) Unregister(subID SubscriptionID) {
	doneCh := make(chan struct{})
	ev := spanPipelineEvent{subID: subID, typ: spanPipelineEventUnregister, doneCh: doneCh}
	if !m.enqueue(subID, ev) {
		return
	}
	select {
	case <-doneCh:
	case <-m.ctx.Done():
	}
}

func (m *spanPipelineManager) EnqueueData(
	ctx context.Context, span *subscribedSpan, kvs []common.RawKVEntry,
) {
	if span == nil || len(kvs) == 0 {
		return
	}
	weight := approximateRawKVEntriesSize(kvs)
	start := time.Now()
	err := m.quota.Acquire(ctx, weight)
	metrics.LogPullerSpanPipelineQuotaAcquireDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		return
	}
	metrics.LogPullerSpanPipelineInflightBytes.Add(float64(weight))
	metrics.LogPullerSpanPipelineInflightBatches.Inc()

	if !m.enqueue(span.subID, spanPipelineEvent{
		subID:  span.subID,
		typ:    spanPipelineEventData,
		kvs:    kvs,
		weight: weight,
	}) {
		m.releaseQuota(weight)
	}
}

func (m *spanPipelineManager) EnqueueResolved(span *subscribedSpan, ts uint64) {
	if span == nil || ts == 0 {
		return
	}
	_ = m.enqueue(span.subID, spanPipelineEvent{
		subID:      span.subID,
		typ:        spanPipelineEventResolved,
		resolvedTs: ts,
	})
}

func (m *spanPipelineManager) enqueuePersisted(subID SubscriptionID, seq uint64, weight int64) {
	_ = m.enqueue(subID, spanPipelineEvent{
		subID:  subID,
		typ:    spanPipelineEventPersisted,
		seq:    seq,
		weight: weight,
	})
}

func (m *spanPipelineManager) enqueue(subID SubscriptionID, ev spanPipelineEvent) bool {
	if len(m.workers) == 0 {
		return false
	}
	idx := int(uint64(subID) % uint64(len(m.workers)))
	w := m.workers[idx]
	select {
	case <-m.ctx.Done():
		return false
	case w.ch <- ev:
		return true
	}
}

func (m *spanPipelineManager) releaseQuota(weight int64) {
	if weight <= 0 {
		return
	}
	m.quota.Release(weight)
	metrics.LogPullerSpanPipelineInflightBytes.Sub(float64(weight))
	metrics.LogPullerSpanPipelineInflightBatches.Dec()
}

func approximateRawKVEntriesSize(kvs []common.RawKVEntry) int64 {
	var bytes int64
	for i := range kvs {
		bytes += int64(len(kvs[i].Key) + len(kvs[i].Value) + len(kvs[i].OldValue))
	}
	if bytes <= 0 {
		return 1
	}
	return bytes
}

func (w *spanPipelineWorker) run() {
	for {
		select {
		case <-w.mgr.ctx.Done():
			return
		case ev := <-w.ch:
			w.handleEvent(ev)
		}
	}
}

func (w *spanPipelineWorker) handleEvent(ev spanPipelineEvent) {
	switch ev.typ {
	case spanPipelineEventRegister:
		w.handleRegister(ev)
	case spanPipelineEventUnregister:
		w.handleUnregister(ev)
	case spanPipelineEventData:
		w.handleData(ev)
	case spanPipelineEventResolved:
		w.handleResolved(ev)
	case spanPipelineEventPersisted:
		w.handlePersisted(ev)
	default:
		log.Panic("unknown span pipeline event type",
			zap.Uint64("subID", uint64(ev.subID)),
			zap.Uint8("type", uint8(ev.typ)))
	}
}

func (w *spanPipelineWorker) handleRegister(ev spanPipelineEvent) {
	state := w.states[ev.subID]
	if state == nil {
		state = &spanPipelineState{
			span:        ev.span,
			nextDataSeq: 1,
			ackedSeq:    0,
		}
		w.states[ev.subID] = state
		metrics.LogPullerSpanPipelineActiveSubscriptions.Inc()
	} else {
		state.span = ev.span
	}
	close(ev.doneCh)
}

func (w *spanPipelineWorker) handleUnregister(ev spanPipelineEvent) {
	state := w.states[ev.subID]
	if state != nil {
		pending := len(state.pendingResolved) - state.pendingResolvedHead
		if pending > 0 {
			metrics.LogPullerSpanPipelinePendingResolvedBarriers.Sub(float64(pending))
		}
		delete(w.states, ev.subID)
		metrics.LogPullerSpanPipelineActiveSubscriptions.Dec()
	}
	close(ev.doneCh)
}

func (w *spanPipelineWorker) handleData(ev spanPipelineEvent) {
	state := w.states[ev.subID]
	if state == nil || state.span == nil {
		w.mgr.releaseQuota(ev.weight)
		return
	}

	seq := state.nextDataSeq
	state.nextDataSeq++

	await := state.span.consumeKVEvents(ev.kvs, func() {
		w.mgr.enqueuePersisted(ev.subID, seq, ev.weight)
	})
	if !await {
		w.mgr.releaseQuota(ev.weight)
		w.onPersisted(state, seq)
	}
}

func (w *spanPipelineWorker) handleResolved(ev spanPipelineEvent) {
	state := w.states[ev.subID]
	if state == nil || state.span == nil {
		return
	}
	waitSeq := state.nextDataSeq - 1
	w.addResolvedBarrier(state, waitSeq, ev.resolvedTs)
	w.flushResolvedIfReady(state)
}

func (w *spanPipelineWorker) handlePersisted(ev spanPipelineEvent) {
	w.mgr.releaseQuota(ev.weight)
	state := w.states[ev.subID]
	if state == nil || state.span == nil {
		return
	}
	w.onPersisted(state, ev.seq)
}

func (w *spanPipelineWorker) onPersisted(state *spanPipelineState, seq uint64) {
	if seq <= state.ackedSeq {
		return
	}
	if state.doneSet == nil {
		state.doneSet = make(map[uint64]struct{}, 8)
	}
	state.doneSet[seq] = struct{}{}
	for {
		next := state.ackedSeq + 1
		if _, ok := state.doneSet[next]; !ok {
			break
		}
		delete(state.doneSet, next)
		state.ackedSeq = next
	}
	w.flushResolvedIfReady(state)
}

func (w *spanPipelineWorker) addResolvedBarrier(state *spanPipelineState, waitSeq uint64, ts uint64) {
	if ts == 0 {
		return
	}

	if state.pendingResolvedHead == len(state.pendingResolved) {
		state.pendingResolved = state.pendingResolved[:0]
		state.pendingResolvedHead = 0
	}

	if len(state.pendingResolved) == state.pendingResolvedHead {
		state.pendingResolved = append(state.pendingResolved, resolvedBarrier{waitSeq: waitSeq, ts: ts})
		metrics.LogPullerSpanPipelinePendingResolvedBarriers.Inc()
		return
	}

	lastIdx := len(state.pendingResolved) - 1
	last := &state.pendingResolved[lastIdx]
	if last.waitSeq == waitSeq {
		metrics.LogPullerSpanPipelineResolvedBarrierDropped.Inc()
		if ts > last.ts {
			last.ts = ts
		}
		return
	}
	if last.waitSeq > waitSeq {
		log.Panic("span pipeline resolved waitSeq must be non-decreasing",
			zap.Uint64("subID", uint64(state.span.subID)),
			zap.Uint64("waitSeq", waitSeq),
			zap.Uint64("lastWaitSeq", last.waitSeq))
	}

	state.pendingResolved = append(state.pendingResolved, resolvedBarrier{waitSeq: waitSeq, ts: ts})
	metrics.LogPullerSpanPipelinePendingResolvedBarriers.Inc()
}

func (w *spanPipelineWorker) flushResolvedIfReady(state *spanPipelineState) {
	for state.pendingResolvedHead < len(state.pendingResolved) {
		barrier := state.pendingResolved[state.pendingResolvedHead]
		if state.ackedSeq < barrier.waitSeq {
			break
		}
		state.pendingResolvedHead++
		metrics.LogPullerSpanPipelinePendingResolvedBarriers.Dec()
		state.span.advanceResolvedTs(barrier.ts)
	}

	// Compact the already-flushed prefix periodically to avoid retaining memory.
	const compactThreshold = 1024
	if state.pendingResolvedHead >= compactThreshold &&
		state.pendingResolvedHead*2 >= len(state.pendingResolved) {
		copy(state.pendingResolved, state.pendingResolved[state.pendingResolvedHead:])
		state.pendingResolved = state.pendingResolved[:len(state.pendingResolved)-state.pendingResolvedHead]
		state.pendingResolvedHead = 0
		metrics.LogPullerSpanPipelineResolvedBarrierCompaction.Inc()
	}
}
