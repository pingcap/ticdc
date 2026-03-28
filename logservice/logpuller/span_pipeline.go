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

package logpuller

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type pipelineMsgType uint8

const (
	pipelineMsgRegister pipelineMsgType = iota + 1
	pipelineMsgUnregister
	pipelineMsgData
	pipelineMsgResolved
	pipelineMsgPersisted
)

type pipelineMsg struct {
	subID   SubscriptionID
	msgType pipelineMsgType

	span *subscribedSpan

	kvs []common.RawKVEntry
	ts  uint64

	// For Persisted callback.
	dataSeq uint64

	// quotaWeight is the amount acquired from the global quota by this Data batch.
	quotaWeight int64
}

type spanPipelineManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	workerCount uint64
	workers     []spanPipelineWorker

	quotaLimitBytes int64
	quota           *semaphore.Weighted

	inflightBytes   atomic.Int64
	inflightBatches atomic.Int64
	pendingResolved atomic.Int64
	activeSubs      atomic.Int64

	closed atomic.Bool
	wg     sync.WaitGroup
}

func newSpanPipelineManager(
	ctx context.Context,
	workerCount int,
	queueSize int,
	quotaLimitBytes int64,
) *spanPipelineManager {
	if workerCount <= 0 {
		workerCount = runtime.GOMAXPROCS(0)
		if workerCount <= 0 {
			workerCount = 1
		}
	}
	if queueSize <= 0 {
		queueSize = 409600
	}
	if quotaLimitBytes <= 0 {
		quotaLimitBytes = 20 << 30 // 1GiB
	}

	log.Info("span pipeline manager started",
		zap.Int("workerCount", workerCount),
		zap.Int("queueSize", queueSize),
		zap.Int64("quotaLimitBytes", quotaLimitBytes))
	mgr := &spanPipelineManager{
		workerCount:     uint64(workerCount),
		workers:         make([]spanPipelineWorker, workerCount),
		quotaLimitBytes: quotaLimitBytes,
		quota:           semaphore.NewWeighted(quotaLimitBytes),
	}
	mgr.ctx, mgr.cancel = context.WithCancel(ctx)

	for i := 0; i < workerCount; i++ {
		w := spanPipelineWorker{
			ch:     make(chan pipelineMsg, queueSize),
			states: make(map[SubscriptionID]*spanPipelineState),
			mgr:    mgr,
		}
		mgr.workers[i] = w
		mgr.wg.Add(1)
		go func(w *spanPipelineWorker) {
			defer mgr.wg.Done()
			w.run(mgr.ctx)
		}(&mgr.workers[i])
	}
	return mgr
}

func (m *spanPipelineManager) Close() {
	if m == nil || !m.closed.CompareAndSwap(false, true) {
		return
	}
	m.cancel()
	for i := range m.workers {
		close(m.workers[i].ch)
	}
	m.wg.Wait()
}

func (m *spanPipelineManager) shard(subID SubscriptionID) *spanPipelineWorker {
	idx := uint64(subID) % m.workerCount
	return &m.workers[idx]
}

func (m *spanPipelineManager) enqueue(msg pipelineMsg) bool {
	if m == nil || m.closed.Load() {
		return false
	}
	w := m.shard(msg.subID)
	select {
	case w.ch <- msg:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func (m *spanPipelineManager) Register(span *subscribedSpan) {
	if span == nil {
		return
	}
	_ = m.enqueue(pipelineMsg{
		subID:   span.subID,
		msgType: pipelineMsgRegister,
		span:    span,
	})
}

func (m *spanPipelineManager) Unregister(subID SubscriptionID) {
	_ = m.enqueue(pipelineMsg{
		subID:   subID,
		msgType: pipelineMsgUnregister,
	})
}

func (m *spanPipelineManager) EnqueueData(
	ctx context.Context,
	span *subscribedSpan,
	kvs []common.RawKVEntry,
) bool {
	if span == nil || len(kvs) == 0 {
		return true
	}

	weight := rawKVEntriesApproxBytes(kvs)
	if weight <= 0 {
		weight = 1
	}
	if weight > m.quotaLimitBytes {
		weight = m.quotaLimitBytes
	}
	start := time.Now()
	if err := m.quota.Acquire(ctx, weight); err != nil {
		return false
	}
	metrics.LogPullerSpanPipelineQuotaAcquireDuration.Observe(time.Since(start).Seconds())
	m.addInflight(weight)

	if ok := m.enqueue(pipelineMsg{
		subID:       span.subID,
		msgType:     pipelineMsgData,
		span:        span,
		kvs:         kvs,
		quotaWeight: weight,
	}); !ok {
		m.releaseInflight(weight)
		return false
	}
	return true
}

func (m *spanPipelineManager) EnqueueResolved(span *subscribedSpan, ts uint64) bool {
	if span == nil || ts == 0 {
		return true
	}
	return m.enqueue(pipelineMsg{
		subID:   span.subID,
		msgType: pipelineMsgResolved,
		span:    span,
		ts:      ts,
	})
}

func (m *spanPipelineManager) enqueuePersisted(subID SubscriptionID, dataSeq uint64) {
	log.Info("enqueue persisted callback",
		zap.Uint64("dataSeq", dataSeq),
		zap.Uint64("subID", uint64(subID)))
	_ = m.enqueue(pipelineMsg{
		subID:   subID,
		msgType: pipelineMsgPersisted,
		dataSeq: dataSeq,
	})
}

func (m *spanPipelineManager) addInflight(weight int64) {
	if weight <= 0 {
		return
	}
	metrics.LogPullerSpanPipelineInflightBytes.Set(float64(m.inflightBytes.Add(weight)))
	metrics.LogPullerSpanPipelineInflightBatches.Set(float64(m.inflightBatches.Add(1)))
}

func (m *spanPipelineManager) releaseInflight(weight int64) {
	if weight <= 0 {
		return
	}
	m.quota.Release(weight)
	metrics.LogPullerSpanPipelineInflightBytes.Set(float64(m.inflightBytes.Add(-weight)))
	metrics.LogPullerSpanPipelineInflightBatches.Set(float64(m.inflightBatches.Add(-1)))
}

func (m *spanPipelineManager) incPendingResolved(delta int64) {
	if delta == 0 {
		return
	}
	metrics.LogPullerSpanPipelinePendingResolvedBarriers.Set(float64(m.pendingResolved.Add(delta)))
}

func (m *spanPipelineManager) incActiveSubs(delta int64) {
	if delta == 0 {
		return
	}
	metrics.LogPullerSpanPipelineActiveSubscriptions.Set(float64(m.activeSubs.Add(delta)))
}

func rawKVEntriesApproxBytes(kvs []common.RawKVEntry) int64 {
	size := int64(unsafe.Sizeof(kvs)) + int64(unsafe.Sizeof(common.RawKVEntry{}))*int64(len(kvs))
	for i := range kvs {
		size += int64(len(kvs[i].Key) + len(kvs[i].Value) + len(kvs[i].OldValue))
	}
	return size
}

type spanPipelineWorker struct {
	ch chan pipelineMsg

	states map[SubscriptionID]*spanPipelineState
	mgr    *spanPipelineManager
}

func (w *spanPipelineWorker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-w.ch:
			if !ok {
				return
			}
			w.handleMsg(msg)
		}
	}
}

func (w *spanPipelineWorker) handleMsg(msg pipelineMsg) {
	switch msg.msgType {
	case pipelineMsgRegister:
		state := w.getOrCreateState(msg.subID)
		state.span = msg.span
	case pipelineMsgUnregister:
		if state := w.states[msg.subID]; state != nil {
			w.mgr.incPendingResolved(-int64(len(state.pendingResolved) - state.pendingResolvedHead))
			state.unregistered = true
			state.span = nil
			state.pendingResolved = nil
			state.pendingResolvedHead = 0
			if len(state.inflightWeight) == 0 {
				delete(w.states, msg.subID)
				w.mgr.incActiveSubs(-1)
			}
		}
	case pipelineMsgPersisted:
		state := w.states[msg.subID]
		if state == nil {
			return
		}
		state.onPersisted(w.mgr, msg.dataSeq)
		if state.unregistered && len(state.inflightWeight) == 0 {
			delete(w.states, msg.subID)
			w.mgr.incActiveSubs(-1)
		}
	case pipelineMsgData, pipelineMsgResolved:
		state := w.getOrCreateState(msg.subID)
		if msg.span != nil {
			state.span = msg.span
		}
		state.handleInOrderInput(w.mgr, msg)
	default:
		return
	}
}

func (w *spanPipelineWorker) getOrCreateState(subID SubscriptionID) *spanPipelineState {
	if st := w.states[subID]; st != nil {
		return st
	}
	st := newSpanPipelineState()
	w.states[subID] = st
	w.mgr.incActiveSubs(1)
	return st
}

type resolvedBarrier struct {
	waitSeq uint64
	ts      uint64
}

type spanPipelineState struct {
	span *subscribedSpan

	// Data sequence barrier state.
	nextDataSeq uint64
	ackedSeq    uint64
	doneSet     map[uint64]struct{}

	// seq -> quotaWeight; released when persisted callback arrives (out-of-order allowed).
	inflightWeight map[uint64]int64

	pendingResolved     []resolvedBarrier
	pendingResolvedHead int
	lastAdvancedTs      uint64

	unregistered bool
}

func newSpanPipelineState() *spanPipelineState {
	return &spanPipelineState{
		nextDataSeq:     1,
		doneSet:         make(map[uint64]struct{}, 16),
		inflightWeight:  make(map[uint64]int64, 16),
		pendingResolved: make([]resolvedBarrier, 0, 4),
	}
}

func (s *spanPipelineState) handleInOrderInput(mgr *spanPipelineManager, msg pipelineMsg) {
	switch msg.msgType {
	case pipelineMsgData:
		s.onData(mgr, msg)
	case pipelineMsgResolved:
		s.onResolved(mgr, msg.ts)
		s.flushResolvedIfReady(mgr)
	}
}

func (s *spanPipelineState) onData(mgr *spanPipelineManager, msg pipelineMsg) {
	seq := s.nextDataSeq
	s.nextDataSeq++

	span := s.span
	if span == nil || span.stopped.Load() {
		mgr.releaseInflight(msg.quotaWeight)
		s.onPersisted(mgr, seq)
		return
	}

	s.inflightWeight[seq] = msg.quotaWeight
	metricsEventCount.Add(float64(len(msg.kvs)))

	await := span.consumeKVEvents(msg.kvs, func() {
		mgr.enqueuePersisted(span.subID, seq)
	})
	if !await {
		mgr.enqueuePersisted(span.subID, seq)
	}
}

func (s *spanPipelineState) onResolved(mgr *spanPipelineManager, ts uint64) {
	if ts == 0 {
		return
	}
	if ts <= s.lastAdvancedTs {
		metrics.LogPullerSpanPipelineResolvedBarrierDroppedCounter.Inc()
		return
	}

	waitSeq := s.nextDataSeq - 1
	if len(s.pendingResolved) == 0 || s.pendingResolved[len(s.pendingResolved)-1].waitSeq != waitSeq {
		// If the previous barrier has the same ts, the new one is redundant because it
		// would advance to the same resolved-ts but requires waiting for more data.
		if len(s.pendingResolved) != 0 && s.pendingResolved[len(s.pendingResolved)-1].ts == ts {
			metrics.LogPullerSpanPipelineResolvedBarrierDroppedCounter.Inc()
			return
		}
		s.pendingResolved = append(s.pendingResolved, resolvedBarrier{
			waitSeq: waitSeq,
			ts:      ts,
		})
		mgr.incPendingResolved(1)
		return
	}
	last := &s.pendingResolved[len(s.pendingResolved)-1]
	if ts > last.ts {
		last.ts = ts
	}
}

func (s *spanPipelineState) onPersisted(mgr *spanPipelineManager, seq uint64) {
	if weight, ok := s.inflightWeight[seq]; ok {
		delete(s.inflightWeight, seq)
		mgr.releaseInflight(weight)
	}

	if seq <= s.ackedSeq {
		return
	}
	s.doneSet[seq] = struct{}{}
	for {
		next := s.ackedSeq + 1
		if _, ok := s.doneSet[next]; !ok {
			break
		}
		delete(s.doneSet, next)
		s.ackedSeq++
	}
	s.flushResolvedIfReady(mgr)
}

func (s *spanPipelineState) flushResolvedIfReady(mgr *spanPipelineManager) {
	for s.pendingResolvedHead < len(s.pendingResolved) {
		barrier := s.pendingResolved[s.pendingResolvedHead]
		if s.ackedSeq < barrier.waitSeq {
			s.compactPendingResolvedIfNeeded()
			return
		}
		log.Info("advance resolved ts",
			zap.Uint64("subID", uint64(s.span.subID)),
			zap.Uint64("ts", barrier.ts),
			zap.Uint64("ackedSeq", s.ackedSeq),
			zap.Uint64("waitSeq", barrier.waitSeq))

		if s.span != nil && !s.span.stopped.Load() {
			s.span.advanceResolvedTs(barrier.ts)
		}
		if barrier.ts > s.lastAdvancedTs {
			s.lastAdvancedTs = barrier.ts
		}
		mgr.incPendingResolved(-1)
		s.pendingResolvedHead++
	}

	if s.pendingResolvedHead == len(s.pendingResolved) {
		s.pendingResolved = s.pendingResolved[:0]
		s.pendingResolvedHead = 0
		return
	}
	s.compactPendingResolvedIfNeeded()
}

func (s *spanPipelineState) compactPendingResolvedIfNeeded() {
	// If resolved keeps coming while some data is still in-flight, it is normal that
	// `pendingResolvedHead` never reaches `len(pendingResolved)`. In that case we still
	// want to avoid retaining the already-flushed prefix in the backing array.
	if s.pendingResolvedHead == 0 {
		return
	}
	const minHeadToCompact = 1024
	if s.pendingResolvedHead < minHeadToCompact && s.pendingResolvedHead < len(s.pendingResolved)/2 {
		return
	}
	metrics.LogPullerSpanPipelineResolvedBarrierCompactionCounter.Inc()
	s.pendingResolved = append([]resolvedBarrier(nil), s.pendingResolved[s.pendingResolvedHead:]...)
	s.pendingResolvedHead = 0
}
