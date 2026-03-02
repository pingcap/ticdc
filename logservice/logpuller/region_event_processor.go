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

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	defaultRegionEventProcessorQueueSize = 4096
)

// regionEventProcessor serializes per-region event handling by sharding events by regionID.
// It transforms region-level events into:
// - KV batches: EnqueueData into span pipeline
// - span-level resolved-ts: EnqueueResolved into span pipeline
//
// All region states for a regionID are always mutated by the same worker goroutine.
type regionEventProcessor struct {
	ctx       context.Context
	subClient *subscriptionClient
	pipeline  *spanPipelineManager

	workers []*regionEventWorker
}

type regionEventWorker struct {
	ch        chan regionEvent
	processor *regionEventProcessor
}

func newRegionEventProcessor(
	ctx context.Context,
	subClient *subscriptionClient,
	pipeline *spanPipelineManager,
	workerCount int,
	queueSize int,
) *regionEventProcessor {
	if workerCount <= 0 {
		workerCount = runtime.GOMAXPROCS(0)
		if workerCount <= 0 {
			workerCount = 1
		}
	}
	if queueSize <= 0 {
		queueSize = defaultRegionEventProcessorQueueSize
	}

	p := &regionEventProcessor{
		ctx:       ctx,
		subClient: subClient,
		pipeline:  pipeline,
		workers:   make([]*regionEventWorker, 0, workerCount),
	}
	for i := 0; i < workerCount; i++ {
		w := &regionEventWorker{
			ch:        make(chan regionEvent, queueSize),
			processor: p,
		}
		p.workers = append(p.workers, w)
		go w.run()
	}
	return p
}

func (p *regionEventProcessor) dispatch(event regionEvent) {
	if len(event.states) == 0 {
		log.Panic("region event has empty states", zap.Any("event", event))
	}
	if len(p.workers) == 0 {
		return
	}

	// Batch resolved-ts event: shard state pointers by regionID.
	if event.entries == nil && event.resolvedTs != 0 && len(event.states) > 1 {
		statesByWorker := make([][]*regionFeedState, len(p.workers))
		for _, state := range event.states {
			if state == nil {
				continue
			}
			idx := p.workerIndex(state.getRegionID())
			statesByWorker[idx] = append(statesByWorker[idx], state)
		}
		for idx, states := range statesByWorker {
			if len(states) == 0 {
				continue
			}
			p.enqueue(idx, regionEvent{states: states, resolvedTs: event.resolvedTs})
		}
		return
	}

	firstState := event.mustFirstState()
	idx := p.workerIndex(firstState.getRegionID())
	p.enqueue(idx, event)
}

func (p *regionEventProcessor) workerIndex(regionID uint64) int {
	return int(regionID % uint64(len(p.workers)))
}

func (p *regionEventProcessor) enqueue(workerIdx int, event regionEvent) {
	w := p.workers[workerIdx]
	select {
	case <-p.ctx.Done():
		return
	case w.ch <- event:
		return
	}
}

func (w *regionEventWorker) run() {
	for {
		select {
		case <-w.processor.ctx.Done():
			return
		case event := <-w.ch:
			w.handle(event)
		}
	}
}

func (w *regionEventWorker) handle(event regionEvent) {
	startTime := time.Now()
	eventType := "error"
	defer func() {
		metrics.SubscriptionClientRegionEventHandleDuration.WithLabelValues(eventType).
			Observe(time.Since(startTime).Seconds())
	}()

	if len(event.states) == 1 && event.states[0] != nil && event.states[0].isStale() {
		eventType = "error"
		w.handleRegionError(event.states[0])
		return
	}

	if event.entries != nil {
		eventType = "entries"
		state := event.mustFirstState()
		span := state.region.subscribedSpan
		if span == nil {
			return
		}
		kvs := appendKVEntriesFromRegionEntries(span, state, event.entries, nil)
		if len(kvs) == 0 {
			return
		}
		metricsEventCount.Add(float64(len(kvs)))
		w.processor.pipeline.EnqueueData(w.processor.ctx, span, kvs)
		return
	}

	if event.resolvedTs != 0 {
		eventType = "resolved"
		firstState := event.mustFirstState()
		span := firstState.region.subscribedSpan
		if span == nil {
			return
		}

		updated := false
		for _, state := range event.states {
			if updateRegionResolvedTs(span, state, event.resolvedTs) {
				updated = true
			}
		}
		if !updated {
			return
		}

		if ts := maybeAdvanceSpanResolvedTs(span, firstState.getRegionID()); ts != 0 {
			w.processor.pipeline.EnqueueResolved(span, ts)
		}
		return
	}

	log.Panic("unknown region event type", zap.Any("event", event))
}

func (w *regionEventWorker) handleRegionError(state *regionFeedState) {
	stepsToRemoved := state.markRemoved()
	err := state.takeError()
	worker := state.worker
	if err != nil {
		log.Debug("region event processor get a region error",
			zap.Uint64("workerID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", state.region.verID.GetID()),
			zap.Bool("reschedule", stepsToRemoved),
			zap.Error(err))
	}
	if stepsToRemoved {
		worker.takeRegionState(SubscriptionID(state.requestID), state.getRegionID())
		w.processor.subClient.onRegionFail(newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

func appendKVEntriesFromRegionEntries(
	span *subscribedSpan,
	state *regionFeedState,
	entries *cdcpb.Event_Entries_,
	kvs []common.RawKVEntry,
) []common.RawKVEntry {
	if entries == nil || entries.Entries == nil {
		return kvs
	}

	regionID, _, _ := state.getRegionMeta()
	assembleRowEvent := func(regionID uint64, entry *cdcpb.Event_Row) common.RawKVEntry {
		var opType common.OpType
		switch entry.GetOpType() {
		case cdcpb.Event_Row_DELETE:
			opType = common.OpTypeDelete
		case cdcpb.Event_Row_PUT:
			opType = common.OpTypePut
		default:
			log.Panic("meet unknown op type", zap.Any("entry", entry))
		}
		return common.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
			OldValue: entry.GetOldValue(),
		}
	}

	for _, entry := range entries.Entries.GetEntries() {
		switch entry.Type {
		case cdcpb.Event_INITIALIZED:
			state.setInitialized()
			log.Debug("region is initialized",
				zap.Int64("tableID", span.span.TableID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", state.requestID),
				zap.String("startKey", spanz.HexKey(span.span.StartKey)),
				zap.String("endKey", spanz.HexKey(span.span.EndKey)))
			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				kvs = append(kvs, assembleRowEvent(regionID, cachedEvent))
			}
			state.matcher.matchCachedRollbackRow(true)
		case cdcpb.Event_COMMITTED:
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Fatal("The CommitTs must be greater than the resolvedTs",
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("EventType", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.String("key", util.RedactKey(entry.GetKey())))
			}
			kvs = append(kvs, assembleRowEvent(regionID, entry))
		case cdcpb.Event_PREWRITE:
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
			// NOTE: matchRow should always be called even if the event is stale.
			if !state.matcher.matchRow(entry, state.isInitialized()) {
				if !state.isInitialized() {
					state.matcher.cacheCommitRow(entry)
					continue
				}
				log.Fatal("prewrite not match",
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", state.getRegionID()),
					zap.Uint64("requestID", state.requestID),
					zap.Uint64("startTs", entry.GetStartTs()),
					zap.Uint64("commitTs", entry.GetCommitTs()),
					zap.String("key", util.RedactKey(entry.GetKey())))
			}

			// TiKV can send events with StartTs/CommitTs less than startTs.
			isStaleEvent := entry.CommitTs <= span.startTs
			if isStaleEvent {
				continue
			}

			// NOTE: state.getLastResolvedTs() will never less than startTs.
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Fatal("The CommitTs must be greater than the resolvedTs",
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.String("key", util.RedactKey(entry.GetKey())))
			}
			kvs = append(kvs, assembleRowEvent(regionID, entry))
		case cdcpb.Event_ROLLBACK:
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}

	return kvs
}

func updateRegionResolvedTs(span *subscribedSpan, state *regionFeedState, resolvedTs uint64) bool {
	if span == nil || state == nil {
		return false
	}
	if state.isStale() || !state.isInitialized() {
		return false
	}
	state.matcher.tryCleanUnmatchedValue()
	regionID := state.getRegionID()
	lastResolvedTs := state.getLastResolvedTs()
	if resolvedTs < lastResolvedTs {
		log.Debug("The resolvedTs is fallen back in subscription client",
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", regionID),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("lastResolvedTs", lastResolvedTs))
		return false
	}

	state.updateResolvedTs(resolvedTs)
	if span.advanceInterval == 0 {
		span.rangeLock.UpdateLockedRangeStateHeap(state.region.lockedRangeState)
	}
	return true
}

func maybeAdvanceSpanResolvedTs(span *subscribedSpan, triggerRegionID uint64) uint64 {
	if span == nil || span.rangeLock == nil {
		return 0
	}

	ts := uint64(0)
	shouldAdvance := false

	// advanceInterval defaults to 100ms; setting it to 0 means resolving the timestamp as soon as possible.
	// Note: If a single span contains an extremely large number of regions (e.g., 500k), advanceInterval = 0 may cause performance issues.
	if span.advanceInterval == 0 {
		ts = span.rangeLock.GetHeapMinTs()
		shouldAdvance = true
	} else {
		now := time.Now().UnixMilli()
		lastAdvance := span.lastAdvanceTime.Load()
		if now-lastAdvance >= span.advanceInterval && span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
			ts = span.rangeLock.ResolvedTs()
			shouldAdvance = true
		}
	}

	if !shouldAdvance {
		return 0
	}

	if ts > 0 && span.initialized.CompareAndSwap(false, true) {
		log.Info("subscription client is initialized",
			zap.Uint64("subscriptionID", uint64(span.subID)),
			zap.Uint64("regionID", triggerRegionID),
			zap.Uint64("resolvedTs", ts))
	}

	for {
		lastResolvedTs := span.resolvedTs.Load()
		if ts < lastResolvedTs {
			return 0
		}
		// Generally, we don't want to send duplicate resolved ts.
		// But when ts == lastResolvedTs == startTs, the span may just be initialized and have not
		// received any resolved ts before, so we also send it for quick notification to downstream.
		if ts == lastResolvedTs {
			if lastResolvedTs == span.startTs {
				span.resolvedTsUpdated.Store(time.Now().Unix())
				return ts
			}
			return 0
		}

		// ts > lastResolvedTs
		resolvedPhyTs := oracle.ExtractPhysical(lastResolvedTs)
		nextResolvedPhyTs := oracle.ExtractPhysical(ts)
		decreaseLag := float64(nextResolvedPhyTs-resolvedPhyTs) / 1e3
		const largeResolvedTsAdvanceStepInSecs = 30
		if decreaseLag > largeResolvedTsAdvanceStepInSecs {
			log.Warn("resolved ts advance step is too large",
				zap.Uint64("subID", uint64(span.subID)),
				zap.Int64("tableID", span.span.TableID),
				zap.Uint64("regionID", triggerRegionID),
				zap.Uint64("resolvedTs", ts),
				zap.Uint64("lastResolvedTs", lastResolvedTs),
				zap.Float64("decreaseLag(s)", decreaseLag))
		}

		if span.resolvedTs.CompareAndSwap(lastResolvedTs, ts) {
			span.resolvedTsUpdated.Store(time.Now().Unix())
			return ts
		}
	}
}

func handleResolvedTs(span *subscribedSpan, state *regionFeedState, resolvedTs uint64) uint64 {
	if !updateRegionResolvedTs(span, state, resolvedTs) {
		return 0
	}
	return maybeAdvanceSpanResolvedTs(span, state.getRegionID())
}
