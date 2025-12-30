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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type regionEvent struct {
	states []*regionFeedState

	// only one of the following fields is set
	entries    *cdcpb.Event_Entries_
	resolvedTs uint64
}

type regionEventProcessor struct {
	workerCount uint64
	queueSize   int

	workerChans []chan regionEvent
	wg          sync.WaitGroup
	closed      atomic.Bool

	subClient *subscriptionClient
}

func newRegionEventProcessor(workerCount int, queueSize int, subClient *subscriptionClient) *regionEventProcessor {
	if workerCount <= 0 {
		workerCount = 1
	}
	if queueSize <= 0 {
		queueSize = 1024
	}
	p := &regionEventProcessor{
		workerCount: uint64(workerCount),
		queueSize:   queueSize,
		workerChans: make([]chan regionEvent, workerCount),
		subClient:   subClient,
	}
	for i := 0; i < workerCount; i++ {
		ch := make(chan regionEvent, queueSize)
		p.workerChans[i] = ch
		p.wg.Add(1)
		go func(ch <-chan regionEvent) {
			defer p.wg.Done()
			p.run(ch)
		}(ch)
	}
	return p
}

func (p *regionEventProcessor) close() {
	if p == nil || !p.closed.CompareAndSwap(false, true) {
		return
	}
	for _, ch := range p.workerChans {
		close(ch)
	}
	p.wg.Wait()
}

func (p *regionEventProcessor) dispatch(event regionEvent) {
	if p == nil || p.closed.Load() || len(event.states) == 0 || event.states[0] == nil {
		return
	}
	if len(event.states) != 1 {
		log.Panic("should not happen: dispatch multi-region event", zap.Any("event", event))
	}
	regionID := event.states[0].getRegionID()
	idx := regionID % p.workerCount
	p.workerChans[idx] <- event
}

func (p *regionEventProcessor) dispatchResolvedTsBatch(resolvedTs uint64, states []*regionFeedState) {
	if p == nil || p.closed.Load() || resolvedTs == 0 || len(states) == 0 {
		return
	}
	if p.workerCount == 1 {
		p.workerChans[0] <- regionEvent{
			resolvedTs: resolvedTs,
			states:     states,
		}
		return
	}

	shards := make([][]*regionFeedState, p.workerCount)
	for _, state := range states {
		idx := state.getRegionID() % p.workerCount
		shards[idx] = append(shards[idx], state)
	}
	for i, shardStates := range shards {
		if len(shardStates) == 0 {
			continue
		}
		p.workerChans[i] <- regionEvent{
			resolvedTs: resolvedTs,
			states:     shardStates,
		}
	}
}

func (p *regionEventProcessor) run(ch <-chan regionEvent) {
	for event := range ch {
		if len(event.states) != 0 && event.resolvedTs != 0 && event.entries == nil && len(event.states) > 1 {
			span, triggerRegionID := p.handleResolvedTsBatch(event.resolvedTs, event.states)
			if span == nil {
				continue
			}
			if ts := maybeAdvanceSpanResolvedTs(span, triggerRegionID); ts != 0 {
				p.subClient.pushSubscriptionEventToDS(subscriptionEvent{
					subID:      span.subID,
					resolvedTs: ts,
				})
			}
			continue
		}

		if len(event.states) == 1 && event.states[0] != nil {
			state := event.states[0]
			if state.isStale() {
				p.handleRegionError(state)
				continue
			}
			span := state.region.subscribedSpan

			switch {
			case event.entries != nil:
				batch := make([]common.RawKVEntry, 0, len(event.entries.Entries.GetEntries()))
				batch = appendKVEntriesFromRegionEntries(batch, span, state, event.entries)
				if len(batch) == 0 {
					continue
				}
				p.subClient.pushSubscriptionEventToDS(subscriptionEvent{
					subID:     span.subID,
					kvEntries: batch,
				})
			case event.resolvedTs != 0:
				updateRegionResolvedTs(span, state, event.resolvedTs)
				if ts := maybeAdvanceSpanResolvedTs(span, state.getRegionID()); ts != 0 {
					p.subClient.pushSubscriptionEventToDS(subscriptionEvent{
						subID:      span.subID,
						resolvedTs: ts,
					})
				}
			default:
				log.Panic("unknown region event", zap.Any("event", event))
			}
			continue
		}

		log.Panic("unknown region event", zap.Any("event", event))
	}
}

func (p *regionEventProcessor) handleRegionError(state *regionFeedState) {
	stepsToRemoved := state.markRemoved()
	err := state.takeError()
	if err != nil {
		log.Debug("region event processor get a region error",
			zap.Uint64("workerID", state.worker.workerID),
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", state.region.verID.GetID()),
			zap.Bool("reschedule", stepsToRemoved),
			zap.Error(err))
	}
	if stepsToRemoved {
		state.worker.takeRegionState(SubscriptionID(state.requestID), state.getRegionID())
		p.subClient.onRegionFail(newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

func (p *regionEventProcessor) handleResolvedTsBatch(resolvedTs uint64, states []*regionFeedState) (*subscribedSpan, uint64) {
	if resolvedTs == 0 || len(states) == 0 {
		return nil, 0
	}

	var span *subscribedSpan
	var triggerRegionID uint64
	for _, state := range states {
		if state.isStale() || !state.isInitialized() {
			continue
		}
		if span == nil {
			span = state.region.subscribedSpan
			triggerRegionID = state.getRegionID()
		}
		updateRegionResolvedTs(span, state, resolvedTs)
	}
	return span, triggerRegionID
}

func appendKVEntriesFromRegionEntries(
	dst []common.RawKVEntry,
	span *subscribedSpan,
	state *regionFeedState,
	entries *cdcpb.Event_Entries_,
) []common.RawKVEntry {
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
			cached := state.matcher.matchCachedRow(true)
			dst = slices.Grow(dst, len(cached))
			for _, cachedEvent := range cached {
				dst = append(dst, assembleRowEvent(regionID, cachedEvent))
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
					zap.String("key", spanz.HexKey(entry.GetKey())))
			}
			dst = append(dst, assembleRowEvent(regionID, entry))
		case cdcpb.Event_PREWRITE:
			state.matcher.putPrewriteRow(entry)
		case cdcpb.Event_COMMIT:
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
					zap.String("key", spanz.HexKey(entry.GetKey())))
			}

			isStaleEvent := entry.CommitTs <= span.startTs
			if isStaleEvent {
				continue
			}

			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Fatal("The CommitTs must be greater than the resolvedTs",
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.String("key", spanz.HexKey(entry.GetKey())))
			}
			dst = append(dst, assembleRowEvent(regionID, entry))
		case cdcpb.Event_ROLLBACK:
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
	return dst
}

func updateRegionResolvedTs(span *subscribedSpan, state *regionFeedState, resolvedTs uint64) {
	if state.isStale() || !state.isInitialized() {
		return
	}
	state.matcher.tryCleanUnmatchedValue()
	regionID := state.getRegionID()
	lastResolvedTs := state.getLastResolvedTs()
	if resolvedTs < lastResolvedTs {
		log.Info("The resolvedTs is fallen back in subscription client",
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", regionID),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("lastResolvedTs", lastResolvedTs))
		return
	}

	state.updateResolvedTs(resolvedTs)
	span.rangeLock.UpdateLockedRangeStateHeap(state.region.lockedRangeState)
}

func maybeAdvanceSpanResolvedTs(span *subscribedSpan, triggerRegionID uint64) uint64 {
	now := time.Now().UnixMilli()
	lastAdvance := span.lastAdvanceTime.Load()
	if (span.advanceInterval != 0 && now-lastAdvance < span.advanceInterval) || !span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		return 0
	}

	ts := span.rangeLock.GetHeapMinTs()
	if ts == 0 {
		return 0
	}

	if span.initialized.CompareAndSwap(false, true) {
		log.Info("subscription client is initialized",
			zap.Uint64("subscriptionID", uint64(span.subID)),
			zap.Uint64("regionID", triggerRegionID),
			zap.Uint64("resolvedTs", ts))
	}

	lastResolvedTs := span.resolvedTs.Load()
	if ts <= lastResolvedTs && !(ts == lastResolvedTs && lastResolvedTs == span.startTs) {
		return 0
	}

	nextResolvedPhyTs := oracle.ExtractPhysical(ts)
	resolvedPhyTs := oracle.ExtractPhysical(lastResolvedTs)
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

	span.resolvedTs.Store(ts)
	span.resolvedTsUpdated.Store(time.Now().Unix())
	return ts
}
