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
	"encoding/hex"
	"time"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

var (
	metricsResolvedTsCount = metrics.PullerEventCounter.WithLabelValues("resolved_ts")
	metricsEventCount      = metrics.PullerEventCounter.WithLabelValues("event")
)

const (
	DataGroupEntriesOrResolvedTs = 1
	DataGroupError               = 2
)

type regionEvent struct {
	state  *regionFeedState
	worker *regionRequestWorker // TODO: remove the field

	// only one of the following fields will be set
	entries    *cdcpb.Event_Entries_
	resolvedTs uint64
}

func (event *regionEvent) getSize() int {
	if event == nil {
		return 0
	}
	size := int(unsafe.Sizeof(*event))
	if event.entries != nil {
		size += int(unsafe.Sizeof(*event.entries))
		size += int(unsafe.Sizeof(*event.entries.Entries))
		for _, row := range event.entries.Entries.GetEntries() {
			size += int(unsafe.Sizeof(*row))
			size += len(row.Key)
			size += len(row.Value)
			size += len(row.OldValue)
		}
	}
	return size
}

type regionEventHandler struct {
	subClient *SubscriptionClient
}

func (h *regionEventHandler) Path(event regionEvent) SubscriptionID {
	return SubscriptionID(event.state.requestID)
}

func (h *regionEventHandler) Handle(span *subscribedSpan, events ...regionEvent) bool {
	if len(span.kvEventsCache) != 0 {
		log.Panic("kvEventsCache is not empty",
			zap.Int("kvEventsCacheLen", len(span.kvEventsCache)),
			zap.Uint64("subID", uint64(span.subID)))
	}

	newResolvedTs := uint64(0)
	for _, event := range events {
		if event.state.isStale() {
			h.handleRegionError(event.state, event.worker)
			continue
		}
		if event.entries != nil {
			handleEventEntries(span, event.state, event.entries)
		} else if event.resolvedTs != 0 {
			resolvedTs := handleResolvedTs(span, event.state, event.resolvedTs)
			if resolvedTs > newResolvedTs {
				newResolvedTs = resolvedTs
			}
		} else {
			log.Panic("should not reach", zap.Any("event", event), zap.Any("events", events))
		}
	}
	tryAdvanceResolvedTs := func() {
		if newResolvedTs != 0 {
			span.advanceResolvedTs(newResolvedTs)
		}
	}
	if len(span.kvEventsCache) > 0 {
		metricsEventCount.Add(float64(len(span.kvEventsCache)))
		await := span.consumeKVEvents(span.kvEventsCache, func() {
			span.clearKVEventsCache()
			tryAdvanceResolvedTs()
			h.subClient.wakeSubscription(span.subID)
		})
		// if not await, the wake callback will not be called, we need clear the cache manually.
		if !await {
			span.clearKVEventsCache()
			tryAdvanceResolvedTs()
		}
		return await
	} else {
		tryAdvanceResolvedTs()
	}
	return false
}

func (h *regionEventHandler) GetSize(event regionEvent) int {
	return event.getSize()
}

func (h *regionEventHandler) GetArea(path SubscriptionID, dest *subscribedSpan) int {
	return 0
}

func (h *regionEventHandler) GetTimestamp(event regionEvent) dynstream.Timestamp {
	if event.entries != nil {
		entries := event.entries.Entries.GetEntries()
		switch entries[0].Type {
		case cdcpb.Event_INITIALIZED:
			return dynstream.Timestamp(event.state.region.resolvedTs())
		case cdcpb.Event_COMMITTED,
			cdcpb.Event_PREWRITE,
			cdcpb.Event_COMMIT,
			cdcpb.Event_ROLLBACK:
			return dynstream.Timestamp(entries[0].CommitTs)
		default:
			log.Warn("unknown event entries", zap.Any("event", event.entries))
			return 0
		}
	} else {
		return dynstream.Timestamp(event.resolvedTs)
	}
}
func (h *regionEventHandler) IsPaused(event regionEvent) bool { return false }

func (h *regionEventHandler) GetType(event regionEvent) dynstream.EventType {
	if event.entries != nil || event.resolvedTs != 0 {
		// Note: resolved ts may be from different regions, so they are not periodic signal
		return dynstream.EventType{DataGroup: DataGroupEntriesOrResolvedTs, Property: dynstream.BatchableData}
	} else if event.state.isStale() {
		return dynstream.EventType{DataGroup: DataGroupError, Property: dynstream.BatchableData}
	} else {
		log.Panic("unknown event type",
			zap.Uint64("regionID", event.state.getRegionID()),
			zap.Uint64("requestID", event.state.requestID),
			zap.Uint64("workerID", event.worker.workerID))
	}
	return dynstream.DefaultEventType
}

func (h *regionEventHandler) OnDrop(event regionEvent) {
	log.Warn("drop region event",
		zap.Uint64("regionID", event.state.getRegionID()),
		zap.Uint64("requestID", event.state.requestID),
		zap.Uint64("workerID", event.worker.workerID),
		zap.Bool("hasEntries", event.entries != nil),
		zap.Bool("stateIsStale", event.state.isStale()))
}

func (h *regionEventHandler) handleRegionError(state *regionFeedState, worker *regionRequestWorker) {
	stepsToRemoved := state.markRemoved()
	err := state.takeError()
	if err != nil {
		log.Debug("region event handler get a region error",
			zap.Uint64("workerID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(state.region.subscribedSpan.subID)),
			zap.Uint64("regionID", state.region.verID.GetID()),
			zap.Bool("reschedule", stepsToRemoved),
			zap.Error(err))
	}
	if stepsToRemoved {
		worker.takeRegionState(SubscriptionID(state.requestID), state.getRegionID())
		h.subClient.onRegionFail(newRegionErrorInfo(state.getRegionInfo(), err))
	}
}

// func handleEventEntries(span *subscribedSpan, state *regionFeedState, entries *cdcpb.Event_Entries_, kvEvents []common.RawKVEntry) []common.RawKVEntry {
func handleEventEntries(span *subscribedSpan, state *regionFeedState, entries *cdcpb.Event_Entries_) {
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
				zap.Stringer("span", &state.region.span))

			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, cachedEvent))
			}
			state.matcher.matchCachedRollbackRow(true)
		case cdcpb.Event_COMMITTED:
			resolvedTs := state.getLastResolvedTs()
			if entry.CommitTs <= resolvedTs {
				log.Fatal("The CommitTs must be greater than the resolvedTs",
					zap.String("EventType", "COMMITTED"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
			}
			span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, entry))
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
					zap.String("key", hex.EncodeToString(entry.GetKey())),
					zap.Uint64("startTs", entry.GetStartTs()),
					zap.Uint64("commitTs", entry.GetCommitTs()),
					zap.Any("type", entry.GetType()),
					zap.Uint64("regionID", state.getRegionID()),
					zap.Any("opType", entry.GetOpType()))
				return
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
					zap.String("EventType", "COMMIT"),
					zap.Uint64("CommitTs", entry.CommitTs),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("regionID", regionID))
				return
			}
			// kvEvents = append(kvEvents, assembleRowEvent(regionID, entry))
			span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, entry))
		case cdcpb.Event_ROLLBACK:
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
}

func handleResolvedTs(span *subscribedSpan, state *regionFeedState, resolvedTs uint64) uint64 {
	if state.isStale() || !state.isInitialized() {
		return 0
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
		return 0
	}
	state.updateResolvedTs(resolvedTs)

	now := time.Now().UnixMilli()
	lastAdvance := span.lastAdvanceTime.Load()
	if now-lastAdvance > span.advanceInterval && span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		ts := span.rangeLock.ResolvedTs()
		if ts > 0 && span.initialized.CompareAndSwap(false, true) {
			log.Info("subscription client is initialized",
				zap.Uint64("subscriptionID", uint64(span.subID)),
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", ts))
		}
		lastResolvedTs := span.resolvedTs.Load()
		if ts > lastResolvedTs {
			span.resolvedTs.Store(ts)
			span.resolvedTsUpdated.Store(time.Now().Unix())
			return ts
		}
	}
	return 0
}
