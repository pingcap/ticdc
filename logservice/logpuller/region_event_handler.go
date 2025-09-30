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
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/tikv/client-go/v2/oracle"
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
	subClient *subscriptionClient
}

func (h *regionEventHandler) Path(event regionEvent) SubscriptionID {
	return SubscriptionID(event.state.requestID)
}

func (h *regionEventHandler) Handle(span *subscribedSpan, events ...regionEvent) bool {
	if len(span.kvEventsCache) != 0 {
		log.Panic("kvEventsCache is not empty",
			zap.Int("kvEventsCacheLen", len(span.kvEventsCache)),
			zap.Uint64("subscriptionID", uint64(span.subID)))
	}

	newResolvedTs := uint64(0)
	for _, event := range events {
		if event.state.isStale() {
			h.handleRegionError(event.state, event.worker)
			continue
		}
		if event.entries != nil {
			err := handleEventEntries(span, event.state, event.entries)
			if err != nil {
				event.state.markStopped(err)
				h.handleRegionError(event.state, event.worker)
			}
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
	if event.entries != nil && event.entries.Entries != nil {
		for _, entry := range event.entries.Entries.GetEntries() {
			switch entry.Type {
			case cdcpb.Event_INITIALIZED:
				return dynstream.Timestamp(event.state.region.resolvedTs())
			case cdcpb.Event_COMMITTED,
				cdcpb.Event_PREWRITE,
				cdcpb.Event_COMMIT,
				cdcpb.Event_ROLLBACK:
				return dynstream.Timestamp(entry.CommitTs)
			default:
				// ignore other event types
			}
		}
		return 0
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

func (h *regionEventHandler) OnDrop(event regionEvent) interface{} {
	// TODO: Distinguish between drop events caused by "path not found" errors and memory control.
	log.Warn("drop region event",
		zap.Uint64("regionID", event.state.getRegionID()),
		zap.Uint64("requestID", event.state.requestID),
		zap.Uint64("workerID", event.worker.workerID),
		zap.Bool("hasEntries", event.entries != nil),
		zap.Bool("stateIsStale", event.state.isStale()))
	return nil
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

func handleEventEntries(span *subscribedSpan, state *regionFeedState, entries *cdcpb.Event_Entries_) error {
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
			// todo: remove this log after the next-gen is stable
			if kerneltype.IsNextGen() {
				log.Info("region is initialized",
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("startKey", spanz.HexKey(span.span.StartKey)),
					zap.String("endKey", spanz.HexKey(span.span.EndKey)))
			} else {
				log.Debug("region is initialized",
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("startKey", spanz.HexKey(span.span.StartKey)),
					zap.String("endKey", spanz.HexKey(span.span.EndKey)))
			}
			for _, cachedEvent := range state.matcher.matchCachedRow(true) {
				span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, cachedEvent))
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
				if kerneltype.IsNextGen() {
					// this is a temporary workaround for the case that:
					// Next-Gen may cannot found prewrite entry during the incremental scan phase
					// due to it is by design. By report error, trigger the region reconnection, so that
					// the commit entry convert to the committed entry automatically.
					// After the Next-Gen fix this issue, this workaround can be removed.
					return errors.New("prewrite not found")
				}
				log.Fatal("prewrite not match",
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", state.getRegionID()),
					zap.Uint64("requestID", state.requestID),
					zap.Uint64("startTs", entry.GetStartTs()),
					zap.Uint64("commitTs", entry.GetCommitTs()),
					zap.String("key", spanz.HexKey(entry.GetKey())))
				return nil
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
					zap.String("key", spanz.HexKey(entry.GetKey())))
				return nil
			}
			span.kvEventsCache = append(span.kvEventsCache, assembleRowEvent(regionID, entry))
		case cdcpb.Event_ROLLBACK:
			if !state.isInitialized() {
				state.matcher.cacheRollbackRow(entry)
				continue
			}
			state.matcher.rollbackRow(entry)
		}
	}
	return nil
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
	span.rangeLock.UpdateLockedRangeStateHeap(state.region.lockedRangeState)

	now := time.Now().UnixMilli()
	lastAdvance := span.lastAdvanceTime.Load()
	if now-lastAdvance >= span.advanceInterval && span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		ts := span.rangeLock.GetHeapMinTs()
		if ts > 0 && span.initialized.CompareAndSwap(false, true) {
			log.Info("subscription client is initialized",
				zap.Uint64("subscriptionID", uint64(span.subID)),
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", ts))
		}
		lastResolvedTs := span.resolvedTs.Load()
		nextResolvedPhyTs := oracle.ExtractPhysical(ts)
		// Generally, we don't want to send duplicate resolved ts,
		// so we check whether `ts` is larger than `lastResolvedTs` before send it.
		// but when `ts` == `lastResolvedTs` == `span.startTs`,
		// the span may just be initialized and have not receive any resolved ts before,
		// so we also send ts in this case for quick notification to downstream.
		if ts > lastResolvedTs || (ts == lastResolvedTs && lastResolvedTs == span.startTs) {
			resolvedPhyTs := oracle.ExtractPhysical(lastResolvedTs)
			decreaseLag := float64(nextResolvedPhyTs-resolvedPhyTs) / 1e3
			const largeResolvedTsAdvanceStepInSecs = 30
			if decreaseLag > largeResolvedTsAdvanceStepInSecs {
				log.Warn("resolved ts advance step is too large",
					zap.Uint64("subID", uint64(span.subID)),
					zap.Int64("tableID", span.span.TableID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("resolvedTs", ts),
					zap.Uint64("lastResolvedTs", lastResolvedTs),
					zap.Float64("decreaseLag(s)", decreaseLag))
			}
			span.resolvedTs.Store(ts)
			span.resolvedTsUpdated.Store(time.Now().Unix())
			return ts
		}
	}
	return 0
}
