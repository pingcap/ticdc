package logpuller

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type resolvedTsEvent struct {
	state      *regionFeedState
	resolvedTs uint64
	worker     *regionRequestWorker
}

type resolvedTsEventHandler struct {
	subClient *subscriptionClient
}

func (h *resolvedTsEventHandler) Path(event resolvedTsEvent) SubscriptionID {
	return SubscriptionID(event.state.requestID)
}

func (h *resolvedTsEventHandler) Handle(span *subscribedSpan, events ...resolvedTsEvent) bool {
	updatedStates := make([]*regionFeedState, 0, len(events))
	for _, event := range events {
		if event.state.isStale() {
			continue
		}
		if handleResolvedState(span, event.state, event.resolvedTs) {
			updatedStates = append(updatedStates, event.state)
		}
	}
	newResolvedTs := updateSpanResolvedTs(span, updatedStates)
	if newResolvedTs != 0 {
		h.subClient.emitResolvedTs(span.subID, newResolvedTs)
	}
	return false
}

func (h *resolvedTsEventHandler) GetSize(resolvedTsEvent) int { return 0 }
func (h *resolvedTsEventHandler) GetArea(path SubscriptionID, dest *subscribedSpan) int {
	return 0
}
func (h *resolvedTsEventHandler) GetTimestamp(resolvedTsEvent) dynstream.Timestamp {
	return 0
}
func (h *resolvedTsEventHandler) GetType(resolvedTsEvent) dynstream.EventType {
	return dynstream.DefaultEventType
}
func (h *resolvedTsEventHandler) IsPaused(resolvedTsEvent) bool { return false }
func (h *resolvedTsEventHandler) OnDrop(resolvedTsEvent) interface{} {
	return nil
}

func handleResolvedState(span *subscribedSpan, state *regionFeedState, resolvedTs uint64) bool {
	if state.isStale() || !state.isInitialized() {
		return false
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
		return false
	}
	state.updateResolvedTs(resolvedTs)
	span.rangeLock.UpdateLockedRangeStateHeap(state.region.lockedRangeState)
	return true
}

func updateSpanResolvedTs(span *subscribedSpan, updatedStates []*regionFeedState) uint64 {
	if len(updatedStates) == 0 {
		return 0
	}

	now := time.Now().UnixMilli()
	lastAdvance := span.lastAdvanceTime.Load()
	if now-lastAdvance < span.advanceInterval && !span.lastAdvanceTime.CompareAndSwap(lastAdvance, now) {
		return 0
	}

	ts := span.rangeLock.GetHeapMinTs()
	if ts == 0 {
		return 0
	}

	firstRegionID := updatedStates[0].getRegionID()
	if span.initialized.CompareAndSwap(false, true) {
		log.Info("subscription client is initialized",
			zap.Uint64("subscriptionID", uint64(span.subID)),
			zap.Uint64("regionID", firstRegionID),
			zap.Uint64("resolvedTs", ts))
	}

	lastResolvedTs := span.resolvedTs.Load()
	nextResolvedPhyTs := oracle.ExtractPhysical(ts)
	if ts > lastResolvedTs || (ts == lastResolvedTs && lastResolvedTs == span.startTs) {
		resolvedPhyTs := oracle.ExtractPhysical(lastResolvedTs)
		decreaseLag := float64(nextResolvedPhyTs-resolvedPhyTs) / 1e3
		const largeResolvedTsAdvanceStepInSecs = 30
		if decreaseLag > largeResolvedTsAdvanceStepInSecs {
			log.Warn("resolved ts advance step is too large",
				zap.Uint64("subID", uint64(span.subID)),
				zap.Int64("tableID", span.span.TableID),
				zap.Uint64("regionID", firstRegionID),
				zap.Uint64("resolvedTs", ts),
				zap.Uint64("lastResolvedTs", lastResolvedTs),
				zap.Float64("decreaseLag(s)", decreaseLag))
		}
		span.resolvedTs.Store(ts)
		span.resolvedTsUpdated.Store(time.Now().Unix())
		return ts
	}
	return 0
}
