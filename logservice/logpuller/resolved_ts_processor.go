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

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func handleResolvedTs(span *subscribedSpan, state *regionFeedState, resolvedTs uint64) uint64 {
	if state.isStale() || !state.isInitialized() {
		return 0
	}
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
