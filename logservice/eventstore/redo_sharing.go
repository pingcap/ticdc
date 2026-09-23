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

package eventstore

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

// reuseRedoSubscription attaches a local consumer to its opposite-mode peer.
// registerMu is held by the caller. The metadata lock also excludes checkpoint
// advancement and detach while checking the retained range and attaching.
func (e *eventStore) reuseRedoSubscription(stat *dispatcherStat, span *heartbeatpb.TableSpan,
	startTs uint64, requiredConfig subscriptionConfig, notifier ResolvedTsNotifier,
) bool {
	if stat.mode != common.DefaultMode && stat.mode != common.RedoMode {
		return false
	}
	e.dispatcherMeta.Lock()
	defer e.dispatcherMeta.Unlock()
	for _, sub := range e.dispatcherMeta.tableStats[newTableStatsKey(span)] {
		if sub.config != requiredConfig || sub.tableSpan.KeyspaceID != span.KeyspaceID || !sub.tableSpan.Equal(span) ||
			sub.checkpointTs.Load() > startTs || startTs > sub.resolvedTs.Load() {
			continue
		}
		subscribers := sub.subscribers.Load()
		if subscribers == nil {
			continue
		}
		for id, subscriber := range subscribers.subscribers {
			peer := e.dispatcherMeta.dispatcherStats[id]
			if peer == nil || subscriber.isStopped || peer.changefeedID != stat.changefeedID ||
				(peer.mode != common.DefaultMode && peer.mode != common.RedoMode) || peer.mode == stat.mode {
				continue
			}
			stat.subStat = sub
			e.dispatcherMeta.dispatcherStats[stat.dispatcherID] = stat
			e.addSubscriberToSubStat(sub, stat.dispatcherID, &Subscriber{notifyFunc: notifier})
			log.Info("reuse subscription for redo pair", zap.Stringer("dispatcherID", stat.dispatcherID),
				zap.Stringer("peerDispatcherID", id), zap.Stringer("changefeedID", stat.changefeedID),
				zap.Int64("mode", stat.mode),
				zap.Uint64("subscriptionID", uint64(sub.subID)), zap.Int64("tableID", span.TableID))
			return true
		}
	}
	return false
}
