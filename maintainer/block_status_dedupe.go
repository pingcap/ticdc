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

package maintainer

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// blockStatusDedupeKey identifies the minimal logical status that can be
// suppressed while an equivalent event is pending in the maintainer queue.
type blockStatusDedupeKey struct {
	from         node.ID
	mode         int64
	dispatcherID common.DispatcherID
	blockTs      uint64
	isSyncPoint  bool
	stage        heartbeatpb.BlockStage
}

// blockStatusPendingSet tracks which logical statuses are already admitted into
// the maintainer event queue and not yet fully handled. It gives the maintainer
// the same "one representative while pending" guarantee that dispatcher manager
// already applies on its own local queues.
type blockStatusPendingSet struct {
	mu      sync.Mutex
	pending map[blockStatusDedupeKey]struct{}
}

// reserve returns false when an equivalent status is already queued or being
// handled by the maintainer. Callers must later release every reserved key
// exactly once after HandleEvent returns.
func (s *blockStatusPendingSet) reserve(key blockStatusDedupeKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending == nil {
		s.pending = make(map[blockStatusDedupeKey]struct{})
	}
	if _, ok := s.pending[key]; ok {
		return false
	}
	s.pending[key] = struct{}{}
	return true
}

// release reopens the dedupe window for statuses that just finished maintainer
// handling, so the next resend can enter if it is still needed.
func (s *blockStatusPendingSet) release(keys []blockStatusDedupeKey) {
	if len(keys) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		delete(s.pending, key)
	}
}

// buildBlockStatusDedupeKey keeps the dedupe identity aligned with dispatcher
// and dispatcher-manager stages: sender, pipeline mode, dispatcher, barrier ts,
// syncpoint bit, and stage all matter for correctness.
func buildBlockStatusDedupeKey(from node.ID, status *heartbeatpb.TableSpanBlockStatus) (blockStatusDedupeKey, bool) {
	if status == nil || status.ID == nil || status.State == nil {
		return blockStatusDedupeKey{}, false
	}
	return blockStatusDedupeKey{
		from:         from,
		mode:         status.Mode,
		dispatcherID: common.NewDispatcherIDFromPB(status.ID),
		blockTs:      status.State.BlockTs,
		isSyncPoint:  status.State.IsSyncPoint,
		stage:        status.State.Stage,
	}, true
}

// cloneBlockStatusRequest rebuilds only the outer request so filtering can
// replace the BlockStatuses slice without mutating the original message object.
// The individual status protobufs are already treated as immutable payloads.
func cloneBlockStatusRequest(
	req *heartbeatpb.BlockStatusRequest,
	blockStatuses []*heartbeatpb.TableSpanBlockStatus,
) *heartbeatpb.BlockStatusRequest {
	return &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  req.ChangefeedID,
		BlockStatuses: blockStatuses,
		Mode:          req.Mode,
	}
}

// filterBlockStatusEvent is the maintainer-side queue gate for block statuses.
// It keeps at most one logically equivalent status in the event queue at a
// time, then arranges for the reservation to be released after HandleEvent
// finishes processing that representative copy.
func (m *Maintainer) filterBlockStatusEvent(event *Event) (*Event, bool) {
	if event == nil || event.eventType != EventMessage || event.message == nil ||
		event.message.Type != messaging.TypeBlockStatusRequest || len(event.message.Message) == 0 {
		return event, true
	}

	req, ok := event.message.Message[0].(*heartbeatpb.BlockStatusRequest)
	if !ok || req == nil || len(req.BlockStatuses) == 0 {
		return event, true
	}

	filteredStatuses := make([]*heartbeatpb.TableSpanBlockStatus, 0, len(req.BlockStatuses))
	releaseKeys := make([]blockStatusDedupeKey, 0, len(req.BlockStatuses))
	seen := make(map[blockStatusDedupeKey]struct{}, len(req.BlockStatuses))
	suppressedCount := 0

	for _, status := range req.BlockStatuses {
		key, ok := buildBlockStatusDedupeKey(event.message.From, status)
		if !ok {
			filteredStatuses = append(filteredStatuses, status)
			continue
		}
		// Collapse duplicates inside the same request first so only one copy
		// competes for the queue-level reservation below.
		if _, ok := seen[key]; ok {
			suppressedCount++
			continue
		}
		seen[key] = struct{}{}
		// reserve extends dedupe across already-queued events and the event that
		// is currently executing in the maintainer main loop.
		if !m.blockStatusPending.reserve(key) {
			suppressedCount++
			continue
		}

		filteredStatuses = append(filteredStatuses, status)
		releaseKeys = append(releaseKeys, key)
	}

	if suppressedCount > 0 {
		log.Debug("suppress duplicate maintainer block statuses",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.Stringer("from", event.message.From),
			zap.Int64("mode", req.Mode),
			zap.Int("kept", len(filteredStatuses)),
			zap.Int("suppressed", suppressedCount))
	}
	if len(filteredStatuses) == 0 {
		return nil, false
	}

	filteredMessage := &messaging.TargetMessage{
		From:     event.message.From,
		To:       event.message.To,
		Epoch:    event.message.Epoch,
		Sequence: event.message.Sequence,
		Topic:    event.message.Topic,
		Type:     event.message.Type,
		Message: []messaging.IOTypeT{
			cloneBlockStatusRequest(req, filteredStatuses),
		},
		CreateAt: event.message.CreateAt,
		Group:    event.message.Group,
	}

	return &Event{
		changefeedID:           event.changefeedID,
		eventType:              event.eventType,
		message:                filteredMessage,
		blockStatusReleaseKeys: releaseKeys,
	}, true
}
