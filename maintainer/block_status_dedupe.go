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

type blockStatusPendingSet struct {
	mu      sync.Mutex
	pending map[blockStatusDedupeKey]struct{}
}

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
		if _, ok := seen[key]; ok {
			suppressedCount++
			continue
		}
		seen[key] = struct{}{}
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
