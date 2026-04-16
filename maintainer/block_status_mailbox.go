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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/deque"
)

const (
	defaultBlockStatusMailboxMaxEntries = 16384
	defaultBlockStatusDrainBatchSize    = 256
)

type blockStatusMailboxSourceKey struct {
	from node.ID
	mode int64
}

type blockStatusMailboxLogicalKey struct {
	dispatcherID common.DispatcherID
	blockTs      uint64
	isSyncPoint  bool
	isBlocked    bool
}

type blockStatusMailboxPayload struct {
	dispatcherID      common.DispatcherID
	blockTs           uint64
	isSyncPoint       bool
	isBlocked         bool
	stage             heartbeatpb.BlockStage
	mode              int64
	blockTables       *heartbeatpb.InfluencedTables
	needDroppedTables *heartbeatpb.InfluencedTables
	needAddedTables   []*heartbeatpb.Table
	updatedSchemas    []*heartbeatpb.SchemaIDChange
}

func newBlockStatusMailboxPayload(status *heartbeatpb.TableSpanBlockStatus) *blockStatusMailboxPayload {
	if status == nil || status.State == nil {
		return nil
	}
	return &blockStatusMailboxPayload{
		dispatcherID:      common.NewDispatcherIDFromPB(status.ID),
		blockTs:           status.State.BlockTs,
		isSyncPoint:       status.State.IsSyncPoint,
		isBlocked:         status.State.IsBlocked,
		stage:             status.State.Stage,
		mode:              status.Mode,
		blockTables:       status.State.BlockTables,
		needDroppedTables: status.State.NeedDroppedTables,
		needAddedTables:   status.State.NeedAddedTables,
		updatedSchemas:    status.State.UpdatedSchemas,
	}
}

func (p *blockStatusMailboxPayload) toPB() *heartbeatpb.TableSpanBlockStatus {
	if p == nil {
		return nil
	}
	return &heartbeatpb.TableSpanBlockStatus{
		ID: p.dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:         p.isBlocked,
			BlockTs:           p.blockTs,
			BlockTables:       p.blockTables,
			NeedDroppedTables: p.needDroppedTables,
			NeedAddedTables:   p.needAddedTables,
			UpdatedSchemas:    p.updatedSchemas,
			IsSyncPoint:       p.isSyncPoint,
			Stage:             p.stage,
		},
		Mode: p.mode,
	}
}

type blockStatusMailboxEntry struct {
	blocked        *blockStatusMailboxPayload
	done           *blockStatusMailboxPayload
	nonBlocked     *blockStatusMailboxPayload
	blockedDrained bool
	inOrder        bool
}

func (e *blockStatusMailboxEntry) hasPending() bool {
	return e != nil && (e.blocked != nil || e.done != nil || e.nonBlocked != nil)
}

type blockStatusMailboxBucket struct {
	entries map[blockStatusMailboxLogicalKey]*blockStatusMailboxEntry
	order   *deque.Deque[blockStatusMailboxLogicalKey]
}

func newBlockStatusMailboxBucket() *blockStatusMailboxBucket {
	return &blockStatusMailboxBucket{
		entries: make(map[blockStatusMailboxLogicalKey]*blockStatusMailboxEntry),
		order:   deque.NewDequeDefault[blockStatusMailboxLogicalKey](),
	}
}

type blockStatusMailboxBatch struct {
	from     node.ID
	mode     int64
	statuses []*heartbeatpb.TableSpanBlockStatus
}

type blockStatusMailbox struct {
	mu            sync.Mutex
	maxEntries    int
	maxDrainBatch int

	entries      int
	buckets      map[blockStatusMailboxSourceKey]*blockStatusMailboxBucket
	readySources *deque.Deque[blockStatusMailboxSourceKey]
	readySet     map[blockStatusMailboxSourceKey]struct{}
	notifyCh     chan struct{}
}

func newBlockStatusMailbox(maxEntries int, maxDrainBatch int) *blockStatusMailbox {
	if maxEntries <= 0 {
		maxEntries = defaultBlockStatusMailboxMaxEntries
	}
	if maxDrainBatch <= 0 {
		maxDrainBatch = defaultBlockStatusDrainBatchSize
	}
	return &blockStatusMailbox{
		maxEntries:    maxEntries,
		maxDrainBatch: maxDrainBatch,
		buckets:       make(map[blockStatusMailboxSourceKey]*blockStatusMailboxBucket),
		readySources:  deque.NewDequeDefault[blockStatusMailboxSourceKey](),
		readySet:      make(map[blockStatusMailboxSourceKey]struct{}),
		notifyCh:      make(chan struct{}, 1),
	}
}

func (m *blockStatusMailbox) Notify() <-chan struct{} {
	return m.notifyCh
}

func (m *blockStatusMailbox) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries
}

func (m *blockStatusMailbox) signal() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

func (m *blockStatusMailbox) enqueueRequest(from node.ID, request *heartbeatpb.BlockStatusRequest) {
	if request == nil || len(request.BlockStatuses) == 0 {
		return
	}

	sourceKey := blockStatusMailboxSourceKey{
		from: from,
		mode: request.Mode,
	}

	shouldSignal := false

	m.mu.Lock()
	for _, status := range request.BlockStatuses {
		if m.enqueueStatusLocked(sourceKey, status) {
			shouldSignal = true
		}
	}
	m.mu.Unlock()

	if shouldSignal {
		m.signal()
	}
}

func (m *blockStatusMailbox) enqueueStatusLocked(
	sourceKey blockStatusMailboxSourceKey,
	status *heartbeatpb.TableSpanBlockStatus,
) bool {
	payload := newBlockStatusMailboxPayload(status)
	if payload == nil {
		return false
	}

	bucket, ok := m.buckets[sourceKey]
	if !ok {
		bucket = newBlockStatusMailboxBucket()
		m.buckets[sourceKey] = bucket
	}

	logicalKey := blockStatusMailboxLogicalKey{
		dispatcherID: payload.dispatcherID,
		blockTs:      payload.blockTs,
		isSyncPoint:  payload.isSyncPoint,
		isBlocked:    payload.isBlocked,
	}

	entry, ok := bucket.entries[logicalKey]
	if !ok {
		if m.entries >= m.maxEntries {
			return false
		}
		entry = &blockStatusMailboxEntry{}
		bucket.entries[logicalKey] = entry
		m.entries++
	}

	if !payload.isBlocked {
		entry.nonBlocked = payload
		return m.markReadyLocked(sourceKey, bucket, logicalKey, entry)
	}

	if payload.stage == heartbeatpb.BlockStage_DONE {
		entry.done = payload
		return m.markReadyLocked(sourceKey, bucket, logicalKey, entry)
	}

	if entry.blockedDrained {
		return false
	}

	entry.blocked = payload
	return m.markReadyLocked(sourceKey, bucket, logicalKey, entry)
}

func (m *blockStatusMailbox) markReadyLocked(
	sourceKey blockStatusMailboxSourceKey,
	bucket *blockStatusMailboxBucket,
	logicalKey blockStatusMailboxLogicalKey,
	entry *blockStatusMailboxEntry,
) bool {
	queuedSource := false
	if !entry.inOrder {
		bucket.order.PushBack(logicalKey)
		entry.inOrder = true
	}
	if _, ok := m.readySet[sourceKey]; !ok {
		m.readySources.PushBack(sourceKey)
		m.readySet[sourceKey] = struct{}{}
		queuedSource = true
	}
	return queuedSource
}

func (m *blockStatusMailbox) drainBatch() (blockStatusMailboxBatch, bool) {
	for {
		var needSignal bool

		m.mu.Lock()
		sourceKey, ok := m.readySources.PopFront()
		if !ok {
			m.mu.Unlock()
			return blockStatusMailboxBatch{}, false
		}
		delete(m.readySet, sourceKey)

		bucket := m.buckets[sourceKey]
		if bucket == nil {
			m.mu.Unlock()
			continue
		}

		batch := blockStatusMailboxBatch{
			from:     sourceKey.from,
			mode:     sourceKey.mode,
			statuses: make([]*heartbeatpb.TableSpanBlockStatus, 0, m.maxDrainBatch),
		}

		for len(batch.statuses) < m.maxDrainBatch {
			logicalKey, ok := bucket.order.PopFront()
			if !ok {
				break
			}

			entry := bucket.entries[logicalKey]
			if entry == nil {
				continue
			}
			entry.inOrder = false

			remaining := m.maxDrainBatch - len(batch.statuses)
			drainedStatuses, removeEntry := m.drainEntryLocked(entry, remaining)
			batch.statuses = append(batch.statuses, drainedStatuses...)

			if removeEntry {
				delete(bucket.entries, logicalKey)
				m.entries--
				continue
			}

			if entry.hasPending() {
				bucket.order.PushBack(logicalKey)
				entry.inOrder = true
			}
		}

		if bucket.order.Length() > 0 {
			m.readySources.PushBack(sourceKey)
			m.readySet[sourceKey] = struct{}{}
		}
		if len(bucket.entries) == 0 && bucket.order.Length() == 0 {
			delete(m.buckets, sourceKey)
		}
		needSignal = m.readySources.Length() > 0
		m.mu.Unlock()

		if needSignal {
			m.signal()
		}
		if len(batch.statuses) == 0 {
			continue
		}
		return batch, true
	}
}

func (m *blockStatusMailbox) drainEntryLocked(
	entry *blockStatusMailboxEntry,
	remaining int,
) ([]*heartbeatpb.TableSpanBlockStatus, bool) {
	if remaining <= 0 || entry == nil {
		return nil, false
	}

	drained := make([]*heartbeatpb.TableSpanBlockStatus, 0, 2)
	appendStatus := func(payload *blockStatusMailboxPayload) bool {
		if payload == nil || remaining <= 0 {
			return false
		}
		drained = append(drained, payload.toPB())
		remaining--
		return true
	}

	if appendStatus(entry.nonBlocked) {
		entry.nonBlocked = nil
		return drained, true
	}

	if appendStatus(entry.blocked) {
		entry.blocked = nil
		entry.blockedDrained = true
	}

	if entry.done != nil && (entry.blockedDrained || entry.blocked == nil) && appendStatus(entry.done) {
		entry.done = nil
		entry.blockedDrained = false
		return drained, true
	}

	return drained, false
}
