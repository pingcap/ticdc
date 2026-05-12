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

package dispatcher

import (
	"context"
	"sync"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

type blockStatusKey struct {
	dispatcherID common.DispatcherID
	blockTs      uint64
	mode         int64
	isSyncPoint  bool
	stage        heartbeatpb.BlockStage
}

// BlockStatusEntry stores the minimal dispatcher block-status data before the
// dispatcher manager materializes a heartbeatpb.TableSpanBlockStatus.
//
// The dispatcher resend path can offer the same WAITING or DONE status many
// times before the dispatcher manager drains it. Keeping this internal payload
// avoids repeatedly allocating protobuf messages that may be coalesced locally.
type BlockStatusEntry struct {
	dispatcherID      common.DispatcherID
	blockTs           uint64
	mode              int64
	isBlocked         bool
	isSyncPoint       bool
	stage             heartbeatpb.BlockStage
	blockTables       *commonEvent.InfluencedTables
	needDroppedTables *commonEvent.InfluencedTables
	needAddedTables   []commonEvent.Table
	updatedSchemas    []commonEvent.SchemaIDChange
}

// NewWaitingBlockStatusEntry creates a WAITING status entry for a dispatcher
// that is blocked by a DDL or sync point event.
func NewWaitingBlockStatusEntry(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	blockTables *commonEvent.InfluencedTables,
	needDroppedTables *commonEvent.InfluencedTables,
	needAddedTables []commonEvent.Table,
	updatedSchemas []commonEvent.SchemaIDChange,
	isSyncPoint bool,
	mode int64,
) BlockStatusEntry {
	return BlockStatusEntry{
		dispatcherID:      dispatcherID,
		blockTs:           blockTs,
		mode:              mode,
		isBlocked:         true,
		isSyncPoint:       isSyncPoint,
		stage:             heartbeatpb.BlockStage_WAITING,
		blockTables:       blockTables,
		needDroppedTables: needDroppedTables,
		needAddedTables:   needAddedTables,
		updatedSchemas:    updatedSchemas,
	}
}

// NewNoneBlockStatusEntry creates a scheduling status entry for a non-blocking
// DDL whose add/drop-table metadata still needs maintainer coordination.
func NewNoneBlockStatusEntry(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	needDroppedTables *commonEvent.InfluencedTables,
	needAddedTables []commonEvent.Table,
	mode int64,
) BlockStatusEntry {
	return BlockStatusEntry{
		dispatcherID:      dispatcherID,
		blockTs:           blockTs,
		mode:              mode,
		needDroppedTables: needDroppedTables,
		needAddedTables:   needAddedTables,
		stage:             heartbeatpb.BlockStage_NONE,
	}
}

// NewDoneBlockStatusEntry creates a DONE status entry after a dispatcher has
// written or passed a block event.
func NewDoneBlockStatusEntry(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	isSyncPoint bool,
	mode int64,
) BlockStatusEntry {
	return BlockStatusEntry{
		dispatcherID: dispatcherID,
		blockTs:      blockTs,
		mode:         mode,
		isBlocked:    true,
		isSyncPoint:  isSyncPoint,
		stage:        heartbeatpb.BlockStage_DONE,
	}
}

func (e BlockStatusEntry) toPB() *heartbeatpb.TableSpanBlockStatus {
	return &heartbeatpb.TableSpanBlockStatus{
		ID: e.dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:         e.isBlocked,
			BlockTs:           e.blockTs,
			BlockTables:       e.blockTables.ToPB(),
			NeedDroppedTables: e.needDroppedTables.ToPB(),
			NeedAddedTables:   commonEvent.ToTablesPB(e.needAddedTables),
			UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(e.updatedSchemas),
			IsSyncPoint:       e.isSyncPoint,
			Stage:             e.stage,
		},
		Mode: e.mode,
	}
}

func (e BlockStatusEntry) key() blockStatusKey {
	return blockStatusKey{
		dispatcherID: e.dispatcherID,
		blockTs:      e.blockTs,
		mode:         e.mode,
		isSyncPoint:  e.isSyncPoint,
		stage:        e.stage,
	}
}

func (e BlockStatusEntry) shouldDeduplicate() bool {
	return e.isBlocked &&
		(e.stage == heartbeatpb.BlockStage_WAITING ||
			e.stage == heartbeatpb.BlockStage_DONE)
}

// BlockStatusBuffer keeps block statuses in enqueue order while coalescing
// identical pending WAITING and DONE entries.
//
// The dedupe key includes dispatcher ID, block ts, mode, sync-point flag, and
// block stage, so WAITING and DONE for the same barrier remain separate entries.
// Offer reserves the key before enqueueing; Take releases it after local dequeue.
// The later BlockStatusRequestQueue still deduplicates queued and in-flight
// protobuf requests until the heartbeat collector finishes sending them.
type BlockStatusBuffer struct {
	queue chan BlockStatusEntry

	mu      sync.Mutex
	pending map[blockStatusKey]struct{}
}

// NewBlockStatusBuffer creates a bounded local mailbox for dispatcher block
// statuses.
func NewBlockStatusBuffer(size int) *BlockStatusBuffer {
	if size <= 0 {
		size = 1
	}
	return &BlockStatusBuffer{
		queue:   make(chan BlockStatusEntry, size),
		pending: make(map[blockStatusKey]struct{}),
	}
}

// Offer enqueues a block-status entry unless an identical WAITING or DONE entry
// is already pending in this buffer.
func (b *BlockStatusBuffer) Offer(status BlockStatusEntry) {
	if !status.shouldDeduplicate() {
		b.queue <- status
		return
	}

	key := status.key()
	if !b.reserve(key) {
		return
	}
	b.queue <- status
}

// Take waits for the next entry and materializes it as a protobuf message.
// Returning nil means the context was canceled before an entry was available.
func (b *BlockStatusBuffer) Take(ctx context.Context) *heartbeatpb.TableSpanBlockStatus {
	select {
	case <-ctx.Done():
		return nil
	case status := <-b.queue:
		b.release(status)
		return status.toPB()
	}
}

// Len returns the number of locally queued entries. It is used only for metrics.
func (b *BlockStatusBuffer) Len() int {
	return len(b.queue)
}

func (b *BlockStatusBuffer) reserve(key blockStatusKey) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[key]; ok {
		return false
	}
	b.pending[key] = struct{}{}
	return true
}

func (b *BlockStatusBuffer) release(status BlockStatusEntry) {
	if !status.shouldDeduplicate() {
		return
	}
	b.mu.Lock()
	delete(b.pending, status.key())
	b.mu.Unlock()
}
