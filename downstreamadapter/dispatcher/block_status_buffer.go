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
)

type blockStatusKey struct {
	dispatcherID common.DispatcherID
	blockTs      uint64
	mode         int64
	isSyncPoint  bool
}

type blockStatusQueueEntry struct {
	status  *heartbeatpb.TableSpanBlockStatus
	doneKey *blockStatusKey
}

// BlockStatusBuffer keeps block statuses ordered while coalescing identical
// WAITING and DONE statuses that are still pending locally. Other statuses keep
// the original protobuf object and ordering.
type BlockStatusBuffer struct {
	queue chan blockStatusQueueEntry

	mu             sync.Mutex
	pendingDone    map[blockStatusKey]struct{}
	pendingWaiting map[blockStatusKey]struct{}
}

// NewBlockStatusBuffer creates a bounded local mailbox for dispatcher block
// statuses. The buffer keeps enqueue order while coalescing identical pending
// WAITING and DONE statuses before protobuf materialization.
func NewBlockStatusBuffer(size int) *BlockStatusBuffer {
	if size <= 0 {
		size = 1
	}
	return &BlockStatusBuffer{
		queue:          make(chan blockStatusQueueEntry, size),
		pendingDone:    make(map[blockStatusKey]struct{}),
		pendingWaiting: make(map[blockStatusKey]struct{}),
	}
}

func (b *BlockStatusBuffer) Offer(status *heartbeatpb.TableSpanBlockStatus) {
	if status == nil {
		return
	}
	if isWaitingBlockStatus(status) {
		key := newBlockStatusKey(status)
		if !b.reserveWaiting(key) {
			return
		}
		b.queue <- blockStatusQueueEntry{status: status}
		return
	}
	if !isDoneBlockStatus(status) {
		b.queue <- blockStatusQueueEntry{status: status}
		return
	}

	key := newBlockStatusKey(status)
	if !b.reserveDone(key) {
		return
	}
	b.queue <- blockStatusQueueEntry{doneKey: &key}
}

func (b *BlockStatusBuffer) OfferDone(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	isSyncPoint bool,
	mode int64,
) {
	key := blockStatusKey{
		dispatcherID: dispatcherID,
		blockTs:      blockTs,
		mode:         mode,
		isSyncPoint:  isSyncPoint,
	}
	if !b.reserveDone(key) {
		return
	}
	b.queue <- blockStatusQueueEntry{doneKey: &key}
}

func (b *BlockStatusBuffer) Take(ctx context.Context) *heartbeatpb.TableSpanBlockStatus {
	select {
	case <-ctx.Done():
		return nil
	case entry := <-b.queue:
		return b.materialize(entry)
	}
}

func (b *BlockStatusBuffer) TryTake() (*heartbeatpb.TableSpanBlockStatus, bool) {
	select {
	case entry := <-b.queue:
		return b.materialize(entry), true
	default:
		return nil, false
	}
}

func (b *BlockStatusBuffer) Len() int {
	return len(b.queue)
}

func (b *BlockStatusBuffer) reserveWaiting(key blockStatusKey) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pendingWaiting[key]; ok {
		return false
	}
	b.pendingWaiting[key] = struct{}{}
	return true
}

func (b *BlockStatusBuffer) reserveDone(key blockStatusKey) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pendingDone[key]; ok {
		return false
	}
	b.pendingDone[key] = struct{}{}
	return true
}

func (b *BlockStatusBuffer) materialize(entry blockStatusQueueEntry) *heartbeatpb.TableSpanBlockStatus {
	if entry.status != nil {
		if isWaitingBlockStatus(entry.status) {
			key := newBlockStatusKey(entry.status)
			b.mu.Lock()
			delete(b.pendingWaiting, key)
			b.mu.Unlock()
		}
		return entry.status
	}

	key := *entry.doneKey
	b.mu.Lock()
	delete(b.pendingDone, key)
	b.mu.Unlock()

	return &heartbeatpb.TableSpanBlockStatus{
		ID: key.dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:   true,
			BlockTs:     key.blockTs,
			IsSyncPoint: key.isSyncPoint,
			Stage:       heartbeatpb.BlockStage_DONE,
		},
		Mode: key.mode,
	}
}

func newBlockStatusKey(status *heartbeatpb.TableSpanBlockStatus) blockStatusKey {
	return blockStatusKey{
		dispatcherID: common.NewDispatcherIDFromPB(status.ID),
		blockTs:      status.State.BlockTs,
		mode:         status.Mode,
		isSyncPoint:  status.State.IsSyncPoint,
	}
}

func isWaitingBlockStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		status.State.Stage == heartbeatpb.BlockStage_WAITING
}

func isDoneBlockStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		status.State.Stage == heartbeatpb.BlockStage_DONE
}
