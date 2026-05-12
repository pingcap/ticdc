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
	stage        heartbeatpb.BlockStage
}

// BlockStatusBuffer keeps block statuses ordered while coalescing identical
// WAITING and DONE statuses that are still pending locally. The key includes
// the block stage, so WAITING and DONE for the same barrier remain distinct
// queue entries while duplicate pending statuses are collapsed into one.
type BlockStatusBuffer struct {
	queue chan *heartbeatpb.TableSpanBlockStatus

	mu      sync.Mutex
	pending map[blockStatusKey]struct{}
}

// NewBlockStatusBuffer creates a bounded local mailbox for dispatcher block
// statuses. The buffer keeps enqueue order while coalescing identical pending
// WAITING and DONE statuses using the same message based flow for every stage.
func NewBlockStatusBuffer(size int) *BlockStatusBuffer {
	if size <= 0 {
		size = 1
	}
	return &BlockStatusBuffer{
		queue:   make(chan *heartbeatpb.TableSpanBlockStatus, size),
		pending: make(map[blockStatusKey]struct{}),
	}
}

func (b *BlockStatusBuffer) Offer(status *heartbeatpb.TableSpanBlockStatus) {
	if status == nil {
		return
	}
	if !shouldDeduplicateBlockStatus(status) {
		b.queue <- status
		return
	}

	key := newBlockStatusKey(status)
	if !b.reserve(key) {
		return
	}
	b.queue <- status
}

func (b *BlockStatusBuffer) Take(ctx context.Context) *heartbeatpb.TableSpanBlockStatus {
	select {
	case <-ctx.Done():
		return nil
	case status := <-b.queue:
		b.release(status)
		return status
	}
}

func (b *BlockStatusBuffer) TryTake() (*heartbeatpb.TableSpanBlockStatus, bool) {
	select {
	case status := <-b.queue:
		b.release(status)
		return status, true
	default:
		return nil, false
	}
}

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

func (b *BlockStatusBuffer) release(status *heartbeatpb.TableSpanBlockStatus) {
	if !shouldDeduplicateBlockStatus(status) {
		return
	}
	b.mu.Lock()
	delete(b.pending, newBlockStatusKey(status))
	b.mu.Unlock()
}

func newBlockStatusKey(status *heartbeatpb.TableSpanBlockStatus) blockStatusKey {
	return blockStatusKey{
		dispatcherID: common.NewDispatcherIDFromPB(status.ID),
		blockTs:      status.State.BlockTs,
		mode:         status.Mode,
		isSyncPoint:  status.State.IsSyncPoint,
		stage:        status.State.Stage,
	}
}

// newDoneBlockStatus builds the canonical DONE status used by dispatcher
// block event completion paths before it enters the shared deduplication buffer.
func newDoneBlockStatus(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	isSyncPoint bool,
	mode int64,
) *heartbeatpb.TableSpanBlockStatus {
	return &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:   true,
			BlockTs:     blockTs,
			IsSyncPoint: isSyncPoint,
			Stage:       heartbeatpb.BlockStage_DONE,
		},
		Mode: mode,
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

func shouldDeduplicateBlockStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return isWaitingBlockStatus(status) || isDoneBlockStatus(status)
}
