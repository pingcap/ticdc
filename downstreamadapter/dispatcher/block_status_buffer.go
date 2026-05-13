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

// blockStatusKey identifies a pending WAITING or DONE status in the local
// mailbox. Stage is part of the key so the same block event can keep both
// WAITING and DONE in order when they are pending at the same time.
type blockStatusKey struct {
	dispatcherID common.DispatcherID
	blockTs      uint64
	mode         int64
	isSyncPoint  bool
	stage        heartbeatpb.BlockStage
}

// BlockStatusBuffer keeps block statuses ordered while coalescing identical
// WAITING and DONE statuses that are still pending locally.
//
// The key stays reserved until the status is drained by the dispatcher manager.
// This collapses repeated resends in the local mailbox while preserving the
// original enqueue order for the first pending status of each key.
type BlockStatusBuffer struct {
	queue chan *heartbeatpb.TableSpanBlockStatus

	mu      sync.Mutex
	pending map[blockStatusKey]struct{}
}

// NewBlockStatusBuffer creates a bounded local mailbox for dispatcher block
// statuses. The buffer keeps enqueue order while coalescing identical pending
// WAITING and DONE statuses.
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
	key, ok := newDeduplicatedBlockStatusKey(status)
	if ok && !b.reserve(key) {
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
	key, ok := newDeduplicatedBlockStatusKey(status)
	if !ok {
		return
	}
	b.mu.Lock()
	delete(b.pending, key)
	b.mu.Unlock()
}

// newDeduplicatedBlockStatusKey returns the dedupe key for statuses whose
// repeated copies are expected during resend or repeated maintainer actions.
func newDeduplicatedBlockStatusKey(status *heartbeatpb.TableSpanBlockStatus) (blockStatusKey, bool) {
	if !shouldDeduplicateBlockStatus(status) {
		return blockStatusKey{}, false
	}
	return blockStatusKey{
		dispatcherID: common.NewDispatcherIDFromPB(status.ID),
		blockTs:      status.State.BlockTs,
		mode:         status.Mode,
		isSyncPoint:  status.State.IsSyncPoint,
		stage:        status.State.Stage,
	}, true
}

func shouldDeduplicateBlockStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		(status.State.Stage == heartbeatpb.BlockStage_WAITING ||
			status.State.Stage == heartbeatpb.BlockStage_DONE)
}
