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
	"container/list"
	"context"
	"sync"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

// blockStatusKey is the local dedupe identity. WAITING and DONE for the same
// barrier intentionally use different keys because stage is part of the state
// machine that maintainer must observe in order.
type blockStatusKey struct {
	dispatcherID common.DispatcherID
	blockTs      uint64
	mode         int64
	isSyncPoint  bool
	stage        heartbeatpb.BlockStage
}

// blockStatusQueueEntry stores either a ready-to-send WAITING/NONE protobuf or
// a minimal DONE key that will be materialized when the manager drains it.
type blockStatusQueueEntry struct {
	status  *heartbeatpb.TableSpanBlockStatus
	doneKey *blockStatusKey
}

type retryBlockStatus struct {
	status   *heartbeatpb.TableSpanBlockStatus
	canceled <-chan struct{}
	element  *list.Element
}

// BlockStatusBuffer keeps block statuses ordered while coalescing identical
// pending WAITING and DONE statuses in dispatcher-local memory.
//
// WAITING and NONE statuses keep a single protobuf object that is reused by the
// resend path. DONE uses a dedicated minimal-key path so duplicate DONE reports
// are suppressed before allocating another protobuf object.
type BlockStatusBuffer struct {
	queue chan blockStatusQueueEntry

	mu            sync.Mutex
	pending       map[blockStatusKey]struct{}
	retries       *list.List
	retryByStatus map[*heartbeatpb.TableSpanBlockStatus]*retryBlockStatus
	retryWake     chan struct{}
	retryStop     chan struct{}
	retryDone     chan struct{}
	retrySending  bool
	closed        bool
}

// NewBlockStatusBuffer creates a bounded local mailbox for dispatcher block
// statuses.
func NewBlockStatusBuffer(size int) *BlockStatusBuffer {
	if size <= 0 {
		size = 1
	}
	return &BlockStatusBuffer{
		queue:         make(chan blockStatusQueueEntry, size),
		pending:       make(map[blockStatusKey]struct{}),
		retries:       list.New(),
		retryByStatus: make(map[*heartbeatpb.TableSpanBlockStatus]*retryBlockStatus),
		retryWake:     make(chan struct{}, 1),
		retryStop:     make(chan struct{}),
	}
}

// OfferStatus enqueues a protobuf block status. WAITING statuses are deduplicated
// while pending. NONE keeps its original protobuf object and ordering.
func (b *BlockStatusBuffer) OfferStatus(status *heartbeatpb.TableSpanBlockStatus) {
	if status == nil {
		return
	}
	if isDoneBlockStatus(status) {
		b.offerDoneKey(newBlockStatusKey(status))
		return
	}
	if !isWaitingBlockStatus(status) {
		b.queue <- blockStatusQueueEntry{status: status}
		return
	}

	key := newBlockStatusKey(status)
	if !b.reserve(key) {
		return
	}
	b.queue <- blockStatusQueueEntry{status: status}
}

// offerRetryStatus queues a full-buffer retry for one dedicated sender instead
// of blocking a shared pool worker. The sender joins the original channel's
// send queue, so new status producers cannot repeatedly overtake the retry.
func (b *BlockStatusBuffer) offerRetryStatus(status *heartbeatpb.TableSpanBlockStatus, canceled <-chan struct{}) bool {
	if status == nil {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-canceled:
		return false
	default:
	}
	if b.closed {
		return false
	}
	if _, ok := b.retryByStatus[status]; ok {
		return false
	}

	if isWaitingBlockStatus(status) {
		key := newBlockStatusKey(status)
		if _, ok := b.pending[key]; ok {
			return true
		}
	}

	if !b.retrySending && b.retries.Len() == 0 {
		select {
		case b.queue <- blockStatusQueueEntry{status: status}:
			if isWaitingBlockStatus(status) {
				b.pending[newBlockStatusKey(status)] = struct{}{}
			}
			return true
		default:
		}
	}

	if isWaitingBlockStatus(status) {
		b.pending[newBlockStatusKey(status)] = struct{}{}
	}
	retry := &retryBlockStatus{status: status, canceled: canceled}
	retry.element = b.retries.PushBack(retry)
	b.retryByStatus[status] = retry
	if b.retryDone == nil {
		b.retryDone = make(chan struct{})
		go b.sendRetries()
	}
	select {
	case b.retryWake <- struct{}{}:
	default:
	}
	return false
}

func (b *BlockStatusBuffer) sendRetries() {
	defer close(b.retryDone)
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return
		}
		front := b.retries.Front()
		if front == nil {
			b.mu.Unlock()
			select {
			case <-b.retryWake:
			case <-b.retryStop:
				return
			}
			continue
		}
		retry := front.Value.(*retryBlockStatus)
		b.retries.Remove(front)
		retry.element = nil
		b.retrySending = true
		b.mu.Unlock()

		sent := false
		select {
		case <-retry.canceled:
		case <-b.retryStop:
			return
		default:
			select {
			case b.queue <- blockStatusQueueEntry{status: retry.status}:
				sent = true
			case <-retry.canceled:
			case <-b.retryStop:
				return
			}
		}

		b.mu.Lock()
		delete(b.retryByStatus, retry.status)
		b.retrySending = false
		if !sent && isWaitingBlockStatus(retry.status) {
			delete(b.pending, newBlockStatusKey(retry.status))
		}
		b.mu.Unlock()
	}
}

func (b *BlockStatusBuffer) cancelRetryStatus(status *heartbeatpb.TableSpanBlockStatus) {
	b.mu.Lock()
	defer b.mu.Unlock()
	retry := b.retryByStatus[status]
	if retry == nil || retry.element == nil {
		return
	}
	b.retries.Remove(retry.element)
	delete(b.retryByStatus, status)
	if isWaitingBlockStatus(status) {
		delete(b.pending, newBlockStatusKey(status))
	}
}

// Close stops the dedicated retry sender when its dispatcher manager closes.
func (b *BlockStatusBuffer) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	close(b.retryStop)
	done := b.retryDone
	b.mu.Unlock()
	if done != nil {
		<-done
	}
}

// OfferDone enqueues a DONE status via the dedicated minimal-key path. Duplicate
// DONE statuses are suppressed before protobuf allocation.
func (b *BlockStatusBuffer) OfferDone(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	isSyncPoint bool,
	mode int64,
) {
	b.offerDoneKey(blockStatusKey{
		dispatcherID: dispatcherID,
		blockTs:      blockTs,
		mode:         mode,
		isSyncPoint:  isSyncPoint,
		stage:        heartbeatpb.BlockStage_DONE,
	})
}

// Take waits for the next entry and materializes it as a protobuf message.
// Returning nil means the context was canceled before an entry was available.
func (b *BlockStatusBuffer) Take(ctx context.Context) *heartbeatpb.TableSpanBlockStatus {
	select {
	case <-ctx.Done():
		return nil
	case entry := <-b.queue:
		return b.materialize(entry)
	}
}

// TryTake returns the next ready entry without blocking.
func (b *BlockStatusBuffer) TryTake() (*heartbeatpb.TableSpanBlockStatus, bool) {
	select {
	case entry := <-b.queue:
		return b.materialize(entry), true
	default:
		return nil, false
	}
}

// Len returns the number of locally queued entries. It is used only for metrics.
func (b *BlockStatusBuffer) Len() int {
	return len(b.queue)
}

// offerDoneKey keeps the DONE fast path lightweight: duplicates are filtered by
// key before another protobuf object is allocated.
func (b *BlockStatusBuffer) offerDoneKey(key blockStatusKey) {
	if !b.reserve(key) {
		return
	}
	b.queue <- blockStatusQueueEntry{doneKey: &key}
}

// reserve records a pending WAITING/DONE key until the corresponding entry is
// drained from the local queue.
func (b *BlockStatusBuffer) reserve(key blockStatusKey) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[key]; ok {
		return false
	}
	b.pending[key] = struct{}{}
	return true
}

// release makes a dedupe key eligible again after the manager has taken its
// representative entry.
func (b *BlockStatusBuffer) release(key blockStatusKey) {
	b.mu.Lock()
	delete(b.pending, key)
	b.mu.Unlock()
}

// materialize converts the queued representation back to a protobuf message and
// releases the corresponding dedupe key exactly once when the entry is drained.
func (b *BlockStatusBuffer) materialize(entry blockStatusQueueEntry) *heartbeatpb.TableSpanBlockStatus {
	if entry.status != nil {
		if isWaitingBlockStatus(entry.status) {
			b.release(newBlockStatusKey(entry.status))
		}
		return entry.status
	}

	key := *entry.doneKey
	b.release(key)
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

// newBlockStatusKey extracts the dedupe identity shared by the local mailbox
// and the request queue.
func newBlockStatusKey(status *heartbeatpb.TableSpanBlockStatus) blockStatusKey {
	return blockStatusKey{
		dispatcherID: common.NewDispatcherIDFromPB(status.ID),
		blockTs:      status.State.BlockTs,
		mode:         status.Mode,
		isSyncPoint:  status.State.IsSyncPoint,
		stage:        status.State.Stage,
	}
}

// isWaitingBlockStatus identifies the only protobuf form that participates in
// local pending dedupe while still reusing a full payload object.
func isWaitingBlockStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		status.State.Stage == heartbeatpb.BlockStage_WAITING
}

// isDoneBlockStatus identifies statuses that should be redirected to the
// minimal-key fast path before another protobuf is kept in memory.
func isDoneBlockStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		status.State.Stage == heartbeatpb.BlockStage_DONE
}
