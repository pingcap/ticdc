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

package dispatcherorchestrator

import (
	"sync"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/utils/chann"
)

type pendingMessageKey struct {
	changefeedID common.ChangeFeedID
	msgType      messaging.IOType
}

type pendingMessageState struct {
	// queued stores the next request waiting to be processed for this key.
	queued *messaging.TargetMessage
	// inFlight stores the request currently being processed for this key.
	inFlight *messaging.TargetMessage
}

// pendingMessageQueue de-duplicates messages by (changefeedID, messageType) to prevent
// floods of retry messages from blocking or starving other requests.
//
// The queue keeps at most one queued request and one in-flight request for each key.
// Subsequent messages with the same key are dropped unless they strengthen the queued
// or in-flight close semantics from removed=false to removed=true.
//
// For MaintainerCloseRequest, we treat removed=true as stronger semantics than removed=false.
// If an in-flight removed=false request exists, a later removed=true request is queued for
// the next round instead of overwriting the request the worker is already executing.
type pendingMessageQueue struct {
	mu      sync.Mutex
	pending map[pendingMessageKey]*pendingMessageState
	queue   *chann.UnlimitedChannel[pendingMessageKey, any]
}

func newPendingMessageQueue() *pendingMessageQueue {
	return &pendingMessageQueue{
		pending: make(map[pendingMessageKey]*pendingMessageState),
		queue:   chann.NewUnlimitedChannelDefault[pendingMessageKey](),
	}
}

// TryEnqueue enqueues the message if there is no pending message with the same key.
// It returns true if the message is accepted, otherwise false.
func (q *pendingMessageQueue) TryEnqueue(key pendingMessageKey, msg *messaging.TargetMessage) bool {
	q.mu.Lock()
	state, ok := q.pending[key]
	if !ok {
		state = &pendingMessageState{}
		q.pending[key] = state
	}

	if state.queued != nil {
		if shouldReplacePendingMessage(key, state.queued, msg) {
			state.queued = msg
			q.mu.Unlock()
			return true
		}
		q.mu.Unlock()
		return false
	}

	if state.inFlight != nil {
		if shouldReplacePendingMessage(key, state.inFlight, msg) {
			state.queued = msg
			q.mu.Unlock()
			q.queue.Push(key)
			return true
		}
		q.mu.Unlock()
		return false
	}

	state.queued = msg
	q.mu.Unlock()

	q.queue.Push(key)
	return true
}

func shouldReplacePendingMessage(key pendingMessageKey, oldMsg, newMsg *messaging.TargetMessage) bool {
	if key.msgType != messaging.TypeMaintainerCloseRequest {
		return false
	}
	if oldMsg == nil || newMsg == nil {
		return false
	}
	if len(oldMsg.Message) == 0 || len(newMsg.Message) == 0 {
		return false
	}
	oldReq, ok1 := oldMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	newReq, ok2 := newMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	if !ok1 || !ok2 {
		return false
	}
	// Only upgrade semantics: allow removed=true to override removed=false.
	return !oldReq.Removed && newReq.Removed
}

// Pop blocks until a key is available or the queue is closed.
// The returned key is removed from the queue and promoted to in-flight until Done is called.
func (q *pendingMessageQueue) Pop() (pendingMessageKey, bool) {
	key, ok := q.queue.Get()
	if !ok {
		return pendingMessageKey{}, false
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	state := q.pending[key]
	if state == nil || state.queued == nil {
		return pendingMessageKey{}, false
	}
	state.inFlight = state.queued
	state.queued = nil
	return key, true
}

func (q *pendingMessageQueue) Get(key pendingMessageKey) *messaging.TargetMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	state := q.pending[key]
	if state == nil {
		return nil
	}
	if state.inFlight != nil {
		return state.inFlight
	}
	return state.queued
}

func (q *pendingMessageQueue) Done(key pendingMessageKey) {
	q.mu.Lock()
	state := q.pending[key]
	if state == nil {
		q.mu.Unlock()
		return
	}
	state.inFlight = nil
	if state.queued == nil {
		delete(q.pending, key)
	}
	q.mu.Unlock()
}

func (q *pendingMessageQueue) Close() {
	q.queue.Close()
}
