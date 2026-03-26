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

type maintainerRequestDecision int

const (
	// maintainerRequestStale means the request belongs to an older maintainer
	// instance and must never affect the current dispatcher manager state.
	maintainerRequestStale maintainerRequestDecision = iota
	// maintainerRequestRetry means the request belongs to the current maintainer
	// instance and can be treated as an idempotent resend.
	maintainerRequestRetry
	// maintainerRequestTakeover means a newer maintainer instance is taking over
	// ownership through the bootstrap path.
	maintainerRequestTakeover
)

// pendingMessageQueue de-duplicates messages by (changefeedID, messageType) to prevent
// floods of retry messages from blocking or starving other requests.
//
// The queue keeps the first message while it is pending or being processed. Subsequent
// messages with the same key are dropped. This is safe because the sender periodically
// retries until it receives a response.
//
// The queue is also epoch-aware for direct maintainer requests:
// - newer maintainer epochs replace older pending requests for the same key
// - older maintainer epochs are dropped
// - within the same epoch, close removed=true overrides removed=false
//
// This lets a new maintainer instance take over promptly even when the old
// instance still has retries buffered in the queue.
type pendingMessageQueue struct {
	mu      sync.Mutex
	pending map[pendingMessageKey]*messaging.TargetMessage
	queue   *chann.UnlimitedChannel[pendingMessageKey, any]
}

func newPendingMessageQueue() *pendingMessageQueue {
	return &pendingMessageQueue{
		pending: make(map[pendingMessageKey]*messaging.TargetMessage),
		queue:   chann.NewUnlimitedChannelDefault[pendingMessageKey](),
	}
}

// TryEnqueue enqueues the message if there is no pending message with the same key.
// It returns true if the message is accepted, otherwise false.
func (q *pendingMessageQueue) TryEnqueue(key pendingMessageKey, msg *messaging.TargetMessage) bool {
	q.mu.Lock()
	if oldMsg, ok := q.pending[key]; ok {
		if shouldReplacePendingMessage(key, oldMsg, msg) {
			q.pending[key] = msg
			q.mu.Unlock()
			return true
		}
		q.mu.Unlock()
		return false
	}
	q.pending[key] = msg
	q.mu.Unlock()

	q.queue.Push(key)
	return true
}

func shouldReplacePendingMessage(key pendingMessageKey, oldMsg, newMsg *messaging.TargetMessage) bool {
	if oldMsg == nil || newMsg == nil {
		return false
	}
	oldEpoch, oldHasEpoch := getRequestMaintainerEpoch(oldMsg)
	newEpoch, newHasEpoch := getRequestMaintainerEpoch(newMsg)
	if oldHasEpoch && newHasEpoch {
		if newEpoch > oldEpoch {
			return true
		}
		if newEpoch < oldEpoch {
			return false
		}
	}
	if key.msgType != messaging.TypeMaintainerCloseRequest {
		return false
	}

	oldReq, ok1 := oldMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	newReq, ok2 := newMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	if !ok1 || !ok2 {
		return false
	}
	// Only upgrade semantics within the same maintainer instance: allow
	// removed=true to override removed=false.
	return !oldReq.Removed && newReq.Removed
}

// compareBootstrapMaintainerEpoch is intentionally asymmetric:
// bootstrap is the only request that can establish or transfer ownership.
func compareBootstrapMaintainerEpoch(activeEpoch, requestEpoch uint64) maintainerRequestDecision {
	switch {
	case requestEpoch < activeEpoch:
		return maintainerRequestStale
	case requestEpoch == activeEpoch:
		return maintainerRequestRetry
	default:
		return maintainerRequestTakeover
	}
}

// post-bootstrap only applies to the bootstrap sequence started by the active
// maintainer instance. A newer maintainer must restart from bootstrap first.
func shouldAcceptPostBootstrapRequest(activeEpoch, requestEpoch uint64) bool {
	return requestEpoch == activeEpoch
}

// close accepts a newer epoch so removal can still clean up an old dispatcher
// manager before the new maintainer finishes bootstrap on this node.
func shouldAcceptCloseRequest(activeEpoch, requestEpoch uint64) bool {
	return requestEpoch >= activeEpoch
}

func shouldUseStrictMaintainerEpoch(activeEpoch, requestEpoch uint64) bool {
	return activeEpoch != 0 && requestEpoch != 0
}

// getRequestMaintainerEpoch extracts the maintainer instance identity from the
// direct request types protected by the epoch mechanism.
func getRequestMaintainerEpoch(msg *messaging.TargetMessage) (uint64, bool) {
	if msg == nil || len(msg.Message) == 0 {
		return 0, false
	}
	switch req := msg.Message[0].(type) {
	case *heartbeatpb.MaintainerBootstrapRequest:
		if req.MaintainerEpoch == 0 {
			return 0, false
		}
		return req.MaintainerEpoch, true
	case *heartbeatpb.MaintainerPostBootstrapRequest:
		if req.MaintainerEpoch == 0 {
			return 0, false
		}
		return req.MaintainerEpoch, true
	case *heartbeatpb.MaintainerCloseRequest:
		if req.MaintainerEpoch == 0 {
			return 0, false
		}
		return req.MaintainerEpoch, true
	default:
		return 0, false
	}
}

// Pop blocks until a key is available or the queue is closed.
// The returned key is removed from the queue but remains pending until Done is called.
func (q *pendingMessageQueue) Pop() (pendingMessageKey, bool) {
	return q.queue.Get()
}

func (q *pendingMessageQueue) Get(key pendingMessageKey) *messaging.TargetMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending[key]
}

func (q *pendingMessageQueue) Done(key pendingMessageKey) {
	q.mu.Lock()
	delete(q.pending, key)
	q.mu.Unlock()
}

func (q *pendingMessageQueue) Close() {
	q.queue.Close()
}
