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
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/chann"
)

type pendingMessageKey struct {
	changefeedID common.ChangeFeedID
	msgType      messaging.IOType
}

type pendingMessageEntry struct {
	msg          *messaging.TargetMessage
	inFlight     bool
	needsRequeue bool
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

type maintainerRequestKind int

const (
	maintainerBootstrapRequest maintainerRequestKind = iota
	maintainerPostBootstrapRequest
	maintainerCloseRequest
)

type maintainerRequestAdmission struct {
	accept      bool
	updateOwner bool
}

// pendingMessageQueue de-duplicates messages by (changefeedID, messageType) to prevent
// floods of retry messages from blocking or starving other requests.
//
// The queue keeps the first message while it is pending or being processed. Subsequent
// messages with the same key are dropped. This is safe because the sender periodically
// retries until it receives a response.
//
// Queueing does not participate in ownership transfer. The request handlers
// apply the epoch checks, while the queue only suppresses duplicate retries for
// the current in-flight request.
//
// For MaintainerCloseRequest, removed=true still overrides removed=false so a
// removal retry does not lose the stronger cleanup intent.
type pendingMessageQueue struct {
	mu      sync.Mutex
	pending map[pendingMessageKey]*pendingMessageEntry
	queue   *chann.UnlimitedChannel[pendingMessageKey, any]
}

func newPendingMessageQueue() *pendingMessageQueue {
	return &pendingMessageQueue{
		pending: make(map[pendingMessageKey]*pendingMessageEntry),
		queue:   chann.NewUnlimitedChannelDefault[pendingMessageKey](),
	}
}

// TryEnqueue enqueues the message if there is no pending message with the same key.
// It returns true if the message is accepted, otherwise false.
func (q *pendingMessageQueue) TryEnqueue(key pendingMessageKey, msg *messaging.TargetMessage) bool {
	q.mu.Lock()
	if entry, ok := q.pending[key]; ok {
		if shouldReplacePendingMessage(key, entry.msg, msg) {
			entry.msg = msg
			if entry.inFlight {
				entry.needsRequeue = true
			}
			q.mu.Unlock()
			return true
		}
		q.mu.Unlock()
		return false
	}
	q.pending[key] = &pendingMessageEntry{msg: msg}
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

// admitMaintainerRequest centralizes ownership admission for maintainer
// requests. It decides whether the request can proceed and whether it must
// update the active maintainer identity on the dispatcher manager.
func admitMaintainerRequest(
	kind maintainerRequestKind,
	activeMaintainer, requestMaintainer node.ID,
	activeEpoch, requestEpoch uint64,
) maintainerRequestAdmission {
	if activeEpoch != 0 && requestEpoch != 0 {
		// Once both sides carry a real maintainer epoch, ownership is fully
		// ordered by epoch instead of arrival order. Each request kind still
		// has its own admission rule on top of that ordering.
		switch kind {
		case maintainerBootstrapRequest:
			// Bootstrap is the only request that may transfer ownership.
			switch compareBootstrapMaintainerEpoch(activeEpoch, requestEpoch) {
			case maintainerRequestStale:
				return maintainerRequestAdmission{}
			case maintainerRequestRetry:
				// An equal epoch is only a retry when it comes from the same
				// maintainer instance that already owns the manager.
				if activeMaintainer != requestMaintainer {
					return maintainerRequestAdmission{}
				}
				return maintainerRequestAdmission{accept: true}
			case maintainerRequestTakeover:
				return maintainerRequestAdmission{accept: true, updateOwner: true}
			}
		case maintainerPostBootstrapRequest:
			// Post-bootstrap is tied to the bootstrap attempt that created the
			// partially initialized state. A newer maintainer must restart from
			// bootstrap instead of reusing another instance's intermediate state.
			if requestEpoch != activeEpoch || activeMaintainer != requestMaintainer {
				return maintainerRequestAdmission{}
			}
			return maintainerRequestAdmission{accept: true}
		case maintainerCloseRequest:
			// Close accepts a newer epoch so a takeover can still clean up the
			// old manager before the new owner finishes bootstrap locally.
			switch {
			case requestEpoch < activeEpoch:
				return maintainerRequestAdmission{}
			case requestEpoch > activeEpoch:
				return maintainerRequestAdmission{accept: true, updateOwner: true}
			case activeMaintainer != requestMaintainer:
				return maintainerRequestAdmission{}
			default:
				return maintainerRequestAdmission{accept: true}
			}
		}
		return maintainerRequestAdmission{}
	}

	// Zero epoch means at least one side still follows the legacy protocol
	// during rolling upgrade. Keep compatibility, but do not let zero-epoch
	// traffic take ownership back once a strict owner is already established.
	switch kind {
	case maintainerBootstrapRequest:
		if requestEpoch == 0 {
			if activeEpoch != 0 && activeMaintainer != requestMaintainer {
				return maintainerRequestAdmission{}
			}
			// Before any strict epoch exists on this manager, bootstrap still
			// defines ownership by the latest compatible sender.
			if activeEpoch == 0 && activeMaintainer != requestMaintainer {
				return maintainerRequestAdmission{accept: true, updateOwner: true}
			}
			return maintainerRequestAdmission{accept: true}
		}
		// A non-zero bootstrap request from a newer binary can establish the
		// strict owner even when the current manager was created by legacy code.
		if activeMaintainer != requestMaintainer || activeEpoch != requestEpoch {
			return maintainerRequestAdmission{accept: true, updateOwner: true}
		}
		return maintainerRequestAdmission{accept: true}
	case maintainerPostBootstrapRequest:
		// Legacy post-bootstrap cannot prove sequence ownership with epoch, so
		// the best compatible rule is to reject only obvious cross-maintainer use.
		if (activeEpoch != 0 || requestEpoch != 0) && activeMaintainer != requestMaintainer {
			return maintainerRequestAdmission{}
		}
		return maintainerRequestAdmission{accept: true}
	case maintainerCloseRequest:
		if requestEpoch == 0 {
			if activeEpoch != 0 && activeMaintainer != requestMaintainer {
				return maintainerRequestAdmission{}
			}
			return maintainerRequestAdmission{accept: true}
		}
		// A non-zero close request is allowed to move ownership forward so the
		// cleanup path does not depend on bootstrap finishing first.
		return maintainerRequestAdmission{accept: true, updateOwner: true}
	default:
		return maintainerRequestAdmission{}
	}
}

// Pop blocks until a key is available or the queue is closed.
// The returned key is removed from the queue but remains pending until Done is called.
func (q *pendingMessageQueue) Pop() (pendingMessageKey, bool) {
	key, ok := q.queue.Get()
	if !ok {
		return pendingMessageKey{}, false
	}

	q.mu.Lock()
	if entry, exists := q.pending[key]; exists {
		entry.inFlight = true
	}
	q.mu.Unlock()

	return key, true
}

func (q *pendingMessageQueue) Get(key pendingMessageKey) *messaging.TargetMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	entry, ok := q.pending[key]
	if !ok {
		return nil
	}
	return entry.msg
}

func (q *pendingMessageQueue) Done(key pendingMessageKey) {
	q.mu.Lock()
	entry, ok := q.pending[key]
	if !ok {
		q.mu.Unlock()
		return
	}
	if entry.needsRequeue {
		entry.inFlight = false
		entry.needsRequeue = false
		q.mu.Unlock()
		q.queue.Push(key)
		return
	}
	delete(q.pending, key)
	q.mu.Unlock()
}

func (q *pendingMessageQueue) Close() {
	q.queue.Close()
}
