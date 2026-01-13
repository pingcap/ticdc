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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/utils/chann"
)

type pendingMessageKey struct {
	changefeedID common.ChangeFeedID
	msgType      messaging.IOType
}

// pendingMessageQueue de-duplicates messages by (changefeedID, messageType) to prevent
// floods of retry messages from blocking or starving other requests.
//
// The queue keeps the first message while it is pending or being processed. Subsequent
// messages with the same key are dropped. This is safe because the sender periodically
// retries until it receives a response.
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
	if _, ok := q.pending[key]; ok {
		q.mu.Unlock()
		return false
	}
	q.pending[key] = msg
	q.mu.Unlock()

	q.queue.Push(key)
	return true
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
	defer q.mu.Unlock()
	delete(q.pending, key)
}

func (q *pendingMessageQueue) Close() {
	q.queue.Close()
}
