// Copyright 2025 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestPendingMessageQueue_TryEnqueueDropsDuplicatesOnlyWhileQueued(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerBootstrapRequest,
	}
	msg := &messaging.TargetMessage{Type: messaging.TypeMaintainerBootstrapRequest}

	require.True(t, q.TryEnqueue(key, msg))
	require.False(t, q.TryEnqueue(key, msg))

	poppedKey, poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)
	require.Same(t, msg, poppedMsg)

	// Once the request is popped, allow one queued retry for the next round.
	require.True(t, q.TryEnqueue(key, msg))
	require.False(t, q.TryEnqueue(key, msg))

	nextKey, nextMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, nextKey)
	require.Same(t, msg, nextMsg)

	require.True(t, q.TryEnqueue(key, msg))
}

func TestPendingMessageQueue_OrderPreservedAcrossKeys(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID1 := common.NewChangeFeedIDWithName("cf1", "default")
	cfID2 := common.NewChangeFeedIDWithName("cf2", "default")

	key1 := pendingMessageKey{changefeedID: cfID1, msgType: messaging.TypeMaintainerBootstrapRequest}
	key2 := pendingMessageKey{changefeedID: cfID2, msgType: messaging.TypeMaintainerCloseRequest}

	require.True(t, q.TryEnqueue(key1, &messaging.TargetMessage{Type: key1.msgType}))
	require.True(t, q.TryEnqueue(key2, &messaging.TargetMessage{Type: key2.msgType}))

	poppedKey, poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key1, poppedKey)
	require.Equal(t, key1.msgType, poppedMsg.Type)

	poppedKey, poppedMsg, ok = q.Pop()
	require.True(t, ok)
	require.Equal(t, key2, poppedKey)
	require.Equal(t, key2.msgType, poppedMsg.Type)
}

func TestPendingMessageQueue_PopReturnsAfterClose(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	doneCh := make(chan bool, 1)
	go func() {
		_, _, ok := q.Pop()
		doneCh <- ok
	}()

	time.Sleep(10 * time.Millisecond)
	q.Close()

	select {
	case ok := <-doneCh:
		require.False(t, ok)
	case <-time.After(time.Second):
		require.FailNow(t, "Pop did not return after context cancel")
	}
}

func TestPendingMessageQueue_PopSkipsStaleKey(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	staleKey := pendingMessageKey{
		changefeedID: common.NewChangeFeedIDWithName("stale", "default"),
		msgType:      messaging.TypeMaintainerBootstrapRequest,
	}
	validKey := pendingMessageKey{
		changefeedID: common.NewChangeFeedIDWithName("valid", "default"),
		msgType:      messaging.TypeMaintainerBootstrapRequest,
	}
	validMsg := &messaging.TargetMessage{Type: messaging.TypeMaintainerBootstrapRequest}

	q.queue.Push(staleKey)
	require.True(t, q.TryEnqueue(validKey, validMsg))

	poppedKey, poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, validKey, poppedKey)
	require.Same(t, validMsg, poppedMsg)
}

func TestPendingMessageQueue_CloseRequestRemovedTrueOverridesPendingFalse(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerCloseRequest,
	}

	msgFalse := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB(), Removed: false},
	)
	msgTrue := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB(), Removed: true},
	)

	require.True(t, q.TryEnqueue(key, msgFalse))
	require.True(t, q.TryEnqueue(key, msgTrue))

	poppedKey, poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)
	require.NotNil(t, poppedMsg)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.True(t, req.Removed)
}

func TestPendingMessageQueue_CloseRequestUpgradeAfterPopKeepsReturnedMessageStable(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerCloseRequest,
	}

	msgFalse := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB(), Removed: false},
	)
	msgTrue := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB(), Removed: true},
	)

	require.True(t, q.TryEnqueue(key, msgFalse))
	poppedKey, poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)
	require.NotNil(t, poppedMsg)

	require.True(t, q.TryEnqueue(key, msgTrue))
	req2 := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.False(t, req2.Removed)
}

func TestPendingMessageQueue_CloseRequestUpgradeAfterPopRequeuesNextRound(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerCloseRequest,
	}

	msgFalse := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB(), Removed: false},
	)
	msgTrue := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB(), Removed: true},
	)

	require.True(t, q.TryEnqueue(key, msgFalse))

	poppedKey, poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)

	require.NotNil(t, poppedMsg)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.False(t, req.Removed)

	require.True(t, q.TryEnqueue(key, msgTrue))

	type popResult struct {
		key pendingMessageKey
		msg *messaging.TargetMessage
		ok  bool
	}
	resultCh := make(chan popResult, 1)
	go func() {
		nextKey, nextMsg, nextOK := q.Pop()
		resultCh <- popResult{key: nextKey, msg: nextMsg, ok: nextOK}
	}()

	select {
	case result := <-resultCh:
		require.True(t, result.ok)
		require.Equal(t, key, result.key)
		require.NotNil(t, result.msg)
		nextReq := result.msg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
		require.True(t, nextReq.Removed)
	case <-time.After(time.Second):
		q.Close()
		require.FailNow(t, "upgraded close request was not requeued after the first pop")
	}
}

func TestGetPendingMessageKey_SupportedTypes(t *testing.T) {
	t.Parallel()

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	from := node.ID("from")

	bootstrap := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{ChangefeedID: cfID.ToPB()},
	)
	bootstrap.From = from
	key, ok := getPendingMessageKey(bootstrap)
	require.True(t, ok)
	require.Equal(t, pendingMessageKey{changefeedID: cfID, msgType: messaging.TypeMaintainerBootstrapRequest}, key)

	postBootstrap := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerPostBootstrapRequest{ChangefeedID: cfID.ToPB()},
	)
	postBootstrap.From = from
	key, ok = getPendingMessageKey(postBootstrap)
	require.True(t, ok)
	require.Equal(t, pendingMessageKey{changefeedID: cfID, msgType: messaging.TypeMaintainerPostBootstrapRequest}, key)

	closeReq := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB()},
	)
	closeReq.From = from
	key, ok = getPendingMessageKey(closeReq)
	require.True(t, ok)
	require.Equal(t, pendingMessageKey{changefeedID: cfID, msgType: messaging.TypeMaintainerCloseRequest}, key)
}
