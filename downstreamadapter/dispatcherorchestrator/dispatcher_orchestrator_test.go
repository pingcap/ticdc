// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

func TestPendingMessageQueue_TryEnqueueDropsDuplicatesUntilDone(t *testing.T) {
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

	poppedKey, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)

	// The key remains pending while being processed.
	require.False(t, q.TryEnqueue(key, msg))

	q.Done(key)
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

	poppedKey, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key1, poppedKey)
	q.Done(poppedKey)

	poppedKey, ok = q.Pop()
	require.True(t, ok)
	require.Equal(t, key2, poppedKey)
	q.Done(poppedKey)
}

func TestPendingMessageQueue_PopReturnsAfterClose(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	doneCh := make(chan bool, 1)
	go func() {
		_, ok := q.Pop()
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

	poppedKey, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)
	poppedMsg := q.Get(poppedKey)
	require.NotNil(t, poppedMsg)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.True(t, req.Removed)
	q.Done(key)
}

func TestPendingMessageQueue_CloseRequestUpgradeBetweenPopAndGet(t *testing.T) {
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
	poppedKey, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)

	require.True(t, q.TryEnqueue(key, msgTrue))
	poppedMsg := q.Get(poppedKey)
	require.NotNil(t, poppedMsg)
	req2 := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.True(t, req2.Removed)
	q.Done(key)
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
