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
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func newTestDispatcherManagerForEpoch(
	t *testing.T,
	changefeedID common.ChangeFeedID,
	maintainerID node.ID,
	withTableTrigger bool,
) (*dispatchermanager.DispatcherManager, *config.ChangefeedConfig, *heartbeatpb.DispatcherID) {
	t.Helper()

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	appcontext.SetService(appcontext.HeartbeatCollector, dispatchermanager.NewHeartBeatCollector(node.ID("dispatcher")))
	appcontext.SetService(appcontext.EventCollector, eventcollector.New(node.ID("dispatcher")))

	info := &config.ChangeFeedInfo{
		ChangefeedID: changefeedID,
		SinkURI:      "blackhole://",
		Config:       config.GetDefaultReplicaConfig(),
	}
	cfConfig := info.ToChangefeedConfig()
	cfConfig.Epoch = 42

	var tableTriggerID *heartbeatpb.DispatcherID
	if withTableTrigger {
		tableTriggerID = common.NewDispatcherID().ToPB()
	}

	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		changefeedID,
		cfConfig,
		tableTriggerID,
		nil,
		1,
		maintainerID,
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { manager.TryClose(true) })
	return manager, cfConfig, tableTriggerID
}

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

func TestPendingMessageQueue_KeepsFirstBootstrapRequestUntilCurrentAttemptFinishes(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerBootstrapRequest,
	}

	oldMsg := messaging.NewSingleTargetMessage(
		node.ID("old"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{
			ChangefeedID:    cfID.ToPB(),
			MaintainerEpoch: 10,
		},
	)
	newMsg := messaging.NewSingleTargetMessage(
		node.ID("new"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{
			ChangefeedID:    cfID.ToPB(),
			MaintainerEpoch: 11,
		},
	)

	require.True(t, q.TryEnqueue(key, oldMsg))

	poppedKey, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key, poppedKey)
	require.Same(t, oldMsg, q.Get(poppedKey))

	// Ownership handoff is decided in the request handler. The queue should only
	// de-duplicate retries for the in-flight attempt instead of overwriting it.
	require.False(t, q.TryEnqueue(key, newMsg))
	require.Same(t, oldMsg, q.Get(poppedKey))
	q.Done(key)
}

func TestShouldAcceptMaintainerRequest(t *testing.T) {
	t.Parallel()

	require.Equal(t, maintainerRequestStale, compareBootstrapMaintainerEpoch(20, 10))
	require.Equal(t, maintainerRequestRetry, compareBootstrapMaintainerEpoch(20, 20))
	require.Equal(t, maintainerRequestTakeover, compareBootstrapMaintainerEpoch(20, 30))

	require.False(t, shouldAcceptPostBootstrapRequest(20, 10))
	require.True(t, shouldAcceptPostBootstrapRequest(20, 20))
	require.False(t, shouldAcceptPostBootstrapRequest(20, 30))

	require.False(t, shouldAcceptCloseRequest(20, 10))
	require.True(t, shouldAcceptCloseRequest(20, 20))
	require.True(t, shouldAcceptCloseRequest(20, 30))
}

func TestHandleBootstrapRequestDropsLegacyTakeoverAfterStrictOwnerEstablished(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager, cfConfig, _ := newTestDispatcherManagerForEpoch(t, cfID, node.ID("owner-a"), false)
	manager.SetActiveMaintainer(node.ID("owner-a"), 22)

	cfgBytes, err := json.Marshal(cfConfig)
	require.NoError(t, err)

	orchestrator := &DispatcherOrchestrator{
		mc: messaging.NewMockMessageCenter(),
		dispatcherManagers: map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{
			cfID: manager,
		},
		msgQueue: newPendingMessageQueue(),
	}

	req := &heartbeatpb.MaintainerBootstrapRequest{
		ChangefeedID:    cfID.ToPB(),
		Config:          cfgBytes,
		StartTs:         1,
		KeyspaceId:      0,
		MaintainerEpoch: 0,
	}

	require.NoError(t, orchestrator.handleBootstrapRequest(node.ID("owner-b"), req))
	require.Equal(t, node.ID("owner-a"), manager.GetMaintainerID())
	require.EqualValues(t, 22, manager.GetActiveMaintainerEpoch())
}

func TestHandleBootstrapRequestAllowsNewerEpochTakeover(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager, cfConfig, _ := newTestDispatcherManagerForEpoch(t, cfID, node.ID("owner-a"), false)
	manager.SetActiveMaintainer(node.ID("owner-a"), 22)

	cfgBytes, err := json.Marshal(cfConfig)
	require.NoError(t, err)

	orchestrator := &DispatcherOrchestrator{
		mc: messaging.NewMockMessageCenter(),
		dispatcherManagers: map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{
			cfID: manager,
		},
		msgQueue: newPendingMessageQueue(),
	}

	req := &heartbeatpb.MaintainerBootstrapRequest{
		ChangefeedID:    cfID.ToPB(),
		Config:          cfgBytes,
		StartTs:         1,
		KeyspaceId:      0,
		MaintainerEpoch: 23,
	}

	require.NoError(t, orchestrator.handleBootstrapRequest(node.ID("owner-b"), req))
	require.Equal(t, node.ID("owner-b"), manager.GetMaintainerID())
	require.EqualValues(t, 23, manager.GetActiveMaintainerEpoch())
}

func TestHandleCloseRequestDropsLegacyCloseAfterStrictOwnerEstablished(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager, _, _ := newTestDispatcherManagerForEpoch(t, cfID, node.ID("owner-a"), false)
	manager.SetActiveMaintainer(node.ID("owner-a"), 22)

	orchestrator := &DispatcherOrchestrator{
		mc: messaging.NewMockMessageCenter(),
		dispatcherManagers: map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{
			cfID: manager,
		},
		msgQueue: newPendingMessageQueue(),
	}

	req := &heartbeatpb.MaintainerCloseRequest{
		ChangefeedID:    cfID.ToPB(),
		Removed:         false,
		MaintainerEpoch: 0,
	}

	require.NoError(t, orchestrator.handleCloseRequest(node.ID("owner-b"), req))
	require.Equal(t, node.ID("owner-a"), manager.GetMaintainerID())
	require.EqualValues(t, 22, manager.GetActiveMaintainerEpoch())
	_, ok := orchestrator.dispatcherManagers[cfID]
	require.True(t, ok)
}

func TestHandlePostBootstrapRequestDropsUnexpectedMaintainerAfterStrictOwnerEstablished(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager, _, tableTriggerID := newTestDispatcherManagerForEpoch(t, cfID, node.ID("owner-a"), true)
	manager.SetActiveMaintainer(node.ID("owner-a"), 22)
	require.NotNil(t, tableTriggerID)

	orchestrator := &DispatcherOrchestrator{
		mc: messaging.NewMockMessageCenter(),
		dispatcherManagers: map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{
			cfID: manager,
		},
		msgQueue: newPendingMessageQueue(),
	}

	req := &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  cfID.ToPB(),
		TableTriggerEventDispatcherId: tableTriggerID,
		MaintainerEpoch:               0,
	}

	require.NoError(t, orchestrator.handlePostBootstrapRequest(node.ID("owner-b"), req))
	require.Equal(t, node.ID("owner-a"), manager.GetMaintainerID())
	require.EqualValues(t, 22, manager.GetActiveMaintainerEpoch())
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
