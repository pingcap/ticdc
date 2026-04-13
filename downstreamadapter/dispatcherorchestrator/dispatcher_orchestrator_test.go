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
	"context"
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

func TestPendingMessageQueue_CloseRequestUpgradeAfterGetRequeuesLatest(t *testing.T) {
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

	poppedMsg := q.Get(poppedKey)
	require.NotNil(t, poppedMsg)
	require.False(t, poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest).Removed)

	require.True(t, q.TryEnqueue(key, msgTrue))
	q.Done(key)

	requeuedKeyCh := make(chan pendingMessageKey, 1)
	go func() {
		requeuedKey, requeuedOK := q.Pop()
		if requeuedOK {
			requeuedKeyCh <- requeuedKey
		}
	}()

	select {
	case requeuedKey := <-requeuedKeyCh:
		require.Equal(t, key, requeuedKey)
		requeuedMsg := q.Get(requeuedKey)
		require.NotNil(t, requeuedMsg)
		require.True(t, requeuedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest).Removed)
		q.Done(requeuedKey)
	case <-time.After(time.Second):
		t.Fatal("close request upgrade should be requeued after the in-flight attempt finishes")
	}
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

func TestAdmitMaintainerRequest(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		kind              maintainerRequestKind
		activeMaintainer  node.ID
		requestMaintainer node.ID
		activeEpoch       uint64
		requestEpoch      uint64
		want              maintainerRequestAdmission
	}{
		{
			name:              "bootstrap stale strict request is rejected",
			kind:              maintainerBootstrapRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-a"),
			activeEpoch:       20,
			requestEpoch:      10,
			want:              maintainerRequestAdmission{},
		},
		{
			name:              "bootstrap retry from active owner is accepted",
			kind:              maintainerBootstrapRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-a"),
			activeEpoch:       20,
			requestEpoch:      20,
			want:              maintainerRequestAdmission{accept: true},
		},
		{
			name:              "bootstrap newer epoch takes ownership",
			kind:              maintainerBootstrapRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-b"),
			activeEpoch:       20,
			requestEpoch:      21,
			want:              maintainerRequestAdmission{accept: true, updateOwner: true},
		},
		{
			name:              "legacy bootstrap cannot take over strict owner with zero epoch",
			kind:              maintainerBootstrapRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-b"),
			activeEpoch:       20,
			requestEpoch:      0,
			want:              maintainerRequestAdmission{},
		},
		{
			name:              "legacy bootstrap can establish owner before strict epoch exists",
			kind:              maintainerBootstrapRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-b"),
			activeEpoch:       0,
			requestEpoch:      0,
			want:              maintainerRequestAdmission{accept: true, updateOwner: true},
		},
		{
			name:              "post bootstrap only accepts active sequence",
			kind:              maintainerPostBootstrapRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-a"),
			activeEpoch:       20,
			requestEpoch:      20,
			want:              maintainerRequestAdmission{accept: true},
		},
		{
			name:              "post bootstrap rejects newer epoch",
			kind:              maintainerPostBootstrapRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-a"),
			activeEpoch:       20,
			requestEpoch:      21,
			want:              maintainerRequestAdmission{},
		},
		{
			name:              "close accepts newer epoch takeover",
			kind:              maintainerCloseRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-b"),
			activeEpoch:       20,
			requestEpoch:      21,
			want:              maintainerRequestAdmission{accept: true, updateOwner: true},
		},
		{
			name:              "legacy close cannot override strict owner with zero epoch",
			kind:              maintainerCloseRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-b"),
			activeEpoch:       20,
			requestEpoch:      0,
			want:              maintainerRequestAdmission{},
		},
		{
			name:              "close can establish strict epoch from legacy owner",
			kind:              maintainerCloseRequest,
			activeMaintainer:  node.ID("owner-a"),
			requestMaintainer: node.ID("owner-b"),
			activeEpoch:       0,
			requestEpoch:      20,
			want:              maintainerRequestAdmission{accept: true, updateOwner: true},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := admitMaintainerRequest(
				tc.kind,
				tc.activeMaintainer,
				tc.requestMaintainer,
				tc.activeEpoch,
				tc.requestEpoch,
			)
			require.Equal(t, tc.want, got)
		})
	}
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

func TestRecvMaintainerRequestDropsNilOrEmptyMessage(t *testing.T) {
	t.Parallel()

	orchestrator := &DispatcherOrchestrator{}

	require.NotPanics(t, func() {
		require.NoError(t, orchestrator.RecvMaintainerRequest(context.Background(), nil))
	})

	emptyMsg := &messaging.TargetMessage{Type: messaging.TypeMaintainerBootstrapRequest}
	require.NotPanics(t, func() {
		require.NoError(t, orchestrator.RecvMaintainerRequest(context.Background(), emptyMsg))
	})
}
