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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func TestOrchestratorShard_CloseWaitsForRunningHandler(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan struct{})
	shard := newOrchestratorShard(func(msg *messaging.TargetMessage) {
		close(started)
		<-release
	})
	shard.Run()

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerBootstrapRequest,
	}
	msg := &messaging.TargetMessage{Type: messaging.TypeMaintainerBootstrapRequest}
	require.True(t, shard.TryEnqueue(key, msg))

	select {
	case <-started:
	case <-time.After(time.Second):
		require.FailNow(t, "handler did not start")
	}

	go func() {
		shard.CloseAsync()
		shard.Wait()
		close(closed)
	}()

	select {
	case <-closed:
		require.FailNow(t, "shard closed before running handler returned")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case <-closed:
	case <-time.After(time.Second):
		require.FailNow(t, "shard close did not finish after handler returned")
	}
}

func TestOrchestratorShard_RunIsIdempotent(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	shard := newOrchestratorShard(func(msg *messaging.TargetMessage) {
		started <- struct{}{}
		<-release
	})
	shard.Run()
	shard.Run()

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerBootstrapRequest,
	}
	require.True(t, shard.TryEnqueue(key, &messaging.TargetMessage{Type: messaging.TypeMaintainerBootstrapRequest}))

	select {
	case <-started:
	case <-time.After(time.Second):
		require.FailNow(t, "handler did not start")
	}

	select {
	case <-started:
		require.FailNow(t, "Run started more than one worker")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	shard.CloseAsync()
	shard.Wait()
}

func TestDispatcherOrchestrator_RecvMaintainerRequestRoutesDifferentShardsInParallel(t *testing.T) {
	t.Parallel()

	orchestrator := newTestDispatcherOrchestrator()
	cfID1, shardIndex1 := findChangefeedIDOnShard(orchestrator, -1)
	cfID2, shardIndex2 := findChangefeedIDOnShard(orchestrator, shardIndex1)

	enterShard1 := make(chan struct{})
	enterShard2 := make(chan struct{})
	releaseShard1 := make(chan struct{})
	releaseShard2 := make(chan struct{})
	orchestrator.shards[shardIndex1] = newOrchestratorShard(func(msg *messaging.TargetMessage) {
		close(enterShard1)
		<-releaseShard1
	})
	orchestrator.shards[shardIndex2] = newOrchestratorShard(func(msg *messaging.TargetMessage) {
		close(enterShard2)
		<-releaseShard2
	})
	for _, shard := range orchestrator.shards {
		shard.Run()
	}
	t.Cleanup(func() {
		close(releaseShard1)
		close(releaseShard2)
		for _, shard := range orchestrator.shards {
			shard.CloseAsync()
			shard.Wait()
		}
	})

	msg0 := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{ChangefeedID: cfID1.ToPB()},
	)
	msg1 := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{ChangefeedID: cfID2.ToPB()},
	)

	require.NoError(t, orchestrator.RecvMaintainerRequest(context.Background(), msg0))
	require.NoError(t, orchestrator.RecvMaintainerRequest(context.Background(), msg1))

	select {
	case <-enterShard1:
	case <-time.After(time.Second):
		require.FailNow(t, "first shard handler did not start")
	}
	select {
	case <-enterShard2:
	case <-time.After(time.Second):
		require.FailNow(t, "second shard handler did not start")
	}
}

func newTestDispatcherOrchestrator() *DispatcherOrchestrator {
	// This test only exercises local routing through RecvMaintainerRequest, so it
	// needs shard state and the dispatcher manager map but not a message center.
	orchestrator := &DispatcherOrchestrator{
		dispatcherManagers:             make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		initializingDispatcherManagers: make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		closedMaintainerEpochs:         make(map[common.ChangeFeedID]uint64),
		shards:                         make([]*orchestratorShard, dispatcherOrchestratorShardCount),
	}
	for i := range orchestrator.shards {
		orchestrator.shards[i] = newOrchestratorShard(func(msg *messaging.TargetMessage) {})
	}
	return orchestrator
}

func findChangefeedIDOnShard(orchestrator *DispatcherOrchestrator, excludedShard int) (common.ChangeFeedID, int) {
	for i := range dispatcherOrchestratorShardCount * 4 {
		cfID := newTestChangefeedID(i)
		shardIndex := orchestrator.shardIndexForChangefeedID(cfID)
		if shardIndex != excludedShard {
			return cfID, shardIndex
		}
	}
	panic("failed to find changefeed ID on a different shard")
}

func newTestChangefeedID(seed int) common.ChangeFeedID {
	return common.ChangeFeedID{
		Id: common.NewGIDWithValue(uint64(seed+1), uint64(seed+17)),
		DisplayName: common.NewChangeFeedDisplayName(
			fmt.Sprintf("cf-%d", seed),
			"default",
		),
	}
}

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

	poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Same(t, msg, poppedMsg)

	// Once the request is popped, allow one queued retry for the next round.
	require.True(t, q.TryEnqueue(key, msg))
	require.False(t, q.TryEnqueue(key, msg))

	nextMsg, ok := q.Pop()
	require.True(t, ok)
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

	poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, key1.msgType, poppedMsg.Type)

	poppedMsg, ok = q.Pop()
	require.True(t, ok)
	require.Equal(t, key2.msgType, poppedMsg.Type)
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

	poppedMsg, ok := q.Pop()
	require.True(t, ok)
	require.NotNil(t, poppedMsg)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.True(t, req.Removed)
}

func TestPendingMessageQueue_StaleRemovedCloseCannotOverrideNewerEpochClose(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerCloseRequest,
	}

	newerClose := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{
			ChangefeedID:    cfID.ToPB(),
			MaintainerEpoch: 2,
			Removed:         false,
		},
	)
	staleRemovedClose := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{
			ChangefeedID:    cfID.ToPB(),
			MaintainerEpoch: 1,
			Removed:         true,
		},
	)

	require.True(t, q.TryEnqueue(key, newerClose))
	require.False(t, q.TryEnqueue(key, staleRemovedClose))

	poppedMsg, ok := q.Pop()
	require.True(t, ok)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.Equal(t, uint64(2), req.MaintainerEpoch)
	require.False(t, req.Removed)
}

func TestPendingMessageQueue_NewerMaintainerEpochOverridesPendingRequest(t *testing.T) {
	t.Parallel()

	q := newPendingMessageQueue()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	key := pendingMessageKey{
		changefeedID: cfID,
		msgType:      messaging.TypeMaintainerBootstrapRequest,
	}

	oldMsg := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{ChangefeedID: cfID.ToPB(), MaintainerEpoch: 1},
	)
	newMsg := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{ChangefeedID: cfID.ToPB(), MaintainerEpoch: 2},
	)

	require.True(t, q.TryEnqueue(key, oldMsg))
	require.True(t, q.TryEnqueue(key, newMsg))

	poppedMsg, ok := q.Pop()
	require.True(t, ok)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerBootstrapRequest)
	require.Equal(t, uint64(2), req.MaintainerEpoch)
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

	poppedMsg, ok := q.Pop()
	require.True(t, ok)

	require.NotNil(t, poppedMsg)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.False(t, req.Removed)

	require.True(t, q.TryEnqueue(key, msgTrue))
	require.False(t, req.Removed)

	type popResult struct {
		msg *messaging.TargetMessage
		ok  bool
	}
	resultCh := make(chan popResult, 1)
	go func() {
		nextMsg, nextOK := q.Pop()
		resultCh <- popResult{msg: nextMsg, ok: nextOK}
	}()

	select {
	case result := <-resultCh:
		require.True(t, result.ok)
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

func TestDispatcherOrchestratorLocalFenceDropsNewMessages(t *testing.T) {
	mc, _, stop := messaging.NewMessageCenterForTest(t)
	defer stop()

	orchestrator := &DispatcherOrchestrator{
		mc:                 mc,
		dispatcherManagers: make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		shards:             make([]*orchestratorShard, dispatcherOrchestratorShardCount),
	}
	processed := make(chan struct{}, 1)
	for i := range orchestrator.shards {
		orchestrator.shards[i] = newOrchestratorShard(func(msg *messaging.TargetMessage) {
			processed <- struct{}{}
		})
		orchestrator.shards[i].Run()
	}
	orchestrator.LocalFence()
	for _, shard := range orchestrator.shards {
		shard.Wait()
	}

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	msg := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerCloseRequest{ChangefeedID: cfID.ToPB()},
	)
	require.NoError(t, orchestrator.RecvMaintainerRequest(context.Background(), msg))

	select {
	case <-processed:
		require.FailNow(t, "local fence should drop new maintainer messages")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDispatcherOrchestratorLocalFenceFencesManagersImmediately(t *testing.T) {
	mc, _, stop := messaging.NewMessageCenterForTest(t)
	defer stop()

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager := &dispatchermanager.DispatcherManager{}
	orchestrator := &DispatcherOrchestrator{
		mc: mc,
		dispatcherManagers: map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{
			cfID: manager,
		},
		shards: make([]*orchestratorShard, dispatcherOrchestratorShardCount),
	}
	for i := range orchestrator.shards {
		orchestrator.shards[i] = newOrchestratorShard(func(msg *messaging.TargetMessage) {})
		orchestrator.shards[i].Run()
	}
	orchestrator.LocalFence()
	for _, shard := range orchestrator.shards {
		shard.Wait()
	}

	err := manager.InitalizeTableTriggerEventDispatcher(nil)
	require.True(t, dispatchermanager.IsWritePathClosedError(err))
}

func TestDispatcherOrchestratorLocalFenceFencesInitializingManagersImmediately(t *testing.T) {
	mc, _, stop := messaging.NewMessageCenterForTest(t)
	defer stop()

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager := &dispatchermanager.DispatcherManager{}
	orchestrator := &DispatcherOrchestrator{
		mc: mc,
		initializingDispatcherManagers: map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{
			cfID: manager,
		},
		dispatcherManagers: make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		shards:             make([]*orchestratorShard, dispatcherOrchestratorShardCount),
	}
	for i := range orchestrator.shards {
		orchestrator.shards[i] = newOrchestratorShard(func(msg *messaging.TargetMessage) {})
		orchestrator.shards[i].Run()
	}
	orchestrator.LocalFence()
	for _, shard := range orchestrator.shards {
		shard.Wait()
	}

	err := manager.InitalizeTableTriggerEventDispatcher(nil)
	require.True(t, dispatchermanager.IsWritePathClosedError(err))
}

func TestBootstrapResponseRestoresCurrentOperatorsAndStaleRemoves(t *testing.T) {
	setupBootstrapTestServices(t, messaging.NewMockMessageCenter(), true)

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	currentDispatcherID := common.NewDispatcherID()
	oldCreateDispatcherID := common.NewDispatcherID()
	staleRemoveDispatcherID := common.NewDispatcherID()
	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		cfID,
		newBootstrapResponseTestChangefeedConfig(cfID),
		staleRemoveDispatcherID.ToPB(),
		nil,
		100,
		node.ID("current-maintainer"),
		2,
		false,
		nil,
	)
	require.NoError(t, err)
	cleanupDispatcherManager(t, manager)

	manager.GetCurrentOperatorMap().Store(
		currentDispatcherID,
		dispatchermanager.NewSchedulerDispatcherRequest(
			node.ID("current-maintainer"),
			newBootstrapResponseTestScheduleRequest(cfID, currentDispatcherID, 2),
		),
	)
	manager.GetCurrentOperatorMap().Store(
		oldCreateDispatcherID,
		dispatchermanager.NewSchedulerDispatcherRequest(
			node.ID("old-maintainer"),
			newBootstrapResponseTestScheduleRequest(cfID, oldCreateDispatcherID, 1),
		),
	)
	staleRemoveReq := newBootstrapResponseTestScheduleRequest(cfID, staleRemoveDispatcherID, 1)
	staleRemoveReq.ScheduleAction = heartbeatpb.ScheduleAction_Remove
	staleRemoveReq.OperatorType = heartbeatpb.OperatorType_O_Move
	manager.GetCurrentOperatorMap().Store(
		staleRemoveDispatcherID,
		dispatchermanager.NewSchedulerDispatcherRequest(
			node.ID("old-maintainer"),
			staleRemoveReq,
		),
	)

	response := createBootstrapResponse(cfID.ToPB(), manager, 0, 0)
	require.Len(t, response.Operators, 2)
	operators := make(map[common.DispatcherID]*heartbeatpb.ScheduleDispatcherRequest)
	for _, op := range response.Operators {
		operators[common.NewDispatcherIDFromPB(op.Config.DispatcherID)] = op
	}
	currentOp, ok := operators[currentDispatcherID]
	require.True(t, ok)
	require.Equal(t, uint64(2), currentOp.MaintainerEpoch)
	require.Equal(t, heartbeatpb.ScheduleAction_Create, currentOp.ScheduleAction)
	require.NotContains(t, operators, oldCreateDispatcherID)
	staleRemoveOp, ok := operators[staleRemoveDispatcherID]
	require.True(t, ok)
	require.Equal(t, uint64(1), staleRemoveOp.MaintainerEpoch)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, staleRemoveOp.ScheduleAction)
	require.Equal(t, heartbeatpb.OperatorType_O_Move, staleRemoveOp.OperatorType)
}

func TestBootstrapResponseUsesSpanSnapshotForStaleRemove(t *testing.T) {
	setupBootstrapTestServices(t, messaging.NewMockMessageCenter(), false)

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		cfID,
		newBootstrapResponseTestChangefeedConfig(cfID),
		nil,
		nil,
		100,
		node.ID("current-maintainer"),
		2,
		false,
		nil,
	)
	require.NoError(t, err)
	cleanupDispatcherManager(t, manager)

	reportedRemoveDispatcherID := common.NewDispatcherID()
	reportedRemoveReq := newBootstrapResponseTestScheduleRequest(cfID, reportedRemoveDispatcherID, 1)
	reportedRemoveReq.ScheduleAction = heartbeatpb.ScheduleAction_Remove
	reportedRemoveReq.OperatorType = heartbeatpb.OperatorType_O_Move
	manager.GetCurrentOperatorMap().Store(
		reportedRemoveDispatcherID,
		dispatchermanager.NewSchedulerDispatcherRequest(node.ID("old-maintainer"), reportedRemoveReq),
	)

	missingRemoveDispatcherID := common.NewDispatcherID()
	missingRemoveReq := newBootstrapResponseTestScheduleRequest(cfID, missingRemoveDispatcherID, 1)
	missingRemoveReq.ScheduleAction = heartbeatpb.ScheduleAction_Remove
	missingRemoveReq.OperatorType = heartbeatpb.OperatorType_O_Split
	manager.GetCurrentOperatorMap().Store(
		missingRemoveDispatcherID,
		dispatchermanager.NewSchedulerDispatcherRequest(node.ID("old-maintainer"), missingRemoveReq),
	)

	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: cfID.ToPB(),
		Spans: []*heartbeatpb.BootstrapTableSpan{
			{
				ID:              reportedRemoveDispatcherID.ToPB(),
				Span:            &heartbeatpb.TableSpan{TableID: 1},
				ComponentStatus: heartbeatpb.ComponentState_Working,
				Mode:            common.DefaultMode,
			},
		},
	}
	retrieveOperatorsForBootstrapResponse(cfID.ToPB(), manager, response)

	require.Len(t, response.Operators, 1)
	require.Equal(t, reportedRemoveDispatcherID, common.NewDispatcherIDFromPB(response.Operators[0].Config.DispatcherID))
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, response.Operators[0].ScheduleAction)
	require.Equal(t, heartbeatpb.OperatorType_O_Move, response.Operators[0].OperatorType)
}

func TestHandleBootstrapTriggerMismatchKeepsOldMaintainerOwner(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	setupBootstrapTestServices(t, mc, true)

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	oldTableTriggerID := common.NewDispatcherID()
	newTableTriggerID := common.NewDispatcherID()
	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		cfID,
		newBootstrapResponseTestChangefeedConfig(cfID),
		oldTableTriggerID.ToPB(),
		nil,
		100,
		node.ID("old-maintainer"),
		2,
		false,
		nil,
	)
	require.NoError(t, err)
	cleanupDispatcherManager(t, manager)

	orchestrator := &DispatcherOrchestrator{
		mc:                     mc,
		dispatcherManagers:     map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{cfID: manager},
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
	}

	err = orchestrator.handleBootstrapRequest(node.ID("new-maintainer"), &heartbeatpb.MaintainerBootstrapRequest{
		ChangefeedID:                  cfID.ToPB(),
		TableTriggerEventDispatcherId: newTableTriggerID.ToPB(),
		StartTs:                       100,
		MaintainerEpoch:               3,
	})
	require.NoError(t, err)

	bootstrapResponseMsg := requireMessage(t, mc.GetMessageChannel(), "bootstrap response was not sent")
	bootstrapResponse := bootstrapResponseMsg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
	require.Equal(t, uint64(3), bootstrapResponse.MaintainerEpoch)
	require.NotNil(t, bootstrapResponse.Err)
	require.Equal(t, node.ID("old-maintainer"), manager.GetMaintainerID())
	require.Equal(t, uint64(2), manager.GetMaintainerEpoch())

	err = orchestrator.handleCloseRequest(node.ID("old-maintainer"), &heartbeatpb.MaintainerCloseRequest{
		ChangefeedID:    cfID.ToPB(),
		MaintainerEpoch: 2,
	})
	require.NoError(t, err)

	closeResponseMsg := requireMessage(t, mc.GetMessageChannel(), "close response was not sent")
	closeResponse := closeResponseMsg.Message[0].(*heartbeatpb.MaintainerCloseResponse)
	require.Equal(t, uint64(2), closeResponse.MaintainerEpoch)
	require.False(t, closeResponse.Success)
}

func TestHandleBootstrapNilTriggerRejectsExistingTableTrigger(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	setupBootstrapTestServices(t, mc, true)

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	tableTriggerID := common.NewDispatcherID()
	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		cfID,
		newBootstrapResponseTestChangefeedConfig(cfID),
		tableTriggerID.ToPB(),
		nil,
		100,
		node.ID("old-maintainer"),
		2,
		false,
		nil,
	)
	require.NoError(t, err)
	cleanupDispatcherManager(t, manager)

	orchestrator := &DispatcherOrchestrator{
		mc:                     mc,
		dispatcherManagers:     map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{cfID: manager},
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
	}

	err = orchestrator.handleBootstrapRequest(node.ID("new-maintainer"), &heartbeatpb.MaintainerBootstrapRequest{
		ChangefeedID:    cfID.ToPB(),
		StartTs:         100,
		MaintainerEpoch: 3,
	})
	require.NoError(t, err)

	bootstrapResponseMsg := requireMessage(t, mc.GetMessageChannel(), "nil trigger bootstrap response was not sent")
	bootstrapResponse := bootstrapResponseMsg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
	require.Equal(t, uint64(3), bootstrapResponse.MaintainerEpoch)
	require.NotNil(t, bootstrapResponse.Err)
	require.Equal(t, node.ID("old-maintainer"), manager.GetMaintainerID())
	require.Equal(t, uint64(2), manager.GetMaintainerEpoch())
}

func TestValidateBootstrapNilRedoTriggerRejectsExistingRedoTrigger(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	redoID := common.NewDispatcherID()
	redoDispatcher := dispatcher.NewRedoDispatcher(
		redoID,
		common.KeyspaceDDLSpan(0),
		100,
		0,
		dispatcher.NewSchemaIDToDispatchers(),
		false,
		false,
		0,
		0,
		nil,
		nil,
	)
	manager := &dispatchermanager.DispatcherManager{}
	manager.SetTableTriggerRedoDispatcher(redoDispatcher)

	err := bootstrapRedoTableTrigger(manager, nil, 0).validate(cfID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table trigger redo dispatcher present during nil trigger bootstrap")
}

func TestHandleCloseRequestAcksStaleMaintainerEpoch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	setupBootstrapTestServices(t, mc, false)

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		cfID,
		newBootstrapResponseTestChangefeedConfig(cfID),
		nil,
		nil,
		100,
		node.ID("current-maintainer"),
		2,
		false,
		nil,
	)
	require.NoError(t, err)
	cleanupDispatcherManager(t, manager)

	orchestrator := &DispatcherOrchestrator{
		mc:                     mc,
		dispatcherManagers:     map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{cfID: manager},
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
	}
	err = orchestrator.handleCloseRequest(node.ID("old-maintainer"), &heartbeatpb.MaintainerCloseRequest{
		ChangefeedID:    cfID.ToPB(),
		MaintainerEpoch: 1,
	})
	require.NoError(t, err)

	responseMsg := requireMessage(t, mc.GetMessageChannel(), "close response was not sent")
	response := responseMsg.Message[0].(*heartbeatpb.MaintainerCloseResponse)
	require.True(t, response.Success)
	require.Equal(t, uint64(1), response.MaintainerEpoch)
	require.Contains(t, orchestrator.dispatcherManagers, cfID)
	require.NotContains(t, orchestrator.closedMaintainerEpochs, cfID)
}

func TestHandlePostBootstrapWritePathClosedReleasesMaintainerFence(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	setupBootstrapTestServices(t, mc, true)

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	tableTriggerID := common.NewDispatcherID()
	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		cfID,
		newBootstrapResponseTestChangefeedConfig(cfID),
		tableTriggerID.ToPB(),
		nil,
		100,
		node.ID("current-maintainer"),
		2,
		false,
		nil,
	)
	require.NoError(t, err)
	cleanupDispatcherManager(t, manager)

	orchestrator := &DispatcherOrchestrator{
		mc:                     mc,
		dispatcherManagers:     map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{cfID: manager},
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
	}
	manager.LocalFence()

	err = orchestrator.handlePostBootstrapRequest(node.ID("current-maintainer"), &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  cfID.ToPB(),
		TableTriggerEventDispatcherId: tableTriggerID.ToPB(),
		MaintainerEpoch:               2,
		Schemas:                       nil,
		RedoSchemas:                   nil,
	})
	require.NoError(t, err)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- orchestrator.handleCloseRequest(node.ID("current-maintainer"), &heartbeatpb.MaintainerCloseRequest{
			ChangefeedID:    cfID.ToPB(),
			MaintainerEpoch: 2,
		})
	}()

	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "close request blocked after post-bootstrap write-path-closed return")
	}
}

func TestHandlePostBootstrapMissingTriggerReportsError(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	setupBootstrapTestServices(t, mc, false)

	cfID := common.NewChangeFeedIDWithName("cf", "default")
	manager, err := dispatchermanager.NewDispatcherManager(
		0,
		cfID,
		newBootstrapResponseTestChangefeedConfig(cfID),
		nil,
		nil,
		100,
		node.ID("current-maintainer"),
		2,
		false,
		nil,
	)
	require.NoError(t, err)
	cleanupDispatcherManager(t, manager)

	orchestrator := &DispatcherOrchestrator{
		mc:                     mc,
		dispatcherManagers:     map[common.ChangeFeedID]*dispatchermanager.DispatcherManager{cfID: manager},
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
	}

	err = orchestrator.handlePostBootstrapRequest(node.ID("current-maintainer"), &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  cfID.ToPB(),
		TableTriggerEventDispatcherId: common.NewDispatcherID().ToPB(),
		MaintainerEpoch:               2,
	})
	require.NoError(t, err)

	msg := requireMessage(t, mc.GetMessageChannel(), "post-bootstrap error response was not sent")
	require.Equal(t, messaging.TypeMaintainerPostBootstrapResponse, msg.Type)
	response := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapResponse)
	require.Equal(t, uint64(2), response.MaintainerEpoch)
	require.NotNil(t, response.Err)
	expectedErr := errors.ErrChangefeedInitTableTriggerDispatcherFailed.FastGenByArgs("inject")
	require.Equal(t, string(errors.ErrorCode(expectedErr)), response.Err.Code)
	require.Contains(t, response.Err.Message, "no table trigger event dispatcher")
}

func TestHandleBootstrapWritePathClosedDoesNotReportBootstrapError(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	orchestrator := &DispatcherOrchestrator{
		mc: mc,
	}
	manager := &dispatchermanager.DispatcherManager{}
	manager.LocalFence()

	err := manager.NewTableTriggerEventDispatcher(common.NewDispatcherID().ToPB(), 100, false)
	require.True(t, dispatchermanager.IsWritePathClosedError(err))

	err = orchestrator.handleBootstrapTriggerError(
		node.ID("current-maintainer"),
		cfID.ToPB(),
		2,
		cfID,
		"table trigger event dispatcher",
		err,
	)
	require.NoError(t, err)
	requireNoMessage(t, mc.GetMessageChannel(), "bootstrap write path closed should not send response")
}

func TestHandleBootstrapTriggerErrorReportsNonWritePathClosedError(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	orchestrator := &DispatcherOrchestrator{
		mc: mc,
	}
	bootstrapErr := errors.ErrChangefeedInitTableTriggerDispatcherFailed.FastGenByArgs("inject")

	err := orchestrator.handleBootstrapTriggerError(
		node.ID("current-maintainer"),
		cfID.ToPB(),
		2,
		cfID,
		"table trigger event dispatcher",
		bootstrapErr,
	)
	require.NoError(t, err)

	msg := requireMessage(t, mc.GetMessageChannel(), "bootstrap error response was not sent")
	require.Equal(t, messaging.TypeMaintainerBootstrapResponse, msg.Type)
	response := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
	require.Equal(t, uint64(2), response.MaintainerEpoch)
	require.NotNil(t, response.Err)
	require.Equal(t, string(errors.ErrorCode(bootstrapErr)), response.Err.Code)
	require.Contains(t, response.Err.Message, "inject")
}

func requireNoMessage(t *testing.T, messageCh <-chan *messaging.TargetMessage, message string) {
	t.Helper()

	select {
	case msg := <-messageCh:
		require.FailNow(t, message, "message: %v", msg.Message)
	case <-time.After(50 * time.Millisecond):
	}
}

func requireMessage(t *testing.T, messageCh <-chan *messaging.TargetMessage, message string) *messaging.TargetMessage {
	t.Helper()

	select {
	case msg := <-messageCh:
		return msg
	case <-time.After(time.Second):
		require.FailNow(t, message)
		return nil
	}
}

func cleanupDispatcherManager(t *testing.T, manager *dispatchermanager.DispatcherManager) {
	t.Helper()

	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			return manager.TryClose(false)
		}, time.Second, 10*time.Millisecond)
	})
}

func setupBootstrapTestServices(t *testing.T, mc messaging.MessageCenter, withEventCollector bool) {
	t.Helper()

	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	appcontext.SetService(appcontext.MessageCenter, mc)
	heartbeatCollector := dispatchermanager.NewHeartBeatCollector(node.ID("receiver"))
	heartbeatCollector.Run(context.Background())
	appcontext.SetService(appcontext.HeartbeatCollector, heartbeatCollector)
	t.Cleanup(heartbeatCollector.Close)
	if withEventCollector {
		appcontext.SetService(appcontext.EventCollector, eventcollector.New(node.ID("receiver")))
	} else {
		// Store a typed nil so tests that do not need EventCollector cannot
		// accidentally reuse an instance from an earlier test.
		appcontext.SetService[*eventcollector.EventCollector](appcontext.EventCollector, nil)
	}
}

func TestHandleBootstrapRequestRejectsClosedOlderMaintainerEpoch(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")

	orchestrator := &DispatcherOrchestrator{
		mc:                 messaging.NewMockMessageCenter(),
		dispatcherManagers: make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		closedMaintainerEpochs: map[common.ChangeFeedID]uint64{
			cfID: 2,
		},
	}
	err := orchestrator.handleBootstrapRequest(node.ID("old-maintainer"), &heartbeatpb.MaintainerBootstrapRequest{
		ChangefeedID:    cfID.ToPB(),
		Config:          []byte("invalid config"),
		MaintainerEpoch: 1,
	})
	require.NoError(t, err)
	require.Empty(t, orchestrator.dispatcherManagers)
}

func TestHandleCloseRequestDoesNotRecordEpochZeroTombstoneForCompatClose(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")

	orchestrator := &DispatcherOrchestrator{
		mc:                     messaging.NewMockMessageCenter(),
		dispatcherManagers:     make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
	}

	err := orchestrator.handleCloseRequest(node.ID("compat-maintainer"), &heartbeatpb.MaintainerCloseRequest{
		ChangefeedID:    cfID.ToPB(),
		Removed:         false,
		MaintainerEpoch: 0,
	})
	require.NoError(t, err)
	_, ok := orchestrator.closedMaintainerEpochs[cfID]
	require.False(t, ok)
}

func TestHandleCloseRequestRecordsEpochZeroTombstoneForRemovedChangefeed(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "default")
	configBytes, err := json.Marshal(newBootstrapResponseTestChangefeedConfig(cfID))
	require.NoError(t, err)

	orchestrator := &DispatcherOrchestrator{
		mc:                     messaging.NewMockMessageCenter(),
		dispatcherManagers:     make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
	}

	err = orchestrator.handleCloseRequest(node.ID("compat-maintainer"), &heartbeatpb.MaintainerCloseRequest{
		ChangefeedID:    cfID.ToPB(),
		Removed:         true,
		MaintainerEpoch: 0,
	})
	require.NoError(t, err)
	closedEpoch, ok := orchestrator.closedMaintainerEpochs[cfID]
	require.True(t, ok)
	require.Zero(t, closedEpoch)

	err = orchestrator.handleBootstrapRequest(node.ID("compat-maintainer"), &heartbeatpb.MaintainerBootstrapRequest{
		ChangefeedID:    cfID.ToPB(),
		Config:          configBytes,
		MaintainerEpoch: 0,
	})
	require.NoError(t, err)
	require.Empty(t, orchestrator.dispatcherManagers)
}

func newBootstrapResponseTestChangefeedConfig(cfID common.ChangeFeedID) *config.ChangefeedConfig {
	replicaConfig := config.GetDefaultReplicaConfig()
	return &config.ChangefeedConfig{
		ChangefeedID: cfID,
		SinkURI:      "blackhole://",
		SinkConfig:   replicaConfig.Sink,
		Filter:       replicaConfig.Filter,
	}
}

func newBootstrapResponseTestScheduleRequest(
	cfID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	maintainerEpoch uint64,
) *heartbeatpb.ScheduleDispatcherRequest {
	return &heartbeatpb.ScheduleDispatcherRequest{
		ChangefeedID: cfID.ToPB(),
		Config: &heartbeatpb.DispatcherConfig{
			Span:         &heartbeatpb.TableSpan{TableID: 1},
			StartTs:      100,
			DispatcherID: dispatcherID.ToPB(),
			Mode:         common.DefaultMode,
		},
		ScheduleAction:  heartbeatpb.ScheduleAction_Create,
		OperatorType:    heartbeatpb.OperatorType_O_Add,
		MaintainerEpoch: maintainerEpoch,
	}
}
