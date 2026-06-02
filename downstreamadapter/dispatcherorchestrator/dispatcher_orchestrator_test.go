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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
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
		dispatcherManagers: make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		shards:             make([]*orchestratorShard, dispatcherOrchestratorShardCount),
	}
	for i := range orchestrator.shards {
		orchestrator.shards[i] = newOrchestratorShard(func(msg *messaging.TargetMessage) {})
	}
	return orchestrator
}

func findChangefeedIDOnShard(orchestrator *DispatcherOrchestrator, excludedShard int) (common.ChangeFeedID, int) {
	for i := 0; i < dispatcherOrchestratorShardCount*4; i++ {
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

func TestPendingMessageQueue_NewerGenerationOverridesPendingRequest(t *testing.T) {
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
		&heartbeatpb.MaintainerBootstrapRequest{ChangefeedID: cfID.ToPB(), Generation: 1},
	)
	newMsg := messaging.NewSingleTargetMessage(
		node.ID("to"),
		messaging.DispatcherManagerManagerTopic,
		&heartbeatpb.MaintainerBootstrapRequest{ChangefeedID: cfID.ToPB(), Generation: 2},
	)

	require.True(t, q.TryEnqueue(key, oldMsg))
	require.True(t, q.TryEnqueue(key, newMsg))

	poppedMsg, ok := q.Pop()
	require.True(t, ok)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerBootstrapRequest)
	require.Equal(t, uint64(2), req.Generation)
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
	poppedMsg, ok := q.Pop()
	require.True(t, ok)
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

	poppedMsg, ok := q.Pop()
	require.True(t, ok)

	require.NotNil(t, poppedMsg)
	req := poppedMsg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
	require.False(t, req.Removed)

	require.True(t, q.TryEnqueue(key, msgTrue))

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

func TestBootstrapResponseRestoresOnlyCurrentGenerationOperators(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())
	heartbeatCollector := dispatchermanager.NewHeartBeatCollector(node.ID("receiver"))
	heartbeatCollector.Run(context.Background())
	appcontext.SetService(appcontext.HeartbeatCollector, heartbeatCollector)
	t.Cleanup(heartbeatCollector.Close)

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
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		manager.TryClose(false)
	})

	currentDispatcherID := common.NewDispatcherID()
	oldDispatcherID := common.NewDispatcherID()
	manager.GetCurrentOperatorMap().Store(
		"current-generation",
		dispatchermanager.NewSchedulerDispatcherRequest(
			node.ID("current-maintainer"),
			newBootstrapResponseTestScheduleRequest(cfID, currentDispatcherID, 2),
		),
	)
	manager.GetCurrentOperatorMap().Store(
		"old-generation",
		dispatchermanager.NewSchedulerDispatcherRequest(
			node.ID("old-maintainer"),
			newBootstrapResponseTestScheduleRequest(cfID, oldDispatcherID, 1),
		),
	)

	response := createBootstrapResponse(cfID.ToPB(), manager, 0, 0)
	require.Len(t, response.Operators, 1)
	require.Equal(t, uint64(2), response.Operators[0].Generation)
	require.Equal(t, currentDispatcherID, common.NewDispatcherIDFromPB(response.Operators[0].Config.DispatcherID))
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
	generation uint64,
) *heartbeatpb.ScheduleDispatcherRequest {
	return &heartbeatpb.ScheduleDispatcherRequest{
		ChangefeedID: cfID.ToPB(),
		Config: &heartbeatpb.DispatcherConfig{
			Span:         &heartbeatpb.TableSpan{TableID: 1},
			StartTs:      100,
			DispatcherID: dispatcherID.ToPB(),
			Mode:         common.DefaultMode,
		},
		ScheduleAction: heartbeatpb.ScheduleAction_Create,
		Generation:     generation,
	}
}
