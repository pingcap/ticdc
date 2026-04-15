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

package operator

import (
	"reflect"
	"sync"
	syncatomic "sync/atomic"
	"testing"
	"unsafe"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	maintainertestutil "github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

type blockingFinishOperator struct {
	id common.DispatcherID

	isFinishedCalled chan struct{}
	taskRemoved      chan struct{}

	postFinishCount syncatomic.Int32
}

func newBlockingFinishOperator(id common.DispatcherID) *blockingFinishOperator {
	return &blockingFinishOperator{
		id:               id,
		isFinishedCalled: make(chan struct{}),
		taskRemoved:      make(chan struct{}),
	}
}

func (o *blockingFinishOperator) ID() common.DispatcherID { return o.id }
func (o *blockingFinishOperator) Type() string            { return "occupy" }
func (o *blockingFinishOperator) Start()                  {}
func (o *blockingFinishOperator) Schedule() *messaging.TargetMessage {
	return nil
}

func (o *blockingFinishOperator) IsFinished() bool {
	select {
	case <-o.isFinishedCalled:
	default:
		close(o.isFinishedCalled)
	}
	<-o.taskRemoved
	return true
}
func (o *blockingFinishOperator) PostFinish() { o.postFinishCount.Add(1) }
func (o *blockingFinishOperator) Check(node.ID, *heartbeatpb.TableSpanStatus) {
}

func (o *blockingFinishOperator) OnNodeRemove(node.ID) {
}
func (o *blockingFinishOperator) AffectedNodes() []node.ID { return nil }
func (o *blockingFinishOperator) OnTaskRemoved() {
	select {
	case <-o.taskRemoved:
	default:
		close(o.taskRemoved)
	}
}
func (o *blockingFinishOperator) String() string       { return "blocking-finish" }
func (o *blockingFinishOperator) BlockTsForward() bool { return false }

type neverFinishOperator struct {
	id common.DispatcherID
}

func (o *neverFinishOperator) ID() common.DispatcherID { return o.id }
func (o *neverFinishOperator) Type() string            { return "occupy" }
func (o *neverFinishOperator) Start()                  {}
func (o *neverFinishOperator) Schedule() *messaging.TargetMessage {
	return nil
}
func (o *neverFinishOperator) IsFinished() bool { return false }
func (o *neverFinishOperator) PostFinish()      {}
func (o *neverFinishOperator) Check(node.ID, *heartbeatpb.TableSpanStatus) {
}

func (o *neverFinishOperator) OnNodeRemove(node.ID) {
}
func (o *neverFinishOperator) AffectedNodes() []node.ID { return nil }
func (o *neverFinishOperator) OnTaskRemoved()           {}
func (o *neverFinishOperator) String() string           { return "never-finish" }
func (o *neverFinishOperator) BlockTsForward() bool     { return false }

type countingOperator struct {
	id               common.DispatcherID
	targetNode       node.ID
	blockTsForward   bool
	scheduleCount    syncatomic.Int32
	checkCount       syncatomic.Int32
	nodeRemovedCount syncatomic.Int32
}

func (o *countingOperator) ID() common.DispatcherID { return o.id }
func (o *countingOperator) Type() string            { return "add" }
func (o *countingOperator) Start()                  {}
func (o *countingOperator) Schedule() *messaging.TargetMessage {
	o.scheduleCount.Add(1)
	return messaging.NewSingleTargetMessage(o.targetNode, messaging.MaintainerManagerTopic, &heartbeatpb.RemoveMaintainerRequest{})
}
func (o *countingOperator) IsFinished() bool { return false }
func (o *countingOperator) PostFinish()      {}
func (o *countingOperator) Check(node.ID, *heartbeatpb.TableSpanStatus) {
	o.checkCount.Add(1)
}
func (o *countingOperator) OnNodeRemove(node.ID) {
	o.nodeRemovedCount.Add(1)
}
func (o *countingOperator) AffectedNodes() []node.ID { return []node.ID{o.targetNode} }
func (o *countingOperator) OnTaskRemoved()           {}
func (o *countingOperator) String() string           { return "counting-operator" }
func (o *countingOperator) BlockTsForward() bool     { return o.blockTsForward }

func setAliveNodes(nodeManager *watcher.NodeManager, alive map[node.ID]*node.Info) {
	type nodeMap = map[node.ID]*node.Info
	v := reflect.ValueOf(nodeManager).Elem().FieldByName("nodes")
	ptr := (*syncatomic.Pointer[nodeMap])(unsafe.Pointer(v.UnsafeAddr()))
	aliveCopy := nodeMap(alive)
	ptr.Store(&aliveCopy)
}

func TestController_PostFinishCalledOnceOnReplace(t *testing.T) {
	messageCenter, _, _ := messaging.NewMessageCenterForTest(t)
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, _, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	oc := NewOperatorController(changefeedID, spanController, 1, common.DefaultMode)
	op := newBlockingFinishOperator(replicaSet.ID)
	require.True(t, oc.AddOperator(op))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = oc.pollQueueingOperator()
	}()
	<-op.isFinishedCalled

	oc.removeReplicaSet(newRemoveDispatcherOperator(spanController, replicaSet, heartbeatpb.OperatorType_O_Remove))
	wg.Wait()

	require.Equal(t, int32(1), op.postFinishCount.Load())
}

func TestController_OnNodeRemoved_WithOccupyOperatorMarksSpanAbsent(t *testing.T) {
	messageCenter, _, _ := messaging.NewMessageCenterForTest(t)
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{nodeA: {ID: nodeA}})

	oc := NewOperatorController(changefeedID, spanController, 1, common.DefaultMode)
	require.True(t, oc.AddOperator(NewOccupyDispatcherOperator(spanController, replicaSet)))

	absentSizeBefore := spanController.GetAbsentSize()
	oc.OnNodeRemoved(nodeA)
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())
	require.Equal(t, "", replicaSet.GetNodeID().String())
}

func TestController_AddMergeOperatorFailureCleansOccupyOperators(t *testing.T) {
	messageCenter, _, _ := messaging.NewMessageCenterForTest(t)
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, toMergedReplicaSets, _, nodeA := setupMergeTestEnvironment(t)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{nodeA: {ID: nodeA}})

	oc := NewOperatorController(toMergedReplicaSets[0].ChangefeedID, spanController, 1, common.DefaultMode)

	// Make adding occupy operator for the second replica set fail.
	require.True(t, oc.AddOperator(&neverFinishOperator{id: toMergedReplicaSets[1].ID}))

	ret := oc.AddMergeOperator(toMergedReplicaSets)
	require.Nil(t, ret)

	require.Nil(t, oc.GetOperator(toMergedReplicaSets[0].ID))
	require.NotNil(t, oc.GetOperator(toMergedReplicaSets[1].ID))
	require.Equal(t, 1, oc.OperatorSize())
}

func TestController_RemoveReplicaSet_ReplacesRemoveOperatorOnTaskRemoved(t *testing.T) {
	// Scenario: the barrier can enqueue the same remove task multiple times during failover/bootstrap.
	// Steps:
	// 1) Add a remove operator with a post-finish hook (simulates move/split remove phase).
	// 2) Replace it with another remove operator via removeReplicaSet (task is removed / re-enqueued).
	// Expect: replacement should not panic, and the canceled operator must not run its post-finish hook.
	messageCenter, _, _ := messaging.NewMessageCenterForTest(t)
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, _, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	var postFinishCount syncatomic.Int32
	oc := NewOperatorController(changefeedID, spanController, 1, common.DefaultMode)

	require.True(t, oc.AddOperator(NewRemoveDispatcherOperator(
		spanController,
		replicaSet,
		heartbeatpb.OperatorType_O_Move,
		func() { postFinishCount.Add(1) },
	)))

	oc.removeReplicaSet(newRemoveDispatcherOperator(spanController, replicaSet, heartbeatpb.OperatorType_O_Remove))

	require.Equal(t, int32(0), postFinishCount.Load())
	require.NotNil(t, oc.GetOperator(replicaSet.ID))
}

func TestController_QuiesceExceptFreezesNonAllowedOperators(t *testing.T) {
	messageCenter := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	allowedID := common.NewDispatcherID()
	allowedReplica := setupReplicaSetWithID(t, changefeedID, allowedID, nodeA)
	spanController.AddReplicatingSpan(allowedReplica)

	blockedID := common.NewDispatcherID()
	blockedReplica := setupReplicaSetWithID(t, changefeedID, blockedID, nodeA)
	spanController.AddReplicatingSpan(blockedReplica)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{nodeA: {ID: nodeA}})

	oc := NewOperatorController(changefeedID, spanController, 10, common.DefaultMode)
	allowedOp := &countingOperator{id: allowedID, targetNode: nodeA, blockTsForward: true}
	blockedOp := &countingOperator{id: blockedID, targetNode: nodeA, blockTsForward: true}
	require.True(t, oc.AddOperator(allowedOp))
	require.True(t, oc.AddOperator(blockedOp))

	oc.QuiesceExcept(allowedID)

	oc.UpdateOperatorStatus(allowedID, nodeA, &heartbeatpb.TableSpanStatus{ID: allowedID.ToPB()})
	oc.UpdateOperatorStatus(blockedID, nodeA, &heartbeatpb.TableSpanStatus{ID: blockedID.ToPB()})
	require.Equal(t, int32(1), allowedOp.checkCount.Load())
	require.Equal(t, int32(0), blockedOp.checkCount.Load())

	oc.OnNodeRemoved(nodeA)
	require.Equal(t, int32(0), allowedOp.nodeRemovedCount.Load())
	require.Equal(t, int32(0), blockedOp.nodeRemovedCount.Load())
	require.Equal(t, 0, spanController.GetAbsentSize())

	newBlockedID := common.NewDispatcherID()
	newBlockedReplica := setupReplicaSetWithID(t, changefeedID, newBlockedID, nodeA)
	spanController.AddReplicatingSpan(newBlockedReplica)
	require.False(t, oc.AddOperator(&countingOperator{id: newBlockedID, targetNode: nodeA}))

	require.Equal(t, uint64(10), oc.GetMinCheckpointTs(^uint64(0)))

	next := oc.Execute()
	require.False(t, next.IsZero())
	require.Equal(t, int32(1), allowedOp.scheduleCount.Load())
	require.Equal(t, int32(0), blockedOp.scheduleCount.Load())
	require.Len(t, messageCenter.GetMessageChannel(), 1)
	require.Equal(t, 2, oc.OperatorSize())
}

func setupReplicaSetWithID(
	t *testing.T,
	changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	nodeID node.ID,
) *replica.SpanReplication {
	t.Helper()

	tableID := int64(dispatcherID.Low + 100)
	span := maintainertestutil.GetTableSpanByID(tableID)
	return replica.NewWorkingSpanReplication(changefeedID, dispatcherID, 1, span, &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    10,
		Mode:            common.DefaultMode,
	}, nodeID, false)
}
