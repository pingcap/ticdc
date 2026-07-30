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
	"time"
	"unsafe"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestController_CountInflightDrainMovesFromNode(t *testing.T) {
	messageCenter, _, _ := messaging.NewMessageCenterForTest(t)
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, nodeB := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	otherDispatcherID := common.NewDispatcherID()
	otherReplicaSet := replica.NewWorkingSpanReplication(
		changefeedID,
		otherDispatcherID,
		2,
		testutil.GetTableSpanByID(101),
		&heartbeatpb.TableSpanStatus{
			ID:              otherDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1000,
		},
		nodeB,
		false,
	)
	spanController.AddReplicatingSpan(otherReplicaSet)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{
		nodeA: {ID: nodeA},
		nodeB: {ID: nodeB},
	})

	oc := NewOperatorController(changefeedID, spanController, 1, common.DefaultMode)
	require.True(t, oc.AddOperator(NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB, 7)))
	require.True(t, oc.AddOperator(NewMoveDispatcherOperator(spanController, otherReplicaSet, nodeB, nodeA, 7)))

	require.Equal(t, 1, oc.CountInflightDrainMovesFromNode(nodeA))
	require.Equal(t, 1, oc.CountInflightDrainMovesFromNode(nodeB))
	require.Equal(t, 0, oc.CountInflightDrainMovesFromNode(node.ID("node-c")))
}

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

type blockingScheduleOperator struct {
	id         common.DispatcherID
	targetNode node.ID

	scheduleEntered chan struct{}
	releaseSchedule chan struct{}
	scheduleOnce    sync.Once
	scheduleCount   syncatomic.Int32
}

func newBlockingScheduleOperator(id common.DispatcherID, targetNode node.ID) *blockingScheduleOperator {
	return &blockingScheduleOperator{
		id:              id,
		targetNode:      targetNode,
		scheduleEntered: make(chan struct{}),
		releaseSchedule: make(chan struct{}),
	}
}

func (o *blockingScheduleOperator) ID() common.DispatcherID { return o.id }
func (o *blockingScheduleOperator) Type() string            { return "add" }
func (o *blockingScheduleOperator) Start()                  {}
func (o *blockingScheduleOperator) Schedule() *messaging.TargetMessage {
	o.scheduleCount.Add(1)
	o.scheduleOnce.Do(func() { close(o.scheduleEntered) })
	<-o.releaseSchedule
	return messaging.NewSingleTargetMessage(o.targetNode, messaging.MaintainerManagerTopic, &heartbeatpb.RemoveMaintainerRequest{})
}

func (o *blockingScheduleOperator) IsFinished() bool { return false }
func (o *blockingScheduleOperator) PostFinish()      {}
func (o *blockingScheduleOperator) Check(node.ID, *heartbeatpb.TableSpanStatus) {
}

func (o *blockingScheduleOperator) OnNodeRemove(node.ID) {
}
func (o *blockingScheduleOperator) AffectedNodes() []node.ID { return []node.ID{o.targetNode} }
func (o *blockingScheduleOperator) OnTaskRemoved()           {}
func (o *blockingScheduleOperator) String() string           { return "blocking-schedule" }
func (o *blockingScheduleOperator) BlockTsForward() bool     { return false }

type blockingStartOperator struct {
	id         common.DispatcherID
	targetNode node.ID

	startEntered chan struct{}
	releaseStart chan struct{}
	startOnce    sync.Once
	startCount   syncatomic.Int32
}

func newBlockingStartOperator(id common.DispatcherID, targetNode node.ID) *blockingStartOperator {
	return &blockingStartOperator{
		id:           id,
		targetNode:   targetNode,
		startEntered: make(chan struct{}),
		releaseStart: make(chan struct{}),
	}
}

func (o *blockingStartOperator) ID() common.DispatcherID { return o.id }
func (o *blockingStartOperator) Type() string            { return "add" }
func (o *blockingStartOperator) Start() {
	o.startCount.Add(1)
	o.startOnce.Do(func() { close(o.startEntered) })
	<-o.releaseStart
}

func (o *blockingStartOperator) Schedule() *messaging.TargetMessage { return nil }
func (o *blockingStartOperator) IsFinished() bool                   { return false }
func (o *blockingStartOperator) PostFinish()                        {}
func (o *blockingStartOperator) Check(node.ID, *heartbeatpb.TableSpanStatus) {
}

func (o *blockingStartOperator) OnNodeRemove(node.ID) {
}
func (o *blockingStartOperator) AffectedNodes() []node.ID { return []node.ID{o.targetNode} }
func (o *blockingStartOperator) OnTaskRemoved()           {}
func (o *blockingStartOperator) String() string           { return "blocking-start" }
func (o *blockingStartOperator) BlockTsForward() bool     { return false }

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

	oc.removeReplicaSet(newRemoveDispatcherOperator(spanController, replicaSet, heartbeatpb.OperatorType_O_Remove, 7))
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

func TestMergeDispatcherOperatorScheduleMaintainerEpoch(t *testing.T) {
	spanController, toMergedReplicaSets, occupyOperators, _ := setupMergeTestEnvironment(t)

	op := NewMergeDispatcherOperator(spanController, toMergedReplicaSets, occupyOperators, 7)
	msg := op.Schedule()

	req := msg.Message[0].(*heartbeatpb.MergeDispatcherRequest)
	require.Equal(t, uint64(7), req.MaintainerEpoch)
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
		7,
		func() { postFinishCount.Add(1) },
	)))

	oc.removeReplicaSet(newRemoveDispatcherOperator(spanController, replicaSet, heartbeatpb.OperatorType_O_Remove, 7))

	require.Equal(t, int32(0), postFinishCount.Load())
	require.NotNil(t, oc.GetOperator(replicaSet.ID))
}

func TestController_QuiesceExceptFreezesNonAllowedOperators(t *testing.T) {
	// Scenario: removing mode allows only the DDL close operator to keep running.
	// Steps: quiesce the controller with one allowed dispatcher, then verify the
	// allowed operator still accepts status and schedules, while the frozen operator
	// does not run but still blocks checkpoint advancement.
	messageCenter := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	allowedID := common.NewDispatcherID()
	allowedReplica := setupReplicaSetWithID(t, changefeedID, allowedID, nodeA)
	allowedReplica.UpdateStatus(&heartbeatpb.TableSpanStatus{
		ID:              allowedID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    20,
		Mode:            common.DefaultMode,
	})
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

func TestController_QuiesceExceptDropsBlockedOnlyQueueFromExecution(t *testing.T) {
	// Scenario: after removing starts, the running queue can contain only frozen ordinary operators.
	// Steps: poll a quiesced controller with one non-allowed operator and verify it leaves the heap,
	// remains in the operator map for checkpoint safety, and the next poll terminates the Execute loop.
	messageCenter := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{nodeA: {ID: nodeA}})

	oc := NewOperatorController(changefeedID, spanController, 10, common.DefaultMode)
	blockedOp := &countingOperator{id: replicaSet.ID, targetNode: nodeA, blockTsForward: true}
	require.True(t, oc.AddOperator(blockedOp))

	oc.QuiesceExcept(common.NewDispatcherID())

	op, next := oc.pollQueueingOperator()
	require.Nil(t, op)
	require.True(t, next)
	require.Equal(t, 0, oc.runningQueue.Len())
	require.Equal(t, 1, oc.OperatorSize())
	require.Equal(t, uint64(1000), oc.GetMinCheckpointTs(^uint64(0)))

	op, next = oc.pollQueueingOperator()
	require.Nil(t, op)
	require.False(t, next)
	require.Equal(t, int32(0), blockedOp.scheduleCount.Load())
}

func TestController_QuiesceExceptWaitsForInFlightSchedule(t *testing.T) {
	// Scenario: Execute has already passed the queue poll and is inside a normal operator's Schedule.
	// Steps: block Schedule with a channel, start QuiesceExcept, verify quiesce cannot return until
	// Schedule/SendCommand leaves the admission boundary, then verify later Execute calls do not reschedule it.
	messageCenter := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{nodeA: {ID: nodeA}})

	oc := NewOperatorController(changefeedID, spanController, 10, common.DefaultMode)
	op := newBlockingScheduleOperator(replicaSet.ID, nodeA)
	require.True(t, oc.AddOperator(op))

	executeDone := make(chan struct{})
	go func() {
		defer close(executeDone)
		oc.Execute()
	}()
	<-op.scheduleEntered

	quiesceDone := make(chan struct{})
	go func() {
		defer close(quiesceDone)
		oc.QuiesceExcept(common.NewDispatcherID())
	}()

	require.Never(t, func() bool {
		select {
		case <-quiesceDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)

	close(op.releaseSchedule)
	require.Eventually(t, func() bool {
		select {
		case <-executeDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-quiesceDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(1), op.scheduleCount.Load())
	require.Len(t, messageCenter.GetMessageChannel(), 1)

	oc.Execute()
	require.Equal(t, int32(1), op.scheduleCount.Load())
	require.Len(t, messageCenter.GetMessageChannel(), 1)
}

func TestController_QuiesceExceptWaitsForInFlightPush(t *testing.T) {
	// Scenario: a normal operator has passed admission and is inside Start while removing mode begins.
	// Steps: block Start with a channel, start QuiesceExcept, verify quiesce cannot return until Start
	// finishes, then verify a later ordinary operator is rejected without being started.
	messageCenter := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{nodeA: {ID: nodeA}})

	oc := NewOperatorController(changefeedID, spanController, 10, common.DefaultMode)
	op := newBlockingStartOperator(replicaSet.ID, nodeA)

	addResult := make(chan bool, 1)
	go func() {
		addResult <- oc.AddOperator(op)
	}()
	<-op.startEntered

	quiesceDone := make(chan struct{})
	go func() {
		defer close(quiesceDone)
		oc.QuiesceExcept(common.NewDispatcherID())
	}()

	require.Never(t, func() bool {
		select {
		case <-quiesceDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)

	close(op.releaseStart)
	require.Eventually(t, func() bool { return len(addResult) == 1 }, time.Second, 10*time.Millisecond)
	require.True(t, <-addResult)
	require.Eventually(t, func() bool {
		select {
		case <-quiesceDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), op.startCount.Load())

	blockedID := common.NewDispatcherID()
	blockedReplica := setupReplicaSetWithID(t, changefeedID, blockedID, nodeA)
	spanController.AddReplicatingSpan(blockedReplica)
	blockedOp := newBlockingStartOperator(blockedID, nodeA)
	require.False(t, oc.AddOperator(blockedOp))
	require.Equal(t, int32(0), blockedOp.startCount.Load())
}

func setupReplicaSetWithID(
	t *testing.T,
	changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	nodeID node.ID,
) *replica.SpanReplication {
	t.Helper()

	tableID := int64(dispatcherID.Low + 100)
	span := testutil.GetTableSpanByID(tableID)
	return replica.NewWorkingSpanReplication(changefeedID, dispatcherID, 1, span, &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    10,
		Mode:            common.DefaultMode,
	}, nodeID, false)
}
