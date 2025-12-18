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

	oc.removeReplicaSet(newRemoveDispatcherOperator(spanController, replicaSet))
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
