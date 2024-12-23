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

package replica

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func appendNew(origin []byte, c byte) []byte {
	nb := bytes.Clone(origin)
	return append(nb, c)
}

func TestHotSpanChecker(t *testing.T) {
	t.Parallel()

	db := newDBWithCheckerForTest(t)
	absent := NewReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, getTableSpanByID(4), 1)
	db.AddAbsentReplicaSet(absent)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingReplicaSet(db.changefeedID, replicaSpanID,
		db.ddlSpan.tsoClient, 1,
		getTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)
	require.Equal(t, 1, db.GetReplicatingSize())
	require.Equal(t, 1, db.GetAbsentSize())

	require.Equal(t, replicaSpan.GetGroupID(), absent.GetGroupID())
	groupID := replicaSpan.GetGroupID()
	checker := db.GetGroupChecker(groupID).(*hotSpanChecker)
	require.Equal(t, 0, len(checker.hotTasks))

	// test update status and track hot task dynamically
	db.MarkSpanReplicating(absent)
	db.UpdateStatus(absent, &heartbeatpb.TableSpanStatus{
		CheckpointTs:       9,
		EventSizePerSecond: HotSpanWriteThreshold,
	})
	require.Equal(t, 1, len(checker.hotTasks))
	for i := 0; i < HotSpanScoreThreshold; i++ {
		db.UpdateStatus(replicaSpan, &heartbeatpb.TableSpanStatus{
			CheckpointTs:       9,
			EventSizePerSecond: HotSpanWriteThreshold,
		})
	}
	require.Equal(t, 2, len(checker.hotTasks))

	// test check
	results := checker.Check(20).([]CheckResult)
	require.Equal(t, 1, len(results))
	ret := results[0]
	require.Equal(t, OpSplit, ret.OpType)
	require.Equal(t, 1, len(ret.Replications))
	require.Equal(t, replicaSpan, ret.Replications[0])
	require.Equal(t, 2, len(checker.hotTasks))

	// test remove
	db.ReplaceReplicaSet(ret.Replications, nil, 10)
	require.Equal(t, 1, len(checker.hotTasks))
}

// Not parallel because it will change the global node manager
func TestRebalanceChecker(t *testing.T) {
	nodeManager := watcher.NewNodeManager(nil, nil)
	allNodes := nodeManager.GetAliveNodes()
	for i := 0; i < 3; i++ {
		idx := fmt.Sprintf("node%d", i)
		allNodes[node.ID(idx)] = &node.Info{ID: node.ID(idx)}
	}

	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	db := newDBWithCheckerForTest(t)
	totalSpan := getTableSpanByID(4)
	partialSpans := []*heartbeatpb.TableSpan{
		{StartKey: totalSpan.StartKey, EndKey: appendNew(totalSpan.StartKey, 'a')},
		{StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')},
		{StartKey: appendNew(totalSpan.StartKey, 'b'), EndKey: appendNew(totalSpan.StartKey, 'c')},
	}
	allReplicas := make([]*SpanReplication, 0, len(partialSpans))
	for _, span := range partialSpans {
		absent := NewReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, span, 1)
		allReplicas = append(allReplicas, absent)
		db.AddAbsentReplicaSet(absent)
	}
	replicaSpan := &heartbeatpb.TableSpan{
		StartKey: appendNew(totalSpan.StartKey, 'c'),
		EndKey:   totalSpan.EndKey,
	}
	replica := NewWorkingReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, replicaSpan,
		&heartbeatpb.TableSpanStatus{CheckpointTs: 9, ComponentStatus: heartbeatpb.ComponentState_Working}, "node0")
	allReplicas = append(allReplicas, replica)
	db.AddReplicatingSpan(replica)
	require.Equal(t, 3, db.GetAbsentSize())
	require.Equal(t, 1, db.GetReplicatingSize())

	// test add replica
	groupID := replica.GetGroupID()
	for _, r := range allReplicas {
		require.Equal(t, groupID, r.GetGroupID())
	}
	checker := db.GetGroupChecker(groupID).(*rebalanceChecker)
	require.Equal(t, 4, len(checker.allTasks))

	// test update replica status
	require.Nil(t, checker.Check(20))
	require.Panics(t, func() {
		invalidReplica := NewReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, replicaSpan, 1)
		db.UpdateStatus(invalidReplica, &heartbeatpb.TableSpanStatus{
			CheckpointTs:       9,
			EventSizePerSecond: checker.softWriteThreshold,
		})
	})
	for idx, r := range allReplicas {
		r.SetNodeID(node.ID(fmt.Sprintf("node%d", idx%3)))
		db.MarkSpanScheduling(r)
		db.UpdateStatus(r, &heartbeatpb.TableSpanStatus{
			CheckpointTs:       9,
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: checker.softWriteThreshold,
		})
	}

	// test hard threadhold
	require.Nil(t, checker.Check(20))
	db.UpdateStatus(replica, &heartbeatpb.TableSpanStatus{
		CheckpointTs:       9,
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: checker.hardWriteThreshold,
	})
	require.NotNil(t, checker.Check(20))

	// test scale out too much nodes
	db.UpdateStatus(replica, &heartbeatpb.TableSpanStatus{
		CheckpointTs:       9,
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: checker.softWriteThreshold,
	})
	require.Nil(t, checker.Check(20))
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	require.Nil(t, checker.Check(20))
	nodeManager.GetAliveNodes()["node4"] = &node.Info{ID: "node4"}
	rets := checker.Check(20).([]CheckResult)
	require.NotNil(t, rets)
	ret := rets[0]
	require.Equal(t, OpMergeAndSplit, ret.OpType)

	// test remove
	db.ReplaceReplicaSet(ret.Replications, nil, 10)
	require.Equal(t, 0, len(checker.allTasks))
}

// Not parallel because it will change the global node manager
func TestSoftRebalanceChecker(t *testing.T) {
	nodeManager := watcher.NewNodeManager(nil, nil)
	allNodes := nodeManager.GetAliveNodes()
	totalNodes := 3
	for i := 0; i < totalNodes; i++ {
		idx := fmt.Sprintf("node%d", i)
		allNodes[node.ID(idx)] = &node.Info{ID: node.ID(idx)}
	}

	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	db := newDBWithCheckerForTest(t)
	totalSpan := getTableSpanByID(4)
	partialSpans := []*heartbeatpb.TableSpan{
		{StartKey: totalSpan.StartKey, EndKey: appendNew(totalSpan.StartKey, 'a')},
		{StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')},
		{StartKey: appendNew(totalSpan.StartKey, 'b'), EndKey: appendNew(totalSpan.StartKey, 'c')},
		{StartKey: appendNew(totalSpan.StartKey, 'c'), EndKey: totalSpan.EndKey},
	}

	groupID := replica.GroupID(0)
	allReplicas := make([]*SpanReplication, 0, len(partialSpans))
	for i, span := range partialSpans {
		idx := node.ID(fmt.Sprintf("node%d", i%totalNodes))
		replicating := NewWorkingReplicaSet(db.changefeedID, common.NewDispatcherID(), db.ddlSpan.tsoClient, 1, span,
			&heartbeatpb.TableSpanStatus{
				CheckpointTs:       9,
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				EventSizePerSecond: 0, // test the cornoer case that no write
			}, idx)
		if groupID == 0 {
			groupID = replicating.GetGroupID()
		} else {
			require.Equal(t, groupID, replicating.GetGroupID())
		}
		allReplicas = append(allReplicas, replicating)
		db.AddReplicatingSpan(replicating)
	}

	checker := db.GetGroupChecker(groupID).(*rebalanceChecker)
	require.Equal(t, 4, len(checker.allTasks))

	// test soft rebalance
	replica := allReplicas[0]
	require.Nil(t, checker.Check(20))
	require.Equal(t, 0, checker.softRebalanceScore)
	require.Equal(t, 1, checker.softMergeScore)
	db.UpdateStatus(replica, &heartbeatpb.TableSpanStatus{
		CheckpointTs:       9,
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: checker.softWriteThreshold * float32(checker.hardImbalanceThreshold),
	})
	for i := 1; i < checker.softRebalanceScoreThreshold; i++ {
		checker.softMergeScore = 2
		require.Nil(t, checker.Check(20))
		require.Equal(t, i, checker.softRebalanceScore)
		require.Zero(t, checker.softMergeScore)
	}
	ret := checker.Check(20).([]CheckResult)[0]
	require.Equal(t, OpMergeAndSplit, ret.OpType)
	require.Equal(t, 4, len(ret.Replications))
	require.Equal(t, 0, checker.softRebalanceScore)
	require.Zero(t, checker.softMergeScore)

	// test soft merge
	require.Nil(t, checker.Check(20))
	require.Equal(t, 1, checker.softRebalanceScore)
	require.Equal(t, 0, checker.softMergeScore)
	db.UpdateStatus(replica, &heartbeatpb.TableSpanStatus{
		CheckpointTs:       9,
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: checker.softWriteThreshold - 1,
	})
	for i := 1; i < checker.softMergeScoreThreshold; i++ {
		checker.softRebalanceScore = 2
		require.Nil(t, checker.Check(20))
		require.Equal(t, i, checker.softMergeScore)
		require.Zero(t, checker.softRebalanceScore)
	}
	ret = checker.Check(20).([]CheckResult)[0]
	require.Equal(t, OpMerge, ret.OpType)
	require.Equal(t, 4, len(ret.Replications))
}
