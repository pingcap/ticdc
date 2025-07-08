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

package maintainer

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestSchedule(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller := NewController(cfID, 1, nil, nil, ddlSpan, 9, time.Minute)
	for i := 0; i < 10; i++ {
		controller.spanController.AddNewTable(commonEvent.Table{
			SchemaID: 1,
			TableID:  int64(i + 1),
		}, 1)
	}
	controller.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Equal(t, 9, controller.operatorController.OperatorSize())
	for _, span := range controller.spanController.GetTasksBySchemaID(1) {
		if op := controller.operatorController.GetOperator(span.ID); op != nil {
			op.Start()
		}
	}
	require.Equal(t, 1, controller.spanController.GetAbsentSize())
	require.Equal(t, 3, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 3, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 3, controller.spanController.GetTaskSizeByNodeID("node3"))
}

func TestRemoveAbsentTask(t *testing.T) {
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller := NewController(cfID, 1, nil, nil, ddlSpan, 9, time.Minute)
	controller.spanController.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  int64(1),
	}, 1)
	require.Equal(t, 1, controller.spanController.GetAbsentSize())
	controller.operatorController.RemoveAllTasks()
	require.Equal(t, 0, controller.spanController.GetAbsentSize())
}

// This case test the scenario that the balance scheduler when a new node join in.
// In this case, the num of split tables is more than the num of nodes,
// and we can select appropriate split spans to move
func TestBalanceGroupsNewNodeAdd_SplitsTableMoreThanNodeNum(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			WriteKeyThreshold:      500,
		},
	}, ddlSpan, 1000, 0)

	nodeID := node.ID("node1")
	for i := 0; i < 100; i++ {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(int64(i))
		for j := 0; j < 4; j++ {
			span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, byte('a'+j)), EndKey: appendNew(totalSpan.StartKey, byte('b'+j))}
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
			spanReplica.SetNodeID(nodeID)
			s.spanController.AddReplicatingSpan(spanReplica)

			status := &heartbeatpb.TableSpanStatus{
				ID:                 spanReplica.ID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				EventSizePerSecond: 300,
				CheckpointTs:       2,
			}
			s.spanController.UpdateStatus(spanReplica, status)
		}
	}
	require.Equal(t, 0, s.operatorController.OperatorSize())
	require.Equal(t, 400, s.spanController.GetReplicatingSize())
	require.Equal(t, 400, s.spanController.GetTaskSizeByNodeID(nodeID))

	// add new node
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	s.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, 200, s.operatorController.OperatorSize())
	require.Equal(t, 200, s.spanController.GetSchedulingSize())
	require.Equal(t, 200, s.spanController.GetReplicatingSize())
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			_, ok := op.(*operator.MoveDispatcherOperator)
			require.True(t, ok)
		}
	}
	// still on the primary node
	require.Equal(t, 400, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node2"))

	// remove the node2
	delete(nodeManager.GetAliveNodes(), "node2")
	s.operatorController.OnNodeRemoved("node2")
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			require.Equal(t, "node1", msg.To.String())
			require.True(t, msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).ScheduleAction ==
				heartbeatpb.ScheduleAction_Create)
			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
		}
	}

	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	// changed to working status
	require.Equal(t, 400, s.spanController.GetReplicatingSize())
	require.Equal(t, 400, s.spanController.GetTaskSizeByNodeID("node1"))
}

// This case test the scenario that the balance scheduler when a new node join in.
// In this case, the num of split tables is less than the num of nodes,
// and we should choose span to split.
func TestBalanceGroupsNewNodeAdd_SplitsTableLessThanNodeNum(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			WriteKeyThreshold:      500,
		},
	}, ddlSpan, 1000, 0)

	regionCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)

	nodeIDList := []node.ID{"node1", "node2"}
	for i := 0; i < 100; i++ {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(int64(i))
		for j := 0; j < 2; j++ {
			span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, byte('a'+j)), EndKey: appendNew(totalSpan.StartKey, byte('b'+j))}
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
			spanReplica.SetNodeID(nodeIDList[j%2])
			s.spanController.AddReplicatingSpan(spanReplica)

			status := &heartbeatpb.TableSpanStatus{
				ID:                 spanReplica.ID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				EventSizePerSecond: 300,
				CheckpointTs:       2,
			}
			s.spanController.UpdateStatus(spanReplica, status)

			regionCache.SetRegions(fmt.Sprintf("%s-%s", span.StartKey, span.EndKey), []*tikv.Region{
				testutil.MockRegionWithKeyRange(uint64(i), appendNew(totalSpan.StartKey, byte('a'+j)), appendNew(appendNew(totalSpan.StartKey, byte('a'+j)), 'a')),
				testutil.MockRegionWithKeyRange(uint64(i), appendNew(appendNew(totalSpan.StartKey, byte('a'+j)), 'a'), appendNew(totalSpan.StartKey, byte('b'+j))),
			})
		}

	}
	require.Equal(t, 0, s.operatorController.OperatorSize())
	require.Equal(t, 200, s.spanController.GetReplicatingSize())
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node2"))

	// add new node
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	s.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, 100, s.operatorController.OperatorSize())
	require.Equal(t, 100, s.spanController.GetSchedulingSize())
	require.Equal(t, 100, s.spanController.GetReplicatingSize())
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			_, ok := op.(*operator.SplitDispatcherOperator)
			require.True(t, ok)
		}
	}
	// still on the primary node
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node2"))

	// remove the node3
	delete(nodeManager.GetAliveNodes(), "node3")
	s.operatorController.OnNodeRemoved("node3")
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			op.OnTaskRemoved()
			require.True(t, op.IsFinished())
			op.PostFinish()
		}
	}

	require.Equal(t, 100, s.spanController.GetAbsentSize())
	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	// changed to working status
	require.Equal(t, 100, s.spanController.GetReplicatingSize())
	// ensure not always choose span in one node to split
	// TODO:to use a better to ensure select node more balanced
	require.Greater(t, 100, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Greater(t, 100, s.spanController.GetTaskSizeByNodeID("node2"))
}

// this test is to test the scenario that the split balance scheduler when a node is removed.
func TestSplitBalanceGroupsWithNodeRemove(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			WriteKeyThreshold:      500,
		},
	}, ddlSpan, 1000, 0)

	nodeIDList := []node.ID{"node1", "node2", "node3"}
	for i := 0; i < 100; i++ {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(int64(i))
		for j := 0; j < 6; j++ {
			span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, byte('a'+j)), EndKey: appendNew(totalSpan.StartKey, byte('b'+j))}
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
			spanReplica.SetNodeID(nodeIDList[j%3])
			s.spanController.AddReplicatingSpan(spanReplica)

			status := &heartbeatpb.TableSpanStatus{
				ID:                 spanReplica.ID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				EventSizePerSecond: 300,
				CheckpointTs:       2,
			}
			s.spanController.UpdateStatus(spanReplica, status)

		}

	}
	require.Equal(t, 0, s.operatorController.OperatorSize())
	require.Equal(t, 600, s.spanController.GetReplicatingSize())
	require.Equal(t, 200, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 200, s.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 200, s.spanController.GetTaskSizeByNodeID("node3"))

	// remove the node3
	delete(nodeManager.GetAliveNodes(), "node3")
	s.operatorController.OnNodeRemoved("node3")

	require.Equal(t, 200, s.spanController.GetAbsentSize())
	require.Equal(t, 0, s.spanController.GetSchedulingSize())

	s.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Equal(t, 200, s.operatorController.OperatorSize())

	for _, span := range s.spanController.GetAbsent() {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			op.Start()
		}
	}

	require.Equal(t, 200, s.spanController.GetSchedulingSize())

	for _, span := range s.spanController.GetScheduling() {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			op.PostFinish()
			s.operatorController.RemoveOp(op.ID())
		}
	}
	require.Equal(t, 600, s.spanController.GetReplicatingSize())

	// balance the spans
	s.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.LessOrEqual(t, 0, s.operatorController.OperatorSize())
	require.GreaterOrEqual(t, 200, s.operatorController.OperatorSize())

	for _, op := range s.operatorController.GetAllOperators() {
		require.Equal(t, op.Type(), "move")
		op.Start()
		op.Schedule()
		op.PostFinish()
	}

	require.Equal(t, 600, s.spanController.GetReplicatingSize())
	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	require.Equal(t, 0, s.spanController.GetAbsentSize())

	// balance after the schedule
	require.Equal(t, 300, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 300, s.spanController.GetTaskSizeByNodeID("node2"))
}

func TestBalance(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1, nil, nil, ddlSpan, 1000, 0)
	for i := 0; i < 100; i++ {
		sz := common.TableIDToComparableSpan(int64(i))
		span := &heartbeatpb.TableSpan{TableID: sz.TableID, StartKey: sz.StartKey, EndKey: sz.EndKey}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
		spanReplica.SetNodeID("node1")
		s.spanController.AddReplicatingSpan(spanReplica)
	}
	s.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 0, s.operatorController.OperatorSize())
	require.Equal(t, 100, s.spanController.GetReplicatingSize())
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node1"))

	// add new node
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	s.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 50, s.operatorController.OperatorSize())
	require.Equal(t, 50, s.spanController.GetSchedulingSize())
	require.Equal(t, 50, s.spanController.GetReplicatingSize())
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			_, ok := op.(*operator.MoveDispatcherOperator)
			require.True(t, ok)
		}
	}
	// still on the primary node
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node2"))

	// remove the node2
	delete(nodeManager.GetAliveNodes(), "node2")
	s.operatorController.OnNodeRemoved("node2")
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			require.Equal(t, "node1", msg.To.String())
			require.True(t, msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).ScheduleAction ==
				heartbeatpb.ScheduleAction_Create)
			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
		}
	}

	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	// changed to working status
	require.Equal(t, 100, s.spanController.GetReplicatingSize())
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node1"))
}

func TestStoppedWhenMoving(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID, common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1, nil, nil, ddlSpan, 1000, 0)
	for i := 0; i < 2; i++ {
		sz := common.TableIDToComparableSpan(int64(i))
		span := &heartbeatpb.TableSpan{TableID: sz.TableID, StartKey: sz.StartKey, EndKey: sz.EndKey}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
		spanReplica.SetNodeID("node1")
		s.spanController.AddReplicatingSpan(spanReplica)
	}
	require.Equal(t, 2, s.spanController.GetReplicatingSize())
	require.Equal(t, 2, s.spanController.GetTaskSizeByNodeID("node1"))
	// add new node
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	require.Equal(t, 0, s.spanController.GetAbsentSize())
	s.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 1, s.operatorController.OperatorSize())
	require.Equal(t, 1, s.spanController.GetSchedulingSize())
	require.Equal(t, 1, s.spanController.GetReplicatingSize())
	require.Equal(t, 2, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node2"))

	s.operatorController.OnNodeRemoved("node2")
	s.operatorController.OnNodeRemoved("node1")
	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	// changed to absent status
	require.Equal(t, 2, s.spanController.GetAbsentSize())
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node2"))
}

func TestFinishBootstrap(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1, &mockThreadPool{},
		config.GetDefaultReplicaConfig(), ddlSpan, 1000, 0)
	totalSpan := common.TableIDToComparableSpan(1)
	span := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: totalSpan.StartKey, EndKey: totalSpan.EndKey}
	schemaStore := &mockSchemaStore{
		tables: []commonEvent.Table{
			{
				TableID:         1,
				SchemaID:        1,
				SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"},
			},
		},
	}
	appcontext.SetService(appcontext.SchemaStore, schemaStore)
	dispatcherID2 := common.NewDispatcherID()
	require.False(t, s.bootstrapped)
	barrier, msg, err := s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"node1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID:              dispatcherID2.ToPB(),
					SchemaID:        1,
					Span:            span,
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    10,
				},
			},
			CheckpointTs: 10,
		},
	}, false)
	_ = msg
	require.Nil(t, err)
	require.NotNil(t, barrier)
	require.True(t, s.bootstrapped)
	require.Equal(t, msg.GetSchemas(), []*heartbeatpb.SchemaInfo{
		{
			SchemaID:   1,
			SchemaName: "test",
			Tables: []*heartbeatpb.TableInfo{
				{
					TableID:   1,
					TableName: "t",
				},
			},
		},
	})
	require.Equal(t, 1, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 1, s.spanController.GetReplicatingSize())
	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	require.NotNil(t, s.spanController.GetTaskByID(dispatcherID2))
	require.Panics(t, func() {
		_, _, _ = s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{}, false)
	})
}

func TestSplitTableWhenBootstrapFinished(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	defaultConfig := config.GetDefaultReplicaConfig().Clone()
	defaultConfig.Scheduler = &config.ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: true,
		RegionThreshold:        1,
		RegionCountPerSpan:     1,
	}
	s := NewController(cfID, 1, nil, defaultConfig, ddlSpan, 1000, 0)
	s.taskPool = &mockThreadPool{}
	schemaStore := &mockSchemaStore{tables: []commonEvent.Table{
		{TableID: 1, SchemaID: 1, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"}},
		{TableID: 2, SchemaID: 2, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t2"}},
	}}
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	totalSpan := common.TableIDToComparableSpan(1)
	totalSpan2 := common.TableIDToComparableSpan(2)

	regionCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)
	regionCache.SetRegions(fmt.Sprintf("%s-%s", totalSpan2.StartKey, totalSpan2.EndKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(1, totalSpan2.StartKey, appendNew(totalSpan2.StartKey, 'a')),
		testutil.MockRegionWithKeyRange(2, appendNew(totalSpan2.StartKey, 'a'), totalSpan2.EndKey),
	})

	span1 := &heartbeatpb.TableSpan{TableID: 1, StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')}
	span2 := &heartbeatpb.TableSpan{TableID: 1, StartKey: appendNew(totalSpan.StartKey, 'b'), EndKey: appendNew(totalSpan.StartKey, 'c')}

	reportedSpans := []*heartbeatpb.BootstrapTableSpan{
		{
			ID:              common.NewDispatcherID().ToPB(),
			SchemaID:        1,
			Span:            span1,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		},
		{
			ID:              common.NewDispatcherID().ToPB(),
			SchemaID:        1,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
			Span:            span2,
		},
	}
	require.False(t, s.bootstrapped)

	barrier, _, err := s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"node1": {
			ChangefeedID: cfID.ToPB(),
			Spans:        reportedSpans,
			CheckpointTs: 10,
		},
	}, false)
	require.Nil(t, err)
	require.NotNil(t, barrier)
	// total 8 regions,
	// table 1: 2 holes will be inserted to absent
	// table 2: split to 2 spans, will be inserted to absent
	require.Equal(t, 4, s.spanController.GetAbsentSize())
	// table 1 has two working span
	require.Equal(t, 2, s.spanController.GetReplicatingSize())
	require.True(t, s.bootstrapped)
}

func TestMapFindHole(t *testing.T) {
	cases := []struct {
		spans        []*heartbeatpb.TableSpan
		rang         *heartbeatpb.TableSpan
		expectedHole []*heartbeatpb.TableSpan
	}{
		{ // 0. all found.
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
		},
		{ // 1. on hole in the middle.
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_3")},
			},
		},
		{ // 2. two holes in the middle.
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")},
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")},
				{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")},
			},
		},
		{ // 3. all missing.
			spans: []*heartbeatpb.TableSpan{},
			rang:  &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			},
		},
		{ // 4. start not found
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_4"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_4")},
			},
		},
		{ // 5. end not found
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
			expectedHole: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_1"), EndKey: []byte("t2_0")},
			},
		},
	}

	for i, cs := range cases {
		m := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](common.LessTableSpan)
		for _, span := range cs.spans {
			m.ReplaceOrInsert(span, &replica.SpanReplication{})
		}
		holes := findHoles(m, cs.rang)
		require.Equalf(t, cs.expectedHole, holes, "case %d, %#v", i, cs)
	}
}

func appendNew(origin []byte, c byte) []byte {
	nb := bytes.Clone(origin)
	return append(nb, c)
}

type mockThreadPool struct {
	threadpool.ThreadPool
}

func (m *mockThreadPool) Submit(_ threadpool.Task, _ time.Time) *threadpool.TaskHandle {
	return nil
}

func (m *mockThreadPool) SubmitFunc(_ threadpool.FuncTask, _ time.Time) *threadpool.TaskHandle {
	return nil
}
