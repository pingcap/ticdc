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
	"context"
	"fmt"
	"math/rand"
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
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler"
	pkgOpearator "github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestSchedule(t *testing.T) {
	testutil.SetNodeManagerAndMessageCenter()
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
	controller := NewController(cfID, 1, nil, nil, nil, ddlSpan, 9, time.Minute)
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
	testutil.SetNodeManagerAndMessageCenter()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller := NewController(cfID, 1, nil, nil, nil, ddlSpan, 9, time.Minute)
	controller.spanController.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  int64(1),
	}, 1)
	require.Equal(t, 1, controller.spanController.GetAbsentSize())
	controller.operatorController.RemoveAllTasks()
	require.Equal(t, 0, controller.spanController.GetAbsentSize())
}

func TestBalanceGlobalEven(t *testing.T) {
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
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
	s := NewController(cfID, 1, nil, nil, nil, ddlSpan, 1000, 0)

	nodeID := node.ID("node1")
	for i := 0; i < 100; i++ {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(int64(i))
		span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
		spanReplica.SetNodeID(nodeID)
		s.spanController.AddReplicatingSpan(spanReplica)
	}
	s.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 0, s.operatorController.OperatorSize())
	require.Equal(t, 100, s.spanController.GetReplicatingSize())
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID(nodeID))

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

func TestBalanceGlobalUneven(t *testing.T) {
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
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
	s := NewController(cfID, 1, nil, nil, nil, ddlSpan, 1000, 0)
	for i := 0; i < 100; i++ {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(int64(i))
		span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
		if i < 50 {
			spanReplica.SetNodeID("node1")
		} else {
			spanReplica.SetNodeID("node2")
		}
		s.spanController.AddReplicatingSpan(spanReplica)
	}
	s.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 0, s.operatorController.OperatorSize())
	require.Equal(t, 100, s.spanController.GetReplicatingSize())
	require.Equal(t, 50, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 50, s.spanController.GetTaskSizeByNodeID("node2"))

	// add new node
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	s.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 33, s.operatorController.OperatorSize())
	require.Equal(t, 33, s.spanController.GetSchedulingSize())
	require.Equal(t, 67, s.spanController.GetReplicatingSize())
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			_, ok := op.(*operator.MoveDispatcherOperator)
			require.True(t, ok)
		}
	}
	// still on the primary node
	require.Equal(t, 50, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 50, s.spanController.GetTaskSizeByNodeID("node2"))

	// remove the node3
	delete(nodeManager.GetAliveNodes(), "node3")
	s.operatorController.OnNodeRemoved("node3")
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			require.NotEqual(t, "node3", msg.To.String())
			require.True(t, msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).ScheduleAction ==
				heartbeatpb.ScheduleAction_Create)
			op.Check(msg.To, &heartbeatpb.TableSpanStatus{
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
	require.Equal(t, 50, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 50, s.spanController.GetTaskSizeByNodeID("node2"))
}

func TestBalance(t *testing.T) {
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
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
	s := NewController(cfID, 1, nil, nil, nil, ddlSpan, 1000, 0)
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
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID, common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1, nil, nil, nil, ddlSpan, 1000, 0)
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
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
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
	s := NewController(cfID, 1, nil, &mockThreadPool{},
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

// 4 tasks and 2 servers, then add one server, no re-balance will be triggered
func TestBalanceUnEvenTask(t *testing.T) {
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
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
	s := NewController(cfID, 1, nil, nil, nil, ddlSpan, 1000, 0)

	for i := 0; i < 4; i++ {
		sz := common.TableIDToComparableSpan(int64(i))
		span := &heartbeatpb.TableSpan{TableID: sz.TableID, StartKey: sz.StartKey, EndKey: sz.EndKey}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1)
		s.spanController.AddAbsentReplicaSet(spanReplica)
	}
	for _, s := range s.schedulerController.GetSchedulers() {
		s.Execute()
	}

	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
			require.True(t, req.ScheduleAction == heartbeatpb.ScheduleAction_Create)
			op.Check(msg.To, &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
		}
	}
	require.Equal(t, 4, s.spanController.GetReplicatingSize())
	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	require.Equal(t, 4, s.operatorController.OperatorSize())
	s.operatorController.Execute()
	require.Equal(t, 0, s.operatorController.OperatorSize())

	// add new node
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	s.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 3, s.spanController.GetReplicatingSize())
	require.Equal(t, 1, s.operatorController.OperatorSize())
	// still on the primary node
	require.Equal(t, 2, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, s.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node3"))

	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
			require.True(t, req.ScheduleAction == heartbeatpb.ScheduleAction_Remove)
			op.Check(msg.To, &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
			})
			require.False(t, op.IsFinished())

			msg = op.Schedule()
			req = msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
			require.True(t, req.ScheduleAction == heartbeatpb.ScheduleAction_Create)
			op.Check(msg.To, &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
		}
	}
	require.Equal(t, 1, s.spanController.GetTaskSizeByNodeID("node3"))
}

/*
func TestSplitTableWhenBootstrapFinished(t *testing.T) {
	pdAPI := &mockPdAPI{
		regions: make(map[int64][]pdutil.RegionInfo),
	}
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	pdClock := pdutil.NewClock4Test()
	ddlSpan := replica.NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID,
		pdClock, common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	defaultConfig := config.GetDefaultReplicaConfig().Clone()
	defaultConfig.Scheduler = &config.ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: true,
		RegionThreshold:        0,
		WriteKeyThreshold:      1,
	}
	s := NewController(cfID, 1,
		pdAPI, pdClock, nil, nil, defaultConfig, ddlSpan, 1000, 0)
	s.taskScheduler = &mockThreadPool{}
	schemaStore := &mockSchemaStore{tables: []commonEvent.Table{
		{TableID: 1, SchemaID: 1, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"}},
		{TableID: 2, SchemaID: 2, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t2"}},
	}}
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	totalSpan := common.TableIDToComparableSpan(1)
	pdAPI.regions[1] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(1, totalSpan.StartKey, appendNew(totalSpan.StartKey, 'a'), uint64(1)),
		pdutil.NewTestRegionInfo(2, appendNew(totalSpan.StartKey, 'a'), appendNew(totalSpan.StartKey, 'b'), uint64(1)),
		pdutil.NewTestRegionInfo(3, appendNew(totalSpan.StartKey, 'b'), appendNew(totalSpan.StartKey, 'c'), uint64(1)),
		pdutil.NewTestRegionInfo(4, appendNew(totalSpan.StartKey, 'c'), totalSpan.EndKey, uint64(1)),
	}
	totalSpan2 := common.TableIDToComparableSpan(2)
	pdAPI.regions[2] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(2, appendNew(totalSpan2.StartKey, 'a'), appendNew(totalSpan2.StartKey, 'b'), uint64(1)),
		pdutil.NewTestRegionInfo(3, appendNew(totalSpan2.StartKey, 'b'), appendNew(totalSpan2.StartKey, 'c'), uint64(1)),
		pdutil.NewTestRegionInfo(4, appendNew(totalSpan2.StartKey, 'c'), appendNew(totalSpan2.StartKey, 'd'), uint64(1)),
		pdutil.NewTestRegionInfo(5, appendNew(totalSpan2.StartKey, 'd'), totalSpan2.EndKey, uint64(1)),
	}
	reportedSpans := []*heartbeatpb.BootstrapTableSpan{
		{
			ID:              common.NewDispatcherID().ToPB(),
			SchemaID:        1,
			Span:            &heartbeatpb.TableSpan{TableID: 1, StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')}, // 1 region // 1 region,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		},
		{
			ID:              common.NewDispatcherID().ToPB(),
			SchemaID:        1,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
			Span:            &heartbeatpb.TableSpan{TableID: 1, StartKey: appendNew(totalSpan.StartKey, 'b'), EndKey: appendNew(totalSpan.StartKey, 'c')},
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
*/

/*
func TestDynamicSplitTableBasic(t *testing.T) {
	pdAPI := &mockPdAPI{
		regions: make(map[int64][]pdutil.RegionInfo),
	}
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test")
	pdClock := pdutil.NewClock4Test()
	ddlSpan := replica.NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID,
		pdClock, common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	s := NewController(cfID, 1,
		pdAPI, pdClock, nil, nil, &config.ReplicaConfig{
			Scheduler: &config.ChangefeedSchedulerConfig{
				EnableTableAcrossNodes: true,
				RegionThreshold:        0,
				WriteKeyThreshold:      1,
			},
		}, ddlSpan, 1000, 0)
	s.taskScheduler = &mockThreadPool{}

	for i := 1; i <= 2; i++ {
		totalSpan := common.TableIDToComparableSpan(int64(i))
		span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: totalSpan.StartKey, EndKey: totalSpan.EndKey}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewReplicaSet(cfID, dispatcherID, pdClock, 1, span, 1)
		spanReplica.SetNodeID(node.ID(fmt.Sprintf("node%d", i)))
		s.spanController.AddReplicatingSpan(spanReplica)
	}
	totalSpan := common.TableIDToComparableSpan(1)
	pdAPI.regions[1] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(1, totalSpan.StartKey, appendNew(totalSpan.StartKey, 'a'), uint64(1)),
		pdutil.NewTestRegionInfo(2, appendNew(totalSpan.StartKey, 'a'), appendNew(totalSpan.StartKey, 'b'), uint64(1)),
		pdutil.NewTestRegionInfo(3, appendNew(totalSpan.StartKey, 'b'), appendNew(totalSpan.StartKey, 'c'), uint64(1)),
		pdutil.NewTestRegionInfo(4, appendNew(totalSpan.StartKey, 'c'), totalSpan.EndKey, uint64(1)),
	}
	totalSpan2 := common.TableIDToComparableSpan(2)
	pdAPI.regions[2] = []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(5, totalSpan2.StartKey, appendNew(totalSpan2.StartKey, 'a'), uint64(1)),
		pdutil.NewTestRegionInfo(6, appendNew(totalSpan2.StartKey, 'a'), appendNew(totalSpan2.StartKey, 'b'), uint64(1)),
		pdutil.NewTestRegionInfo(7, appendNew(totalSpan2.StartKey, 'b'), totalSpan2.EndKey, uint64(1)),
	}
	replicas := s.spanController.GetReplicating()
	require.Equal(t, 2, s.spanController.GetReplicatingSize())

	for _, task := range replicas {
		for cnt := 0; cnt < replica.HotSpanScoreThreshold; cnt++ {
			s.spanController.UpdateStatus(task, &heartbeatpb.TableSpanStatus{
				ID:                 task.ID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				CheckpointTs:       10,
				EventSizePerSecond: replica.HotSpanWriteThreshold,
			})
		}
	}
	s.schedulerController.GetScheduler(scheduler.SplitScheduler).Execute()
	require.Equal(t, 2, s.spanController.GetSchedulingSize())
	require.Equal(t, 2, s.operatorController.OperatorSize())
	for _, task := range replicas {
		op := s.operatorController.GetOperator(task.ID)
		op.Schedule()
		op.Check("node1", &heartbeatpb.TableSpanStatus{
			ID:              op.ID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		})
		op.PostFinish()
	}

	// total 7 regions,
	// table 1: split to 2 spans, will be inserted to absent
	// table 2: split to 2 spans, will be inserted to absent
	require.Equal(t, 4, s.spanController.GetAbsentSize())
}
*/

func TestDynamiSplitTableWhenScaleOut(t *testing.T) {
	t.Skip("skip unimplemented test")
}

func TestDynamicMergeAndSplitTable(t *testing.T) {
	t.Skip("skip flaky test")
	pdAPI := &mockPdAPI{
		regions: make(map[int64][]pdutil.RegionInfo),
	}
	regionCache := newMockRegionCache()
	appcontext.SetService(appcontext.RegionCache, regionCache)
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
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
	s := NewController(cfID, 1,
		pdAPI, nil, &config.ReplicaConfig{
			Scheduler: &config.ChangefeedSchedulerConfig{
				EnableTableAcrossNodes: true,
				RegionThreshold:        0,
				WriteKeyThreshold:      1,
			},
		}, ddlSpan, 1000, 0)
	s.taskPool = &mockThreadPool{}

	totalTables := 10
	victim := rand.Intn(totalTables) + 1
	for i := 1; i <= totalTables; i++ {
		totalSpan := common.TableIDToComparableSpan(int64(i))
		partialSpans := []*heartbeatpb.TableSpan{
			{TableID: int64(i), StartKey: totalSpan.StartKey, EndKey: appendNew(totalSpan.StartKey, 'a')},
			{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')},
			{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, 'b'), EndKey: totalSpan.EndKey},
		}
		if i == victim {
			// victim has hole, should not merged
			k := i % 3
			old := partialSpans
			partialSpans = old[:k]
			partialSpans = append(partialSpans, old[k+1:]...)
		}
		for idx, span := range partialSpans {
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewWorkingSpanReplication(cfID, dispatcherID, 1, span, &heartbeatpb.TableSpanStatus{
				ID:                 dispatcherID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				CheckpointTs:       10,
				EventSizePerSecond: replica.HotSpanWriteThreshold,
			}, node.ID(fmt.Sprintf("node%d", idx%2+1)))
			if idx == 0 {
				spanReplica.GetStatus().EventSizePerSecond = replica.HotSpanWriteThreshold * 100
			}
			s.spanController.AddReplicatingSpan(spanReplica)
		}

		// new split regions
		pdAPI.regions[1] = []pdutil.RegionInfo{
			pdutil.NewTestRegionInfo(1, totalSpan.StartKey, appendNew(totalSpan.StartKey, 'a'), uint64(1)),
			pdutil.NewTestRegionInfo(2, appendNew(totalSpan.StartKey, 'a'), totalSpan.EndKey, uint64(1)),
		}
	}
	replicas := s.spanController.GetReplicating()
	require.Equal(t, totalTables*3-1, s.spanController.GetReplicatingSize())

	scheduler := s.schedulerController.GetScheduler(scheduler.SplitScheduler)
	scheduler.Execute()
	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	require.Equal(t, totalTables*3-1, s.operatorController.OperatorSize())
	finishedCnt := 0
	for _, task := range replicas {
		op := s.operatorController.GetOperator(task.ID)
		op.Schedule()
		op.Check("node1", &heartbeatpb.TableSpanStatus{
			ID:              op.ID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		})
		if op.IsFinished() {
			op.PostFinish()
			finishedCnt++
		}
	}
	require.Less(t, finishedCnt, totalTables*3-1)

	// total 7 regions,
	// table 1: split to 4 spans, will be inserted to absent
	// table 2: split to 3 spans, will be inserted to absent
	require.Equal(t, 7, s.spanController.GetAbsentSize())
}

func TestDynamicMergeTableBasic(t *testing.T) {
	pdAPI := &mockPdAPI{
		regions: make(map[int64][]pdutil.RegionInfo),
	}
	regionCache := newMockRegionCache()
	appcontext.SetService(appcontext.RegionCache, regionCache)
	nodeManager := testutil.SetNodeManagerAndMessageCenter()
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
	s := NewController(cfID, 1,
		pdAPI, nil, &config.ReplicaConfig{
			Scheduler: &config.ChangefeedSchedulerConfig{
				EnableTableAcrossNodes: true,
				RegionThreshold:        0,
				WriteKeyThreshold:      1,
				SplitNumberPerNode:     1,
			},
		}, ddlSpan, 1000, 0)
	s.taskPool = &mockThreadPool{}

	mockPDClock := pdutil.NewClockWithValue4Test(time.Unix(0, 0))
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	totalTables := 10
	victim := rand.Intn(totalTables) + 1
	var holeSpan *heartbeatpb.TableSpan
	for i := 1; i <= totalTables; i++ {
		totalSpan := common.TableIDToComparableSpan(int64(i))
		partialSpans := []*heartbeatpb.TableSpan{
			{TableID: int64(i), StartKey: totalSpan.StartKey, EndKey: appendNew(totalSpan.StartKey, 'a')},
			{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, 'a'), EndKey: appendNew(totalSpan.StartKey, 'b')},
			{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, 'b'), EndKey: totalSpan.EndKey},
		}
		if i == victim {
			// victim has hole, should not merged
			k := i % 3
			old := partialSpans
			holeSpan = old[k]
			partialSpans = old[:k]
			partialSpans = append(partialSpans, old[k+1:]...)
		}
		for idx, span := range partialSpans {
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewWorkingSpanReplication(cfID, dispatcherID, 1, span, &heartbeatpb.TableSpanStatus{
				ID:                 dispatcherID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				CheckpointTs:       10,
				EventSizePerSecond: 0,
			}, node.ID(fmt.Sprintf("node%d", idx%2+1)))
			s.spanController.AddReplicatingSpan(spanReplica)
		}
	}

	expected := (totalTables - 1) * 3
	victimExpected := 2
	replicas := s.spanController.GetReplicating()
	require.Equal(t, expected+victimExpected, s.spanController.GetReplicatingSize())

	scheduler := s.schedulerController.GetScheduler(scheduler.SplitScheduler)
	for i := 0; i < replica.DefaultScoreThreshold; i++ {
		scheduler.Execute()
	}
	scheduler.Execute() // dummy execute does not take effect
	require.Equal(t, victimExpected, s.spanController.GetReplicatingSize())
	require.Equal(t, expected, s.spanController.GetSchedulingSize())
	require.Equal(t, expected, s.operatorController.OperatorSize())

	primarys := make(map[int64]pkgOpearator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus])
	for _, task := range replicas {
		op := s.operatorController.GetOperator(task.ID)
		if op == nil {
			require.Equal(t, int64(victim), task.Span.GetTableID())
			continue
		}
		op.Schedule()
		op.Check(task.GetNodeID(), &heartbeatpb.TableSpanStatus{
			ID:              op.ID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		})
		if op.IsFinished() {
			op.PostFinish()
		} else {
			primarys[task.Span.GetTableID()] = op
		}
	}
	for _, op := range primarys {
		finished := op.IsFinished()
		require.True(t, finished)
		op.PostFinish()
	}

	require.Equal(t, totalTables-1, s.spanController.GetAbsentSize())

	// merge the hole
	dispatcherID := common.NewDispatcherID()
	// the holeSpan is on node0, which is offlined
	spanReplica := replica.NewWorkingSpanReplication(cfID, dispatcherID, 1, holeSpan, &heartbeatpb.TableSpanStatus{
		ID:                 dispatcherID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		CheckpointTs:       10,
		EventSizePerSecond: 0,
	}, node.ID(fmt.Sprintf("node%d", 0)))
	s.spanController.AddReplicatingSpan(spanReplica)
	replicas = s.spanController.GetReplicating()
	require.Equal(t, 3, len(replicas))
	for i := 0; i < replica.DefaultScoreThreshold; i++ {
		scheduler.Execute()
	}
	require.Equal(t, 0, s.spanController.GetReplicatingSize())
	require.Equal(t, 30, s.operatorController.OperatorSize())
	primarys = make(map[int64]pkgOpearator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus])
	for _, task := range replicas {
		op := s.operatorController.GetOperator(task.ID)
		op.Schedule()
		op.Check(task.GetNodeID(), &heartbeatpb.TableSpanStatus{
			ID:              op.ID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
		})
		if op.IsFinished() {
			op.PostFinish()
		} else {
			primarys[task.Span.GetTableID()] = op
		}
	}
	for _, op := range primarys {
		finished := op.IsFinished()
		require.True(t, finished)
		op.PostFinish()
	}
	require.Equal(t, totalTables, s.spanController.GetAbsentSize())
}

func appendNew(origin []byte, c byte) []byte {
	nb := bytes.Clone(origin)
	return append(nb, c)
}

type mockPdAPI struct {
	pdutil.PDAPIClient
	regions map[int64][]pdutil.RegionInfo
}

func (m *mockPdAPI) ScanRegions(_ context.Context, span heartbeatpb.TableSpan) ([]pdutil.RegionInfo, error) {
	return m.regions[span.TableID], nil
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

// mockCache mocks tikv.RegionCache.
type mockCache struct{}

// NewMockRegionCache returns a new MockCache.
func newMockRegionCache() *mockCache {
	return &mockCache{}
}

func (m *mockCache) LoadRegionsInKeyRange(
	bo *tikv.Backoffer, startKey, endKey []byte,
) (regions []*tikv.Region, err error) {
	return nil, nil
}
