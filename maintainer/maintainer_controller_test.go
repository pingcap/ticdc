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
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler"
	pkgoperator "github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var testBalanceMoveBatchSize = config.NewDefaultSchedulerConfig().BalanceMoveBatchSize

var replicaConfig = &config.ReplicaConfig{
	Scheduler: &config.ChangefeedSchedulerConfig{
		BalanceScoreThreshold: util.AddressOf(1),
		MinTrafficPercentage:  util.AddressOf(0.8),
		MaxTrafficPercentage:  util.AddressOf(1.2),
	},
}

func init() {
	log.SetLevel(zap.DebugLevel)
	replica.SetEasyThresholdForTest()
}

func TestSchedule(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, nil, replicaConfig, ddlSpan, nil, 9, time.Minute, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)
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

func TestNewControllerInitializesMaintainerEpoch(t *testing.T) {
	testutil.SetUpTestServices(t)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlDispatcherID := common.NewDispatcherID()
	redoDDLDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	redoDDLSpan := replica.NewWorkingSpanReplication(cfID, redoDDLDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              redoDDLDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	const maintainerEpoch = uint64(42)

	controller := NewController(
		cfID,
		1,
		nil,
		replicaConfig,
		ddlSpan,
		redoDDLSpan,
		9,
		time.Minute,
		replica.NewRegionCountRefresher(cfID, time.Minute),
		common.DefaultKeyspace,
		true,
		testBalanceMoveBatchSize,
		maintainerEpoch,
	)

	require.Equal(t, maintainerEpoch, controller.currentMaintainerEpoch())
	require.Equal(t, maintainerEpoch, controller.operatorController.MaintainerEpoch())
	require.Equal(t, maintainerEpoch, controller.redoOperatorController.MaintainerEpoch())
}

// This case test the scenario that the balance scheduler when a new node join in.
// In this case, the num of split tables is more than the num of nodes,
// and we can select appropriate split spans to move
func TestBalanceGroupsNewNodeAdd_SplitsTableMoreThanNodeNum(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: util.AddressOf(true),
			WriteKeyThreshold:      util.AddressOf(500),
			BalanceScoreThreshold:  util.AddressOf(1),
			MinTrafficPercentage:   util.AddressOf(0.8),
			MaxTrafficPercentage:   util.AddressOf(1.2),
		},
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

	nodeID := node.ID("node1")
	for i := range 100 {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, int64(i))
		for j := 0; j < 4; j++ {
			span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, byte('a'+j)), EndKey: appendNew(totalSpan.StartKey, byte('b'+j))}
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1, common.DefaultMode, false)
			spanReplica.SetNodeID(nodeID)
			s.spanController.AddReplicatingSpan(spanReplica)

			preStatus := &heartbeatpb.TableSpanStatus{
				ID:                 spanReplica.ID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				EventSizePerSecond: 1,
				CheckpointTs:       2,
			}

			s.spanController.UpdateStatus(spanReplica, preStatus)
			s.spanController.UpdateStatus(spanReplica, preStatus)

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
			op.Start()
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
			// After the destination node is removed, the move operator must keep removing
			// the origin dispatcher and finally mark the span absent for rescheduling.
			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
			s.operatorController.RemoveOp(span.ID)
		}
	}

	// Reschedule absent spans back to the remaining node via basic scheduler.
	//
	// For split table spans, basic scheduler limits the number of scheduling spans
	// per group per node (default 1). So we may need multiple rounds to recover all
	// spans when multiple replicas of the same group become absent.
	for i := 0; s.spanController.GetAbsentSize() > 0; i++ {
		require.Less(t, i, 10)
		prevAbsent := s.spanController.GetAbsentSize()

		s.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
		for _, span := range s.spanController.GetTasksBySchemaID(1) {
			if op := s.operatorController.GetOperator(span.ID); op != nil {
				msg := op.Schedule()
				require.NotNil(t, msg)
				require.Equal(t, "node1", msg.To.String())
				require.Equal(t, heartbeatpb.ScheduleAction_Create,
					msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).ScheduleAction)
				op.Check("node1", &heartbeatpb.TableSpanStatus{
					ID:              span.ID.ToPB(),
					ComponentStatus: heartbeatpb.ComponentState_Working,
				})
				require.True(t, op.IsFinished())
				op.PostFinish()
				s.operatorController.RemoveOp(span.ID)
			}
		}
		require.Less(t, s.spanController.GetAbsentSize(), prevAbsent)
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
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: util.AddressOf(true),
			WriteKeyThreshold:      util.AddressOf(500),
			BalanceScoreThreshold:  util.AddressOf(1),
			MinTrafficPercentage:   util.AddressOf(0.8),
			MaxTrafficPercentage:   util.AddressOf(1.2),
		},
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

	regionCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)

	nodeIDList := []node.ID{"node1", "node2"}
	for i := range 100 {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, int64(i))
		for j := 0; j < 2; j++ {
			span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, byte('a'+j)), EndKey: appendNew(totalSpan.StartKey, byte('b'+j))}
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1, common.DefaultMode, false)
			spanReplica.SetNodeID(nodeIDList[j%2])
			s.spanController.AddReplicatingSpan(spanReplica)

			preStatus := &heartbeatpb.TableSpanStatus{
				ID:                 spanReplica.ID.ToPB(),
				ComponentStatus:    heartbeatpb.ComponentState_Working,
				EventSizePerSecond: 1,
				CheckpointTs:       2,
			}
			s.spanController.UpdateStatus(spanReplica, preStatus)
			s.spanController.UpdateStatus(spanReplica, preStatus)

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
			pdAPIClient := appcontext.GetService[*testutil.MockPDAPIClient](appcontext.PDAPIClient)
			pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", span.StartKey, span.EndKey), []pdutil.RegionInfo{
				{
					ID:           1,
					StartKey:     hex.EncodeToString(appendNew(totalSpan.StartKey, byte('a'+j))),
					EndKey:       hex.EncodeToString(appendNew(appendNew(totalSpan.StartKey, byte('a'+j)), 'a')),
					WrittenBytes: 1,
				},
				{
					ID:           2,
					StartKey:     hex.EncodeToString(appendNew(appendNew(totalSpan.StartKey, byte('a'+j)), 'a')),
					EndKey:       hex.EncodeToString(appendNew(totalSpan.StartKey, byte('b'+j))),
					WrittenBytes: 1,
				},
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
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: util.AddressOf(true),
			WriteKeyThreshold:      util.AddressOf(500),
			BalanceScoreThreshold:  util.AddressOf(1),
			MinTrafficPercentage:   util.AddressOf(0.8),
			MaxTrafficPercentage:   util.AddressOf(1.2),
		},
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

	nodeIDList := []node.ID{"node1", "node2", "node3"}
	for i := 0; i < 100; i++ {
		// generate 100 groups
		totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, int64(i))
		for j := 0; j < 6; j++ {
			span := &heartbeatpb.TableSpan{TableID: int64(i), StartKey: appendNew(totalSpan.StartKey, byte('a'+j)), EndKey: appendNew(totalSpan.StartKey, byte('b'+j))}
			dispatcherID := common.NewDispatcherID()
			spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1, common.DefaultMode, false)
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

func TestSplitTableBalanceWhenTrafficUnbalanced(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)

	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes:     util.AddressOf(true),
			WriteKeyThreshold:          util.AddressOf(1000),
			RegionThreshold:            util.AddressOf(20),
			RegionCountRefreshInterval: util.AddressOf(time.Minute),
			BalanceScoreThreshold:      util.AddressOf(1),
			MinTrafficPercentage:       util.AddressOf(0.8),
			MaxTrafficPercentage:       util.AddressOf(1.2),
		},
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

	nodeIDList := []node.ID{"node1", "node2", "node3"}
	// make a group
	regionCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)
	pdAPIClient := appcontext.GetService[*testutil.MockPDAPIClient](appcontext.PDAPIClient)
	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, int64(1))

	spanLists := make([]*replica.SpanReplication, 6)
	// span1
	startKey := appendNew(totalSpan.StartKey, byte('a'))
	endKey := appendNew(totalSpan.StartKey, byte('b'))
	span1 := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: startKey, EndKey: endKey}
	spanReplica := replica.NewSpanReplication(cfID, common.NewDispatcherID(), 1, span1, 1, common.DefaultMode, false)
	spanReplica.SetNodeID(nodeIDList[1])
	regionCache.SetRegions(fmt.Sprintf("%s-%s", span1.StartKey, span1.EndKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(uint64(1), startKey, appendNew(startKey, 'a')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'a'), appendNew(startKey, 'b')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'b'), appendNew(startKey, 'c')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'c'), endKey),
	})
	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", span1.StartKey, span1.EndKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'b')),
			WrittenBytes: 1,
		},
		{
			ID:           3,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'b')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'c')),
			WrittenBytes: 1,
		},
		{
			ID:           4,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'c')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})

	spanLists[0] = spanReplica
	// span2
	startKey = appendNew(totalSpan.StartKey, byte('b'))
	endKey = appendNew(totalSpan.StartKey, byte('c'))
	span2 := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: startKey, EndKey: endKey}
	spanReplica = replica.NewSpanReplication(cfID, common.NewDispatcherID(), 1, span2, 1, common.DefaultMode, false)
	spanReplica.SetNodeID(nodeIDList[0])
	regionCache.SetRegions(fmt.Sprintf("%s-%s", span2.StartKey, span2.EndKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(uint64(1), startKey, appendNew(startKey, 'a')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'a'), endKey),
	})
	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", span2.StartKey, span2.EndKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})
	spanLists[1] = spanReplica
	// span3
	startKey = appendNew(totalSpan.StartKey, byte('c'))
	endKey = appendNew(totalSpan.StartKey, byte('d'))
	span3 := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: startKey, EndKey: endKey}
	spanReplica = replica.NewSpanReplication(cfID, common.NewDispatcherID(), 1, span3, 1, common.DefaultMode, false)
	spanReplica.SetNodeID(nodeIDList[1])
	regionCache.SetRegions(fmt.Sprintf("%s-%s", span3.StartKey, span3.EndKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(uint64(1), startKey, appendNew(startKey, 'a')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'a'), endKey),
	})
	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", span3.StartKey, span3.EndKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})
	spanLists[2] = spanReplica
	// span4
	startKey = appendNew(totalSpan.StartKey, byte('d'))
	endKey = appendNew(totalSpan.StartKey, byte('e'))
	span4 := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: startKey, EndKey: endKey}
	spanReplica = replica.NewSpanReplication(cfID, common.NewDispatcherID(), 1, span4, 1, common.DefaultMode, false)
	spanReplica.SetNodeID(nodeIDList[2])
	regionCache.SetRegions(fmt.Sprintf("%s-%s", span4.StartKey, span4.EndKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(uint64(1), startKey, appendNew(startKey, 'a')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'a'), endKey),
	})
	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", span4.StartKey, span4.EndKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})
	spanLists[3] = spanReplica
	// span5
	startKey = appendNew(totalSpan.StartKey, byte('e'))
	endKey = appendNew(totalSpan.StartKey, byte('f'))
	span5 := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: startKey, EndKey: endKey}
	spanReplica = replica.NewSpanReplication(cfID, common.NewDispatcherID(), 1, span5, 1, common.DefaultMode, false)
	spanReplica.SetNodeID(nodeIDList[0])
	regionCache.SetRegions(fmt.Sprintf("%s-%s", span5.StartKey, span5.EndKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(uint64(1), startKey, appendNew(startKey, 'a')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'a'), endKey),
	})
	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", span5.StartKey, span5.EndKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})
	spanLists[4] = spanReplica
	// span6
	startKey = appendNew(totalSpan.StartKey, byte('f'))
	endKey = totalSpan.EndKey
	span6 := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: startKey, EndKey: endKey}
	spanReplica = replica.NewSpanReplication(cfID, common.NewDispatcherID(), 1, span6, 1, common.DefaultMode, false)
	spanReplica.SetNodeID(nodeIDList[2])
	regionCache.SetRegions(fmt.Sprintf("%s-%s", span6.StartKey, span6.EndKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(uint64(1), startKey, appendNew(startKey, 'a')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'a'), endKey),
	})
	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", span6.StartKey, span6.EndKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})
	spanLists[5] = spanReplica

	currentTime := time.Now()
	for j := 0; j < 6; j++ {
		spanReplica = spanLists[j]
		controller.spanController.AddReplicatingSpan(spanReplica)

		status := &heartbeatpb.TableSpanStatus{
			ID:                 spanReplica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 200,
			CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
		}
		// provide the last three traffic status
		for i := 0; i < 3; i++ {
			controller.spanController.UpdateStatus(spanReplica, status)
		}

		log.Info("spanReplica", zap.Any("j", j), zap.Any("id", spanReplica.ID), zap.Any("span", common.FormatTableSpan(spanReplica.Span)))
	}

	// first keep the system is balanced
	// node1 | node2 | node3
	// 200   | 200   | 200
	// 200   | 200   | 200
	require.Equal(t, 0, controller.operatorController.OperatorSize())
	require.Equal(t, 6, controller.spanController.GetReplicatingSize())
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node3"))

	// increase the traffic of span3 and span4
	// node1 | node2 | node3
	// 200   | 600   | 500
	// 200   | 200   | 200
	spanReplica3 := spanLists[2]
	spanReplica4 := spanLists[3]
	status3 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplica3.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 600,
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}
	status4 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplica4.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 500,
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}
	// provide the last three traffic status
	for i := 0; i < 2; i++ {
		controller.spanController.UpdateStatus(spanReplica3, status3)
		controller.spanController.UpdateStatus(spanReplica4, status4)
	}

	// check the balance result, check no operator here
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, controller.operatorController.OperatorSize(), 0)

	// update the traffic again, will split the spanReplica1
	controller.spanController.UpdateStatus(spanReplica3, status3)
	controller.spanController.UpdateStatus(spanReplica4, status4)

	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, controller.operatorController.OperatorSize(), 1)
	operatorItem := controller.operatorController.GetAllOperators()[0]
	require.Equal(t, operatorItem.Type(), "split")
	require.Equal(t, operatorItem.ID(), spanLists[0].ID)

	operatorItem.Start()
	operatorItem.PostFinish()
	controller.operatorController.RemoveOp(operatorItem.ID())

	// will get 2 new add operator
	require.Equal(t, controller.operatorController.OperatorSize(), 2)
	operators := controller.operatorController.GetAllOperators()
	require.Equal(t, operators[0].Type(), "add")
	require.Equal(t, operators[1].Type(), "add")
	var spanReplica7, spanReplica8 *replica.SpanReplication
	var spanReplicaID7, spanReplicaID8 common.DispatcherID
	spanTmp1 := operators[0].ID()
	spanTmp2 := operators[1].ID()
	spanReplicaTmp1 := controller.spanController.GetTaskByID(spanTmp1)
	spanReplicaTmp2 := controller.spanController.GetTaskByID(spanTmp2)
	if spanReplicaTmp1.GetNodeID() == "node1" {
		spanReplica7 = spanReplicaTmp1
		spanReplicaID7 = spanTmp1
		spanReplica8 = spanReplicaTmp2
		spanReplicaID8 = spanTmp2
	} else {
		spanReplica7 = spanReplicaTmp2
		spanReplicaID7 = spanTmp2
		spanReplica8 = spanReplicaTmp1
		spanReplicaID8 = spanTmp1
	}

	log.Info("spanReplica7", zap.Any("changefeedID", spanReplica7.ID), zap.Any("span", common.FormatTableSpan(spanReplica7.Span)))
	log.Info("spanReplica8", zap.Any("changefeedID", spanReplica8.ID), zap.Any("span", common.FormatTableSpan(spanReplica8.Span)))
	trafficForSpanReplica7 := 50
	trafficForSpanReplica8 := 150

	operators[0].Start()
	operators[0].PostFinish()
	operators[1].Start()
	operators[1].PostFinish()
	controller.operatorController.RemoveOp(operators[0].ID())
	controller.operatorController.RemoveOp(operators[1].ID())

	require.Equal(t, 7, controller.spanController.GetReplicatingSize())
	require.Equal(t, 3, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node3"))

	startKey = spanReplica8.Span.StartKey
	endKey = spanReplica8.Span.EndKey

	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", startKey, endKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})

	// update traffic for the new span replication
	// node1 | node2 | node3
	// 200   | 600   | 500
	// 200   | 150   | 200
	// 50    |       |
	status7 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplicaID7.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: float32(trafficForSpanReplica7),
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}
	status8 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplicaID8.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: float32(trafficForSpanReplica8),
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}

	// provide the last three traffic status
	for i := 0; i < 3; i++ {
		controller.spanController.UpdateStatus(spanReplica7, status7)
		controller.spanController.UpdateStatus(spanReplica8, status8)
	}

	// will split spanReplica8
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, controller.operatorController.OperatorSize(), 1)
	operatorItem = controller.operatorController.GetAllOperators()[0]
	require.Equal(t, operatorItem.Type(), "split")
	require.Equal(t, operatorItem.ID(), spanReplicaID8)

	operatorItem.Start()
	operatorItem.PostFinish()
	controller.operatorController.RemoveOp(operatorItem.ID())

	// will get 2 new add operator
	require.Equal(t, controller.operatorController.OperatorSize(), 2)
	operators = controller.operatorController.GetAllOperators()
	require.Equal(t, operators[0].Type(), "add")
	require.Equal(t, operators[1].Type(), "add")
	spanID1 := operators[0].ID()
	spanID2 := operators[1].ID()
	spanReplica1 := controller.spanController.GetTaskByID(spanID1)
	spanReplica2 := controller.spanController.GetTaskByID(spanID2)
	var spanReplica9, spanReplica10 *replica.SpanReplication
	var spanReplicaID9, spanReplicaID10 common.DispatcherID
	if spanReplica1.GetNodeID() == "node1" {
		spanReplica9 = spanReplica1
		spanReplica10 = spanReplica2
		spanReplicaID9 = spanID1
		spanReplicaID10 = spanID2
	} else {
		spanReplica10 = spanReplica1
		spanReplica9 = spanReplica2
		spanReplicaID10 = spanID1
		spanReplicaID9 = spanID2
	}

	log.Info("spanReplica9", zap.Any("changefeedID", spanReplica9.ID), zap.Any("span", common.FormatTableSpan(spanReplica9.Span)))
	log.Info("spanReplica10", zap.Any("changefeedID", spanReplica10.ID), zap.Any("span", common.FormatTableSpan(spanReplica10.Span)))
	operators[0].Start()
	operators[0].PostFinish()
	operators[1].Start()
	operators[1].PostFinish()
	controller.operatorController.RemoveOp(operators[0].ID())
	controller.operatorController.RemoveOp(operators[1].ID())

	require.Equal(t, 8, controller.spanController.GetReplicatingSize())
	require.Equal(t, 4, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node3"))

	// update the traffic for the new span replication
	// node1 | node2 | node3
	// 200   | 600   | 500
	// 200   | 50   | 200
	// 50    |       |
	// 100   |       |
	status9 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplicaID9.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 100,
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}
	status10 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplicaID10.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 50,
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}

	// provide the last three traffic status
	for i := 0; i < 3; i++ {
		controller.spanController.UpdateStatus(spanReplica9, status9)
		controller.spanController.UpdateStatus(spanReplica10, status10)
	}

	// trigger merge, merge spanReplica7 and spanReplica9
	// node1 | node2 | node3
	// 200   | 600   | 500
	// 200   | 50    | 200
	// 150   |       |
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, controller.operatorController.OperatorSize(), 3)
	typeCount := make(map[string][]pkgoperator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus])

	for _, op := range controller.operatorController.GetAllOperators() {
		typeCount[op.Type()] = append(typeCount[op.Type()], op)
	}
	require.Equal(t, len(typeCount["occupy"]), 2)
	require.Equal(t, len(typeCount["merge"]), 1)
	relatedIDs := []common.DispatcherID{typeCount["occupy"][0].ID(), typeCount["occupy"][1].ID()}
	require.Contains(t, relatedIDs, spanReplicaID7)
	require.Contains(t, relatedIDs, spanReplicaID9)
	spanReplicaID11 := typeCount["merge"][0].ID()
	spanReplica11 := controller.spanController.GetTaskByID(spanReplicaID11)
	log.Info("spanReplica11", zap.Any("changefeedID", spanReplica11.ID), zap.Any("span", common.FormatTableSpan(spanReplica11.Span)))
	typeCount["merge"][0].Start()
	typeCount["merge"][0].PostFinish()
	controller.operatorController.RemoveOp(typeCount["merge"][0].ID())
	controller.operatorController.RemoveOp(typeCount["occupy"][0].ID())
	controller.operatorController.RemoveOp(typeCount["occupy"][1].ID())

	require.Equal(t, 7, controller.spanController.GetReplicatingSize())
	require.Equal(t, 3, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node3"))

	status11 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplicaID11.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 150,
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}

	// provide the last three traffic status
	for i := 0; i < 3; i++ {
		controller.spanController.UpdateStatus(spanReplica11, status11)
	}

	// trigger move
	// node1 | node2 | node3
	// 200   | 600   | 500
	// 200   |       | 200
	// 150   |       |
	// 50    |       |
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	for _, operator := range controller.operatorController.GetAllOperators() {
		log.Info("operator", zap.Any("type", operator.Type()), zap.Any("id", operator.ID()), zap.Any("string", operator.String()))
	}
	require.Equal(t, controller.operatorController.OperatorSize(), 3)

	allOperators := controller.operatorController.GetAllOperators()
	require.Len(t, allOperators, 3)

	expectedIDs := map[common.DispatcherID]bool{
		spanReplicaID10: true,
		spanLists[5].ID: true,
		spanLists[4].ID: true,
	}

	for _, op := range allOperators {
		require.Equal(t, "move", op.Type())
		require.True(t, expectedIDs[op.ID()], "Unexpected operator ID: %v", op.ID())
	}

	for _, op := range allOperators {
		op.Start()
		msg := op.Schedule()
		require.NotNil(t, msg)
		origin := msg.To
		op.Check(origin, &heartbeatpb.TableSpanStatus{
			ID:              op.ID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    1,
		})

		msg = op.Schedule()
		require.NotNil(t, msg)
		dest := msg.To
		op.Check(dest, &heartbeatpb.TableSpanStatus{
			ID:              op.ID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		})
		op.PostFinish()
		controller.operatorController.RemoveOp(op.ID())
	}

	require.Equal(t, 7, controller.spanController.GetReplicatingSize())
	require.Equal(t, 4, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 1, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node3"))

	// trigger merge
	// node1 | node2 | node3
	// 200   | 600   | 700
	// 400   |       |
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	for _, operator := range controller.operatorController.GetAllOperators() {
		log.Info("operator", zap.Any("type", operator.Type()), zap.Any("changefeedID", operator.ID()), zap.Any("string", operator.String()))
	}
	require.Equal(t, controller.operatorController.OperatorSize(), 7)
	typeCount = make(map[string][]pkgoperator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus])

	for _, op := range controller.operatorController.GetAllOperators() {
		typeCount[op.Type()] = append(typeCount[op.Type()], op)
	}
	require.Equal(t, len(typeCount["occupy"]), 5)
	require.Equal(t, len(typeCount["merge"]), 2)
	relatedIDs = []common.DispatcherID{typeCount["occupy"][0].ID(), typeCount["occupy"][1].ID(), typeCount["occupy"][2].ID(), typeCount["occupy"][3].ID(), typeCount["occupy"][4].ID()}
	require.Contains(t, relatedIDs, spanReplicaID10)
	require.Contains(t, relatedIDs, spanReplicaID11)
	require.Contains(t, relatedIDs, spanLists[1].ID)
	require.Contains(t, relatedIDs, spanLists[3].ID)
	require.Contains(t, relatedIDs, spanLists[4].ID)

	spanTmp1 = typeCount["merge"][0].ID()
	spanTmp2 = typeCount["merge"][1].ID()
	spanReplicaTmp1 = controller.spanController.GetTaskByID(spanTmp1)
	spanReplicaTmp2 = controller.spanController.GetTaskByID(spanTmp2)

	var spanReplica12, spanReplica13 *replica.SpanReplication
	var spanReplicaID12, spanReplicaID13 common.DispatcherID
	if spanReplicaTmp1.GetNodeID() == "node1" {
		spanReplica12 = spanReplicaTmp1
		spanReplicaID12 = spanTmp1
		spanReplica13 = spanReplicaTmp2
		spanReplicaID13 = spanTmp2
	} else {
		spanReplica12 = spanReplicaTmp2
		spanReplicaID12 = spanTmp2
		spanReplica13 = spanReplicaTmp1
		spanReplicaID13 = spanTmp1
	}

	typeCount["merge"][0].Start()
	typeCount["merge"][0].PostFinish()

	typeCount["merge"][1].Start()
	typeCount["merge"][1].PostFinish()

	controller.operatorController.RemoveOp(typeCount["merge"][0].ID())
	controller.operatorController.RemoveOp(typeCount["merge"][1].ID())
	controller.operatorController.RemoveOp(typeCount["occupy"][0].ID())
	controller.operatorController.RemoveOp(typeCount["occupy"][1].ID())
	controller.operatorController.RemoveOp(typeCount["occupy"][2].ID())
	controller.operatorController.RemoveOp(typeCount["occupy"][3].ID())
	controller.operatorController.RemoveOp(typeCount["occupy"][4].ID())

	require.Equal(t, 4, controller.spanController.GetReplicatingSize())
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 1, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 1, controller.spanController.GetTaskSizeByNodeID("node3"))

	// update the traffic for the new span replication
	status12 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplicaID12.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 400,
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}

	// provide the last three traffic status
	for i := 0; i < 3; i++ {
		controller.spanController.UpdateStatus(spanReplica12, status12)
	}

	status13 := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplicaID13.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 700,
		CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-5*time.Second).UnixMilli()), 0),
	}
	for i := 0; i < 3; i++ {
		controller.spanController.UpdateStatus(spanReplica13, status13)
	}

	// no more operators
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, controller.operatorController.OperatorSize(), 0)
}

func TestBalance(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, replicaConfig, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)
	for i := 0; i < 100; i++ {
		sz := common.TableIDToComparableSpan(common.DefaultKeyspaceID, int64(i))
		span := &heartbeatpb.TableSpan{TableID: sz.TableID, StartKey: sz.StartKey, EndKey: sz.EndKey}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1, common.DefaultMode, false)
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
			// After the destination node is removed, the move operator should generate
			// a remove message for the origin dispatcher first, then mark the span absent
			// once the origin dispatcher is confirmed stopped.
			msg := op.Schedule()
			require.NotNil(t, msg)
			require.Equal(t, "node1", msg.To.String())
			require.Equal(t, heartbeatpb.ScheduleAction_Remove,
				msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).ScheduleAction)

			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
			s.operatorController.RemoveOp(span.ID)
		}
	}

	// Reschedule absent spans back to the remaining node via basic scheduler.
	s.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	for _, span := range s.spanController.GetTasksBySchemaID(1) {
		if op := s.operatorController.GetOperator(span.ID); op != nil {
			msg := op.Schedule()
			require.NotNil(t, msg)
			require.Equal(t, "node1", msg.To.String())
			require.Equal(t, heartbeatpb.ScheduleAction_Create,
				msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).ScheduleAction)
			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
			})
			require.True(t, op.IsFinished())
			op.PostFinish()
			s.operatorController.RemoveOp(span.ID)
		}
	}

	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	// changed to working status
	require.Equal(t, 100, s.spanController.GetReplicatingSize())
	require.Equal(t, 100, s.spanController.GetTaskSizeByNodeID("node1"))
}

func TestDefaultSpanIntoSplit(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)

	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes:     util.AddressOf(true),
			WriteKeyThreshold:          util.AddressOf(1000),
			RegionThreshold:            util.AddressOf(8),
			RegionCountRefreshInterval: util.AddressOf(time.Minute),
			SchedulingTaskCountPerNode: util.AddressOf(10),
			BalanceScoreThreshold:      util.AddressOf(1),
			MinTrafficPercentage:       util.AddressOf(0.8),
			MaxTrafficPercentage:       util.AddressOf(1.2),
		},
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)
	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 1)
	span := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: totalSpan.StartKey, EndKey: totalSpan.EndKey}
	dispatcherID := common.NewDispatcherID()
	spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1, common.DefaultMode, true)
	spanReplica.SetNodeID("node1")
	controller.spanController.AddReplicatingSpan(spanReplica)

	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	controller.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	controller.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Equal(t, 0, controller.operatorController.OperatorSize())

	require.Equal(t, 1, controller.spanController.GetReplicatingSize())
	require.Equal(t, 1, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, controller.spanController.GetTaskSizeByNodeID("node2"))

	// update the traffic for the new span replication, to make split
	status := &heartbeatpb.TableSpanStatus{
		ID:                 spanReplica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 1200,
		CheckpointTs:       2,
	}

	regionCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)
	startKey := spanReplica.Span.StartKey
	endKey := spanReplica.Span.EndKey
	regionCache.SetRegions(fmt.Sprintf("%s-%s", startKey, endKey), []*tikv.Region{
		testutil.MockRegionWithKeyRange(uint64(1), startKey, appendNew(startKey, 'a')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'a'), appendNew(startKey, 'b')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'b'), appendNew(startKey, 'c')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'c'), appendNew(startKey, 'd')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'd'), appendNew(startKey, 'e')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'e'), appendNew(startKey, 'f')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'f'), appendNew(startKey, 'g')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'g'), appendNew(startKey, 'h')),
		testutil.MockRegionWithKeyRange(uint64(1), appendNew(startKey, 'h'), endKey),
	})
	pdAPIClient := appcontext.GetService[*testutil.MockPDAPIClient](appcontext.PDAPIClient)
	pdAPIClient.SetScanRegionsResult(fmt.Sprintf("%s-%s", startKey, endKey), []pdutil.RegionInfo{
		{
			ID:           1,
			StartKey:     hex.EncodeToString(startKey),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'a')),
			WrittenBytes: 1,
		},
		{
			ID:           2,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'a')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'b')),
			WrittenBytes: 1,
		},
		{
			ID:           3,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'b')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'c')),
			WrittenBytes: 1,
		},
		{
			ID:           4,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'c')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'd')),
			WrittenBytes: 1,
		},
		{
			ID:           5,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'd')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'e')),
			WrittenBytes: 1,
		},
		{
			ID:           6,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'e')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'f')),
			WrittenBytes: 1,
		},
		{
			ID:           7,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'f')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'g')),
			WrittenBytes: 1,
		},
		{
			ID:           8,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'g')),
			EndKey:       hex.EncodeToString(appendNew(startKey, 'h')),
			WrittenBytes: 1,
		},
		{
			ID:           9,
			StartKey:     hex.EncodeToString(appendNew(startKey, 'h')),
			EndKey:       hex.EncodeToString(endKey),
			WrittenBytes: 1,
		},
	})

	for i := 0; i < 3; i++ {
		controller.spanController.UpdateStatus(spanReplica, status)
	}

	controller.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	require.Equal(t, 1, controller.operatorController.OperatorSize())
	operatorItem := controller.operatorController.GetAllOperators()[0]
	require.Equal(t, operatorItem.Type(), "split")
	require.Equal(t, operatorItem.ID(), spanReplica.ID)
	operatorItem.Start()
	operatorItem.PostFinish()
	controller.operatorController.RemoveOp(operatorItem.ID())

	require.Equal(t, 0, controller.spanController.GetReplicatingSize())
	require.Equal(t, 4, controller.spanController.GetAbsentSize())

	controller.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Equal(t, 4, controller.operatorController.OperatorSize())

	for _, operatorItem := range controller.operatorController.GetAllOperators() {
		require.Equal(t, operatorItem.Type(), "add")
		operatorItem.Start()
		operatorItem.PostFinish()
		controller.operatorController.RemoveOp(operatorItem.ID())
	}

	require.Equal(t, 4, controller.spanController.GetReplicatingSize())
	require.Equal(t, 0, controller.spanController.GetAbsentSize())
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node2"))
}

func TestStoppedWhenMoving(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID, common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, replicaConfig, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)
	for i := 0; i < 2; i++ {
		sz := common.TableIDToComparableSpan(common.DefaultKeyspaceID, int64(i))
		span := &heartbeatpb.TableSpan{TableID: sz.TableID, StartKey: sz.StartKey, EndKey: sz.EndKey}
		dispatcherID := common.NewDispatcherID()
		spanReplica := replica.NewSpanReplication(cfID, dispatcherID, 1, span, 1, common.DefaultMode, false)
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

	operatorItem := s.operatorController.GetAllOperators()[0]
	operatorItem.Check("node1", &heartbeatpb.TableSpanStatus{
		ID:              operatorItem.ID().ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
	})
	operatorItem.Start()
	operatorItem.PostFinish()
	s.operatorController.RemoveOp(operatorItem.ID())
	s.operatorController.OnNodeRemoved("node2")
	s.operatorController.OnNodeRemoved("node1")
	require.Equal(t, 0, s.spanController.GetSchedulingSize())
	// changed to absent status
	require.Equal(t, 2, s.spanController.GetAbsentSize())
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, s.spanController.GetTaskSizeByNodeID("node2"))
}

func TestFinishBootstrap(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, &mockThreadPool{},
		config.GetDefaultReplicaConfig(), ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)
	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 1)
	span := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: totalSpan.StartKey, EndKey: totalSpan.EndKey}
	schemaStore := eventservice.NewMockSchemaStore()
	schemaStore.SetTables(
		[]commonEvent.Table{
			{
				TableID:         1,
				SchemaID:        1,
				SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"},
			},
		},
	)
	appcontext.SetService(appcontext.SchemaStore, schemaStore)
	dispatcherID2 := common.NewDispatcherID()
	require.False(t, s.bootstrapped)
	msg, err := s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
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
	require.NotNil(t, s.barrier)
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

	postBootstrapRequest, err := s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{}, false)
	require.NoError(t, err)
	require.Nil(t, postBootstrapRequest)
}

func TestFinishBootstrapReturnsErrorWhenCheckpointMissing(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, &mockThreadPool{},
		config.GetDefaultReplicaConfig(), ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

	postBootstrapRequest, err := controller.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"node1": {
			ChangefeedID: cfID.ToPB(),
		},
	}, false)
	require.Nil(t, postBootstrapRequest)
	require.Error(t, err)
	code, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrChangefeedInitTableTriggerDispatcherFailed.RFCCode(), code)
	require.Contains(t, err.Error(), "all bootstrap responses reported empty checkpointTs")
	require.False(t, controller.bootstrapped)
}

// TestFinishBootstrapSkipsStaleCreateOperatorForDroppedTable covers stale bootstrap Create requests
// for dropped tables across add/move/split operator types. Each subtest boots from an empty schema
// snapshot and verifies bootstrap skips the stale create phase instead of recreating ghost tasks or
// operators, even if dispatcher manager still reports a stale runtime span snapshot.
func TestFinishBootstrapSkipsStaleCreateOperatorForDroppedTable(t *testing.T) {
	testCases := []struct {
		name                string
		operatorType        heartbeatpb.OperatorType
		includeReportedSpan bool
	}{
		{
			name:         "stale add create",
			operatorType: heartbeatpb.OperatorType_O_Add,
		},
		{
			name:         "stale move create phase",
			operatorType: heartbeatpb.OperatorType_O_Move,
		},
		{
			name:         "stale split create phase",
			operatorType: heartbeatpb.OperatorType_O_Split,
		},
		{
			name:                "stale add create with reported runtime span",
			operatorType:        heartbeatpb.OperatorType_O_Add,
			includeReportedSpan: true,
		},
	}

	for _, tc := range testCases {
		// Each subtest restores bootstrap state from a dropped-table snapshot and checks that
		// no maintainer task/operator is recreated for the stale create request.
		t.Run(tc.name, func(t *testing.T) {
			testutil.SetUpTestServices(t)
			nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
			nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

			tableTriggerEventDispatcherID := common.NewDispatcherID()
			cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
			ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
				common.DDLSpanSchemaID,
				common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
					ID:              tableTriggerEventDispatcherID.ToPB(),
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    1,
				}, "node1", false)
			refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
			s := NewController(cfID, 1, &mockThreadPool{},
				config.GetDefaultReplicaConfig(), ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

			// The schema-store snapshot is empty at bootstrap startTs, which models a table
			// that has already been dropped before failover recovery starts.
			schemaStore := eventservice.NewMockSchemaStore()
			schemaStore.SetTables(nil)
			appcontext.SetService(appcontext.SchemaStore, schemaStore)

			droppedDispatcherID := common.NewDispatcherID()
			droppedSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 2)
			resp := &heartbeatpb.MaintainerBootstrapResponse{
				ChangefeedID: cfID.ToPB(),
				Operators: []*heartbeatpb.ScheduleDispatcherRequest{
					{
						ChangefeedID: cfID.ToPB(),
						Config: &heartbeatpb.DispatcherConfig{
							DispatcherID: droppedDispatcherID.ToPB(),
							SchemaID:     2,
							Span:         &droppedSpan,
							StartTs:      10,
							Mode:         common.DefaultMode,
						},
						ScheduleAction: heartbeatpb.ScheduleAction_Create,
						OperatorType:   tc.operatorType,
					},
				},
				CheckpointTs: 10,
			}
			if tc.includeReportedSpan {
				resp.Spans = []*heartbeatpb.BootstrapTableSpan{
					{
						ID:              droppedDispatcherID.ToPB(),
						SchemaID:        2,
						Span:            &droppedSpan,
						ComponentStatus: heartbeatpb.ComponentState_Working,
						CheckpointTs:    10,
						Mode:            common.DefaultMode,
					},
				}
			}

			_, err := s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
				"node1": resp,
			}, false)
			require.NoError(t, err)
			require.True(t, s.bootstrapped)
			require.Nil(t, s.spanController.GetTaskByID(droppedDispatcherID))
			require.Nil(t, s.operatorController.GetOperator(droppedDispatcherID))
			require.Zero(t, s.operatorController.OperatorSize())
			require.Zero(t, s.spanController.GetAbsentSize())
			require.Empty(t, s.spanController.GetTasksByTableID(droppedSpan.TableID))
		})
	}
}

// TestFinishBootstrapRepairsCoverageAfterRestoredStandaloneRemove covers failover after an
// orphan Working dispatcher has already been journaled under a standalone remove request. The
// test restores that request alongside adjacent live coverage, reports one terminal status, and
// finalizes the operator. The removed range must become an exact absent span that the basic
// scheduler can schedule again, while the adjacent dispatcher remains untouched.
func TestFinishBootstrapRepairsCoverageAfterRestoredStandaloneRemove(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)
	responses := env.bootstrapResponses(
		nil,
		env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_Working),
		env.bootstrapSpan(env.sourceDispatcherID2, env.sourceSpan2, heartbeatpb.ComponentState_Working),
	)
	responses[env.nodeID].Operators = []*heartbeatpb.ScheduleDispatcherRequest{
		env.standaloneRemoveRequest(env.sourceDispatcherID1, env.sourceSpan1),
	}

	_, err := env.controller.FinishBootstrap(responses, false)
	require.NoError(t, err)
	require.Equal(t, 2, env.controller.spanController.GetReplicatingSize())
	require.Zero(t, env.controller.spanController.GetAbsentSize())

	restoredRemove := env.controller.operatorController.GetOperator(env.sourceDispatcherID1)
	require.NotNil(t, restoredRemove)
	require.Equal(t, "remove", restoredRemove.Type())

	// A single terminal status finishes the restored operator. The same HandleStatus call still
	// sees the operator, so coverage repair must happen when the controller finalizes it.
	env.controller.HandleStatus(env.nodeID, []*heartbeatpb.TableSpanStatus{
		{
			ID:              env.sourceDispatcherID1.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
			Mode:            common.DefaultMode,
		},
	})
	require.True(t, restoredRemove.IsFinished())
	require.NotNil(t, env.controller.operatorController.GetOperator(env.sourceDispatcherID1))
	require.Zero(t, env.controller.spanController.GetAbsentSize())

	env.controller.operatorController.Execute()

	require.Nil(t, env.controller.operatorController.GetOperator(env.sourceDispatcherID1))
	require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID2))
	require.Equal(t, 1, env.controller.spanController.GetReplicatingSize())
	require.Equal(t, 1, env.controller.spanController.GetAbsentSize())

	var repairedSpan *replica.SpanReplication
	for _, task := range env.controller.spanController.GetTasksByTableID(env.sourceSpan1.TableID) {
		if task.ID != env.sourceDispatcherID2 {
			repairedSpan = task
			break
		}
	}
	require.NotNil(t, repairedSpan)
	require.Equal(t, env.sourceSpan1.StartKey, repairedSpan.Span.StartKey)
	require.Equal(t, env.sourceSpan1.EndKey, repairedSpan.Span.EndKey)

	env.controller.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Zero(t, env.controller.spanController.GetAbsentSize())
	require.Equal(t, 1, env.controller.spanController.GetSchedulingSize())
	addOp := env.controller.operatorController.GetOperator(repairedSpan.ID)
	require.NotNil(t, addOp)
	require.Equal(t, "add", addOp.Type())
}

// TestFinishBootstrapDoesNotRepairDroppedTableAfterRestoredStandaloneRemove covers the same
// restored remove lifecycle after the table has disappeared from the schema snapshot. The test
// reports the stale runtime dispatcher and its remove journal, finishes that operator with one
// terminal status, and verifies finalization never recreates desired state for the dropped table.
func TestFinishBootstrapDoesNotRepairDroppedTableAfterRestoredStandaloneRemove(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)
	schemaStore := eventservice.NewMockSchemaStore()
	schemaStore.SetTables(nil)
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	responses := env.bootstrapResponses(
		nil,
		env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_Working),
	)
	responses[env.nodeID].Operators = []*heartbeatpb.ScheduleDispatcherRequest{
		env.standaloneRemoveRequest(env.sourceDispatcherID1, env.sourceSpan1),
	}

	_, err := env.controller.FinishBootstrap(responses, false)
	require.NoError(t, err)
	require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
	require.Zero(t, env.controller.spanController.GetReplicatingSize())
	require.Zero(t, env.controller.spanController.GetAbsentSize())

	restoredRemove := env.controller.operatorController.GetOperator(env.sourceDispatcherID1)
	require.NotNil(t, restoredRemove)
	env.controller.HandleStatus(env.nodeID, []*heartbeatpb.TableSpanStatus{
		{
			ID:              env.sourceDispatcherID1.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Removed,
			CheckpointTs:    10,
			Mode:            common.DefaultMode,
		},
	})
	require.True(t, restoredRemove.IsFinished())

	env.controller.operatorController.Execute()

	require.Nil(t, env.controller.operatorController.GetOperator(env.sourceDispatcherID1))
	require.Empty(t, env.controller.spanController.GetTasksByTableID(env.sourceSpan1.TableID))
	require.Zero(t, env.controller.spanController.GetReplicatingSize())
	require.Zero(t, env.controller.spanController.GetSchedulingSize())
	require.Zero(t, env.controller.spanController.GetAbsentSize())
}

// TestFinishBootstrapRestoresInFlightMergeWithoutDuplicateCoverage covers maintainer failover in
// the middle of a merge. The bootstrap snapshot contains two source spans in WaitingMerge and the
// overlapping merged target span in Preparing. FinishBootstrap must not panic in findHoles or
// create a fresh absent table task; instead it should rebuild the merge-related tasks/operators
// from the bootstrap snapshot.
func TestFinishBootstrapRestoresInFlightMergeWithoutDuplicateCoverage(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)

	_, err := env.controller.FinishBootstrap(env.bootstrapResponses(
		[]*heartbeatpb.MergeDispatcherRequest{env.mergeRequest()},
		env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.sourceDispatcherID2, env.sourceSpan2, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.mergedDispatcherID, env.mergedSpan, heartbeatpb.ComponentState_Preparing),
	), false)
	require.NoError(t, err)
	require.True(t, env.controller.bootstrapped)

	// No extra absent table span should be created during bootstrap; only the two source spans and
	// the restored merged target should exist.
	require.Zero(t, env.controller.spanController.GetAbsentSize())
	require.Equal(t, 3, len(env.controller.spanController.GetTasksByTableID(1)))
	require.Equal(t, 3, env.controller.spanController.GetSchedulingSize())
	require.Zero(t, env.controller.spanController.GetReplicatingSize())
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID2))
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))

	mergeOp := env.controller.operatorController.GetOperator(env.mergedDispatcherID)
	require.NotNil(t, mergeOp)
	_, ok := mergeOp.(*operator.MergeDispatcherOperator)
	require.True(t, ok)
}

// TestFinishBootstrapKeepsOverlapCoveredAfterMergeJournalCleanup covers a later merge-recovery
// window where dispatcher manager has already committed the merged dispatcher and dropped the merge
// journal, but the old source dispatchers are still waiting to be cleaned up. Bootstrap must still
// treat the overlapping spans as complete coverage and avoid creating absent hole tasks.
func TestFinishBootstrapKeepsOverlapCoveredAfterMergeJournalCleanup(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)

	// Scenario:
	// 1. Source dispatchers are already in WaitingMerge and still present in bootstrap spans.
	// 2. The merged dispatcher is committed and visible as Initializing.
	// 3. The persisted merge operator is already gone, so bootstrap only has overlapping coverage.
	// Expectation: FinishBootstrap succeeds without adding absent spans for fake holes.
	_, err := env.controller.FinishBootstrap(env.bootstrapResponses(
		nil,
		env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.sourceDispatcherID2, env.sourceSpan2, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.mergedDispatcherID, env.mergedSpan, heartbeatpb.ComponentState_Initializing),
	), false)
	require.NoError(t, err)
	require.True(t, env.controller.bootstrapped)
	require.Zero(t, env.controller.spanController.GetAbsentSize())
	require.GreaterOrEqual(t, len(env.controller.spanController.GetTasksByTableID(1)), 2)
	require.GreaterOrEqual(t, env.controller.spanController.GetReplicatingSize(), 2)
	require.Zero(t, env.controller.spanController.GetSchedulingSize())
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))
}

// TestFinishBootstrapRemovesUnexpectedOverlappingWorkingSpan covers a corrupt bootstrap snapshot
// with two partially overlapping Working dispatchers and no operator or merge evidence. The test
// keeps the conflicting dispatcher as temporary coverage under a remove operator, confirms no
// replacement is scheduled early, then repairs the uncovered tail after removal reaches terminal.
func TestFinishBootstrapRemovesUnexpectedOverlappingWorkingSpan(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)
	overlapStart := appendNew(env.mergedSpan.StartKey, 'a')
	keptEnd := appendNew(env.mergedSpan.StartKey, 'b')
	keptSpan := &heartbeatpb.TableSpan{
		TableID:    env.mergedSpan.TableID,
		StartKey:   env.mergedSpan.StartKey,
		EndKey:     keptEnd,
		KeyspaceID: env.mergedSpan.KeyspaceID,
	}
	rejectedSpan := &heartbeatpb.TableSpan{
		TableID:    env.mergedSpan.TableID,
		StartKey:   overlapStart,
		EndKey:     env.mergedSpan.EndKey,
		KeyspaceID: env.mergedSpan.KeyspaceID,
	}
	keptDispatcherID := common.NewDispatcherID()
	rejectedDispatcherID := common.NewDispatcherID()

	_, err := env.controller.FinishBootstrap(env.bootstrapResponses(
		nil,
		env.bootstrapSpan(keptDispatcherID, keptSpan, heartbeatpb.ComponentState_Working),
		env.bootstrapSpan(rejectedDispatcherID, rejectedSpan, heartbeatpb.ComponentState_Working),
	), false)
	require.NoError(t, err)
	require.True(t, env.controller.bootstrapped)
	require.NotNil(t, env.controller.spanController.GetTaskByID(keptDispatcherID))
	require.NotNil(t, env.controller.spanController.GetTaskByID(rejectedDispatcherID))
	require.Equal(t, 2, env.controller.spanController.GetReplicatingSize())
	require.Zero(t, env.controller.spanController.GetAbsentSize())

	removeOp := env.controller.operatorController.GetOperator(rejectedDispatcherID)
	require.NotNil(t, removeOp)
	require.Equal(t, "remove", removeOp.Type())
	env.controller.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Zero(t, env.controller.spanController.GetAbsentSize())

	// The cleanup owner waits for a terminal status before dropping temporary coverage and
	// creating the exact replacement range.
	removeOp.Check(env.nodeID, &heartbeatpb.TableSpanStatus{
		ID:              rejectedDispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    10,
		Mode:            common.DefaultMode,
	})
	require.True(t, removeOp.IsFinished())
	removeOp.PostFinish()
	env.controller.operatorController.RemoveOp(rejectedDispatcherID)

	require.Nil(t, env.controller.spanController.GetTaskByID(rejectedDispatcherID))
	require.Equal(t, 1, env.controller.spanController.GetReplicatingSize())
	require.Equal(t, 1, env.controller.spanController.GetAbsentSize())

	var repairedTail *replica.SpanReplication
	for _, task := range env.controller.spanController.GetTasksByTableID(env.mergedSpan.TableID) {
		if task.ID != keptDispatcherID {
			repairedTail = task
			break
		}
	}
	require.NotNil(t, repairedTail)
	require.Equal(t, keptEnd, repairedTail.Span.StartKey)
	require.Equal(t, env.mergedSpan.EndKey, repairedTail.Span.EndKey)
}

// TestFinishBootstrapHandlesMissingMergeSource covers failover after one source disappears.
// Each subtest reports the surviving source and merge journal, optionally reports a target,
// runs bootstrap, and verifies that the merge commit point selects the correct survivor or owner.
func TestFinishBootstrapHandlesMissingMergeSource(t *testing.T) {
	type recoveryOutcome int
	const (
		skipStaleJournal recoveryOutcome = iota
		abortUncommittedTarget
		continueCommittedTarget
	)
	testCases := []struct {
		name          string
		includeTarget bool
		targetStatus  heartbeatpb.ComponentState
		outcome       recoveryOutcome
	}{
		// With no target snapshot, the stale journal cannot drive recovery and the surviving source stays active.
		{name: "target absent", outcome: skipStaleJournal},
		// Preparing and MergeReady are before the commit point, so the target remains temporary coverage until terminal.
		{
			name:          heartbeatpb.ComponentState_Preparing.String(),
			includeTarget: true,
			targetStatus:  heartbeatpb.ComponentState_Preparing,
			outcome:       abortUncommittedTarget,
		},
		{
			name:          heartbeatpb.ComponentState_MergeReady.String(),
			includeTarget: true,
			targetStatus:  heartbeatpb.ComponentState_MergeReady,
			outcome:       abortUncommittedTarget,
		},
		// Initializing is the commit point, so bootstrap keeps the target and discards the surviving source.
		{
			name:          heartbeatpb.ComponentState_Initializing.String(),
			includeTarget: true,
			targetStatus:  heartbeatpb.ComponentState_Initializing,
			outcome:       continueCommittedTarget,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newMergeBootstrapTestEnv(t)
			spans := []*heartbeatpb.BootstrapTableSpan{
				env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_WaitingMerge),
			}
			if tc.includeTarget {
				spans = append(spans, env.bootstrapSpan(env.mergedDispatcherID, env.mergedSpan, tc.targetStatus))
			}

			_, err := env.controller.FinishBootstrap(env.bootstrapResponses(
				[]*heartbeatpb.MergeDispatcherRequest{env.mergeRequest()},
				spans...,
			), false)
			require.NoError(t, err)
			require.True(t, env.controller.bootstrapped)

			switch tc.outcome {
			case skipStaleJournal:
				require.Zero(t, env.controller.operatorController.OperatorSize())
				require.Zero(t, env.controller.spanController.GetSchedulingSize())
				sourceReplicaSet := env.controller.spanController.GetTaskByID(env.sourceDispatcherID1)
				require.NotNil(t, sourceReplicaSet)
				require.True(t, env.controller.spanController.IsReplicating(sourceReplicaSet))
				require.Nil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))
			case abortUncommittedTarget:
				require.Zero(t, env.controller.spanController.GetAbsentSize())
				require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
				require.NotNil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))

				// Dispatcher manager aborts pre-commit merges, so terminal target cleanup must
				// release temporary coverage before span repair creates the missing range.
				removeOp := env.controller.operatorController.GetOperator(env.mergedDispatcherID)
				require.NotNil(t, removeOp)
				require.Equal(t, "remove", removeOp.Type())
				removeOp.Check(env.nodeID, &heartbeatpb.TableSpanStatus{
					ID:              env.mergedDispatcherID.ToPB(),
					ComponentStatus: heartbeatpb.ComponentState_Stopped,
					CheckpointTs:    10,
					Mode:            common.DefaultMode,
				})
				require.True(t, removeOp.IsFinished())
				removeOp.PostFinish()
				env.controller.operatorController.RemoveOp(env.mergedDispatcherID)

				require.Nil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))
				require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
				require.Equal(t, 1, env.controller.spanController.GetReplicatingSize())
				require.Equal(t, 1, env.controller.spanController.GetAbsentSize())
			case continueCommittedTarget:
				require.Zero(t, env.controller.operatorController.OperatorSize())
				require.Zero(t, env.controller.spanController.GetAbsentSize())
				require.Zero(t, env.controller.spanController.GetSchedulingSize())
				require.Equal(t, 1, env.controller.spanController.GetReplicatingSize())
				require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
				require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID2))
				require.NotNil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))

				// A later target heartbeat should update the committed replica directly without an add operator.
				env.controller.HandleStatus(env.nodeID, []*heartbeatpb.TableSpanStatus{
					{
						ID:              env.mergedDispatcherID.ToPB(),
						ComponentStatus: heartbeatpb.ComponentState_Working,
						CheckpointTs:    20,
						Mode:            common.DefaultMode,
					},
				})
				mergedReplicaSet := env.controller.spanController.GetTaskByID(env.mergedDispatcherID)
				require.NotNil(t, mergedReplicaSet)
				require.Equal(t, heartbeatpb.ComponentState_Working, mergedReplicaSet.GetStatus().ComponentStatus)
				require.Zero(t, env.controller.spanController.GetSchedulingSize())
				require.Equal(t, 1, env.controller.spanController.GetReplicatingSize())
			default:
				require.FailNow(t, "unknown recovery outcome")
			}
		})
	}
}

// TestFinishBootstrapRejectsInvalidMergeJournalGeometry covers a stale journal whose source spans
// are not consecutive. Bootstrap must not use dispatcher IDs alone as overlap proof; it removes the
// uncommitted target and creates only the real source gap after the target reaches terminal.
func TestFinishBootstrapRejectsInvalidMergeJournalGeometry(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)
	gapEnd := appendNew(env.sourceSpan1.EndKey, 'b')
	env.sourceSpan2.StartKey = gapEnd

	_, err := env.controller.FinishBootstrap(env.bootstrapResponses(
		[]*heartbeatpb.MergeDispatcherRequest{env.mergeRequest()},
		env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.sourceDispatcherID2, env.sourceSpan2, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.mergedDispatcherID, env.mergedSpan, heartbeatpb.ComponentState_Preparing),
	), false)
	require.NoError(t, err)
	require.Zero(t, env.controller.spanController.GetAbsentSize())

	removeOp := env.controller.operatorController.GetOperator(env.mergedDispatcherID)
	require.NotNil(t, removeOp)
	require.Equal(t, "remove", removeOp.Type())
	removeOp.Check(env.nodeID, &heartbeatpb.TableSpanStatus{
		ID:              env.mergedDispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Removed,
		CheckpointTs:    10,
		Mode:            common.DefaultMode,
	})
	require.True(t, removeOp.IsFinished())
	removeOp.PostFinish()
	env.controller.operatorController.RemoveOp(env.mergedDispatcherID)

	require.Nil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID2))
	require.Equal(t, 2, env.controller.spanController.GetReplicatingSize())
	require.Equal(t, 1, env.controller.spanController.GetAbsentSize())
}

type mergeBootstrapTestEnv struct {
	controller          *Controller
	cfID                common.ChangeFeedID
	nodeID              node.ID
	sourceSpan1         *heartbeatpb.TableSpan
	sourceSpan2         *heartbeatpb.TableSpan
	mergedSpan          *heartbeatpb.TableSpan
	sourceDispatcherID1 common.DispatcherID
	sourceDispatcherID2 common.DispatcherID
	mergedDispatcherID  common.DispatcherID
}

func newMergeBootstrapTestEnv(t *testing.T) *mergeBootstrapTestEnv {
	t.Helper()

	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeID := node.ID("node1")
	nodeManager.GetAliveNodes()[nodeID] = &node.Info{ID: nodeID}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, nodeID, false)

	cfg := config.GetDefaultReplicaConfig().Clone()
	cfg.Scheduler = &config.ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: util.AddressOf(true),
		BalanceScoreThreshold:  util.AddressOf(1),
		MinTrafficPercentage:   util.AddressOf(0.8),
		MaxTrafficPercentage:   util.AddressOf(1.2),
	}
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, &mockThreadPool{}, cfg, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

	schemaStore := eventservice.NewMockSchemaStore()
	schemaStore.SetTables([]commonEvent.Table{
		{
			TableID:         1,
			SchemaID:        1,
			Splitable:       true,
			SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t1"},
		},
	})
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 1)
	midKey := appendNew(totalSpan.StartKey, 'a')
	return &mergeBootstrapTestEnv{
		controller: controller,
		cfID:       cfID,
		nodeID:     nodeID,
		sourceSpan1: &heartbeatpb.TableSpan{
			TableID:    1,
			StartKey:   totalSpan.StartKey,
			EndKey:     midKey,
			KeyspaceID: common.DefaultKeyspaceID,
		},
		sourceSpan2: &heartbeatpb.TableSpan{
			TableID:    1,
			StartKey:   midKey,
			EndKey:     totalSpan.EndKey,
			KeyspaceID: common.DefaultKeyspaceID,
		},
		mergedSpan: &heartbeatpb.TableSpan{
			TableID:    1,
			StartKey:   totalSpan.StartKey,
			EndKey:     totalSpan.EndKey,
			KeyspaceID: common.DefaultKeyspaceID,
		},
		sourceDispatcherID1: common.NewDispatcherID(),
		sourceDispatcherID2: common.NewDispatcherID(),
		mergedDispatcherID:  common.NewDispatcherID(),
	}
}

// bootstrapSpan keeps the invariant schema, checkpoint, and mode fields out of
// individual scenarios while leaving the dispatcher, range, and state explicit.
func (env *mergeBootstrapTestEnv) bootstrapSpan(
	dispatcherID common.DispatcherID,
	span *heartbeatpb.TableSpan,
	state heartbeatpb.ComponentState,
) *heartbeatpb.BootstrapTableSpan {
	return &heartbeatpb.BootstrapTableSpan{
		ID:              dispatcherID.ToPB(),
		SchemaID:        1,
		Span:            span,
		ComponentStatus: state,
		CheckpointTs:    10,
		Mode:            common.DefaultMode,
	}
}

// mergeRequest builds the canonical two-source merge journal used by recovery scenarios.
func (env *mergeBootstrapTestEnv) mergeRequest() *heartbeatpb.MergeDispatcherRequest {
	return &heartbeatpb.MergeDispatcherRequest{
		ChangefeedID: env.cfID.ToPB(),
		DispatcherIDs: []*heartbeatpb.DispatcherID{
			env.sourceDispatcherID1.ToPB(),
			env.sourceDispatcherID2.ToPB(),
		},
		MergedDispatcherID: env.mergedDispatcherID.ToPB(),
		Mode:               common.DefaultMode,
	}
}

// standaloneRemoveRequest builds the canonical remove journal used by bootstrap recovery tests.
func (env *mergeBootstrapTestEnv) standaloneRemoveRequest(
	dispatcherID common.DispatcherID,
	span *heartbeatpb.TableSpan,
) *heartbeatpb.ScheduleDispatcherRequest {
	return &heartbeatpb.ScheduleDispatcherRequest{
		ChangefeedID: env.cfID.ToPB(),
		Config: &heartbeatpb.DispatcherConfig{
			DispatcherID: dispatcherID.ToPB(),
			SchemaID:     1,
			Span:         span,
			StartTs:      10,
			Mode:         common.DefaultMode,
		},
		ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		OperatorType:   heartbeatpb.OperatorType_O_Remove,
	}
}

// bootstrapResponses builds the single-node snapshot while keeping merge-journal
// presence explicit at each call site because it changes the recovery path.
func (env *mergeBootstrapTestEnv) bootstrapResponses(
	mergeOperators []*heartbeatpb.MergeDispatcherRequest,
	spans ...*heartbeatpb.BootstrapTableSpan,
) map[node.ID]*heartbeatpb.MaintainerBootstrapResponse {
	return map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		env.nodeID: {
			ChangefeedID:   env.cfID.ToPB(),
			Spans:          spans,
			MergeOperators: mergeOperators,
			CheckpointTs:   10,
		},
	}
}

// TestFinishBootstrapSkipsMergeOperatorForDroppedTable covers failover after a DROP TABLE
// while dispatcher manager still reports an in-flight merge journal. The test boots from an
// empty schema snapshot, feeds source and merged dispatcher snapshots plus the stale merge
// request, and expects bootstrap to skip restoring ghost table tasks/operators.
func TestFinishBootstrapSkipsMergeOperatorForDroppedTable(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)

	schemaStore := eventservice.NewMockSchemaStore()
	schemaStore.SetTables(nil)
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	_, err := env.controller.FinishBootstrap(env.bootstrapResponses(
		[]*heartbeatpb.MergeDispatcherRequest{env.mergeRequest()},
		env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.sourceDispatcherID2, env.sourceSpan2, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.mergedDispatcherID, env.mergedSpan, heartbeatpb.ComponentState_Preparing),
	), false)
	require.NoError(t, err)
	require.True(t, env.controller.bootstrapped)
	require.Zero(t, env.controller.operatorController.OperatorSize())
	require.Zero(t, env.controller.spanController.GetAbsentSize())
	require.Zero(t, env.controller.spanController.GetSchedulingSize())
	require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
	require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID2))
	require.Nil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))
	require.Empty(t, env.controller.spanController.GetTasksByTableID(env.sourceSpan1.TableID))
}

// TestFinishBootstrapSkipsMergeJournalConflictingWithWorkingOperator covers non-atomic bootstrap
// snapshots where a stale merge journal overlaps a newer create or remove request. Each subtest
// restores the regular operator first, then verifies merge recovery yields to it and gives the
// uncommitted merged target a cleanup owner instead of leaving unmanaged overlapping desired state.
func TestFinishBootstrapSkipsMergeJournalConflictingWithWorkingOperator(t *testing.T) {
	testCases := []struct {
		name                  string
		scheduleAction        heartbeatpb.ScheduleAction
		operatorType          heartbeatpb.OperatorType
		includeSourceSnapshot bool
		expectedOperatorType  string
	}{
		{
			name:                  "remove source",
			scheduleAction:        heartbeatpb.ScheduleAction_Remove,
			operatorType:          heartbeatpb.OperatorType_O_Remove,
			includeSourceSnapshot: true,
			expectedOperatorType:  "remove",
		},
		{
			name:                 "create source",
			scheduleAction:       heartbeatpb.ScheduleAction_Create,
			operatorType:         heartbeatpb.OperatorType_O_Add,
			expectedOperatorType: "add",
		},
	}

	for _, tc := range testCases {
		// Scenario: the ordinary operator and merge journal reference the same source dispatcher.
		// Steps: restore the ordinary request, process merge snapshots, then ensure the stale journal
		// is skipped while the newer create/remove operator remains active.
		t.Run(tc.name, func(t *testing.T) {
			env := newMergeBootstrapTestEnv(t)
			spans := []*heartbeatpb.BootstrapTableSpan{
				env.bootstrapSpan(env.sourceDispatcherID2, env.sourceSpan2, heartbeatpb.ComponentState_WaitingMerge),
				env.bootstrapSpan(env.mergedDispatcherID, env.mergedSpan, heartbeatpb.ComponentState_Preparing),
			}
			if tc.includeSourceSnapshot {
				spans = append(spans, env.bootstrapSpan(
					env.sourceDispatcherID1,
					env.sourceSpan1,
					heartbeatpb.ComponentState_WaitingMerge,
				))
			}

			responses := env.bootstrapResponses([]*heartbeatpb.MergeDispatcherRequest{env.mergeRequest()}, spans...)
			responses[env.nodeID].Operators = []*heartbeatpb.ScheduleDispatcherRequest{
				{
					ChangefeedID: env.cfID.ToPB(),
					Config: &heartbeatpb.DispatcherConfig{
						DispatcherID: env.sourceDispatcherID1.ToPB(),
						SchemaID:     1,
						Span:         env.sourceSpan1,
						StartTs:      10,
						Mode:         common.DefaultMode,
					},
					ScheduleAction: tc.scheduleAction,
					OperatorType:   tc.operatorType,
				},
			}

			_, err := env.controller.FinishBootstrap(responses, false)
			require.NoError(t, err)
			require.True(t, env.controller.bootstrapped)

			workingOp := env.controller.operatorController.GetOperator(env.sourceDispatcherID1)
			require.NotNil(t, workingOp)
			require.Equal(t, tc.expectedOperatorType, workingOp.Type())
			mergedCleanupOp := env.controller.operatorController.GetOperator(env.mergedDispatcherID)
			require.NotNil(t, mergedCleanupOp)
			require.Equal(t, "remove", mergedCleanupOp.Type())
			require.Equal(t, 2, env.controller.operatorController.OperatorSize())
			require.Zero(t, env.controller.spanController.GetAbsentSize())
		})
	}
}

// TestHandleStatusDropsTerminalSourcesCoveredByMergedSpanAfterJournalCleanup covers the post-bootstrap
// merge cleanup window where source dispatchers still report terminal statuses after the merge journal
// is gone. The test bootstraps overlapping source and merged spans, then sends terminal source
// heartbeats and expects the obsolete sources to be removed instead of rescheduled as absent spans.
func TestHandleStatusDropsTerminalSourcesCoveredByMergedSpanAfterJournalCleanup(t *testing.T) {
	env := newMergeBootstrapTestEnv(t)

	_, err := env.controller.FinishBootstrap(env.bootstrapResponses(
		nil,
		env.bootstrapSpan(env.sourceDispatcherID1, env.sourceSpan1, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.sourceDispatcherID2, env.sourceSpan2, heartbeatpb.ComponentState_WaitingMerge),
		env.bootstrapSpan(env.mergedDispatcherID, env.mergedSpan, heartbeatpb.ComponentState_Initializing),
	), false)
	require.NoError(t, err)

	env.controller.HandleStatus(env.nodeID, []*heartbeatpb.TableSpanStatus{
		{
			ID:              env.sourceDispatcherID1.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Stopped,
			CheckpointTs:    10,
			Mode:            common.DefaultMode,
		},
		{
			ID:              env.sourceDispatcherID2.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Removed,
			CheckpointTs:    10,
			Mode:            common.DefaultMode,
		},
	})

	require.Zero(t, env.controller.spanController.GetAbsentSize())
	require.Zero(t, env.controller.spanController.GetSchedulingSize())
	require.Equal(t, 1, env.controller.spanController.GetReplicatingSize())
	require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
	require.Nil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID2))
	require.NotNil(t, env.controller.spanController.GetTaskByID(env.mergedDispatcherID))
	require.Len(t, env.controller.spanController.GetTasksByTableID(1), 1)
}

func TestSplitTableWhenBootstrapFinished(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	defaultConfig := config.GetDefaultReplicaConfig().Clone()
	defaultConfig.Scheduler = &config.ChangefeedSchedulerConfig{
		EnableTableAcrossNodes:     util.AddressOf(true),
		RegionThreshold:            util.AddressOf(1),
		RegionCountPerSpan:         util.AddressOf(1),
		RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
		BalanceScoreThreshold:      util.AddressOf(1),
		MinTrafficPercentage:       util.AddressOf(0.8),
		MaxTrafficPercentage:       util.AddressOf(1.2),
	}
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, defaultConfig, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)
	s.taskPool = &mockThreadPool{}
	schemaStore := eventservice.NewMockSchemaStore()
	schemaStore.SetTables(
		[]commonEvent.Table{
			{TableID: 1, SchemaID: 1, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"}},
			{TableID: 2, SchemaID: 2, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t2"}, Splitable: true},
		})
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 1)
	totalSpan2 := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 2)

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

	_, err := s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"node1": {
			ChangefeedID: cfID.ToPB(),
			Spans:        reportedSpans,
			CheckpointTs: 10,
		},
	}, false)
	require.Nil(t, err)
	require.NotNil(t, s.barrier)
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
		{ // 6. overlapping spans still cover the whole range.
			spans: []*heartbeatpb.TableSpan{
				{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")},
				{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
				{StartKey: []byte("t1_1"), EndKey: []byte("t2_0")},
			},
			rang: &heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t2_0")},
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

func TestLargeTableInitialization(t *testing.T) {
	testutil.SetUpTestServices(t)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)

	// Configure with the specified parameters
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, nil, &config.ReplicaConfig{
		Scheduler: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes:     util.AddressOf(true),
			WriteKeyThreshold:          util.AddressOf(500),
			RegionThreshold:            util.AddressOf(50),
			RegionCountPerSpan:         util.AddressOf(10),
			RegionCountRefreshInterval: util.AddressOf(time.Minute),
			SchedulingTaskCountPerNode: util.AddressOf(2),
			BalanceScoreThreshold:      util.AddressOf(1),
			MinTrafficPercentage:       util.AddressOf(0.8),
			MaxTrafficPercentage:       util.AddressOf(1.2),
		},
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false, testBalanceMoveBatchSize, 0)

	// Create a large table with 10000 regions
	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, int64(1))
	// Mock 100 regions for the large table
	regionCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)
	regions := make([]*tikv.Region, 100)
	for i := 0; i < 100; i++ {
		startKey := appendNew(totalSpan.StartKey, byte(i))
		endKey := appendNew(totalSpan.StartKey, byte(i+1))
		if i == 0 {
			startKey = totalSpan.StartKey
		}
		if i == 99 {
			endKey = totalSpan.EndKey
		}
		regions[i] = testutil.MockRegionWithKeyRange(uint64(i+1), startKey, endKey)
	}
	regionCache.SetRegions(fmt.Sprintf("%s-%s", totalSpan.StartKey, totalSpan.EndKey), regions)
	controller.spanController.AddNewTable(commonEvent.Table{
		TableID:   1,
		SchemaID:  1,
		Splitable: true,
	}, 1)

	require.Equal(t, 10, controller.spanController.GetAbsentSize())

	spanIDList := []common.DispatcherID{}

	controller.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, 0, controller.operatorController.OperatorSize())

	// first basic scheduler
	controller.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Equal(t, 6, controller.operatorController.OperatorSize())
	require.Equal(t, 4, controller.spanController.GetAbsentSize())
	require.Equal(t, 6, controller.spanController.GetSchedulingSize())

	for _, op := range controller.operatorController.GetAllOperators() {
		require.Equal(t, "add", op.Type())
		spanIDList = append(spanIDList, op.ID())
		op.Start()
		op.PostFinish()
		controller.operatorController.RemoveOp(op.ID())
	}

	require.Equal(t, 6, controller.spanController.GetReplicatingSize())
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 2, controller.spanController.GetTaskSizeByNodeID("node3"))

	controller.schedulerController.GetScheduler(scheduler.BalanceScheduler).Execute()
	controller.schedulerController.GetScheduler(scheduler.BalanceSplitScheduler).Execute()
	require.Equal(t, 0, controller.operatorController.OperatorSize())

	controller.schedulerController.GetScheduler(scheduler.BasicScheduler).Execute()
	require.Equal(t, 4, controller.operatorController.OperatorSize())
	for _, op := range controller.operatorController.GetAllOperators() {
		require.Equal(t, "add", op.Type())
		spanIDList = append(spanIDList, op.ID())
		op.Start()
		op.PostFinish()
		controller.operatorController.RemoveOp(op.ID())
	}

	require.Equal(t, 10, controller.spanController.GetReplicatingSize())
	require.Equal(t, 0, controller.spanController.GetAbsentSize())
	require.Equal(t, 0, controller.spanController.GetSchedulingSize())
	require.LessOrEqual(t, 3, controller.spanController.GetTaskSizeByNodeID("node1"))
	require.LessOrEqual(t, 3, controller.spanController.GetTaskSizeByNodeID("node2"))
	require.LessOrEqual(t, 3, controller.spanController.GetTaskSizeByNodeID("node3"))
}
