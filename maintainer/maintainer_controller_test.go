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
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, nil, replicaConfig, ddlSpan, nil, 9, time.Minute, refresher, common.DefaultKeyspace, false)
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

// This case test the scenario that the balance scheduler when a new node join in.
// In this case, the num of split tables is more than the num of nodes,
// and we can select appropriate split spans to move
func TestBalanceGroupsNewNodeAdd_SplitsTableMoreThanNodeNum(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
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
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)

	nodeID := node.ID("node1")
	for i := 0; i < 100; i++ {
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
			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
			})
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
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
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
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)

	regionCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)

	nodeIDList := []node.ID{"node1", "node2"}
	for i := 0; i < 100; i++ {
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
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
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
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)

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
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
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
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)

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
		op.(*operator.MoveDispatcherOperator).SetOriginNodeStopped()
		op.Schedule()
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
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, replicaConfig, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)
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
			op.Check("node1", &heartbeatpb.TableSpanStatus{
				ID:              span.ID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
			})
			op.Start()
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

func TestDefaultSpanIntoSplit(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
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
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)
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
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID, common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, nil, replicaConfig, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)
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
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	s := NewController(cfID, 1, &mockThreadPool{},
		config.GetDefaultReplicaConfig(), ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)
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
	require.Panics(t, func() {
		_, _ = s.FinishBootstrap(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{}, false)
	})
}

func TestSplitTableWhenBootstrapFinished(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
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
	s := NewController(cfID, 1, nil, defaultConfig, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)
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
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}
	nodeManager.GetAliveNodes()["node3"] = &node.Info{ID: "node3"}

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
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
	}, ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)

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
