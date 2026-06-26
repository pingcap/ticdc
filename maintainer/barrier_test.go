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
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/range_checker"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/routing"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestOneBlockEvent(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	startTs := uint64(10)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, startTs)
	stm := spanController.GetTasksByTableID(1)[0]
	spanController.BindSpanToNode("", "node1", stm)
	spanController.MarkSpanReplicating(stm)

	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, nil)
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
					},
					IsSyncPoint: true,
				},
			},
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: true,
	}
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	event := barrier.blockedEvents.m[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == spanController.GetDDLDispatcherID())
	require.True(t, event.selected.Load())
	require.False(t, event.writerDispatcherAdvanced)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.Action, heartbeatpb.Action_Write)
	require.True(t, resp.DispatcherStatuses[1].Action.IsSyncPoint)

	// test resend action and syncpoint is set
	event.lastResendTime = time.Now().Add(-2 * time.Second)
	resendMsgs := event.resend(common.DefaultMode)
	require.Len(t, resendMsgs, 1)
	require.True(t, resendMsgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	require.True(t, resendMsgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.IsSyncPoint)

	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Len(t, barrier.blockedEvents.m, 0)

	// send event done again
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs:     10,
					IsBlocked:   true,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedEvents.m, 0)
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 0)
}

func TestNormalBlock(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 10)
		stm := spanController.GetTasksByTableID(int64(id))[0]
		blockedDispatcherIDS = append(blockedDispatcherIDS, stm.ID.ToPB())
		spanController.BindSpanToNode("", "node1", stm)
		spanController.MarkSpanReplicating(stm)
	}

	// the last one is the writer
	selectDispatcherID := common.NewDispatcherIDFromPB(blockedDispatcherIDS[2])
	selectedRep := spanController.GetTaskByID(selectDispatcherID)
	spanController.BindSpanToNode("node1", "node2", selectedRep)
	spanController.MarkSpanReplicating(selectedRep)

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 1}
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, nil)

	// first node block request
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 2)

	// other node block request
	msgs = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: selectDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msgs)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: false,
	}
	event := barrier.blockedEvents.m[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == selectDispatcherID)
	// all dispatcher reported, the reported status is reset
	require.False(t, event.rangeChecker.IsFullyCovered())

	// repeated status
	barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1, 2, 3},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == selectDispatcherID)

	// selected node write done
	_ = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[2],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedEvents.m, 1)
	_ = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedEvents.m, 0)
}

func TestNormalBlockWithTableTrigger(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 3; id++ {
		spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 10)
		stm := spanController.GetTasksByTableID(int64(id))[0]
		blockedDispatcherIDS = append(blockedDispatcherIDS, stm.ID.ToPB())
		spanController.BindSpanToNode("", "node1", stm)
		spanController.MarkSpanReplicating(stm)
	}

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 1}
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, nil)

	// first node block request
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0, 1, 2},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{2},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 1)
	require.False(t, barrier.blockedEvents.m[eventKey{blockTs: 10, isSyncPoint: false}].tableTriggerDispatcherRelated)

	// table trigger  block request
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0, 1, 2},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{2},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
			{
				ID: blockedDispatcherIDS[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0, 1, 2},
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{2},
					},
					NeedAddedTables: []*heartbeatpb.Table{newSpan},
				},
			},
		},
	})
	require.NotNil(t, msgs)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: false,
	}
	event := barrier.blockedEvents.m[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == tableTriggerEventDispatcherID)
	// all dispatcher reported, the reported status is reset
	require.False(t, event.rangeChecker.IsFullyCovered())
	require.True(t, event.tableTriggerDispatcherRelated)

	// table trigger write done
	_ = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedEvents.m, 1)
	_ = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Len(t, barrier.blockedEvents.m, 1)
	// resend to check removed tables
	event.lastResendTime = time.Now().Add(-2 * time.Second)
	event.resend(common.DefaultMode)
	barrier.checkEventFinish(event)
	require.Len(t, barrier.blockedEvents.m, 0)
}

func TestBarrierPrechecksDDLRoute(t *testing.T) {
	barrier, _, cfID, tableTriggerEventDispatcherID, _, blockTables := newBarrierRoutePrecheckTestFixture(
		t, routeAllTo("target", "t"))
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					BlockTables: blockTables,
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Nil(t, resp.DispatcherStatuses[0].Action)

	var reportedErr error
	barrier, routeAdmin, cfID, tableTriggerEventDispatcherID, _, blockTables := newBarrierRoutePrecheckTestFixture(
		t, routeAllTo("target", "t"))
	routeAdmin.SetErrorReporter(func(err error) {
		reportedErr = err
	})
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					BlockTables: blockTables,
					NeedAddedTables: []*heartbeatpb.Table{
						{SchemaID: 2, TableID: 2},
					},
					RouteTableAdmissions: []*heartbeatpb.RouteTableAdmission{
						routeAdmitPB("db2", "t", "target", "t"),
					},
				},
			},
		},
	})
	require.NotNil(t, reportedErr)
	require.Contains(t, reportedErr.Error(), "table route conflict")
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Empty(t, resp.DispatcherStatuses)
}

func TestBarrierRejectsNonblockingRouteConflict(t *testing.T) {
	barrier, routeAdmin, cfID, tableTriggerEventDispatcherID, _, _ := newBarrierRoutePrecheckTestFixture(
		t, routeAllTo("target", "t"))
	var reportedErr error
	routeAdmin.SetErrorReporter(func(err error) {
		reportedErr = err
	})

	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					BlockTs: 10,
					NeedAddedTables: []*heartbeatpb.Table{
						{SchemaID: 2, TableID: 2},
					},
					RouteTableAdmissions: []*heartbeatpb.RouteTableAdmission{
						routeAdmitPB("db2", "t", "target", "t"),
					},
					Stage: heartbeatpb.BlockStage_NONE,
				},
			},
		},
	})

	require.NotNil(t, reportedErr)
	require.Contains(t, reportedErr.Error(), "table route conflict")
	require.Equal(t, 0, barrier.pendingEvents.Len())
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Empty(t, resp.DispatcherStatuses)
}

func TestBarrierAppliesRecoveredRouteEventBeforeActionResend(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 10)
	stm := spanController.GetTasksByTableID(1)[0]
	spanController.BindSpanToNode("", "node1", stm)
	spanController.MarkSpanReplicating(stm)

	routeAdmin := newRouteAdminForBarrierTest(t, cfID, routeForRename())
	var reportedErr error
	routeAdmin.SetErrorReporter(func(err error) {
		reportedErr = err
	})
	barrier := NewBarrier(spanController, operatorController, false, map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"node1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: tableTriggerEventDispatcherID.ToPB(),
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   10,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{common.DDLSpanTableID, 1},
						},
						RouteTableAdmissions: []*heartbeatpb.RouteTableAdmission{
							routeReleasePB("db1", "t"),
							routeAdmitPB("db1", "u", "target", "u"),
						},
						Stage: heartbeatpb.BlockStage_DONE,
					},
					Mode: common.DefaultMode,
				},
			},
		},
	}, common.DefaultMode, routeAdmin)

	_ = barrier.Resend()
	ready := routeAdmin.Precheck(20, []routing.Admission{{
		Action:  routing.Admit,
		Binding: routing.NewRouteBinding("db2", "u", "target", "u"),
	}})
	require.False(t, ready)
	require.NotNil(t, reportedErr)
	require.Contains(t, reportedErr.Error(), "table route conflict")
}

func TestBarrierCommitsForwardedRouteEventBeforeLaterRouteEvent(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 10)
	tableSpan := spanController.GetTasksByTableID(1)[0]
	spanController.BindSpanToNode("", "node1", tableSpan)
	spanController.MarkSpanReplicating(tableSpan)
	tableSpan.UpdateStatus(&heartbeatpb.TableSpanStatus{
		ID:              tableSpan.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    11,
		Mode:            common.DefaultMode,
	})

	routeAdmin := newRouteAdminForBarrierTest(t, cfID, routeAllTo("target", "t"))
	var reportedErr error
	routeAdmin.SetErrorReporter(func(err error) {
		reportedErr = err
	})
	barrier := NewBarrier(spanController, operatorController, false, map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"node1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: tableTriggerEventDispatcherID.ToPB(),
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   10,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{common.DDLSpanTableID, 1},
						},
						RouteTableAdmissions: []*heartbeatpb.RouteTableAdmission{
							routeReleasePB("db1", "t"),
						},
						Stage: heartbeatpb.BlockStage_WAITING,
					},
					Mode: common.DefaultMode,
				},
				{
					ID: tableTriggerEventDispatcherID.ToPB(),
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   20,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{common.DDLSpanTableID, 1},
						},
						RouteTableAdmissions: []*heartbeatpb.RouteTableAdmission{
							routeAdmitPB("db2", "t", "target", "t"),
						},
						Stage: heartbeatpb.BlockStage_DONE,
					},
					Mode: common.DefaultMode,
				},
			},
		},
	}, common.DefaultMode, routeAdmin)

	_ = barrier.Resend()
	require.NoError(t, reportedErr)

	ready := routeAdmin.Precheck(30, []routing.Admission{{
		Action:  routing.Admit,
		Binding: routing.NewRouteBinding("db3", "t", "target", "t"),
	}})
	require.False(t, ready)
	require.NotNil(t, reportedErr)
	require.Contains(t, reportedErr.Error(), "db2")
}

func TestBarrierRejectsRouteConflict(t *testing.T) {
	barrier, routeAdmin, cfID, tableTriggerEventDispatcherID, tableDispatcherID, blockTables := newBarrierRoutePrecheckTestFixture(
		t, routeAllTo("target", "t"))
	var reportedErr error
	routeAdmin.SetErrorReporter(func(err error) {
		reportedErr = err
	})
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					BlockTables: blockTables,
					NeedAddedTables: []*heartbeatpb.Table{
						{SchemaID: 2, TableID: 2},
					},
				},
			},
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					BlockTables: blockTables,
					NeedAddedTables: []*heartbeatpb.Table{
						{SchemaID: 2, TableID: 2},
					},
					RouteTableAdmissions: []*heartbeatpb.RouteTableAdmission{
						routeAdmitPB("db2", "t", "target", "t"),
					},
				},
			},
		},
	})

	require.NotNil(t, reportedErr)
	require.Contains(t, reportedErr.Error(), "table route conflict")
	event := barrier.blockedEvents.m[getEventKey(10, false)]
	require.NotNil(t, event)
	require.False(t, event.selected.Load())
	require.False(t, event.allDispatcherReported())
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	requireNoDispatcherActions(t, msgs)
}

func TestBarrierKeepsBlockedRouteConflict(t *testing.T) {
	handleBlockedDispatchers := func(
		barrier *Barrier,
		cfID common.ChangeFeedID,
		tableDispatcherID common.DispatcherID,
		blockTables *heartbeatpb.InfluencedTables,
		addedTables []*heartbeatpb.Table,
		routeTables []*heartbeatpb.RouteTableAdmission,
	) []*messaging.TargetMessage {
		return barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
			ChangefeedID: cfID.ToPB(),
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: tableDispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked:            true,
						BlockTs:              10,
						BlockTables:          blockTables,
						NeedAddedTables:      addedTables,
						RouteTableAdmissions: routeTables,
					},
				},
			},
		})
	}

	barrier, routeAdmin, cfID, _, tableDispatcherID, _ := newBarrierRoutePrecheckTestFixture(
		t, routeBySource())
	ready := routeAdmin.Precheck(5, []routing.Admission{{
		Action:  routing.Admit,
		Binding: routing.NewRouteBinding("db2", "t", "db2_target", "t"),
	}})
	require.True(t, ready)
	blockTables := &heartbeatpb.InfluencedTables{
		InfluenceType: heartbeatpb.InfluenceType_Normal,
		TableIDs:      []int64{1},
	}
	msgs := handleBlockedDispatchers(
		barrier, cfID, tableDispatcherID, blockTables, nil, []*heartbeatpb.RouteTableAdmission{
			routeReleasePB("db1", "t"),
			routeAdmitPB("db1", "u", "db1_target", "u"),
		})
	event := barrier.blockedEvents.m[getEventKey(10, false)]
	require.NotNil(t, event)
	// Keep the barrier event so ACKed WAITING coverage is not lost when
	// route precheck prevents sending Action_Write.
	require.True(t, event.allDispatcherReported())
	require.False(t, event.selected.Load())
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	requireNoDispatcherActions(t, msgs)

	barrier, routeAdmin, cfID, _, tableDispatcherID, _ = newBarrierRoutePrecheckTestFixture(
		t, routeAllTo("target", "t"))
	var reportedErr error
	routeAdmin.SetErrorReporter(func(err error) {
		reportedErr = err
	})
	blockTables = &heartbeatpb.InfluencedTables{
		InfluenceType: heartbeatpb.InfluenceType_Normal,
		TableIDs:      []int64{1},
	}
	msgs = handleBlockedDispatchers(
		barrier, cfID, tableDispatcherID, blockTables, []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		}, []*heartbeatpb.RouteTableAdmission{
			routeAdmitPB("db2", "t", "target", "t"),
		})
	require.NotNil(t, reportedErr)
	require.Contains(t, reportedErr.Error(), "table route conflict")
	event = barrier.blockedEvents.m[getEventKey(10, false)]
	require.NotNil(t, event)
	require.True(t, event.allDispatcherReported())
	require.False(t, event.selected.Load())
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	requireNoDispatcherActions(t, msgs)
}

func newBarrierRoutePrecheckTestFixture(
	t *testing.T,
	rules []*config.DispatchRule,
) (*Barrier, *routing.Admin, common.ChangeFeedID, common.DispatcherID, common.DispatcherID, *heartbeatpb.InfluencedTables) {
	t.Helper()
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 10)
	stm := spanController.GetTasksByTableID(1)[0]
	spanController.BindSpanToNode("", "node1", stm)
	spanController.MarkSpanReplicating(stm)

	routeAdmin := newRouteAdminForBarrierTest(t, cfID, rules)
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, routeAdmin)
	blockTables := &heartbeatpb.InfluencedTables{
		InfluenceType: heartbeatpb.InfluenceType_Normal,
		TableIDs:      []int64{common.DDLSpanTableID, 1},
	}
	return barrier, routeAdmin, cfID, tableTriggerEventDispatcherID, stm.ID, blockTables
}

func newRouteAdminForBarrierTest(
	t *testing.T,
	cfID common.ChangeFeedID,
	rules []*config.DispatchRule,
) *routing.Admin {
	t.Helper()

	admin, err := routing.NewAdmin(
		cfID,
		&config.ReplicaConfig{
			Sink: &config.SinkConfig{
				DispatchRules: rules,
			},
		},
		nil,
		[]commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
				SchemaTableName: &commonEvent.SchemaTableName{
					SchemaName: "db1",
					TableName:  "t",
				},
			},
		},
	)
	require.NoError(t, err)
	return admin
}

func routeAllTo(targetSchema, targetTable string) []*config.DispatchRule {
	return []*config.DispatchRule{{
		Matcher:      []string{"*.*"},
		TargetSchema: targetSchema,
		TargetTable:  targetTable,
	}}
}

func routeBySource() []*config.DispatchRule {
	return []*config.DispatchRule{
		routeExact("db1", "t", "db1_target", "t"),
		routeExact("db1", "u", "db1_target", "u"),
		routeExact("db2", "t", "db2_target", "t"),
	}
}

func routeForRename() []*config.DispatchRule {
	return []*config.DispatchRule{
		routeExact("db1", "t", "target", "t"),
		routeExact("db1", "u", "target", "u"),
	}
}

func routeExact(sourceSchema, sourceTable, targetSchema, targetTable string) *config.DispatchRule {
	return &config.DispatchRule{
		Matcher:      []string{sourceSchema + "." + sourceTable},
		TargetSchema: targetSchema,
		TargetTable:  targetTable,
	}
}

func routeAdmitPB(sourceSchema, sourceTable, targetSchema, targetTable string) *heartbeatpb.RouteTableAdmission {
	return &heartbeatpb.RouteTableAdmission{
		SourceSchemaName: sourceSchema,
		SourceTableName:  sourceTable,
		TargetSchemaName: targetSchema,
		TargetTableName:  targetTable,
		Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
	}
}

func routeReleasePB(sourceSchema, sourceTable string) *heartbeatpb.RouteTableAdmission {
	return &heartbeatpb.RouteTableAdmission{
		SourceSchemaName: sourceSchema,
		SourceTableName:  sourceTable,
		Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE,
	}
}

func requireNoDispatcherActions(t *testing.T, msgs []*messaging.TargetMessage) {
	t.Helper()
	for _, msg := range msgs {
		resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
		for _, status := range resp.DispatcherStatuses {
			require.Nil(t, status.Action)
		}
	}
}

func TestSchemaBlock(t *testing.T) {
	testutil.SetUpTestServices(t)
	nm := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nmap := nm.GetAliveNodes()
	for key := range nmap {
		delete(nmap, key)
	}
	nmap["node1"] = &node.Info{ID: "node1"}
	nmap["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 2}, 1)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 2, TableID: 3}, 1)
	var dispatcherIDs []*heartbeatpb.DispatcherID
	dropTables := []int64{1, 2}
	absents := spanController.GetAbsentForTest(100)
	for _, stm := range absents {
		if stm.GetSchemaID() == 1 {
			dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
		}
		spanController.BindSpanToNode("", "node1", stm)
		spanController.MarkSpanReplicating(stm)
	}

	newTable := &heartbeatpb.Table{TableID: 10, SchemaID: 2}
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode, nil)

	// first dispatcher  block request
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.Action == heartbeatpb.Action_Write)
	key := eventKey{blockTs: 10}
	event := barrier.blockedEvents.m[key]
	require.Equal(t, uint64(10), event.commitTs)
	// the ddl dispatcher will be the writer
	require.Equal(t, event.writerDispatcher, spanController.GetDDLDispatcherID())

	// repeated status
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_DB,
						SchemaID:      1,
					},
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      dropTables,
					},
					NeedAddedTables: []*heartbeatpb.Table{newTable},
				},
			},
		},
	})
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)

	// selected node write done
	_ = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					Stage:     heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	// pass action message to,false no node, because tables are removed
	resendMsgs := barrier.Resend()
	require.Len(t, resendMsgs, 0)
	require.Len(t, barrier.blockedEvents.m, 0)

	require.Equal(t, 1, spanController.GetAbsentSize())
	require.Equal(t, 2, operatorController.OperatorSize())
	// two dispatcher and moved to operator queue, operator will be removed after ack
	require.Equal(t, 1, spanController.GetReplicatingSize())
	for _, task := range spanController.GetReplicating() {
		op := operatorController.GetOperator(task.ID)
		if op != nil {
			op.PostFinish()
		}
	}
	require.Equal(t, 1, spanController.GetReplicatingSize())
}

func TestSyncPointBlock(t *testing.T) {
	testutil.SetUpTestServices(t)
	nm := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nmap := nm.GetAliveNodes()
	for key := range nmap {
		delete(nmap, key)
	}
	nmap["node1"] = &node.Info{ID: "node1"}
	nmap["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 1)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 2}, 1)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 2, TableID: 3}, 1)
	var dispatcherIDs []*heartbeatpb.DispatcherID
	absents := spanController.GetAbsentForTest(10000)
	for _, stm := range absents {
		dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
		spanController.BindSpanToNode("", "node1", stm)
		spanController.MarkSpanReplicating(stm)
	}
	selectDispatcherID := common.NewDispatcherIDFromPB(dispatcherIDs[2])
	selectedRep := spanController.GetTaskByID(selectDispatcherID)
	spanController.BindSpanToNode("node1", "node2", selectedRep)
	spanController.MarkSpanReplicating(selectedRep)

	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode, nil)
	// first dispatcher  block request
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	// 3 ack messages, including the ddl dispatcher
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 3)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msgs = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[2],
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      1,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	// ack and write message
	require.NotNil(t, msgs)
	require.Len(t, msgs, 2)
	resp = msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	actionResp := msgs[1].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, actionResp.DispatcherStatuses, 1)
	require.True(t, actionResp.DispatcherStatuses[0].Action.CommitTs == 10)
	require.True(t, actionResp.DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	key := eventKey{blockTs: 10, isSyncPoint: true}
	event := barrier.blockedEvents.m[key]
	require.Equal(t, uint64(10), event.commitTs)
	// the last one will be the writer
	require.Equal(t, event.writerDispatcher, spanController.GetDDLDispatcherID())

	// selected node write done
	_ = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	resendMsgs := barrier.Resend()
	// 2 pass action messages to one node
	require.Len(t, resendMsgs, 2)
	require.Len(t, barrier.blockedEvents.m, 1)
	// other dispatcher advanced checkpoint ts
	_ = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherIDs[0],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcherIDs[1],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcherIDs[2],
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.Len(t, barrier.blockedEvents.m, 0)
}

func TestNonBlocked(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, nil)

	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 10)
		stm := spanController.GetTasksByTableID(int64(id))[0]
		dispatcherID := stm.ID
		blockedDispatcherIDS = append(blockedDispatcherIDS, dispatcherID.ToPB())
		spanController.MarkSpanReplicating(stm)
	}
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: blockedDispatcherIDS[0],
				State: &heartbeatpb.State{
					IsBlocked: false,
					BlockTs:   10,
					NeedDroppedTables: &heartbeatpb.InfluencedTables{
						TableIDs:      []int64{1, 2, 3},
						InfluenceType: heartbeatpb.InfluenceType_Normal,
					},
					NeedAddedTables: []*heartbeatpb.Table{
						{TableID: 1, SchemaID: 1}, {TableID: 2, SchemaID: 2},
					},
				},
			},
		},
	})
	// 1 ack  message
	require.NotNil(t, msgs)
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, uint64(10), resp.DispatcherStatuses[0].Ack.CommitTs)
	require.True(t, heartbeatpb.InfluenceType_Normal == resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs[0], blockedDispatcherIDS[0])
	require.Len(t, barrier.blockedEvents.m, 0)
	require.Equal(t, 2, spanController.GetAbsentSize(), 2)
}

func TestUpdateCheckpointTs(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, nil)
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{0},
					},
					IsSyncPoint: false,
				},
			},
		},
	})
	require.NotNil(t, msgs)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: false,
	}
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	event := barrier.blockedEvents.m[key]
	require.Equal(t, uint64(10), event.commitTs)
	require.True(t, event.writerDispatcher == spanController.GetDDLDispatcherID())
	require.True(t, event.selected.Load())
	require.False(t, event.writerDispatcherAdvanced)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.CommitTs, uint64(10))
	require.Equal(t, resp.DispatcherStatuses[1].Action.Action, heartbeatpb.Action_Write)
	require.False(t, resp.DispatcherStatuses[1].Action.IsSyncPoint)
	// the checkpoint ts is updated
	scheduleMsg := ddlSpan.NewAddDispatcherMessage("node1", heartbeatpb.OperatorType_O_Add)
	require.Equal(t, uint64(9), scheduleMsg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).Config.StartTs, false)
	require.NotEqual(t, uint64(0), scheduleMsg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).Config.StartTs, false)
}

// TODO:Add more cases here
func TestHandleBlockBootstrapResponse(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	var dispatcherIDs []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 2)
		stm := spanController.GetTasksByTableID(int64(id))[0]
		dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
		spanController.BindSpanToNode("", "node1", stm)
		spanController.MarkSpanReplicating(stm)
	}

	barrier := NewBarrier(spanController, operatorController, false, map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WAITING,
					},
				},
				{
					ID: dispatcherIDs[1],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WAITING,
					},
				},
			},
		},
	}, common.DefaultMode, nil)
	event := barrier.blockedEvents.m[getEventKey(6, false)]
	require.NotNil(t, event)
	require.False(t, event.selected.Load())
	require.False(t, event.writerDispatcherAdvanced)
	require.True(t, event.allDispatcherReported())

	// one waiting dispatcher, and one writing
	barrier = NewBarrier(spanController, operatorController, false, map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WAITING,
					},
				},
				{
					ID: dispatcherIDs[1],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_WRITING,
					},
				},
			},
		},
	}, common.DefaultMode, nil)
	event = barrier.blockedEvents.m[getEventKey(6, false)]
	require.NotNil(t, event)
	require.True(t, event.selected.Load())
	require.False(t, event.writerDispatcherAdvanced)

	// two done dispatchers
	barrier = NewBarrier(spanController, operatorController, false, map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_DONE,
					},
				},
				{
					ID: dispatcherIDs[1],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_DONE,
					},
				},
			},
		},
	}, common.DefaultMode, nil)
	event = barrier.blockedEvents.m[getEventKey(6, false)]
	require.NotNil(t, event)
	require.True(t, event.selected.Load())
	require.True(t, event.writerDispatcherAdvanced)

	// nil, none stage
	barrier = NewBarrier(spanController, operatorController, false, map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"nod1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID: dispatcherIDs[0],
					BlockState: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   6,
						BlockTables: &heartbeatpb.InfluencedTables{
							InfluenceType: heartbeatpb.InfluenceType_Normal,
							TableIDs:      []int64{1, 2},
						},
						Stage: heartbeatpb.BlockStage_NONE,
					},
				},
				{
					ID: dispatcherIDs[1],
				},
			},
		},
	}, common.DefaultMode, nil)
	event = barrier.blockedEvents.m[getEventKey(6, false)]
	require.Nil(t, event)
}

func TestSyncPointBlockPerf(t *testing.T) {
	testutil.SetUpTestServices(t)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode, nil)
	for id := 1; id < 1000; id++ {
		spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 1)
	}
	var dispatcherIDs []*heartbeatpb.DispatcherID
	absent := spanController.GetAbsentForTest(10000)
	for _, stm := range absent {
		spanController.BindSpanToNode("", "node1", stm)
		spanController.MarkSpanReplicating(stm)
		dispatcherIDs = append(dispatcherIDs, stm.ID.ToPB())
	}
	var blockStatus []*heartbeatpb.TableSpanBlockStatus
	for _, id := range dispatcherIDs {
		blockStatus = append(blockStatus, &heartbeatpb.TableSpanBlockStatus{
			ID: id,
			State: &heartbeatpb.State{
				IsBlocked: true,
				BlockTs:   10,
				BlockTables: &heartbeatpb.InfluencedTables{
					InfluenceType: heartbeatpb.InfluenceType_All,
					SchemaID:      1,
				},
				IsSyncPoint: true,
			},
		})
	}

	// f, _ := os.OpenFile("cpu.profile", os.O_CREATE|os.O_RDWR, 0644)
	// defer f.Close()
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()
	now := time.Now()
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  cfID.ToPB(),
		BlockStatuses: blockStatus,
	})
	require.NotNil(t, msgs)
	log.Info("duration", zap.Duration("duration", time.Since(now)))

	now = time.Now()
	var passStatus []*heartbeatpb.TableSpanBlockStatus
	for _, id := range dispatcherIDs {
		passStatus = append(passStatus, &heartbeatpb.TableSpanBlockStatus{
			ID: id,
			State: &heartbeatpb.State{
				IsBlocked:   true,
				BlockTs:     10,
				IsSyncPoint: true,
				Stage:       heartbeatpb.BlockStage_DONE,
			},
		})
	}
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  cfID.ToPB(),
		BlockStatuses: passStatus,
	})
	require.NotNil(t, msgs)
	log.Info("duration", zap.Duration("duration", time.Since(now)))
}

// TestBarrierEventWithDispatcherReallocation tests the barrier's behavior when dispatchers are reallocated
// during a blocking event. The test verifies that:
// 1. When dispatchers are removed and new ones are created to replace them
// 2. The barrier correctly tracks the new dispatchers and their blocking status
// 3. The event selection logic works properly with the reallocated dispatchers
// 4. The barrier maintains consistency when dispatcher IDs change but the same table spans are covered
func TestBarrierEventWithDispatcherReallocation(t *testing.T) {
	testutil.SetUpTestServices(t)

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	tableID := int64(1)
	schemaID := int64(1)
	startTs := uint64(10)
	ddlTs := uint64(10)

	spanController.AddNewTable(commonEvent.Table{SchemaID: schemaID, TableID: tableID}, startTs)

	span := common.TableIDToComparableSpan(0, tableID)
	startKey := span.StartKey
	endKey := span.EndKey

	dispatcherA := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: startKey,
		EndKey:   append(startKey, byte('a')),
	}, startTs, common.DefaultMode, false)

	dispatcherB := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: append(startKey, byte('a')),
		EndKey:   append(startKey, byte('b')),
	}, startTs, common.DefaultMode, false)

	dispatcherC := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: append(startKey, byte('b')),
		EndKey:   endKey,
	}, startTs, common.DefaultMode, false)

	// add dispatcher to spanController and set to replicating state
	spanController.AddReplicatingSpan(dispatcherA)
	spanController.AddReplicatingSpan(dispatcherB)
	spanController.AddReplicatingSpan(dispatcherC)

	// bind to node
	spanController.BindSpanToNode("", "node1", dispatcherA)
	spanController.BindSpanToNode("", "node1", dispatcherB)
	spanController.BindSpanToNode("", "node1", dispatcherC)

	spanController.MarkSpanReplicating(dispatcherA)
	spanController.MarkSpanReplicating(dispatcherB)
	spanController.MarkSpanReplicating(dispatcherC)

	// create barrier
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode, nil)

	// report from dispatcherA
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherA.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   ddlTs,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{tableID},
					},
				},
			},
		},
	})

	require.NotNil(t, msgs)

	// check the event is created, but not selected
	event, ok := barrier.blockedEvents.Get(getEventKey(ddlTs, false))
	require.True(t, ok)
	require.NotNil(t, event)
	require.False(t, event.selected.Load())
	require.Contains(t, event.reportedDispatchers, dispatcherA.ID)

	// remove dispatcherA, B, C
	spanController.RemoveReplicatingSpan(dispatcherA)
	spanController.RemoveReplicatingSpan(dispatcherB)
	spanController.RemoveReplicatingSpan(dispatcherC)

	// check dispatcherA, B, C is removed
	require.Nil(t, spanController.GetTaskByID(dispatcherA.ID))
	require.Nil(t, spanController.GetTaskByID(dispatcherB.ID))
	require.Nil(t, spanController.GetTaskByID(dispatcherC.ID))

	// create new dispatcher E, F, G
	dispatcherE := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: append(startKey, byte('a')),
		EndKey:   append(startKey, byte('b')),
	}, startTs, common.DefaultMode, false)

	dispatcherF := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: append(startKey, byte('b')),
		EndKey:   endKey,
	}, startTs, common.DefaultMode, false)

	dispatcherG := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: startKey,
		EndKey:   append(startKey, byte('a')),
	}, startTs, common.DefaultMode, false)

	spanController.AddReplicatingSpan(dispatcherE)
	spanController.AddReplicatingSpan(dispatcherF)
	spanController.AddReplicatingSpan(dispatcherG)

	spanController.BindSpanToNode("", "node1", dispatcherE)
	spanController.BindSpanToNode("", "node1", dispatcherF)
	spanController.BindSpanToNode("", "node1", dispatcherG)

	spanController.MarkSpanReplicating(dispatcherE)
	spanController.MarkSpanReplicating(dispatcherF)
	spanController.MarkSpanReplicating(dispatcherG)

	// report from dispatcherE and dispatcherF
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherE.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   ddlTs,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{tableID},
					},
				},
			},
			{
				ID: dispatcherF.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   ddlTs,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{tableID},
					},
				},
			},
		},
	})

	require.NotNil(t, msgs)

	// check writer of this event is not selected
	event, ok = barrier.blockedEvents.Get(getEventKey(ddlTs, false))
	require.True(t, ok)
	require.NotNil(t, event)
	require.False(t, event.allDispatcherReported())

	// check remove dispatcherA
	require.NotContains(t, event.reportedDispatchers, dispatcherA.ID)
	require.Contains(t, event.reportedDispatchers, dispatcherE.ID)
	require.Contains(t, event.reportedDispatchers, dispatcherF.ID)

	require.False(t, event.allDispatcherReported())

	// report from dispatcherG
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherG.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   ddlTs,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{tableID},
					},
				},
			},
		},
	})

	require.NotNil(t, msgs)

	// check the event is selected
	event, ok = barrier.blockedEvents.Get(getEventKey(ddlTs, false))
	require.True(t, ok)
	require.NotNil(t, event)
	require.True(t, event.selected.Load())
}

// TestBarrierEventWithDispatcherScheduling tests the barrier's behavior when a dispatcher
// goes through scheduling process (replicating -> scheduling -> replicating) while
// handling DDL events. The test verifies that:
// 1. When dispatcher A reports DDL before table trigger event dispatcher, DDL should not execute
// 2. When dispatcher A enters scheduling state and table trigger event dispatcher reports DDL, DDL should not execute
// 3. When dispatcher A finishes scheduling and reports DDL again, DDL should execute
func TestBarrierEventWithDispatcherScheduling(t *testing.T) {
	testutil.SetUpTestServices(t)

	// Setup table trigger event dispatcher (DDL dispatcher)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	// Setup dispatcher A
	tableID := int64(1)
	schemaID := int64(1)
	startTs := uint64(9)
	ddlTs := uint64(10)

	span := common.TableIDToComparableSpan(0, tableID)
	dispatcherA := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}, startTs, common.DefaultMode, false)

	// Add dispatcher A to spanController and set to replicating state
	spanController.AddReplicatingSpan(dispatcherA)
	spanController.BindSpanToNode("", "node1", dispatcherA)
	// After binding to node, we need to mark it as replicating again
	spanController.MarkSpanReplicating(dispatcherA)

	// Create barrier
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode, nil)

	// Verify dispatcher A is in replicating state
	require.True(t, spanController.IsReplicating(dispatcherA))

	// Phase 1: Dispatcher A reports DDL before table trigger event dispatcher
	// This should not trigger DDL execution since table trigger event dispatcher hasn't reported yet
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherA.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   ddlTs,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{tableID, 0},
					},
				},
			},
		},
	})

	require.NotNil(t, msgs)

	// Verify the event is created but not selected for execution
	event, ok := barrier.blockedEvents.Get(getEventKey(ddlTs, false))
	require.True(t, ok)
	require.NotNil(t, event)
	require.False(t, event.selected.Load())
	require.Contains(t, event.reportedDispatchers, dispatcherA.ID)

	// Phase 2: Dispatcher A enters scheduling state, table trigger event dispatcher reports DDL
	// Move dispatcher A to scheduling state
	spanController.MarkSpanScheduling(dispatcherA)

	// Table trigger event dispatcher reports DDL
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   ddlTs,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{tableID, 0},
					},
				},
			},
		},
	})

	require.NotNil(t, msgs)

	// Verify DDL should not execute because dispatcher A is in scheduling state and was removed from reported dispatchers
	// Only table trigger event dispatcher remains, but range checker still expects all tasks to report
	event, ok = barrier.blockedEvents.Get(getEventKey(ddlTs, false))
	require.True(t, ok)
	require.NotNil(t, event)
	require.False(t, event.selected.Load())
	require.Contains(t, event.reportedDispatchers, tableTriggerEventDispatcherID)
	require.NotContains(t, event.reportedDispatchers, dispatcherA.ID)

	// Phase 3: Dispatcher A finishes scheduling and reports DDL again
	// Move dispatcher A back to replicating state
	spanController.MarkSpanReplicating(dispatcherA)

	// Dispatcher A reports DDL again after scheduling
	msgs = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcherA.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   ddlTs,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{tableID, 0},
					},
				},
			},
		},
	})

	require.NotNil(t, msgs)

	event, ok = barrier.blockedEvents.Get(getEventKey(ddlTs, false))
	require.True(t, ok)
	require.NotNil(t, event)
	require.True(t, event.selected.Load())
}

func TestDeferAllDBBlockEventFromDDLDispatcherWhilePendingSchedule(t *testing.T) {
	testutil.SetUpTestServices(t)
	nm := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nmap := nm.GetAliveNodes()
	for key := range nmap {
		delete(nmap, key)
	}
	nmap["node1"] = &node.Info{ID: "node1"}

	tableTriggerDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	oldTableID := int64(1)
	newTableID := int64(101)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: oldTableID}, 1)
	absents := spanController.GetAbsentForTest(10000)
	require.Len(t, absents, 1)
	oldTableDispatcherID := absents[0].ID
	spanController.BindSpanToNode("", "node1", absents[0])
	spanController.MarkSpanReplicating(absents[0])

	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, nil)

	// Build a TRUNCATE TABLE-like block event, which requires scheduling and will be enqueued into pendingEvents
	// when the write action is issued.
	truncateState := &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		Stage:     heartbeatpb.BlockStage_WAITING,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{oldTableID, common.DDLSpanTableID},
		},
		NeedDroppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{oldTableID},
		},
		NeedAddedTables: []*heartbeatpb.Table{
			{SchemaID: 1, TableID: newTableID, Splitable: false},
		},
	}
	_ = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{ID: spanController.GetDDLDispatcherID().ToPB(), State: truncateState},
			{ID: oldTableDispatcherID.ToPB(), State: truncateState},
		},
	})
	require.Greater(t, barrier.pendingEvents.Len(), 0)

	// Syncpoint arrives after truncate write done but before scheduling finishes.
	syncpointState := &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   20,
		Stage:     heartbeatpb.BlockStage_WAITING,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_All,
		},
		IsSyncPoint: true,
	}
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{ID: spanController.GetDDLDispatcherID().ToPB(), State: syncpointState},
		},
	})
	require.NotEmpty(t, msgs)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 0)

	key := getEventKey(20, true)
	event := barrier.blockedEvents.m[key]
	require.NotNil(t, event)
	require.Nil(t, event.rangeChecker)

	// Truncate finished writing, maintainer schedules it and removes old table task from spanController.
	_ = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: spanController.GetDDLDispatcherID().ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     10,
					IsSyncPoint: false,
					Stage:       heartbeatpb.BlockStage_DONE,
				},
			},
		},
	})
	require.Equal(t, 0, barrier.pendingEvents.Len())

	// Re-send syncpoint status, now maintainer should build a range checker based on the updated task set.
	_ = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{ID: spanController.GetDDLDispatcherID().ToPB(), State: syncpointState},
		},
	})

	event = barrier.blockedEvents.m[key]
	require.NotNil(t, event)
	require.NotNil(t, event.rangeChecker)
	rc, ok := event.rangeChecker.(*range_checker.TableIDRangeChecker)
	require.True(t, ok)
	require.Contains(t, rc.Detail(), "uncovered tables")
	require.Contains(t, rc.Detail(), "101")
}

func TestBarrierReturnsTemporaryIgnoreForUnreplicatingWaitingStatus(t *testing.T) {
	testutil.SetUpTestServices(t)

	tableTriggerDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, 10)
	stm := spanController.GetTasksByTableID(1)[0]
	spanController.BindSpanToNode("", "node1", stm)

	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode, nil)
	msgs := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: stm.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   10,
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						TableIDs:      []int64{1},
					},
					Stage: heartbeatpb.BlockStage_WAITING,
				},
			},
		},
	})

	require.Len(t, msgs, 1)
	resp := msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	status := resp.DispatcherStatuses[0]
	require.Nil(t, status.Ack)
	require.Nil(t, status.Action)
	require.NotNil(t, status.IgnoredBlockStatus)
	require.Equal(t, uint64(10), status.IgnoredBlockStatus.CommitTs)
	require.False(t, status.IgnoredBlockStatus.IsSyncPoint)
	require.NotNil(t, status.InfluencedDispatchers)
	require.Equal(t, heartbeatpb.InfluenceType_Normal, status.InfluencedDispatchers.InfluenceType)
	require.Len(t, status.InfluencedDispatchers.DispatcherIDs, 1)
	require.Equal(t, stm.ID, common.NewDispatcherIDFromPB(status.InfluencedDispatchers.DispatcherIDs[0]))
	require.Equal(t, 0, barrier.blockedEvents.Len())
	require.Nil(t, stm.GetBlockState())
}
