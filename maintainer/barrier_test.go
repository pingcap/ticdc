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
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestOneBlockEvent(t *testing.T) {
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)
	startTs := uint64(10)
	spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: 1}, startTs)
	stm := spanController.GetTasksByTableID(1)[0]
	spanController.BindSpanToNode("", "node1", stm)
	spanController.MarkSpanReplicating(stm)

	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode)
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: true,
	}
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
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
	msgs := event.resend(common.DefaultMode)
	require.Len(t, msgs, 1)
	require.True(t, msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.Action == heartbeatpb.Action_Write)
	require.True(t, msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.IsSyncPoint)

	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Equal(t, resp.DispatcherStatuses[0].Ack.CommitTs, uint64(10))
	require.Len(t, barrier.blockedEvents.m, 0)

	// send event done again
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.Nil(t, msg)
}

func TestNormalBlock(t *testing.T) {
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)
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
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode)

	// first node block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 2)

	// other node block request
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
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
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)
	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 3; id++ {
		spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 10)
		stm := spanController.GetTasksByTableID(int64(id))[0]
		blockedDispatcherIDS = append(blockedDispatcherIDS, stm.ID.ToPB())
		spanController.BindSpanToNode("", "node1", stm)
		spanController.MarkSpanReplicating(stm)
	}

	newSpan := &heartbeatpb.Table{TableID: 10, SchemaID: 1}
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode)

	// first node block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 1)
	require.False(t, barrier.blockedEvents.m[eventKey{blockTs: 10, isSyncPoint: false}].tableTriggerDispatcherRelated)

	// table trigger  block request
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	event.resend(common.DefaultMode)
	barrier.checkEventFinish(event)
	require.Len(t, barrier.blockedEvents.m, 0)
}

func TestSchemaBlock(t *testing.T) {
	testutil.SetUpTestServices()
	nm := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nmap := nm.GetAliveNodes()
	for key := range nmap {
		delete(nmap, key)
	}
	nmap["node1"] = &node.Info{ID: "node1"}
	nmap["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)

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
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode)

	// first dispatcher  block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.Nil(t, msg)

	// selected node write done
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
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
	msgs := barrier.Resend()
	require.Len(t, msgs, 0)
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
	testutil.SetUpTestServices()
	nm := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nmap := nm.GetAliveNodes()
	for key := range nmap {
		delete(nmap, key)
	}
	nmap["node1"] = &node.Info{ID: "node1"}
	nmap["node2"] = &node.Info{ID: "node2"}
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)
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

	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode)
	// first dispatcher  block request
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Len(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs, 3)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)

	// second dispatcher  block request
	msg = barrier.HandleStatus("node2", &heartbeatpb.BlockStatusRequest{
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
	resp = msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 2)
	require.True(t, resp.DispatcherStatuses[0].Ack.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.CommitTs == 10)
	require.True(t, resp.DispatcherStatuses[1].Action.Action == heartbeatpb.Action_Write)
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
	msgs := barrier.Resend()
	// 2 pass action messages to one node
	require.Len(t, msgs, 2)
	require.Len(t, barrier.blockedEvents.m, 1)
	// other dispatcher advanced checkpoint ts
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode)

	var blockedDispatcherIDS []*heartbeatpb.DispatcherID
	for id := 1; id < 4; id++ {
		spanController.AddNewTable(commonEvent.Table{SchemaID: 1, TableID: int64(id)}, 10)
		stm := spanController.GetTasksByTableID(int64(id))[0]
		dispatcherID := stm.ID
		blockedDispatcherIDS = append(blockedDispatcherIDS, dispatcherID.ToPB())
		spanController.MarkSpanReplicating(stm)
	}
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
	require.Len(t, resp.DispatcherStatuses, 1)
	require.Equal(t, uint64(10), resp.DispatcherStatuses[0].Ack.CommitTs)
	require.True(t, heartbeatpb.InfluenceType_Normal == resp.DispatcherStatuses[0].InfluencedDispatchers.InfluenceType)
	require.Equal(t, resp.DispatcherStatuses[0].InfluencedDispatchers.DispatcherIDs[0], blockedDispatcherIDS[0])
	require.Len(t, barrier.blockedEvents.m, 0)
	require.Equal(t, 2, spanController.GetAbsentSize(), 2)
}

func TestUpdateCheckpointTs(t *testing.T) {
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)
	barrier := NewBarrier(spanController, operatorController, false, nil, common.DefaultMode)
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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
	require.NotNil(t, msg)
	key := eventKey{
		blockTs:     10,
		isSyncPoint: false,
	}
	resp := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
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
	msg, err := ddlSpan.NewAddDispatcherMessage("node1")
	require.Nil(t, err)
	require.Equal(t, uint64(9), msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).Config.StartTs, false)
	require.NotEqual(t, uint64(0), msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest).Config.StartTs, false)
}

// TODO:Add more cases here
func TestHandleBlockBootstrapResponse(t *testing.T) {
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)

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
	}, common.DefaultMode)
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
	}, common.DefaultMode)
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
	}, common.DefaultMode)
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
	}, common.DefaultMode)
	event = barrier.blockedEvents.m[getEventKey(6, false)]
	require.Nil(t, event)
}

func TestSyncPointBlockPerf(t *testing.T) {
	testutil.SetUpTestServices()
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode)
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
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  cfID.ToPB(),
		BlockStatuses: blockStatus,
	})
	require.NotNil(t, msg)
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
	barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID:  cfID.ToPB(),
		BlockStatuses: passStatus,
	})
	require.NotNil(t, msg)
	log.Info("duration", zap.Duration("duration", time.Since(now)))
}

// TestBarrierEventWithDispatcherReallocation tests the barrier's behavior when dispatchers are reallocated
// during a blocking event. The test verifies that:
// 1. When dispatchers are removed and new ones are created to replace them
// 2. The barrier correctly tracks the new dispatchers and their blocking status
// 3. The event selection logic works properly with the reallocated dispatchers
// 4. The barrier maintains consistency when dispatcher IDs change but the same table spans are covered
func TestBarrierEventWithDispatcherReallocation(t *testing.T) {
	testutil.SetUpTestServices()

	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)

	tableID := int64(1)
	schemaID := int64(1)
	startTs := uint64(10)
	ddlTs := uint64(10)

	spanController.AddNewTable(commonEvent.Table{SchemaID: schemaID, TableID: tableID}, startTs)

	span := common.TableIDToComparableSpan(tableID)
	startKey := span.StartKey
	endKey := span.EndKey

	dispatcherA := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: startKey,
		EndKey:   append(startKey, byte('a')),
	}, startTs, common.DefaultMode)

	dispatcherB := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: append(startKey, byte('a')),
		EndKey:   append(startKey, byte('b')),
	}, startTs, common.DefaultMode)

	dispatcherC := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: append(startKey, byte('b')),
		EndKey:   endKey,
	}, startTs, common.DefaultMode)

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
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode)

	// report from dispatcherA
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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

	require.NotNil(t, msg)

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
	}, startTs, common.DefaultMode)

	dispatcherF := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: append(startKey, byte('b')),
		EndKey:   endKey,
	}, startTs, common.DefaultMode)

	dispatcherG := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: startKey,
		EndKey:   append(startKey, byte('a')),
	}, startTs, common.DefaultMode)

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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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

	require.NotNil(t, msg)

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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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

	require.NotNil(t, msg)

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
	testutil.SetUpTestServices()

	// Setup table trigger event dispatcher (DDL dispatcher)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)

	// Setup dispatcher A
	tableID := int64(1)
	schemaID := int64(1)
	startTs := uint64(9)
	ddlTs := uint64(10)

	span := common.TableIDToComparableSpan(tableID)
	dispatcherA := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}, startTs, common.DefaultMode)

	// Add dispatcher A to spanController and set to replicating state
	spanController.AddReplicatingSpan(dispatcherA)
	spanController.BindSpanToNode("", "node1", dispatcherA)
	// After binding to node, we need to mark it as replicating again
	spanController.MarkSpanReplicating(dispatcherA)

	// Create barrier
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode)

	// Verify dispatcher A is in replicating state
	require.True(t, spanController.IsReplicating(dispatcherA))

	// Phase 1: Dispatcher A reports DDL before table trigger event dispatcher
	// This should not trigger DDL execution since table trigger event dispatcher hasn't reported yet
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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

	require.NotNil(t, msg)

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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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

	require.NotNil(t, msg)

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
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
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

	require.NotNil(t, msg)

	event, ok = barrier.blockedEvents.Get(getEventKey(ddlTs, false))
	require.True(t, ok)
	require.NotNil(t, event)
	require.True(t, event.selected.Load())
}

// TestBarrierSyncPointEventWithDifferentReceivingOrder tests the barrier's behavior when
// different dispatchers receive syncpoint events with different commitTs batches.
// The test scenario:
// 1. There are 3 related dispatchers (1 table trigger + 2 normal dispatchers)
// 2. There are 4 syncpoint events with different commitTs (A=10, B=11, C=12)
// 3. Different receiving patterns:
//   - Table trigger first receives syncpoint event with commitTsList [A, B], reports B (max commitTs)
//   - Dispatcher1 first receives syncpoint event with commitTsList [A], reports A
//   - Dispatcher2 first receives syncpoint event with commitTsList [A, B, C], reports C (max commitTs)
//
// 4. Expected behavior:
//   - Barrier notifies dispatcher1 and table trigger to pass A and B
//   - After table trigger and dispatcher receive C and report C, barrier notifies table trigger to write C
func TestBarrierSyncPointEventWithDifferentReceivingOrder(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	// Setup table trigger event dispatcher (DDL dispatcher)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	cfID := common.NewChangeFeedIDWithName("test", "")
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	spanController := span.NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000)

	// Setup two normal dispatchers
	tableID1 := int64(1)
	tableID2 := int64(2)
	schemaID := int64(1)
	startTs := uint64(9)

	// Create dispatcher1 for table1
	span1 := common.TableIDToComparableSpan(tableID1)
	dispatcher1 := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID1,
		StartKey: span1.StartKey,
		EndKey:   span1.EndKey,
	}, startTs, common.DefaultMode)

	// Create dispatcher2 for table2
	span2 := common.TableIDToComparableSpan(tableID2)
	dispatcher2 := replica.NewSpanReplication(cfID, common.NewDispatcherID(), schemaID, &heartbeatpb.TableSpan{
		TableID:  tableID2,
		StartKey: span2.StartKey,
		EndKey:   span2.EndKey,
	}, startTs, common.DefaultMode)

	// Add dispatchers to spanController and set to replicating state
	spanController.AddReplicatingSpan(dispatcher1)
	spanController.AddReplicatingSpan(dispatcher2)
	spanController.BindSpanToNode("", "node1", dispatcher1)
	spanController.BindSpanToNode("", "node1", dispatcher2)
	spanController.MarkSpanReplicating(dispatcher1)
	spanController.MarkSpanReplicating(dispatcher2)

	// Create barrier
	barrier := NewBarrier(spanController, operatorController, true, nil, common.DefaultMode)

	// Define syncpoint event timestamps
	eventA := uint64(10)
	eventB := uint64(11)
	eventC := uint64(12)

	// Table trigger receives syncpoint event with commitTsList [A, B], reports B (max commitTs)
	msg := barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   eventB, // Reports B from [A, B]
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      schemaID,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	// Dispatcher1 receives syncpoint event with commitTsList [A], reports A
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher1.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   eventA, // Reports A from [A]
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      schemaID,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	//  Dispatcher2 also reports event C
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher2.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   eventC, // Reports C from [A, B, C]
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      schemaID,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	eventAKey := eventKey{blockTs: eventA, isSyncPoint: true}
	eventA_obj, ok := barrier.blockedEvents.Get(eventAKey)
	eventA_obj.setLastResendTime(time.Now().Add(-60 * time.Second))

	eventBKey := eventKey{blockTs: eventB, isSyncPoint: true}
	eventB_obj, ok := barrier.blockedEvents.Get(eventBKey)
	eventB_obj.setLastResendTime(time.Now().Add(-60 * time.Second))

	eventCKey := eventKey{blockTs: eventC, isSyncPoint: true}
	eventC_obj, ok := barrier.blockedEvents.Get(eventCKey)
	eventC_obj.setLastResendTime(time.Now().Add(-60 * time.Second))

	msgs := barrier.Resend()
	require.Len(t, msgs, 0)

	eventA_obj, ok = barrier.blockedEvents.Get(eventAKey)
	eventA_obj.setLastResendTime(time.Now().Add(-60 * time.Second))
	require.True(t, ok)
	require.NotNil(t, eventA_obj)
	require.True(t, eventA_obj.selected.Load())

	eventB_obj, ok = barrier.blockedEvents.Get(eventBKey)
	eventB_obj.setLastResendTime(time.Now().Add(-60 * time.Second))
	require.True(t, ok)
	require.NotNil(t, eventB_obj)
	require.True(t, eventB_obj.selected.Load())

	// Check that event C is not yet selected (only dispatcher2 reported)
	eventC_obj, ok = barrier.blockedEvents.Get(eventCKey)
	eventC_obj.setLastResendTime(time.Now().Add(-60 * time.Second))
	require.True(t, ok)
	require.NotNil(t, eventC_obj)
	require.False(t, eventC_obj.selected.Load())

	msgs = barrier.Resend()
	require.Len(t, msgs, 2)
	require.Equal(t, msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Pass)

	// dispatcher2 and table trigger reports write done for event B
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher2.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventB,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventB,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})

	// all dispatchers reports write done for event A
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher1.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventA,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcher2.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventA,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventA,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})

	eventA_obj, ok = barrier.blockedEvents.Get(eventAKey)
	require.False(t, ok)

	// Table trigger receives syncpoint event with commitTsList [C], reports C
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   eventC, // Reports C from [C]
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      schemaID,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	// Dispatcher1 receives syncpoint event with commitTsList [B], reports B
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher1.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   eventB, // Reports B from [B]
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      schemaID,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	eventB_obj, ok = barrier.blockedEvents.Get(eventBKey)
	eventB_obj.setLastResendTime(time.Now().Add(-60 * time.Second))

	eventC_obj, ok = barrier.blockedEvents.Get(eventCKey)
	eventC_obj.setLastResendTime(time.Now().Add(-60 * time.Second))

	msgs = barrier.Resend()
	require.Len(t, msgs, 1)
	require.Equal(t, msgs[0].Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].Action.Action, heartbeatpb.Action_Pass)

	// all dispatchers reports write done for event B
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher1.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventB,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcher2.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventB,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventB,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})

	eventB_obj, ok = barrier.blockedEvents.Get(eventBKey)
	require.False(t, ok)

	// Dispatcher1 receives syncpoint event with commitTsList [C], reports C
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher1.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked: true,
					BlockTs:   eventC, // Reports C from [C]
					BlockTables: &heartbeatpb.InfluencedTables{
						InfluenceType: heartbeatpb.InfluenceType_All,
						SchemaID:      schemaID,
					},
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	// Check that event C is now selected
	eventC_obj, ok = barrier.blockedEvents.Get(eventCKey)
	require.True(t, ok)
	require.NotNil(t, eventC_obj)
	require.True(t, eventC_obj.selected.Load())
	require.Equal(t, eventC_obj.writerDispatcher, tableTriggerEventDispatcherID)

	// Table trigger reports write done for event C
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: tableTriggerEventDispatcherID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventC,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	// Check that pass actions are sent for event C
	msgs = barrier.Resend()
	require.Len(t, msgs, 1)

	// Phase 12: All dispatchers report done for event C
	msg = barrier.HandleStatus("node1", &heartbeatpb.BlockStatusRequest{
		ChangefeedID: cfID.ToPB(),
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			{
				ID: dispatcher1.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventC,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
			{
				ID: dispatcher2.ID.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:   true,
					BlockTs:     eventC,
					Stage:       heartbeatpb.BlockStage_DONE,
					IsSyncPoint: true,
				},
			},
		},
	})
	require.NotNil(t, msg)

	// Check that event C is removed from blocked events
	_, ok = barrier.blockedEvents.Get(eventCKey)
	require.False(t, ok)

	// Verify all events are processed
	require.Len(t, barrier.blockedEvents.m, 0)
}
