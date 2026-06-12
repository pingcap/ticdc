// Copyright 2026 PingCAP, Inc.
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

package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/routing"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBlockStatusBufferDeduplicatesPendingDone(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.OfferDone(dispatcherID, 100, false, common.DefaultMode)
	buffer.OfferDone(dispatcherID, 100, false, common.DefaultMode)

	require.Equal(t, 1, buffer.Len())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg := buffer.Take(ctx)
	require.NotNil(t, msg)
	require.Equal(t, dispatcherID, common.NewDispatcherIDFromPB(msg.ID))
	require.Equal(t, uint64(100), msg.State.BlockTs)
	require.Equal(t, heartbeatpb.BlockStage_DONE, msg.State.Stage)
	require.False(t, msg.State.IsSyncPoint)
	require.Equal(t, common.DefaultMode, msg.Mode)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferDeduplicatesPendingWaiting(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	offerWaiting := func() {
		buffer.OfferStatus(newTestWaitingBlockStatus(dispatcherID, 150, common.DefaultMode))
	}

	offerWaiting()
	offerWaiting()

	require.Equal(t, 1, buffer.Len())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg := buffer.Take(ctx)
	require.NotNil(t, msg)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, msg.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferAllowsWaitingAgainAfterTake(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	offerWaiting := func() {
		buffer.OfferStatus(newTestWaitingBlockStatus(dispatcherID, 180, common.DefaultMode))
	}

	offerWaiting()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, first.State.Stage)

	offerWaiting()

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, second.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferAllowsDoneAgainAfterTake(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.OfferDone(dispatcherID, 190, false, common.DefaultMode)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.Equal(t, heartbeatpb.BlockStage_DONE, first.State.Stage)

	buffer.OfferDone(dispatcherID, 190, false, common.DefaultMode)

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.Equal(t, heartbeatpb.BlockStage_DONE, second.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferKeepsWaitingBeforeDone(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.OfferStatus(newTestWaitingBlockStatus(dispatcherID, 200, common.DefaultMode))
	buffer.OfferDone(dispatcherID, 200, false, common.DefaultMode)
	buffer.OfferDone(dispatcherID, 200, false, common.DefaultMode)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, first.State.Stage)

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.Equal(t, heartbeatpb.BlockStage_DONE, second.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferKeepsDistinctDoneKeys(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.OfferDone(dispatcherID, 300, false, common.DefaultMode)
	buffer.OfferDone(dispatcherID, 300, true, common.DefaultMode)
	buffer.OfferDone(dispatcherID, 300, false, common.RedoMode)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.False(t, first.State.IsSyncPoint)
	require.Equal(t, common.DefaultMode, first.Mode)

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.True(t, second.State.IsSyncPoint)
	require.Equal(t, common.DefaultMode, second.Mode)

	third := buffer.Take(ctx)
	require.NotNil(t, third)
	require.False(t, third.State.IsSyncPoint)
	require.Equal(t, common.RedoMode, third.Mode)

	requireNoBlockStatus(t, buffer)
}

func TestWaitingBlockStatusClonesMutableMetadata(t *testing.T) {
	dispatcherID := common.NewDispatcherID()
	dispatcher := newRouteAdmissionTestDispatcher(t, true)
	event := &commonEvent.DDLEvent{
		FinishedTs: 100,
		Type:       byte(model.ActionCreateTable),
		SchemaName: "db",
		TableName:  "t",
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID, 1, 2},
		},
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{3, 4},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID:  10,
				TableID:   11,
				Splitable: true,
			},
		},
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "t"}},
		},
		UpdatedSchemas: []commonEvent.SchemaIDChange{
			{TableID: 12, OldSchemaID: 13, NewSchemaID: 14},
		},
	}
	event = commonEvent.NewRoutedDDLEvent(event, event.Query, "target_db", "t_routed", "", "", &common.TableInfo{
		TableName: common.TableName{
			Schema:       "db",
			Table:        "t",
			TableID:      11,
			TargetSchema: "target_db",
			TargetTable:  "t_routed",
		},
	}, nil, nil)
	routeAdmissions := dispatcher.routeTableAdmissionsForBlockState(event)

	status := &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:            true,
			BlockTs:              event.GetCommitTs(),
			BlockTables:          cloneInfluencedTablesPB(event.GetBlockedTables()),
			NeedDroppedTables:    cloneInfluencedTablesPB(event.GetNeedDroppedTables()),
			NeedAddedTables:      commonEvent.ToTablesPB(event.GetNeedAddedTables()),
			RouteTableAdmissions: routeAdmissions,
			UpdatedSchemas:       commonEvent.ToSchemaIDChangePB(event.GetUpdatedSchemas()),
			Stage:                heartbeatpb.BlockStage_WAITING,
		},
		Mode: common.DefaultMode,
	}
	require.Equal(t, []int64{common.DDLSpanTableID, 1, 2}, status.State.BlockTables.TableIDs)
	require.Equal(t, []int64{3, 4}, status.State.NeedDroppedTables.TableIDs)
	require.Equal(t, int64(11), status.State.NeedAddedTables[0].TableID)
	require.Equal(t, "db", status.State.RouteTableAdmissions[0].SourceSchemaName)
	require.Equal(t, "t", status.State.RouteTableAdmissions[0].SourceTableName)
	require.Equal(t, "target_db", status.State.RouteTableAdmissions[0].TargetSchemaName)
	require.Equal(t, "t_routed", status.State.RouteTableAdmissions[0].TargetTableName)
	require.Equal(t, heartbeatpb.RouteTableAdmissionAction_ADMIT, status.State.RouteTableAdmissions[0].Action)
	require.Equal(t, int64(14), status.State.UpdatedSchemas[0].NewSchemaID)

	renameEvent := &commonEvent.DDLEvent{
		FinishedTs: 201,
		Type:       byte(model.ActionRenameTables),
		SchemaID:   10,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID, 21, 22},
		},
		MultipleTableInfos: []*common.TableInfo{
			{
				TableName: common.TableName{
					Schema:       "db1",
					Table:        "x",
					TableID:      21,
					TargetSchema: "target_db",
					TargetTable:  "x_routed",
				},
			},
			{
				TableName: common.TableName{
					Schema:       "db2",
					Table:        "y",
					TableID:      22,
					TargetSchema: "target_db",
					TargetTable:  "y_routed",
				},
			},
		},
		TableNameChange: &commonEvent.TableNameChange{
			DropName: []commonEvent.SchemaTableName{
				{SchemaName: "db1", TableName: "old_x"},
				{SchemaName: "db2", TableName: "old_y"},
			},
			AddName: []commonEvent.SchemaTableName{
				{SchemaName: "db1", TableName: "x"},
				{SchemaName: "db2", TableName: "y"},
			},
		},
		UpdatedSchemas: []commonEvent.SchemaIDChange{
			{TableID: 22, OldSchemaID: 10, NewSchemaID: 20},
		},
	}
	routeAdmissions = dispatcher.routeTableAdmissionsForBlockState(renameEvent)
	require.Equal(t, []*heartbeatpb.RouteTableAdmission{
		{
			SourceSchemaName: "db1",
			SourceTableName:  "old_x",
			Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE,
		},
		{
			SourceSchemaName: "db2",
			SourceTableName:  "old_y",
			Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE,
		},
		{
			SourceSchemaName: "db1",
			SourceTableName:  "x",
			TargetSchemaName: "target_db",
			TargetTableName:  "x_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
		{
			SourceSchemaName: "db2",
			SourceTableName:  "y",
			TargetSchemaName: "target_db",
			TargetTableName:  "y_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
	}, routeAdmissions)

	event.BlockedTables.TableIDs[0] = 101
	event.NeedDroppedTables.TableIDs[0] = 102
	event.NeedAddedTables[0].TableID = 103
	event.TableNameChange.AddName[0].SchemaName = "changed"
	event.TableNameChange.AddName[0].TableName = "changed"
	event.UpdatedSchemas[0].NewSchemaID = 104

	require.Equal(t, []int64{common.DDLSpanTableID, 1, 2}, status.State.BlockTables.TableIDs)
	require.Equal(t, []int64{3, 4}, status.State.NeedDroppedTables.TableIDs)
	require.Equal(t, int64(11), status.State.NeedAddedTables[0].TableID)
	require.Equal(t, "db", status.State.RouteTableAdmissions[0].SourceSchemaName)
	require.Equal(t, "t", status.State.RouteTableAdmissions[0].SourceTableName)
	require.Equal(t, "target_db", status.State.RouteTableAdmissions[0].TargetSchemaName)
	require.Equal(t, "t_routed", status.State.RouteTableAdmissions[0].TargetTableName)
	require.Equal(t, heartbeatpb.RouteTableAdmissionAction_ADMIT, status.State.RouteTableAdmissions[0].Action)
	require.Equal(t, int64(14), status.State.UpdatedSchemas[0].NewSchemaID)
}

func TestRouteTableAdmissionsForNameLevelDDLs(t *testing.T) {
	dispatcher := newRouteAdmissionTestDispatcher(t, true)

	createEvent := commonEvent.NewRoutedDDLEvent(
		&commonEvent.DDLEvent{
			Type:       byte(model.ActionCreateTable),
			SchemaName: "db",
			TableName:  "p",
			TableNameChange: &commonEvent.TableNameChange{
				AddName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "p"}},
			},
		},
		"create table target_db.p_routed(id int)",
		"target_db",
		"p_routed",
		"",
		"",
		nil,
		nil,
		nil,
	)
	admissions := dispatcher.routeTableAdmissionsForBlockState(createEvent)
	require.Equal(t, []*heartbeatpb.RouteTableAdmission{
		{
			SourceSchemaName: "db",
			SourceTableName:  "p",
			TargetSchemaName: "target_db",
			TargetTableName:  "p_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
	}, admissions)

	createTablesEvent := &commonEvent.DDLEvent{
		Type: byte(model.ActionCreateTables),
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{SchemaName: "db", TableName: "t1"},
				{SchemaName: "db", TableName: "t2"},
			},
		},
	}
	admissions = dispatcher.routeTableAdmissionsForBlockState(createTablesEvent)
	require.Equal(t, []*heartbeatpb.RouteTableAdmission{
		{
			SourceSchemaName: "db",
			SourceTableName:  "t1",
			TargetSchemaName: "target_db",
			TargetTableName:  "t1_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
		{
			SourceSchemaName: "db",
			SourceTableName:  "t2",
			TargetSchemaName: "target_db",
			TargetTableName:  "t2_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
	}, admissions)

	dropEvent := &commonEvent.DDLEvent{
		Type:       byte(model.ActionDropTable),
		SchemaName: "db",
		TableName:  "p",
		TableNameChange: &commonEvent.TableNameChange{
			DropName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "p"}},
		},
	}
	admissions = dispatcher.routeTableAdmissionsForBlockState(dropEvent)
	require.Equal(t, []*heartbeatpb.RouteTableAdmission{
		{
			SourceSchemaName: "db",
			SourceTableName:  "p",
			Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE,
		},
	}, admissions)

	renameEvent := commonEvent.NewRoutedDDLEvent(
		&commonEvent.DDLEvent{
			Type: byte(model.ActionRenameTable),
			TableNameChange: &commonEvent.TableNameChange{
				AddName:  []commonEvent.SchemaTableName{{SchemaName: "db2", TableName: "p2"}},
				DropName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "p"}},
			},
			SchemaName: "db2",
			TableName:  "p2",
		},
		"rename table target_db.p_routed to target_db.p2_routed",
		"target_db",
		"p2_routed",
		"",
		"",
		nil,
		nil,
		nil,
	)
	admissions = dispatcher.routeTableAdmissionsForBlockState(renameEvent)
	require.Equal(t, []*heartbeatpb.RouteTableAdmission{
		{
			SourceSchemaName: "db",
			SourceTableName:  "p",
			Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE,
		},
		{
			SourceSchemaName: "db2",
			SourceTableName:  "p2",
			TargetSchemaName: "target_db",
			TargetTableName:  "p2_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
	}, admissions)

	renameTablesEvent := &commonEvent.DDLEvent{
		Type: byte(model.ActionRenameTables),
		TableNameChange: &commonEvent.TableNameChange{
			DropName: []commonEvent.SchemaTableName{
				{SchemaName: "db", TableName: "t1"},
				{SchemaName: "db", TableName: "t2"},
			},
			AddName: []commonEvent.SchemaTableName{
				{SchemaName: "db2", TableName: "t1_new"},
				{SchemaName: "db2", TableName: "t2_new"},
			},
		},
	}
	admissions = dispatcher.routeTableAdmissionsForBlockState(renameTablesEvent)
	require.Equal(t, []*heartbeatpb.RouteTableAdmission{
		{
			SourceSchemaName: "db",
			SourceTableName:  "t1",
			Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE,
		},
		{
			SourceSchemaName: "db",
			SourceTableName:  "t2",
			Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE,
		},
		{
			SourceSchemaName: "db2",
			SourceTableName:  "t1_new",
			TargetSchemaName: "target_db",
			TargetTableName:  "t1_new_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
		{
			SourceSchemaName: "db2",
			SourceTableName:  "t2_new",
			TargetSchemaName: "target_db",
			TargetTableName:  "t2_new_routed",
			Action:           heartbeatpb.RouteTableAdmissionAction_ADMIT,
		},
	}, admissions)

	dropSchemaEvent := &commonEvent.DDLEvent{
		Type:       byte(model.ActionDropSchema),
		SchemaName: "db",
		TableNameChange: &commonEvent.TableNameChange{
			DropDatabaseName: "db",
		},
	}
	admissions = dispatcher.routeTableAdmissionsForBlockState(dropSchemaEvent)
	require.Equal(t, []*heartbeatpb.RouteTableAdmission{
		{
			SourceSchemaName: "db",
			Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE_SCHEMA,
		},
	}, admissions)

	partitionEvent := &commonEvent.DDLEvent{
		Type:       byte(model.ActionTruncateTablePartition),
		SchemaName: "db",
		TableName:  "p",
		NeedAddedTables: []commonEvent.Table{
			{SchemaID: 1, TableID: 101},
		},
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{100},
		},
	}
	admissions = dispatcher.routeTableAdmissionsForBlockState(partitionEvent)
	require.Nil(t, admissions)

	admissions = dispatcher.routeTableAdmissionsForBlockState(&commonEvent.DDLEvent{
		Type:       byte(model.ActionDropTable),
		SchemaName: "db",
		TableName:  "p",
	})
	require.Nil(t, admissions)
}

func TestRouteTableAdmissionsOnlyReportedByTableTrigger(t *testing.T) {
	tableTriggerDispatcher := newRouteAdmissionTestDispatcher(t, true)
	tableDispatcher := newRouteAdmissionTestDispatcher(t, false)
	event := &commonEvent.DDLEvent{
		Type: byte(model.ActionRenameTable),
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID, 1},
		},
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "t"}},
		},
	}

	require.NotEmpty(t, tableTriggerDispatcher.routeTableAdmissionsForBlockState(event))
	require.Nil(t, tableDispatcher.routeTableAdmissionsForBlockState(event))
}

func newRouteAdmissionTestDispatcher(t *testing.T, tableTrigger bool) *BasicDispatcher {
	t.Helper()

	router, err := routing.NewRouter(
		common.NewChangefeedID(common.DefaultKeyspaceName),
		false,
		[]*config.DispatchRule{
			{Matcher: []string{"*.*"}, TargetSchema: "target_db", TargetTable: "{table}_routed"},
		},
	)
	require.NoError(t, err)

	tableSpan := common.KeyspaceDDLSpan(common.DefaultKeyspaceID)
	if !tableTrigger {
		span := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 1)
		tableSpan = &span
	}
	return &BasicDispatcher{
		sharedInfo: &SharedInfo{router: router},
		tableSpan:  tableSpan,
	}
}

func TestNoneBlockStatusClonesMutableMetadata(t *testing.T) {
	dispatcherID := common.NewDispatcherID()
	event := &commonEvent.DDLEvent{
		FinishedTs: 200,
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{5, 6},
		},
		NeedAddedTables: []commonEvent.Table{
			{SchemaID: 20, TableID: 21, Splitable: true},
		},
	}

	status := &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			BlockTs:           event.GetCommitTs(),
			NeedDroppedTables: cloneInfluencedTablesPB(event.GetNeedDroppedTables()),
			NeedAddedTables:   commonEvent.ToTablesPB(event.GetNeedAddedTables()),
			Stage:             heartbeatpb.BlockStage_NONE,
		},
		Mode: common.DefaultMode,
	}
	require.Equal(t, []int64{5, 6}, status.State.NeedDroppedTables.TableIDs)
	require.Equal(t, int64(21), status.State.NeedAddedTables[0].TableID)

	event.NeedDroppedTables.TableIDs[0] = 201
	event.NeedAddedTables[0].TableID = 202

	require.Equal(t, []int64{5, 6}, status.State.NeedDroppedTables.TableIDs)
	require.Equal(t, int64(21), status.State.NeedAddedTables[0].TableID)
}

func requireNoBlockStatus(t *testing.T, buffer *BlockStatusBuffer) {
	t.Helper()

	status, ok := buffer.TryTake()
	require.False(t, ok)
	require.Nil(t, status)
}

func newTestWaitingBlockStatus(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	mode int64,
) *heartbeatpb.TableSpanBlockStatus {
	return &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked: true,
			BlockTs:   blockTs,
			Stage:     heartbeatpb.BlockStage_WAITING,
		},
		Mode: mode,
	}
}
