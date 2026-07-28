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
	event := &commonEvent.DDLEvent{
		FinishedTs: 100,
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
		UpdatedSchemas: []commonEvent.SchemaIDChange{
			{TableID: 12, OldSchemaID: 13, NewSchemaID: 14},
		},
	}

	status := &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:         true,
			BlockTs:           event.GetCommitTs(),
			BlockTables:       cloneInfluencedTablesPB(event.GetBlockedTables()),
			NeedDroppedTables: cloneInfluencedTablesPB(event.GetNeedDroppedTables()),
			NeedAddedTables:   commonEvent.ToTablesPB(event.GetNeedAddedTables()),
			UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(event.GetUpdatedSchemas()),
			Stage:             heartbeatpb.BlockStage_WAITING,
		},
		Mode: common.DefaultMode,
	}
	require.Equal(t, []int64{common.DDLSpanTableID, 1, 2}, status.State.BlockTables.TableIDs)
	require.Equal(t, []int64{3, 4}, status.State.NeedDroppedTables.TableIDs)
	require.Equal(t, int64(11), status.State.NeedAddedTables[0].TableID)
	require.Equal(t, int64(14), status.State.UpdatedSchemas[0].NewSchemaID)

	event.BlockedTables.TableIDs[0] = 101
	event.NeedDroppedTables.TableIDs[0] = 102
	event.NeedAddedTables[0].TableID = 103
	event.UpdatedSchemas[0].NewSchemaID = 104

	require.Equal(t, []int64{common.DDLSpanTableID, 1, 2}, status.State.BlockTables.TableIDs)
	require.Equal(t, []int64{3, 4}, status.State.NeedDroppedTables.TableIDs)
	require.Equal(t, int64(11), status.State.NeedAddedTables[0].TableID)
	require.Equal(t, int64(14), status.State.UpdatedSchemas[0].NewSchemaID)
}

func TestRouteTableAdmissionsForNameLevelDDLs(t *testing.T) {
	dispatcher := newRouteAdmissionTestDispatcher(t, true)

	tests := []struct {
		name  string
		event commonEvent.BlockEvent
		want  []*heartbeatpb.RouteTableAdmission
	}{
		{
			name: "create table",
			event: &commonEvent.DDLEvent{
				Type: byte(model.ActionCreateTable),
				TableNameChange: &commonEvent.TableNameChange{
					AddName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "p"}},
				},
			},
			want: []*heartbeatpb.RouteTableAdmission{
				routeAdmitPB("db", "p", "target_db", "p_routed"),
			},
		},
		{
			name: "create tables",
			event: &commonEvent.DDLEvent{
				Type: byte(model.ActionCreateTables),
				TableNameChange: &commonEvent.TableNameChange{
					AddName: []commonEvent.SchemaTableName{
						{SchemaName: "db", TableName: "t1"},
						{SchemaName: "db", TableName: "t2"},
					},
				},
			},
			want: []*heartbeatpb.RouteTableAdmission{
				routeAdmitPB("db", "t1", "target_db", "t1_routed"),
				routeAdmitPB("db", "t2", "target_db", "t2_routed"),
			},
		},
		{
			name: "drop table",
			event: &commonEvent.DDLEvent{
				Type: byte(model.ActionDropTable),
				TableNameChange: &commonEvent.TableNameChange{
					DropName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "p"}},
				},
			},
			want: []*heartbeatpb.RouteTableAdmission{
				routeReleasePB("db", "p"),
			},
		},
		{
			name: "rename table",
			event: &commonEvent.DDLEvent{
				Type: byte(model.ActionRenameTable),
				TableNameChange: &commonEvent.TableNameChange{
					DropName: []commonEvent.SchemaTableName{{SchemaName: "db", TableName: "p"}},
					AddName:  []commonEvent.SchemaTableName{{SchemaName: "db2", TableName: "p2"}},
				},
			},
			want: []*heartbeatpb.RouteTableAdmission{
				routeReleasePB("db", "p"),
				routeAdmitPB("db2", "p2", "target_db", "p2_routed"),
			},
		},
		{
			name: "rename tables",
			event: &commonEvent.DDLEvent{
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
			},
			want: []*heartbeatpb.RouteTableAdmission{
				routeReleasePB("db", "t1"),
				routeReleasePB("db", "t2"),
				routeAdmitPB("db2", "t1_new", "target_db", "t1_new_routed"),
				routeAdmitPB("db2", "t2_new", "target_db", "t2_new_routed"),
			},
		},
		{
			name: "drop schema",
			event: &commonEvent.DDLEvent{
				Type: byte(model.ActionDropSchema),
				TableNameChange: &commonEvent.TableNameChange{
					DropDatabaseName: "db",
				},
			},
			want: []*heartbeatpb.RouteTableAdmission{
				routeReleaseSchemaPB("db"),
			},
		},
		{
			name: "partition ddl",
			event: &commonEvent.DDLEvent{
				Type: byte(model.ActionTruncateTablePartition),
				NeedAddedTables: []commonEvent.Table{
					{SchemaID: 1, TableID: 101},
				},
				NeedDroppedTables: &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      []int64{100},
				},
			},
		},
		{
			name: "no table name change",
			event: &commonEvent.DDLEvent{
				Type: byte(model.ActionDropTable),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, dispatcher.routeTableAdmissionsForBlockState(tc.event))
		})
	}
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

func routeReleaseSchemaPB(sourceSchema string) *heartbeatpb.RouteTableAdmission {
	return &heartbeatpb.RouteTableAdmission{
		SourceSchemaName: sourceSchema,
		Action:           heartbeatpb.RouteTableAdmissionAction_RELEASE_SCHEMA,
	}
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
