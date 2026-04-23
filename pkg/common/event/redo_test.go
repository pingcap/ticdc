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

package event

import (
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRedoUsesRoutedTableNames(t *testing.T) {
	t.Parallel()

	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(id int primary key, name varchar(32))`)
	require.NotNil(t, job)

	sourceTableInfo := helper.GetTableInfo(job)
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	redoDDLEvent := (&DDLEvent{
		Query:      "ALTER TABLE `target_db`.`target_table` ADD COLUMN age INT",
		Type:       byte(model.ActionAddColumn),
		SchemaName: "test",
		TableName:  "t",
		TableInfo:  routedTableInfo,
		FinishedTs: 200,
		StartTs:    100,
	}).ToRedoLog().RedoDDL

	require.Equal(t, "target_db", redoDDLEvent.TableName.Schema)
	require.Equal(t, "target_table", redoDDLEvent.TableName.Table)
	require.Empty(t, redoDDLEvent.TableName.TargetSchema)
	require.Empty(t, redoDDLEvent.TableName.TargetTable)

	ddlEvent := redoDDLEvent.ToDDLEvent()
	require.Equal(t, "target_db", ddlEvent.SchemaName)
	require.Equal(t, "target_table", ddlEvent.TableName)
	require.Empty(t, ddlEvent.targetSchemaName)
	require.Empty(t, ddlEvent.targetTableName)
	require.Equal(t, "target_db", ddlEvent.GetTargetSchemaName())
	require.Equal(t, "target_table", ddlEvent.GetTargetTableName())
	require.Equal(t, "target_db", ddlEvent.TableInfo.GetSchemaName())
	require.Equal(t, "target_table", ddlEvent.TableInfo.GetTableName())
	require.Empty(t, ddlEvent.TableInfo.TableName.TargetSchema)
	require.Empty(t, ddlEvent.TableInfo.TableName.TargetTable)
	require.Equal(t, "target_db", ddlEvent.TableInfo.GetTargetSchemaName())
	require.Equal(t, "target_table", ddlEvent.TableInfo.GetTargetTableName())
	require.Equal(t, []SchemaTableName{{
		SchemaName: "target_db",
		TableName:  "target_table",
	}}, ddlEvent.BlockedTableNames)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 'alice')`)
	dmlEvent.TableInfo = routedTableInfo

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	redoRow := (&RedoRowEvent{
		StartTs:         dmlEvent.StartTs,
		CommitTs:        dmlEvent.CommitTs,
		PhysicalTableID: dmlEvent.PhysicalTableID,
		TableInfo:       routedTableInfo,
		Event:           row,
	}).ToRedoLog().RedoRow

	require.Equal(t, "target_db", redoRow.Row.Table.Schema)
	require.Equal(t, "target_table", redoRow.Row.Table.Table)
	require.Empty(t, redoRow.Row.Table.TargetSchema)
	require.Empty(t, redoRow.Row.Table.TargetTable)

	decoded := redoRow.ToDMLEvent()
	require.Equal(t, "target_db", decoded.TableInfo.GetSchemaName())
	require.Equal(t, "target_table", decoded.TableInfo.GetTableName())
	require.Equal(t, "target_db", decoded.TableInfo.GetTargetSchemaName())
	require.Equal(t, "target_table", decoded.TableInfo.GetTargetTableName())
}

func TestRedoDDLEventRoundTripPreservesColumnMetadata(t *testing.T) {
	originalTableInfo := commonType.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID:   1001,
		Name: ast.NewCIStr("t_redo"),
		Columns: []*timodel.ColumnInfo{
			newRedoDDLTestColumn(t, 1, "id", mysql.TypeLonglong, nil, timodel.CurrLatestColumnInfoVersion),
			newRedoDDLTestColumn(t, 2, "status", mysql.TypeVarchar, "ready", timodel.CurrLatestColumnInfoVersion),
			newRedoDDLTestColumn(t, 3, "created_at", mysql.TypeTimestamp, "2024-01-02 03:04:05", timodel.ColumnInfoVersion0),
			newRedoDDLTestColumn(t, 4, "flags", mysql.TypeBit, "1", timodel.CurrLatestColumnInfoVersion),
		},
	})
	originalTableInfo.TableName.IsPartition = true

	ddlEvent := &DDLEvent{
		Type:              byte(timodel.ActionCreateTable),
		Query:             "create table test.t_redo(id bigint, status varchar(16) default 'ready')",
		TableInfo:         originalTableInfo,
		StartTs:           11,
		FinishedTs:        22,
		BlockedTableNames: []SchemaTableName{{SchemaName: "test", TableName: "t_redo"}},
	}

	redoLog := ddlEvent.ToRedoLog()
	data, err := redoLog.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded RedoLog
	left, err := decoded.UnmarshalMsg(data)
	require.NoError(t, err)
	require.Empty(t, left)

	roundTrip := decoded.RedoDDL.ToDDLEvent()
	require.Equal(t, ddlEvent.Query, roundTrip.Query)
	require.Equal(t, ddlEvent.StartTs, roundTrip.StartTs)
	require.Equal(t, ddlEvent.FinishedTs, roundTrip.FinishedTs)
	require.Equal(t, ddlEvent.BlockedTableNames, roundTrip.BlockedTableNames)
	require.Equal(t, originalTableInfo.TableName, roundTrip.TableInfo.TableName)

	originalColumns := originalTableInfo.GetColumns()
	roundTripColumns := roundTrip.TableInfo.GetColumns()
	require.Len(t, roundTripColumns, len(originalColumns))
	for i, originalColumn := range originalColumns {
		roundTripColumn := roundTripColumns[i]
		require.Equal(t, originalColumn.Name.O, roundTripColumn.Name.O)
		require.Equal(t, originalColumn.GetType(), roundTripColumn.GetType())
		require.Equal(t, originalColumn.GetOriginDefaultValue(), roundTripColumn.GetOriginDefaultValue())
		require.Equal(t, originalColumn.Version, roundTripColumn.Version)
	}
}

func TestDDLEventToRedoLogHandlesNilTableInfo(t *testing.T) {
	ddlEvent := &DDLEvent{
		Type:       byte(timodel.ActionCreateSchema),
		Query:      "create database test_redo",
		StartTs:    33,
		FinishedTs: 44,
	}

	require.NotPanics(t, func() {
		redoLog := ddlEvent.ToRedoLog()
		require.Equal(t, ddlEvent.Query, redoLog.RedoDDL.DDL.Query)
		require.Equal(t, ddlEvent.StartTs, redoLog.RedoDDL.DDL.StartTs)
		require.Equal(t, ddlEvent.FinishedTs, redoLog.RedoDDL.DDL.CommitTs)
		require.Nil(t, redoLog.RedoDDL.DDL.Columns)
	})
}

func TestDDLEventToRedoLogHandlesUninitializedTableInfo(t *testing.T) {
	ddlEvent := &DDLEvent{
		Type:       byte(timodel.ActionAddColumn),
		Query:      "alter table test_redo.t add column c int",
		StartTs:    55,
		FinishedTs: 66,
		TableInfo:  &commonType.TableInfo{},
	}

	redoLog := ddlEvent.ToRedoLog()
	require.Equal(t, ddlEvent.Query, redoLog.RedoDDL.DDL.Query)
	require.Equal(t, ddlEvent.StartTs, redoLog.RedoDDL.DDL.StartTs)
	require.Equal(t, ddlEvent.FinishedTs, redoLog.RedoDDL.DDL.CommitTs)
	require.Empty(t, redoLog.RedoDDL.DDL.Columns)
}

func newRedoDDLTestColumn(
	t *testing.T,
	id int64,
	name string,
	tp byte,
	originDefaultValue any,
	version uint64,
) *timodel.ColumnInfo {
	t.Helper()

	fieldType := types.NewFieldType(tp)
	column := &timodel.ColumnInfo{
		ID:        id,
		Offset:    int(id - 1),
		Name:      ast.NewCIStr(name),
		State:     timodel.StatePublic,
		FieldType: *fieldType,
		Version:   version,
	}
	require.NoError(t, column.SetOriginDefaultValue(originDefaultValue))
	return column
}
