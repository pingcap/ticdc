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

	"github.com/pingcap/tidb/pkg/meta/model"
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
	require.Equal(t, "target_db", ddlEvent.GetTargetSchemaName())
	require.Equal(t, "target_table", ddlEvent.GetTargetTableName())
	require.Equal(t, "target_db", ddlEvent.TableInfo.GetSchemaName())
	require.Equal(t, "target_table", ddlEvent.TableInfo.GetTableName())
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
