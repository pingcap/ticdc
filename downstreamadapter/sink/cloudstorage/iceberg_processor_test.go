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

package cloudstorage

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestFormatIcebergColumnValueKeepsTextPlain(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeBlob)
	ft.SetCharset(charset.CharsetUTF8MB4)

	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	chk.AppendString(0, "hello iceberg")
	row := chk.GetRow(0)

	value, err := formatIcebergColumnValue(&row, 0, ft)
	require.NoError(t, err)
	require.NotNil(t, value)
	require.Equal(t, "hello iceberg", *value)
}

func TestFormatIcebergColumnValueBase64EncodesBinaryBlob(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeBlob)
	ft.SetCharset(charset.CharsetBin)

	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	chk.AppendBytes(0, []byte("hello iceberg"))
	row := chk.GetRow(0)

	value, err := formatIcebergColumnValue(&row, 0, ft)
	require.NoError(t, err)
	require.NotNil(t, value)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("hello iceberg")), *value)
}

func TestConvertToIcebergRowsIncludesExtendedMetadata(t *testing.T) {
	commitTs := oracle.GoTimeToTS(time.Date(2026, 4, 16, 9, 0, 0, 0, time.UTC))

	t.Run("primary key update captures old and new identity", func(t *testing.T) {
		idColumn := &timodel.ColumnInfo{
			ID:        1,
			Name:      ast.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
		}
		idColumn.AddFlag(mysql.PriKeyFlag)
		idColumn.AddFlag(mysql.NotNullFlag)

		tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
			ID:         100,
			Name:       ast.NewCIStr("t_pk"),
			PKIsHandle: true,
			Columns: []*timodel.ColumnInfo{
				idColumn,
				{
					ID:        2,
					Name:      ast.NewCIStr("v"),
					FieldType: *types.NewFieldType(mysql.TypeVarchar),
				},
			},
		})

		rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
		rows.AppendInt64(0, 1)
		rows.AppendString(1, "before")
		rows.AppendInt64(0, 2)
		rows.AppendString(1, "after")

		event := commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, 1, commitTs, tableInfo)
		event.SetRows(rows)
		event.RowTypes = []common.RowType{common.RowTypeUpdate}
		event.Length = 1
		event.TableInfoVersion = 77

		plan, err := buildIcebergColumnPlan(tableInfo)
		require.NoError(t, err)

		converted, err := convertToIcebergRows(event, plan)
		require.NoError(t, err)
		require.Len(t, converted, 1)
		require.Equal(t, "U", converted[0].Op)
		require.Equal(t, "77", converted[0].TableVersion)
		require.Equal(t, "pk", converted[0].IdentityKind)
		require.Equal(t, mustJSONStrings(t, "2"), converted[0].RowIdentity)
		require.NotNil(t, converted[0].OldRowIdentity)
		require.Equal(t, mustJSONStrings(t, "1"), *converted[0].OldRowIdentity)
	})

	t.Run("not null unique key is marked as uk identity", func(t *testing.T) {
		emailColumn := &timodel.ColumnInfo{
			ID:        1,
			Name:      ast.NewCIStr("email"),
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		emailColumn.AddFlag(mysql.NotNullFlag)

		tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
			ID:   101,
			Name: ast.NewCIStr("t_uk"),
			Columns: []*timodel.ColumnInfo{
				emailColumn,
				{
					ID:        2,
					Name:      ast.NewCIStr("v"),
					FieldType: *types.NewFieldType(mysql.TypeVarchar),
				},
			},
			Indices: []*timodel.IndexInfo{
				{
					ID:     1,
					Name:   ast.NewCIStr("uk_email"),
					Unique: true,
					Columns: []*timodel.IndexColumn{
						{Name: ast.NewCIStr("email"), Offset: 0},
					},
				},
			},
		})

		rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
		rows.AppendString(0, "a@example.com")
		rows.AppendString(1, "payload")

		event := commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, 1, commitTs, tableInfo)
		event.SetRows(rows)
		event.RowTypes = []common.RowType{common.RowTypeInsert}
		event.Length = 1
		event.TableInfoVersion = 78

		plan, err := buildIcebergColumnPlan(tableInfo)
		require.NoError(t, err)

		converted, err := convertToIcebergRows(event, plan)
		require.NoError(t, err)
		require.Len(t, converted, 1)
		require.Equal(t, "uk", converted[0].IdentityKind)
		require.Equal(t, mustJSONStrings(t, "a@example.com"), converted[0].RowIdentity)
		require.Nil(t, converted[0].OldRowIdentity)
	})

	t.Run("extra handle uses rowid identity and is not emitted as business column", func(t *testing.T) {
		tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
			ID:   102,
			Name: ast.NewCIStr("t_rowid"),
			Columns: []*timodel.ColumnInfo{
				{
					ID:        timodel.ExtraHandleID,
					Name:      ast.NewCIStr("_tidb_rowid"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
				},
				{
					ID:        2,
					Name:      ast.NewCIStr("v"),
					FieldType: *types.NewFieldType(mysql.TypeVarchar),
				},
			},
		})

		rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
		rows.AppendInt64(0, 42)
		rows.AppendString(1, "payload")

		event := commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, 1, commitTs, tableInfo)
		event.SetRows(rows)
		event.RowTypes = []common.RowType{common.RowTypeDelete}
		event.Length = 1
		event.TableInfoVersion = 79

		plan, err := buildIcebergColumnPlan(tableInfo)
		require.NoError(t, err)

		converted, err := convertToIcebergRows(event, plan)
		require.NoError(t, err)
		require.Len(t, converted, 1)
		require.Equal(t, "rowid", converted[0].IdentityKind)
		require.Equal(t, mustJSONStrings(t, "42"), converted[0].RowIdentity)
		require.NotNil(t, converted[0].OldRowIdentity)
		require.Equal(t, mustJSONStrings(t, "42"), *converted[0].OldRowIdentity)
		require.NotContains(t, converted[0].Columns, "_tidb_rowid")
	})
}

func mustJSONStrings(t *testing.T, values ...string) string {
	t.Helper()
	data, err := json.Marshal(values)
	require.NoError(t, err)
	return string(data)
}
