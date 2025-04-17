// Copyright 2025 PingCAP, Inc.
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

package common

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestColumnSchema_GetColumnList(t *testing.T) {
	tests := []struct {
		name           string
		columns        []*model.ColumnInfo
		columnsFlag    map[int]uint
		isUpdate       bool
		wantCount      int
		wantColumnList string
	}{
		{
			name: "normal columns without update",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				{Name: pmodel.CIStr{O: "name", L: "name"}, ID: 2, FieldType: types.FieldType{}},
				{Name: pmodel.CIStr{O: "age", L: "age"}, ID: 3, FieldType: types.FieldType{}},
			},
			columnsFlag: map[int]uint{
				1: mysql.PriKeyFlag,
				2: mysql.UniqueKeyFlag,
				3: mysql.NotNullFlag,
			},
			isUpdate:       false,
			wantCount:      3,
			wantColumnList: "`id`,`name`,`age`",
		},
		{
			name: "normal columns with update",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				{Name: pmodel.CIStr{O: "name", L: "name"}, ID: 2, FieldType: types.FieldType{}},
				{Name: pmodel.CIStr{O: "age", L: "age"}, ID: 3, FieldType: types.FieldType{}},
			},
			columnsFlag: map[int]uint{
				1: mysql.PriKeyFlag,
				2: mysql.UniqueKeyFlag,
				3: mysql.NotNullFlag,
			},
			isUpdate:       true,
			wantCount:      3,
			wantColumnList: "`id` = ?,`name` = ?,`age` = ?",
		},
		{
			name: "with generated columns",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				{Name: pmodel.CIStr{O: "name", L: "name"}, ID: 2, FieldType: types.FieldType{}},
				{Name: pmodel.CIStr{O: "full_name", L: "full_name"}, ID: 3, FieldType: types.FieldType{}}, // generated column
			},
			columnsFlag: map[int]uint{
				1: mysql.PriKeyFlag,
				2: mysql.UniqueKeyFlag,
				3: mysql.GeneratedColumnFlag,
			},
			isUpdate:       false,
			wantCount:      2,
			wantColumnList: "`id`,`name`",
		},
		{
			name: "with nil column",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				nil,
				{Name: pmodel.CIStr{O: "age", L: "age"}, ID: 3, FieldType: types.FieldType{}},
			},
			columnsFlag: map[int]uint{
				1: mysql.PriKeyFlag,
				2: 0,
				3: mysql.NotNullFlag,
			},
			isUpdate:       false,
			wantCount:      2,
			wantColumnList: "`id`,`age`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for idx, col := range tt.columns {
				if col != nil {
					col.SetFlag(tt.columnsFlag[idx+1])
				}
			}
			s := &columnSchema{
				Columns: tt.columns,
			}
			gotCount, gotColumnList := s.getColumnList(tt.isUpdate)
			require.Equal(t, tt.wantCount, gotCount)
			require.Equal(t, tt.wantColumnList, gotColumnList)
		})
	}
}

func newFieldTypeWithFlag(flags ...uint) *types.FieldType {
	ft := &types.FieldType{}
	for _, flag := range flags {
		ft.AddFlag(flag)
	}
	return ft
}

func TestColumnIndex(t *testing.T) {
	columns := []*model.ColumnInfo{
		{ID: 101, Name: pmodel.NewCIStr("a"), FieldType: *newFieldTypeWithFlag(mysql.PriKeyFlag)},
		{ID: 102, Name: pmodel.NewCIStr("b"), FieldType: *newFieldTypeWithFlag(mysql.UniqueKeyFlag)},
		{ID: 103, Name: pmodel.NewCIStr("c"), FieldType: *newFieldTypeWithFlag()},
	}
	tableInfo := WrapTableInfo("test", &model.TableInfo{
		PKIsHandle: true,
		Columns:    columns,
	})
	require.Equal(t, tableInfo.GetIndexColumns(), [][]int64{{101}})
	require.Equal(t, tableInfo.GetPKIndex(), []int64{101})

	indices := []*model.IndexInfo{
		{ID: 1, Primary: true, Columns: []*model.IndexColumn{{Offset: 0}}},
		{ID: 2, Unique: true, Columns: []*model.IndexColumn{{Offset: 1}, {Offset: 2}}},
	}
	tableInfo = WrapTableInfo("test", &model.TableInfo{
		Columns: columns,
		Indices: indices,
	})
	require.Equal(t, tableInfo.GetIndexColumns(), [][]int64{{101}, {102, 103}})
	require.Equal(t, tableInfo.GetPKIndex(), []int64{101})
}

func TestIndexByName(t *testing.T) {
	tableInfo := WrapTableInfo("test", &model.TableInfo{
		Indices: nil,
	})
	names, offsets, ok := tableInfo.IndexByName("idx1")
	require.False(t, ok)
	require.Nil(t, names)
	require.Nil(t, offsets)

	tableInfo = WrapTableInfo("test", &model.TableInfo{
		Indices: []*model.IndexInfo{
			{
				Name: pmodel.NewCIStr("idx1"),
				Columns: []*model.IndexColumn{
					{
						Name: pmodel.NewCIStr("col1"),
					},
				},
			},
		},
	})

	names, offsets, ok = tableInfo.IndexByName("idx2")
	require.False(t, ok)
	require.Nil(t, names)
	require.Nil(t, offsets)

	names, offsets, ok = tableInfo.IndexByName("idx1")
	require.True(t, ok)
	require.Equal(t, []string{"col1"}, names)
	require.Equal(t, []int{0}, offsets)

	names, offsets, ok = tableInfo.IndexByName("IDX1")
	require.True(t, ok)
	require.Equal(t, []string{"col1"}, names)
	require.Equal(t, []int{0}, offsets)

	names, offsets, ok = tableInfo.IndexByName("Idx1")
	require.True(t, ok)
	require.Equal(t, []string{"col1"}, names)
	require.Equal(t, []int{0}, offsets)
}

func TestColumnsByNames(t *testing.T) {
	tableInfo := WrapTableInfo("test", &model.TableInfo{
		Columns: []*model.ColumnInfo{
			{
				Name: pmodel.NewCIStr("col2"),
				ID:   1,
			},
			{
				Name: pmodel.NewCIStr("col1"),
				ID:   0,
			},
			{
				Name: pmodel.NewCIStr("col3"),
				ID:   2,
			},
		},
	})

	names := []string{"col1", "col2", "col3"}
	offsets, ok := tableInfo.OffsetsByNames(names)
	require.True(t, ok)
	require.Equal(t, []int{1, 0, 2}, offsets)

	names = []string{"col2"}
	offsets, ok = tableInfo.OffsetsByNames(names)
	require.True(t, ok)
	require.Equal(t, []int{0}, offsets)

	names = []string{"col1", "col-not-found"}
	offsets, ok = tableInfo.OffsetsByNames(names)
	require.False(t, ok)
	require.Nil(t, offsets)

	names = []string{"Col1", "COL2", "CoL3"}
	offsets, ok = tableInfo.OffsetsByNames(names)
	require.True(t, ok)
	require.Equal(t, []int{1, 0, 2}, offsets)
}
