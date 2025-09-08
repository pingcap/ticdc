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

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// Helper to create a model.ColumnInfo
func newColumnInfo(id int64, name string, tp byte, flag uint) *model.ColumnInfo {
	ft := types.NewFieldType(tp)
	ft.AddFlag(flag)
	return &model.ColumnInfo{
		ID:        id,
		Name:      ast.NewCIStr(name),
		FieldType: *ft,
		State:     model.StatePublic,
		Version:   model.CurrLatestColumnInfoVersion,
	}
}

// Helper to create a model.IndexInfo
func newIndexInfo(name string, cols []*model.IndexColumn, isPrimary, isUnique bool) *model.IndexInfo {
	return &model.IndexInfo{
		Name:    ast.NewCIStr(name),
		Columns: cols,
		Primary: isPrimary,
		Unique:  isUnique,
		State:   model.StatePublic,
	}
}

/*
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
				{Name: ast.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				{Name: ast.CIStr{O: "name", L: "name"}, ID: 2, FieldType: types.FieldType{}},
				{Name: ast.CIStr{O: "age", L: "age"}, ID: 3, FieldType: types.FieldType{}},
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
				{Name: ast.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				{Name: ast.CIStr{O: "name", L: "name"}, ID: 2, FieldType: types.FieldType{}},
				{Name: ast.CIStr{O: "age", L: "age"}, ID: 3, FieldType: types.FieldType{}},
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
				{Name: ast.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				{Name: ast.CIStr{O: "name", L: "name"}, ID: 2, FieldType: types.FieldType{}},
				{Name: ast.CIStr{O: "full_name", L: "full_name"}, ID: 3, FieldType: types.FieldType{}}, // generated column
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
				{Name: ast.CIStr{O: "id", L: "id"}, ID: 1, FieldType: types.FieldType{}},
				nil,
				{Name: ast.CIStr{O: "age", L: "age"}, ID: 3, FieldType: types.FieldType{}},
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
}*/

func newFieldTypeWithFlag(flags ...uint) *types.FieldType {
	ft := &types.FieldType{}
	for _, flag := range flags {
		ft.AddFlag(flag)
	}
	return ft
}

func TestColumnIndex(t *testing.T) {
	columns := []*model.ColumnInfo{
		{ID: 101, Name: ast.NewCIStr("a"), FieldType: *newFieldTypeWithFlag(mysql.PriKeyFlag)},
		{ID: 102, Name: ast.NewCIStr("b"), FieldType: *newFieldTypeWithFlag(mysql.UniqueKeyFlag)},
		{ID: 103, Name: ast.NewCIStr("c"), FieldType: *newFieldTypeWithFlag()},
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
				Name: ast.NewCIStr("idx1"),
				Columns: []*model.IndexColumn{
					{
						Name: ast.NewCIStr("col1"),
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
				Name: ast.NewCIStr("col2"),
				ID:   1,
			},
			{
				Name: ast.NewCIStr("col1"),
				ID:   0,
			},
			{
				Name: ast.NewCIStr("col3"),
				ID:   2,
			},
			{
				Name:                ast.NewCIStr("col4"),
				ID:                  3,
				GeneratedExprString: "generated",
			},
		},
	})

	names := []string{"col1", "col2", "col3"}
	offsets, err := tableInfo.OffsetsByNames(names)
	require.NoError(t, err)
	require.Equal(t, []int{1, 0, 2}, offsets)

	names = []string{"col2"}
	offsets, err = tableInfo.OffsetsByNames(names)
	require.NoError(t, err)
	require.Equal(t, []int{0}, offsets)

	names = []string{"col1", "col-not-found"}
	offsets, err = tableInfo.OffsetsByNames(names)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)
	require.ErrorContains(t, err, "columns not found")
	require.Nil(t, offsets)

	names = []string{"Col1", "COL2", "CoL3"}
	offsets, err = tableInfo.OffsetsByNames(names)
	require.NoError(t, err)
	require.Equal(t, []int{1, 0, 2}, offsets)

	names = []string{"Col4"}
	offsets, err = tableInfo.OffsetsByNames(names)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)
	require.ErrorContains(t, err, "found virtual generated columns")
	require.Nil(t, offsets)
}

func TestHandleKey(t *testing.T) {
	tableInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), Offset: 0},
			{ID: 2, Name: ast.NewCIStr("name"), Offset: 1},
		},
		Indices: []*model.IndexInfo{
			{
				ID: 1,
				Columns: []*model.IndexColumn{
					{Name: ast.NewCIStr("id"), Offset: 0},
					{Name: ast.NewCIStr("name"), Offset: 1},
				},
				Primary: true,
			},
		},
		IsCommonHandle: true,
	}
	columnSchema := newColumnSchema(tableInfo, Digest{})
	for _, colId := range columnSchema.HandleColID {
		_, ok := columnSchema.HandleKeyIDs[colId]
		require.True(t, ok)
	}
	require.Equal(t, len(columnSchema.HandleColID), len(columnSchema.HandleKeyIDs))
}

func TestGetOrSetColumnSchema_SharedSchema(t *testing.T) {
	// Create two tables with the same schema
	// Table 1: CREATE TABLE test1 (id INT PRIMARY KEY, name VARCHAR(255), age INT)

	// Create field types for table1
	idFieldType1 := types.NewFieldType(mysql.TypeLong)
	idFieldType1.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	idFieldType1.SetFlen(11)

	nameFieldType1 := types.NewFieldType(mysql.TypeVarchar)
	nameFieldType1.AddFlag(mysql.NotNullFlag)
	nameFieldType1.SetFlen(255)
	nameFieldType1.SetCharset("utf8mb4")
	nameFieldType1.SetCollate("utf8mb4_bin")

	ageFieldType1 := types.NewFieldType(mysql.TypeLong)
	ageFieldType1.AddFlag(mysql.NotNullFlag)
	ageFieldType1.SetFlen(11)

	tableInfo1 := &model.TableInfo{
		ID:             1,
		Name:           ast.NewCIStr("test1"),
		PKIsHandle:     true,
		IsCommonHandle: false,
		UpdateTS:       1234567890,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				Offset:    0,
				FieldType: *idFieldType1,
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("name"),
				Offset:    1,
				FieldType: *nameFieldType1,
			},
			{
				ID:        3,
				Name:      ast.NewCIStr("age"),
				Offset:    2,
				FieldType: *ageFieldType1,
			},
		},
		Indices: []*model.IndexInfo{
			{
				ID:      1,
				Name:    ast.NewCIStr("PRIMARY"),
				Primary: true,
				Unique:  true,
				Columns: []*model.IndexColumn{
					{
						Name:   ast.NewCIStr("id"),
						Offset: 0,
					},
				},
			},
		},
	}

	// Table 2: CREATE TABLE test2 (id INT PRIMARY KEY, name VARCHAR(255), age INT)
	// Same schema as table1, but different table name and ID

	// Create field types for table2 (same as table1)
	idFieldType2 := types.NewFieldType(mysql.TypeLong)
	idFieldType2.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	idFieldType2.SetFlen(11)

	nameFieldType2 := types.NewFieldType(mysql.TypeVarchar)
	nameFieldType2.AddFlag(mysql.NotNullFlag)
	nameFieldType2.SetFlen(255)
	nameFieldType2.SetCharset("utf8mb4")
	nameFieldType2.SetCollate("utf8mb4_bin")

	ageFieldType2 := types.NewFieldType(mysql.TypeLong)
	ageFieldType2.AddFlag(mysql.NotNullFlag)
	ageFieldType2.SetFlen(11)

	tableInfo2 := &model.TableInfo{
		ID:             2,
		Name:           ast.NewCIStr("test2"),
		PKIsHandle:     true,
		IsCommonHandle: false,
		UpdateTS:       1234567890,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				Offset:    0,
				FieldType: *idFieldType2,
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("name"),
				Offset:    1,
				FieldType: *nameFieldType2,
			},
			{
				ID:        3,
				Name:      ast.NewCIStr("age"),
				Offset:    2,
				FieldType: *ageFieldType2,
			},
		},
		Indices: []*model.IndexInfo{
			{
				ID:      1,
				Name:    ast.NewCIStr("PRIMARY"),
				Primary: true,
				Unique:  true,
				Columns: []*model.IndexColumn{
					{
						Name:   ast.NewCIStr("id"),
						Offset: 0,
					},
				},
			},
		},
	}

	// Get shared column schema storage
	storage := GetSharedColumnSchemaStorage()

	// Get column schema for both tables
	columnSchema1 := storage.GetOrSetColumnSchema(tableInfo1)
	columnSchema2 := storage.GetOrSetColumnSchema(tableInfo2)

	// Verify that both tables share the same columnSchema object
	require.Equal(t, columnSchema1, columnSchema2, "Tables with same schema should share the same columnSchema object")

	// Verify that the digest is the same
	require.Equal(t, columnSchema1.Digest, columnSchema2.Digest, "Digest should be the same for tables with same schema")

	// Verify that the column information is correct
	require.Equal(t, 3, len(columnSchema1.Columns), "Should have 3 columns")
	require.Equal(t, "id", columnSchema1.Columns[0].Name.O, "First column should be 'id'")
	require.Equal(t, "name", columnSchema1.Columns[1].Name.O, "Second column should be 'name'")
	require.Equal(t, "age", columnSchema1.Columns[2].Name.O, "Third column should be 'age'")

	// Verify that the index information is correct
	require.Equal(t, 1, len(columnSchema1.Indices), "Should have 1 index")
	require.Equal(t, "PRIMARY", columnSchema1.Indices[0].Name.O, "Index should be PRIMARY")

	// Verify that the handle key information is correct
	require.Equal(t, 1, len(columnSchema1.HandleKeyIDs), "Should have 1 handle key")
	_, exists := columnSchema1.HandleKeyIDs[1] // column ID for 'id'
	require.True(t, exists, "Column 'id' should be a handle key")

	// Test with a different schema to ensure it creates a new columnSchema
	// Table 3: CREATE TABLE test3 (id INT, name VARCHAR(255)) - different schema

	// Create field types for table3 (different schema - no primary key)
	idFieldType3 := types.NewFieldType(mysql.TypeLong)
	idFieldType3.AddFlag(mysql.NotNullFlag)
	idFieldType3.SetFlen(11)

	nameFieldType3 := types.NewFieldType(mysql.TypeVarchar)
	nameFieldType3.AddFlag(mysql.NotNullFlag)
	nameFieldType3.SetFlen(255)
	nameFieldType3.SetCharset("utf8mb4")
	nameFieldType3.SetCollate("utf8mb4_bin")

	tableInfo3 := &model.TableInfo{
		ID:             3,
		Name:           ast.NewCIStr("test3"),
		PKIsHandle:     false,
		IsCommonHandle: false,
		UpdateTS:       1234567890,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				Offset:    0,
				FieldType: *idFieldType3,
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("name"),
				Offset:    1,
				FieldType: *nameFieldType3,
			},
		},
		Indices: []*model.IndexInfo{},
	}

	columnSchema3 := storage.GetOrSetColumnSchema(tableInfo3)

	// Verify that different schema creates a different columnSchema object
	require.NotEqual(t, columnSchema1, columnSchema3, "Tables with different schema should have different columnSchema objects")
	require.NotEqual(t, columnSchema1.Digest, columnSchema3.Digest, "Digest should be different for tables with different schema")
}

func TestIsEligible(t *testing.T) {
	t.Parallel()

	// 1. Table with PK
	tiWithPK := WrapTableInfo("test", &model.TableInfo{
		Name: ast.NewCIStr("t1"),
		Columns: []*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag),
		},
		PKIsHandle: true,
	})

	// 2. Table with UK on not-null column
	tiWithUK := WrapTableInfo("test", &model.TableInfo{
		Name: ast.NewCIStr("t2"),
		Columns: []*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, mysql.NotNullFlag),
		},
		Indices: []*model.IndexInfo{
			newIndexInfo("uk_id", []*model.IndexColumn{{Name: ast.NewCIStr("id"), Offset: 0}}, false, true),
		},
	})

	// 3. Table with UK on nullable column (ineligible)
	tiWithNullableUK := WrapTableInfo("test", &model.TableInfo{
		Name: ast.NewCIStr("t3"),
		Columns: []*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, 0),
		},
		Indices: []*model.IndexInfo{
			newIndexInfo("uk_id", []*model.IndexColumn{{Name: ast.NewCIStr("id"), Offset: 0}}, false, true),
		},
	})

	// 4. Table with no PK or UK (ineligible)
	tiNoKey := WrapTableInfo("test", &model.TableInfo{
		Name: ast.NewCIStr("t4"),
		Columns: []*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, 0),
		},
	})

	// 5. View (eligible)
	tiView := WrapTableInfo("test", &model.TableInfo{
		Name: ast.NewCIStr("v1"),
	})
	tiView.View = &model.ViewInfo{}

	// 6. Sequence (ineligible)
	tiSeq := WrapTableInfo("test", &model.TableInfo{
		Name: ast.NewCIStr("s1"),
	})
	tiSeq.Sequence = &model.SequenceInfo{}

	require.True(t, tiWithPK.IsEligible(false))
	require.True(t, tiWithUK.IsEligible(false))
	require.False(t, tiWithNullableUK.IsEligible(false))
	require.False(t, tiNoKey.IsEligible(false))
	require.True(t, tiView.IsEligible(false))
	require.False(t, tiSeq.IsEligible(false))

	// test with forceReplicate = true
	require.True(t, tiWithPK.IsEligible(true))
	require.True(t, tiWithUK.IsEligible(true))
	require.True(t, tiWithNullableUK.IsEligible(true))
	require.True(t, tiNoKey.IsEligible(true))
	require.True(t, tiView.IsEligible(true))
	require.False(t, tiSeq.IsEligible(true), "Sequence should always be ineligible")
}
