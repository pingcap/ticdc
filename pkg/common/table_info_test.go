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

package common

import (
	"encoding/binary"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCloneWithRouting(t *testing.T) {
	t.Parallel()

	t.Run("nil TableInfo", func(t *testing.T) {
		var ti *TableInfo
		cloned := ti.CloneWithRouting("target_schema", "target_table")
		require.Nil(t, cloned)
	})

	t.Run("basic cloning with routing", func(t *testing.T) {
		original := &TableInfo{
			TableName: TableName{
				Schema:  "source_db",
				Table:   "source_table",
				TableID: 123,
			},
			Charset:          "utf8mb4",
			Collate:          "utf8mb4_bin",
			Comment:          "test table",
			HasPKOrNotNullUK: true,
			UpdateTS:         1000,
		}

		cloned := original.CloneWithRouting("target_db", "target_table")

		// Verify cloned has routing applied
		require.Equal(t, "source_db", cloned.TableName.Schema)
		require.Equal(t, "source_table", cloned.TableName.Table)
		require.Equal(t, "target_db", cloned.TableName.TargetSchema)
		require.Equal(t, "target_table", cloned.TableName.TargetTable)
		require.Equal(t, int64(123), cloned.TableName.TableID)

		// Verify other fields are copied
		require.Equal(t, "utf8mb4", cloned.Charset)
		require.Equal(t, "utf8mb4_bin", cloned.Collate)
		require.Equal(t, "test table", cloned.Comment)
		require.Equal(t, true, cloned.HasPKOrNotNullUK)
		require.Equal(t, uint64(1000), cloned.UpdateTS)

		// Verify original is NOT modified
		require.Equal(t, "", original.TableName.TargetSchema)
		require.Equal(t, "", original.TableName.TargetTable)
	})

	t.Run("cloning does not affect original preSQLs", func(t *testing.T) {
		original := &TableInfo{
			TableName: TableName{
				Schema:  "source_db",
				Table:   "source_table",
				TableID: 123,
			},
		}

		// Clone with different routing
		cloned1 := original.CloneWithRouting("db1", "table1")
		cloned2 := original.CloneWithRouting("db2", "table2")

		// Each clone should have its own routing
		require.Equal(t, "db1", cloned1.TableName.TargetSchema)
		require.Equal(t, "table1", cloned1.TableName.TargetTable)
		require.Equal(t, "db2", cloned2.TableName.TargetSchema)
		require.Equal(t, "table2", cloned2.TableName.TargetTable)

		// Original should be unchanged
		require.Equal(t, "", original.TableName.TargetSchema)
		require.Equal(t, "", original.TableName.TargetTable)

		// Clones should be independent
		require.NotSame(t, cloned1, cloned2)
	})
}

func TestUnmarshalJSONToTableInfoInvalidData(t *testing.T) {
	t.Parallel()

	sizeTooLargeData := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeTooLargeData, 1)

	tests := []struct {
		name        string
		data        []byte
		errContains string
	}{
		{
			name:        "nil data",
			data:        nil,
			errContains: "too short",
		},
		{
			name:        "data shorter than size footer",
			data:        make([]byte, 7),
			errContains: "too short",
		},
		{
			name:        "column schema size exceeds payload",
			data:        sizeTooLargeData,
			errContains: "exceeds payload length",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalJSONToTableInfo(tc.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestUnmarshalJSONToTableInfoRoundTrip(t *testing.T) {
	t.Parallel()

	idCol := &model.ColumnInfo{
		ID:      1,
		Name:    ast.NewCIStr("id"),
		Offset:  0,
		State:   model.StatePublic,
		Version: model.CurrLatestColumnInfoVersion,
	}
	idCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	idCol.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)

	nameCol := &model.ColumnInfo{
		ID:      2,
		Name:    ast.NewCIStr("name"),
		Offset:  1,
		State:   model.StatePublic,
		Version: model.CurrLatestColumnInfoVersion,
	}
	nameCol.FieldType = *types.NewFieldType(mysql.TypeVarchar)

	source := WrapTableInfo("test", &model.TableInfo{
		ID:         1001,
		Name:       ast.NewCIStr("t_roundtrip"),
		PKIsHandle: true,
		Columns:    []*model.ColumnInfo{idCol, nameCol},
	})
	require.NotNil(t, source)

	data, err := source.Marshal()
	require.NoError(t, err)

	decoded, err := UnmarshalJSONToTableInfo(data)
	require.NoError(t, err)
	require.NotNil(t, decoded)

	require.Equal(t, source.TableName.Schema, decoded.TableName.Schema)
	require.Equal(t, source.TableName.Table, decoded.TableName.Table)
	require.Equal(t, source.TableName.TableID, decoded.TableName.TableID)
	require.Equal(t, len(source.GetColumns()), len(decoded.GetColumns()))
	require.Equal(t, source.GetColumns()[0].Name.O, decoded.GetColumns()[0].Name.O)
	require.Equal(t, source.GetColumns()[1].Name.O, decoded.GetColumns()[1].Name.O)
}

func TestUnquoteName(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "unquoted",
			input:    "p0",
			expected: "p0",
		},
		{
			name:     "quoted",
			input:    "`p0`",
			expected: "p0",
		},
		{
			name:     "quoted with escaped backtick",
			input:    "`p``0`",
			expected: "p`0",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, UnquoteName(tc.input))
		})
	}
}
