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
	"encoding/binary"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

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
