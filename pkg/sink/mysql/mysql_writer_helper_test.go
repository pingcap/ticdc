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

package mysql

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestGenKeyListUsesNotNullUniqueIndex(t *testing.T) {
	t.Parallel()

	columns := []*model.ColumnInfo{
		buildTestColumn(101, "a", 0, mysql.TypeLong, mysql.NotNullFlag),
		buildTestColumn(102, "b", 1, mysql.TypeLong, mysql.NotNullFlag),
	}
	table := &model.TableInfo{
		ID:      100,
		Name:    ast.NewCIStr("t"),
		Columns: columns,
		Indices: []*model.IndexInfo{
			{
				Name:   ast.NewCIStr("uk_ab"),
				Unique: true,
				State:  model.StatePublic,
				Columns: []*model.IndexColumn{
					{Name: ast.NewCIStr("a"), Offset: 0},
					{Name: ast.NewCIStr("b"), Offset: 1},
				},
			},
		},
	}
	tableInfo := common.WrapTableInfo("test", table)

	row := chunk.MutRowFromValues(int64(1), int64(2)).ToRow()

	key := genKeyList(&row, tableInfo)
	require.Equal(t, []byte("1\x002\x00"), key)
}

func buildTestColumn(id int64, name string, offset int, tp byte, flags ...uint) *model.ColumnInfo {
	ft := types.NewFieldType(tp)
	for _, flag := range flags {
		ft.AddFlag(flag)
	}
	return &model.ColumnInfo{
		ID:        id,
		Name:      ast.NewCIStr(name),
		Offset:    offset,
		State:     model.StatePublic,
		FieldType: *ft,
	}
}
