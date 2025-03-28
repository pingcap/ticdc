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
