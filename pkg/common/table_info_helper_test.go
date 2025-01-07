package common

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestColumnSchema_GetColumnList(t *testing.T) {
	tests := []struct {
		name           string
		columns        []*model.ColumnInfo
		columnsFlag    map[int64]*ColumnFlagType
		isUpdate       bool
		wantCount      int
		wantColumnList string
	}{
		{
			name: "normal columns without update",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1},
				{Name: pmodel.CIStr{O: "name", L: "name"}, ID: 2},
				{Name: pmodel.CIStr{O: "age", L: "age"}, ID: 3},
			},
			columnsFlag: map[int64]*ColumnFlagType{
				1: NewColumnFlagType(PrimaryKeyFlag),
				2: NewColumnFlagType(UniqueKeyFlag),
				3: NewColumnFlagType(NullableFlag),
			},
			isUpdate:       false,
			wantCount:      3,
			wantColumnList: "`id`,`name`,`age`",
		},
		{
			name: "normal columns with update",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1},
				{Name: pmodel.CIStr{O: "name", L: "name"}, ID: 2},
				{Name: pmodel.CIStr{O: "age", L: "age"}, ID: 3},
			},
			columnsFlag: map[int64]*ColumnFlagType{
				1: NewColumnFlagType(PrimaryKeyFlag),
				2: NewColumnFlagType(UniqueKeyFlag),
				3: NewColumnFlagType(NullableFlag),
			},
			isUpdate:       true,
			wantCount:      3,
			wantColumnList: "`id` = ?,`name` = ?,`age` = ?",
		},
		{
			name: "with generated columns",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1},
				{Name: pmodel.CIStr{O: "name", L: "name"}, ID: 2},
				{Name: pmodel.CIStr{O: "full_name", L: "full_name"}, ID: 3}, // generated column
			},
			columnsFlag: map[int64]*ColumnFlagType{
				1: NewColumnFlagType(PrimaryKeyFlag),
				2: NewColumnFlagType(UniqueKeyFlag),
				3: NewColumnFlagType(GeneratedColumnFlag),
			},
			isUpdate:       false,
			wantCount:      2,
			wantColumnList: "`id`,`name`",
		},
		{
			name: "with nil column",
			columns: []*model.ColumnInfo{
				{Name: pmodel.CIStr{O: "id", L: "id"}, ID: 1},
				nil,
				{Name: pmodel.CIStr{O: "age", L: "age"}, ID: 3},
			},
			columnsFlag: map[int64]*ColumnFlagType{
				1: NewColumnFlagType(PrimaryKeyFlag),
				3: NewColumnFlagType(NullableFlag),
			},
			isUpdate:       false,
			wantCount:      2,
			wantColumnList: "`id`,`age`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &columnSchema{
				Columns:     tt.columns,
				ColumnsFlag: tt.columnsFlag,
			}
			gotCount, gotColumnList := s.getColumnList(tt.isUpdate)
			require.Equal(t, tt.wantCount, gotCount)
			require.Equal(t, tt.wantColumnList, gotColumnList)
		})
	}
}
