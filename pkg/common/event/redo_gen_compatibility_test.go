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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	parserModel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	oldArchModel "github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestDMLCompatibility(t *testing.T) {
	tableInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{
			&model.ColumnInfo{
				Name:                parserModel.NewCIStr("c1"),
				Offset:              0,
				GeneratedExprString: "xxx",
				FieldType:           *types.NewFieldType(mysql.TypeString),
			},
			&model.ColumnInfo{
				Name:                parserModel.NewCIStr("c2"),
				Offset:              1,
				GeneratedExprString: "",
				FieldType:           *types.NewFieldType(mysql.TypeString),
			},
		},
	}

	rowEvent := &DMLEvent{
		StartTs:   100,
		CommitTs:  200,
		TableInfo: common.WrapTableInfo("test", tableInfo),
		Rows: chunk.NewEmptyChunk([]*types.FieldType{
			&tableInfo.Columns[0].FieldType,
			&tableInfo.Columns[1].FieldType,
		}),
		RowTypes: []RowType{RowTypeUpdate},
	}
	rowEvent.Rows.AppendString(0, "")
	rowEvent.Rows.AppendString(1, "hahaha")

	redoLog := rowEvent.ToRedoLog()
	marshaled, err := redoLog.MarshalMsg(nil)
	require.Nil(t, err)

	redoLogOld := new(oldArchModel.RedoLog)
	_, err = redoLogOld.UnmarshalMsg(marshaled)
	require.Nil(t, err)

	marshaled, err = redoLogOld.MarshalMsg(nil)
	require.Nil(t, err)

	redoLog = new(RedoLog)
	_, err = redoLog.UnmarshalMsg(marshaled)
	require.Nil(t, err)
}
