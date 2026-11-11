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

package causality

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderTableKeys(t *testing.T) {
	pk := types.NewFieldType(mysql.TypeLong)
	pk.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	ti := common.WrapTableInfo("test", &timodel.TableInfo{
		ID: 100, Name: ast.NewCIStr("ordered"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic, FieldType: *pk}},
	})
	makeEvent := func(id int64) *commonEvent.DMLEvent {
		e := commonEvent.NewDMLEvent(common.DispatcherID{Low: 1}, 100, 1, 2, ti)
		e.SetRows(chunk.NewChunkWithCapacity(ti.GetFieldSlice(), 1))
		e.Rows.AppendInt64(0, id)
		e.RowTypes = []common.RowType{common.RowTypeInsert}
		e.Length = 1
		return e
	}
	first, second := makeEvent(1), makeEvent(2)
	require.NotEqual(t, ConflictKeys(first, nil), ConflictKeys(second, nil))
	f, err := config.ParseTxnOrderTables([]string{"test.ordered"}, false)
	require.NoError(t, err)
	want := map[uint64]struct{}{uint64(100) | (1 << 63): {}}
	require.Equal(t, want, ConflictKeys(first, f))
	require.Equal(t, want, ConflictKeys(second, f))
	second.PhysicalTableID = 101
	require.NotEqual(t, want, ConflictKeys(second, f))
}

func TestGenKeyListUsesSchemaIndexWithVirtualGeneratedColumn(t *testing.T) {
	t.Parallel()

	idFieldType := types.NewFieldType(mysql.TypeLong)
	idFieldType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:         100,
		Name:       ast.NewCIStr("t"),
		PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Offset:    0,
				Name:      ast.NewCIStr("a"),
				State:     timodel.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:                  2,
				Offset:              1,
				Name:                ast.NewCIStr("v"),
				State:               timodel.StatePublic,
				GeneratedExprString: "a + 1",
				FieldType:           *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        3,
				Offset:    2,
				Name:      ast.NewCIStr("b"),
				State:     timodel.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        4,
				Offset:    3,
				Name:      ast.NewCIStr("id"),
				State:     timodel.StatePublic,
				FieldType: *idFieldType,
			},
		},
	})
	require.NotNil(t, tableInfo)

	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	chk.AppendInt64(0, 1)
	chk.AppendNull(1) // virtual generated column
	chk.AppendNull(2) // b = NULL
	chk.AppendInt64(3, 1)
	row := chk.GetRow(0)

	keys := tableInfo.GetIndexColumns()
	require.Len(t, keys, 1)

	dispatcherID := common.DispatcherID{Low: 1, High: 2}
	key := genKeyList(&row, 0, keys[0], dispatcherID, tableInfo)
	require.NotNil(t, key)
	require.True(t, len(key) > 0)
	require.Equal(t, []byte("1\x00"), key[:2])
}

func TestGenRowKeysStableAcrossUpdateAndDeleteWithVirtualGeneratedColumn(t *testing.T) {
	t.Parallel()

	idFieldType := types.NewFieldType(mysql.TypeLong)
	idFieldType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:         100,
		Name:       ast.NewCIStr("t"),
		PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Offset:    0,
				Name:      ast.NewCIStr("a"),
				State:     timodel.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:                  2,
				Offset:              1,
				Name:                ast.NewCIStr("v"),
				State:               timodel.StatePublic,
				GeneratedExprString: "a + 1",
				FieldType:           *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        3,
				Offset:    2,
				Name:      ast.NewCIStr("b"),
				State:     timodel.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        4,
				Offset:    3,
				Name:      ast.NewCIStr("id"),
				State:     timodel.StatePublic,
				FieldType: *idFieldType,
			},
		},
	})
	require.NotNil(t, tableInfo)

	dispatcherID := common.DispatcherID{Low: 1, High: 2}

	// Update: (b=1,id=1) -> (b=NULL,id=1)
	updateChunk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	updateChunk.AppendInt64(0, 1)
	updateChunk.AppendNull(1)
	updateChunk.AppendInt64(2, 1)
	updateChunk.AppendInt64(3, 1)
	updateChunk.AppendInt64(0, 1)
	updateChunk.AppendNull(1)
	updateChunk.AppendNull(2)
	updateChunk.AppendInt64(3, 1)
	update := commonEvent.RowChange{
		PreRow:  updateChunk.GetRow(0),
		Row:     updateChunk.GetRow(1),
		RowType: common.RowTypeUpdate,
	}

	// Delete: delete the updated row (b=NULL,id=1)
	delChunk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	delChunk.AppendInt64(0, 1)
	delChunk.AppendNull(1)
	delChunk.AppendNull(2)
	delChunk.AppendInt64(3, 1)
	del := commonEvent.RowChange{
		PreRow:  delChunk.GetRow(0),
		RowType: common.RowTypeDelete,
	}

	updateKeys := genRowKeys(update, tableInfo, dispatcherID)
	deleteKeys := genRowKeys(del, tableInfo, dispatcherID)
	require.NotEmpty(t, updateKeys)
	require.NotEmpty(t, deleteKeys)

	set := make(map[string]struct{}, len(updateKeys))
	for _, k := range updateKeys {
		set[string(k)] = struct{}{}
	}

	found := false
	for _, k := range deleteKeys {
		if _, ok := set[string(k)]; ok {
			found = true
			break
		}
	}
	require.True(t, found)
}
