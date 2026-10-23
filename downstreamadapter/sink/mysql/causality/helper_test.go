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
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/stretchr/testify/require"
)

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
	indexColumns := []*timodel.IndexColumn{{Offset: tableInfo.MustGetColumnOffsetByID(keys[0][0]), Length: types.UnspecifiedLength}}
	key := genKeyList(&row, 0, indexColumns, dispatcherID, tableInfo)
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

func TestConflictKeysSecondaryUniqueIndexes(t *testing.T) {
	// CDC disables the global switch for handle decoding. Conflict detection
	// must still honor the collation explicitly, without changing that switch.
	previous := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(previous)

	testCases := []struct {
		name         string
		collation    string
		values       [2]string
		prefix       int
		composite    bool
		suffixes     [2]int64
		generated    bool
		stored       bool
		unsupported  bool
		null         bool
		primaryIndex bool
		nonClustered bool
		wantConflict bool
	}{
		{name: "accent", collation: "utf8mb4_general_ci", values: [2]string{"e", "é"}, wantConflict: true},
		{name: "case insensitive", collation: "utf8mb4_general_ci", values: [2]string{"A", "a"}, wantConflict: true},
		{name: "pad space", collation: "utf8mb4_bin", values: [2]string{"key", "key "}, wantConflict: true},
		{name: "case sensitive", collation: "utf8mb4_bin", values: [2]string{"A", "a"}},
		{name: "no pad", collation: "utf8mb4_0900_bin", values: [2]string{"key", "key "}},
		{name: "prefix", collation: "utf8mb4_bin", values: [2]string{"same-a", "same-b"}, prefix: 4, wantConflict: true},
		{name: "unicode prefix", collation: "utf8mb4_bin", values: [2]string{"你好-a", "你好-b"}, prefix: 2, wantConflict: true},
		{name: "different unicode prefixes", collation: "utf8mb4_bin", values: [2]string{"你-a", "好-b"}, prefix: 1},
		{name: "prefix and accent", collation: "utf8mb4_general_ci", values: [2]string{"e-a", "é-b"}, prefix: 1, wantConflict: true},
		{name: "composite prefix", collation: "utf8mb4_bin", values: [2]string{"same-a", "same-b"}, prefix: 4, composite: true, wantConflict: true},
		{name: "different composite suffixes", collation: "utf8mb4_bin", values: [2]string{"same-a", "same-b"}, prefix: 4, composite: true, suffixes: [2]int64{1, 2}},
		{name: "binary prefix", collation: "binary", values: [2]string{"\xc3\xa9-a", "\xc3\xaa-b"}, prefix: 1, wantConflict: true},
		{name: "binary preserves space", collation: "binary", values: [2]string{"key", "key "}},
		{name: "nullable unique key", collation: "utf8mb4_bin", null: true},
		{name: "virtual generated", collation: "utf8mb4_bin", values: [2]string{"a", "b"}, generated: true, wantConflict: true},
		{name: "stored generated", collation: "utf8mb4_bin", values: [2]string{"a", "b"}, generated: true, stored: true, wantConflict: true},
		{name: "different stored values", collation: "utf8mb4_bin", values: [2]string{"a", "bb"}, generated: true, stored: true},
		{name: "unsupported collation", collation: "latin1_swedish_ci", values: [2]string{"A", "a"}, unsupported: true, wantConflict: true},
		{name: "integer handle with primary index metadata", collation: "utf8mb4_bin", values: [2]string{"a", "b"}, primaryIndex: true},
		{name: "nonclustered primary index", collation: "utf8mb4_bin", values: [2]string{"a", "b"}, primaryIndex: true, nonClustered: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			idType := types.NewFieldType(mysql.TypeLong)
			idType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
			uType := types.NewFieldType(mysql.TypeVarchar)
			uType.SetCharset("utf8mb4")
			if tc.collation == "binary" {
				uType.SetCharset("binary")
			}
			uType.SetCollate(tc.collation)
			columns := []*timodel.ColumnInfo{
				{ID: 1, Offset: 0, Name: ast.NewCIStr("id"), State: timodel.StatePublic, FieldType: *idType},
				{ID: 2, Offset: 1, Name: ast.NewCIStr("u"), State: timodel.StatePublic, FieldType: *uType},
			}
			idxCol := &timodel.IndexColumn{Name: ast.NewCIStr("u"), Offset: 1, Length: types.UnspecifiedLength}
			if tc.prefix > 0 {
				idxCol.Length = tc.prefix
			}
			if tc.generated {
				columns = append(columns, &timodel.ColumnInfo{
					ID: 3, Offset: 2, Name: ast.NewCIStr("g"), State: timodel.StatePublic,
					FieldType: *types.NewFieldType(mysql.TypeLong), GeneratedExprString: "length(u)", GeneratedStored: tc.stored,
				})
				idxCol = &timodel.IndexColumn{Name: ast.NewCIStr("g"), Offset: 2, Length: types.UnspecifiedLength}
			}
			indexInfo := &timodel.IndexInfo{
				ID: 2, Name: ast.NewCIStr("uk"), Unique: true, State: timodel.StatePublic,
				Columns: []*timodel.IndexColumn{idxCol},
			}
			if tc.composite {
				columns = append(columns, &timodel.ColumnInfo{
					ID: 3, Offset: 2, Name: ast.NewCIStr("v"), State: timodel.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLong),
				})
				indexInfo.Columns = append(indexInfo.Columns, &timodel.IndexColumn{Name: ast.NewCIStr("v"), Offset: 2, Length: types.UnspecifiedLength})
			}
			// A non-unique index before the UK must neither affect its identity
			// nor introduce dependencies for values that do not conflict.
			info := &timodel.TableInfo{
				ID: 100, Name: ast.NewCIStr("t"), PKIsHandle: !tc.nonClustered, Columns: columns,
				Indices: []*timodel.IndexInfo{
					{ID: 1, Name: ast.NewCIStr("idx"), State: timodel.StatePublic, Columns: []*timodel.IndexColumn{idxCol}},
					indexInfo,
				},
			}
			if tc.primaryIndex {
				info.Indices = append(info.Indices, &timodel.IndexInfo{
					ID: 3, Name: ast.NewCIStr("PRIMARY"), Primary: true, Unique: true, State: timodel.StatePublic,
					Columns: []*timodel.IndexColumn{{Name: ast.NewCIStr("id"), Offset: 0, Length: types.UnspecifiedLength}},
				})
			}
			tableInfo := common.WrapTableInfo("test", info)
			dispatcherID := common.DispatcherID{Low: 1, High: 2}
			makeEvent := func(rowType common.RowType, id int64, value string, suffix int64) *commonEvent.DMLEvent {
				event := commonEvent.NewDMLEvent(dispatcherID, info.ID, 99, 100, tableInfo)
				event.Length = 1
				event.RowTypes = []common.RowType{rowType}
				event.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
				event.Rows.AppendInt64(0, id)
				if tc.null {
					event.Rows.AppendNull(1)
				} else {
					event.Rows.AppendString(1, value)
				}
				if tc.generated {
					if tc.stored {
						event.Rows.AppendInt64(2, int64(len(value)))
					} else {
						event.Rows.AppendNull(2)
					}
				}
				if tc.composite {
					event.Rows.AppendInt64(2, suffix)
				}
				return event
			}

			// TiDB's actual unique index encoder is the oracle for the dependency.
			dbKeys := make([][]byte, 2)
			for i, value := range tc.values {
				if tc.unsupported {
					// TiDB's binary fallback is not an oracle for this collation.
					require.NotContains(t, conflictCollators, tc.collation)
					continue
				}
				datum := types.NewCollationStringDatum(value, tc.collation)
				if tc.generated {
					datum = types.NewIntDatum(int64(len(value)))
				}
				if tc.null {
					datum.SetNull()
				}
				values := []types.Datum{datum}
				if tc.composite {
					values = append(values, types.NewIntDatum(tc.suffixes[i]))
				}
				key, distinct, err := tablecodec.GenIndexKey(codec.NewEncoder(true), time.UTC, info, indexInfo, info.ID, values, nil, nil)
				require.NoError(t, err)
				require.Equal(t, !tc.null, distinct)
				dbKeys[i] = key
			}
			if !tc.unsupported {
				require.Equal(t, tc.wantConflict, !tc.null && string(dbKeys[0]) == string(dbKeys[1]))
			}

			events := []*commonEvent.DMLEvent{
				makeEvent(common.RowTypeDelete, 1, tc.values[0], tc.suffixes[0]),
				makeEvent(common.RowTypeInsert, 2, tc.values[1], tc.suffixes[1]),
				makeEvent(common.RowTypeDelete, 2, tc.values[1], tc.suffixes[1]),
				makeEvent(common.RowTypeInsert, 3, tc.values[0], tc.suffixes[0]),
			}
			if tc.primaryIndex {
				row, ok := events[1].GetNextRow()
				require.True(t, ok)
				events[1].Rewind()
				require.Len(t, genRowKeys(row, tableInfo, dispatcherID), 2, "generate exactly one PK and one UK dependency")
			}
			hasConflict := func(keysA, keysB map[uint64]struct{}) bool {
				for key := range keysA {
					if _, ok := keysB[key]; ok {
						return true
					}
				}
				return false
			}
			require.Equal(t, tc.wantConflict, hasConflict(ConflictKeys(events[1]), ConflictKeys(events[3])))

			if !tc.null {
				before := makeEvent(common.RowTypeInsert, 4, tc.values[0], tc.suffixes[0])
				after := makeEvent(common.RowTypeInsert, 4, tc.values[1], tc.suffixes[1])
				before.RowTypes[0] = common.RowTypeUpdate
				before.Rows.AppendRow(after.Rows.GetRow(0))
				updateKeys := ConflictKeys(before)
				// UPDATE must retain both the old and the new unique values,
				// including when they do not conflict with one another.
				require.True(t, hasConflict(updateKeys, ConflictKeys(events[1])))
				require.True(t, hasConflict(updateKeys, ConflictKeys(events[3])))
			}

			if tc.wantConflict {
				// Every transaction must depend on its predecessor, preventing
				// safe-mode replay in the unsafe order T100, T103, T101, T102.
				slots := NewSlots(1024)
				for i, event := range events {
					node := slots.AllocNode(ConflictKeys(event))
					node.RandCacheID = func() int64 { return 0 }
					slots.Add(node)
					if i > 0 {
						require.Positive(t, node.totalDependencies)
						require.Equal(t, int32(0), node.removedDependencies)
					}
				}
			}
		})
	}
}
