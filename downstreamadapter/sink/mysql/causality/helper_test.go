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

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/ordering"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/open"
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
	f, err := ordering.NewTxnOrderMatcher([]string{"test.ordered"}, false)
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
			require.Equal(t, tc.wantConflict, hasConflict(ConflictKeys(events[1], nil), ConflictKeys(events[3], nil)))
			redoKeys := func(event *commonEvent.DMLEvent) map[uint64]struct{} {
				row, ok := event.GetNextRow()
				require.True(t, ok)
				event.Rewind()
				redo := (&commonEvent.RedoRowEvent{
					TableInfo: tableInfo, PhysicalTableID: info.ID, StartTs: event.StartTs, CommitTs: event.CommitTs, Event: row,
				}).ToRedoLog()
				data, err := redo.MarshalMsg(nil)
				require.NoError(t, err)
				var decoded commonEvent.RedoLog
				remaining, err := decoded.UnmarshalMsg(data)
				require.NoError(t, err)
				require.Empty(t, remaining)
				replayed := decoded.RedoRow.ToDMLEvent()
				replayed.DispatcherID = dispatcherID
				t.Cleanup(replayed.PostFlush)
				return ConflictKeys(replayed, nil)
			}
			redoEventKeys := make([]map[uint64]struct{}, len(events))
			for i, event := range events {
				redoEventKeys[i] = redoKeys(event)
			}
			require.Equal(t, tc.wantConflict, hasConflict(redoEventKeys[1], redoEventKeys[3]), "redo must preserve unique index dependencies")

			if !tc.null {
				before := makeEvent(common.RowTypeInsert, 4, tc.values[0], tc.suffixes[0])
				after := makeEvent(common.RowTypeInsert, 4, tc.values[1], tc.suffixes[1])
				before.RowTypes[0] = common.RowTypeUpdate
				before.Rows.AppendRow(after.Rows.GetRow(0))
				updateKeys := ConflictKeys(before, nil)
				// UPDATE must retain both the old and the new unique values,
				// including when they do not conflict with one another.
				require.True(t, hasConflict(updateKeys, ConflictKeys(events[1], nil)))
				require.True(t, hasConflict(updateKeys, ConflictKeys(events[3], nil)))
				redoUpdateKeys := redoKeys(before)
				require.True(t, hasConflict(redoUpdateKeys, redoEventKeys[1]))
				require.True(t, hasConflict(redoUpdateKeys, redoEventKeys[3]))
			}

			if tc.wantConflict {
				// Every transaction must depend on its predecessor, preventing
				// safe-mode replay in the unsafe order T100, T103, T101, T102.
				slots := NewSlots(1024)
				for i, event := range events {
					node := slots.AllocNode(ConflictKeys(event, nil))
					node.RandCacheID = func() int64 { return 0 }
					slots.Add(node)
					if i > 0 {
						require.Positive(t, node.totalDependencies)
						require.Equal(t, int32(0), node.removedDependencies)
						require.True(t, hasConflict(redoEventKeys[i-1], redoEventKeys[i]), "redo transactions must depend on their predecessors")
					}
				}
			}
		})
	}
}

func TestConflictKeysOpenProtocolWithoutPrimaryKey(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Job(`create table test.t(
		a int, b int as (a + 1) virtual not null, c int not null,
		unique key uk_generated(b), unique key uk_visible(c))`)
	source := helper.DML2Event("test", "t",
		"insert into test.t(a, c) values (1, 1)",
		"insert into test.t(a, c) values (2, 2)",
		"insert into test.t(a, c) values (3, 3)")
	cfg := codeccommon.NewConfig(config.ProtocolOpen)
	encoder, err := open.NewBatchEncoder(cfg, nil)
	require.NoError(t, err)
	decoder, err := open.NewDecoder(t.Context(), 0, cfg, nil)
	require.NoError(t, err)
	for {
		row, ok := source.GetNextRow()
		if !ok {
			break
		}
		require.NoError(t, encoder.AppendRowChangedEvent(t.Context(), "", &commonEvent.RowEvent{
			TableInfo: source.TableInfo, CommitTs: source.CommitTs, Event: row,
			ColumnSelector: columnselector.NewDefaultColumnSelector(), Callback: func() {},
		}))
	}

	var keys []map[uint64]struct{}
	for _, message := range encoder.Build() {
		decoder.AddKeyValue(message.Key, message.Value)
		for {
			typ, ok := decoder.HasNext()
			if !ok {
				break
			}
			require.Equal(t, codeccommon.MessageTypeRow, typ)
			event := decoder.NextDMLMessage().ToDMLEvent()
			t.Cleanup(event.PostFlush)
			event.DispatcherID = common.DispatcherID{Low: 1, High: 2}
			// The virtual indexed column is absent from the reconstructed schema.
			require.Empty(t, event.TableInfo.GetPKIndex())
			require.Len(t, event.TableInfo.GetColumns(), 2)
			require.Len(t, event.TableInfo.GetIndices(), 1)
			var conflictKeys map[uint64]struct{}
			require.NotPanics(t, func() { conflictKeys = ConflictKeys(event) })
			require.NotEmpty(t, conflictKeys)
			keys = append(keys, conflictKeys)

			row, ok := event.GetNextRow()
			require.True(t, ok)
			var redo *commonEvent.RedoLog
			require.NotPanics(t, func() {
				redo = (&commonEvent.RedoRowEvent{
					TableInfo: event.TableInfo, PhysicalTableID: event.PhysicalTableID, Event: row,
				}).ToRedoLog()
			})
			data, err := redo.MarshalMsg(nil)
			require.NoError(t, err)
			var decoded commonEvent.RedoLog
			remaining, err := decoded.UnmarshalMsg(data)
			require.NoError(t, err)
			require.Empty(t, remaining)
			replayed := decoded.RedoRow.ToDMLEvent()
			t.Cleanup(replayed.PostFlush)
			replayed.DispatcherID = event.DispatcherID
			require.Equal(t, conflictKeys, ConflictKeys(replayed))
		}
	}
	require.Len(t, keys, 3)
	for i := 1; i < len(keys); i++ {
		for key := range keys[0] {
			require.NotContains(t, keys[i], key, "different unique values must remain independent")
		}
	}
}

func TestConflictKeysReconstructedSchemas(t *testing.T) {
	testCases := []struct {
		name       string
		idType     byte
		uniqueType byte
		values     [2][2]any
	}{
		{name: "string primary key", idType: mysql.TypeVarchar, values: [2][2]any{{"alice", nil}, {"bob", nil}}},
		{name: "string unique key", idType: mysql.TypeLong, uniqueType: mysql.TypeVarchar, values: [2][2]any{{int64(1), "alice"}, {int64(2), "bob"}}},
		{name: "integer index namespaces", idType: mysql.TypeLong, uniqueType: mysql.TypeLong, values: [2][2]any{{int64(1), int64(2)}, {int64(2), int64(3)}}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			info := &timodel.TableInfo{ID: 100, Name: ast.NewCIStr("t"), PKIsHandle: tc.idType != mysql.TypeVarchar}
			for i, tp := range []byte{tc.idType, tc.uniqueType} {
				if tp == 0 {
					continue
				}
				ft := types.NewFieldType(tp)
				ft.AddFlag(mysql.NotNullFlag)
				name := "id"
				if i == 0 {
					ft.AddFlag(mysql.PriKeyFlag)
				} else {
					name = "u"
					ft.AddFlag(mysql.UniqueKeyFlag)
				}
				if tp == mysql.TypeVarchar {
					ft.SetCharset("utf8mb4")
					ft.SetCollate("utf8mb4_bin")
				}
				info.Columns = append(info.Columns, &timodel.ColumnInfo{
					ID: int64(i + 1), Name: ast.NewCIStr(name), Offset: i, State: timodel.StatePublic, FieldType: *ft,
				})
				if i > 0 || !info.PKIsHandle {
					info.Indices = append(info.Indices, &timodel.IndexInfo{
						ID: int64(i + 1), Name: ast.NewCIStr(name), Primary: i == 0, Unique: true, State: timodel.StatePublic,
						Columns: []*timodel.IndexColumn{{Name: ast.NewCIStr(name), Offset: i, Length: types.UnspecifiedLength}},
					})
				}
			}
			info.IsCommonHandle = !info.PKIsHandle
			tableInfo := common.WrapTableInfo("test", info)
			rowValues := [][2]any{tc.values[0], tc.values[1], tc.values[0]}
			if tc.uniqueType != 0 {
				rowValues = append(rowValues, [2]any{tc.values[1][0], tc.values[0][1]})
			}
			rows := make([]commonEvent.RowChange, 0, len(rowValues))
			for _, values := range rowValues {
				chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
				for i := range info.Columns {
					datum := types.NewDatum(values[i])
					chk.AppendDatum(i, &datum)
				}
				rows = append(rows, commonEvent.RowChange{Row: chk.GetRow(0), RowType: common.RowTypeInsert})
			}

			for _, source := range []string{"open", "redo"} {
				t.Run(source, func(t *testing.T) {
					var events []*commonEvent.DMLEvent
					if source == "open" {
						cfg := codeccommon.NewConfig(config.ProtocolOpen)
						encoder, err := open.NewBatchEncoder(cfg, nil)
						require.NoError(t, err)
						decoder, err := open.NewDecoder(t.Context(), 0, cfg, nil)
						require.NoError(t, err)
						for i, row := range rows {
							require.NoError(t, encoder.AppendRowChangedEvent(t.Context(), "", &commonEvent.RowEvent{
								TableInfo: tableInfo, CommitTs: uint64(100 + i), Event: row,
								ColumnSelector: columnselector.NewDefaultColumnSelector(), Callback: func() {},
							}))
						}
						for _, message := range encoder.Build() {
							decoder.AddKeyValue(message.Key, message.Value)
							for {
								typ, ok := decoder.HasNext()
								if !ok {
									break
								}
								require.Equal(t, codeccommon.MessageTypeRow, typ)
								events = append(events, decoder.NextDMLMessage().ToDMLEvent())
							}
						}
					} else {
						for i, row := range rows {
							redo := (&commonEvent.RedoRowEvent{
								TableInfo: tableInfo, PhysicalTableID: info.ID, CommitTs: uint64(100 + i), Event: row,
							}).ToRedoLog().RedoRow
							events = append(events, redo.ToDMLEvent())
						}
					}
					require.Len(t, events, len(rows))
					for _, event := range events {
						event.DispatcherID = common.DispatcherID{Low: 1, High: 2}
						t.Cleanup(event.PostFlush)
					}
					first, second := ConflictKeys(events[0], nil), ConflictKeys(events[1], nil)
					require.NotEmpty(t, first)
					for key := range first {
						require.NotContains(t, second, key, "independent reconstructed rows must not conflict")
					}
					require.Equal(t, first, ConflictKeys(events[2], nil), "repeated rows must retain their dependencies")
					if tc.uniqueType != 0 {
						uniqueKeys := ConflictKeys(events[3], nil)
						sharedKeys := 0
						for key := range first {
							if _, ok := uniqueKeys[key]; ok {
								sharedKeys++
							}
						}
						require.Equal(t, 1, sharedKeys, "different primary keys with the same UK must share its dependency")
					}
				})
			}
		})
	}
}
