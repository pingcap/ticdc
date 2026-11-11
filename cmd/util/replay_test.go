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

package util

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newReplayTestTableInfo(tp byte, names ...string) *common.TableInfo {
	table := &model.TableInfo{ID: 1, Name: ast.NewCIStr("t")}
	primary := &model.IndexInfo{Name: ast.NewCIStr("PRIMARY"), Primary: true, Unique: true, State: model.StatePublic}
	for offset, name := range names {
		field := types.NewFieldType(tp)
		field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
		table.Columns = append(table.Columns, &model.ColumnInfo{
			ID: int64(offset + 1), Name: ast.NewCIStr(name), Offset: offset, State: model.StatePublic, FieldType: *field,
		})
		primary.Columns = append(primary.Columns, &model.IndexColumn{Name: ast.NewCIStr(name), Offset: offset})
	}
	table.Indices = []*model.IndexInfo{primary}
	return common.NewTableInfo4Decoder("test", table)
}

func newReplayTestEvent(ts uint64, ids ...int64) *commonEvent.DMLEvent {
	table := newReplayTestTableInfo(mysql.TypeLonglong, "id")
	e := commonEvent.NewDMLEvent(common.DispatcherID{}, 1, ts-1, ts, table)
	e.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), len(ids))
	for _, id := range ids {
		e.Rows.AppendInt64(0, id)
		e.RowTypes = append(e.RowTypes, common.RowTypeInsert)
	}
	e.Length = int32(len(ids))
	return e
}

func TestReplayFilterDropsWithinEventDuplicates(t *testing.T) {
	// The dispatcher merge handover replays a batch: every mutation of the replayed copy
	// is delivered twice with the same commit-ts. The consumer merges both copies into one
	// event before flushing, so the duplicate rows have to be dropped inside that event.
	filter := NewReplayFilter()
	replayed := newReplayTestEvent(100, 1714, 1715, 1714, 1715)
	callbacks := 0
	replayed.AddPostFlushFunc(func() { callbacks++ })
	// The sink batches event-by-event only for a single event, so a second event with
	// another commit-ts keeps the group on the cross-event merge path that rejects input
	// with duplicate insert rows of the same key.
	other := newReplayTestEvent(101, 42)

	retained, dropped := filter.FilterBatch([]*commonEvent.DMLEvent{replayed, other})
	require.Empty(t, dropped)
	require.Len(t, retained, 2)
	require.Equal(t, int32(2), retained[0].Len())
	require.Equal(t, []common.RowType{common.RowTypeInsert, common.RowTypeInsert}, retained[0].RowTypes)
	require.Equal(t, 2, retained[0].Rows.NumRows())
	require.Same(t, other, retained[1])
	// The retained copy owns the callbacks of the original event, so the decoder chunk
	// and the flush barrier of the replayed event are still released.
	retained[0].PostFlush()
	require.Equal(t, 1, callbacks)

	// Everything replayed: the event is dropped and the caller has to release it.
	filter.Commit(100)
	retained, dropped = filter.FilterBatch([]*commonEvent.DMLEvent{newReplayTestEvent(100, 1714, 1715)})
	require.Empty(t, retained)
	require.Len(t, dropped, 1)
}

func TestReplayFilterBoundary(t *testing.T) {
	filter := NewReplayFilter()
	retained, dropped := filter.FilterBatch([]*commonEvent.DMLEvent{newReplayTestEvent(100, 1, 2, 1, 2)})
	require.Len(t, retained, 1)
	require.Empty(t, dropped)
	require.Equal(t, int32(2), retained[0].Len())

	filter.Commit(100)
	require.Equal(t, 2, filter.Len())
	// An unseen handle of the same commit-ts must survive while the others are dropped
	// by the boundary.
	retained, dropped = filter.FilterBatch([]*commonEvent.DMLEvent{newReplayTestEvent(100, 1, 3, 2)})
	require.Len(t, retained, 1)
	require.Empty(t, dropped)
	require.Equal(t, []int64{3}, rowIDsOf(retained[0]))

	// Once the watermark passed the commit-ts, the boundary is released: callers drop
	// those messages by the watermark itself.
	filter.Advance(101)
	require.Zero(t, filter.Len())
	retained, dropped = filter.FilterBatch([]*commonEvent.DMLEvent{newReplayTestEvent(100, 1, 2)})
	require.Len(t, retained, 1)
	require.Empty(t, dropped)
	require.Equal(t, []int64{1, 2}, rowIDsOf(retained[0]))
}

func rowIDsOf(e *commonEvent.DMLEvent) []int64 {
	ids := make([]int64, 0, e.Len())
	e.Rewind()
	for row, ok := e.GetNextRow(); ok; row, ok = e.GetNextRow() {
		ids = append(ids, row.Row.GetInt64(0))
	}
	e.Rewind()
	return ids
}

func TestReplayRowMetadata(t *testing.T) {
	// A(10), B(20), A(10), B(20) retains exactly A(10), B(20).
	replayed := make(map[replayKey]struct{})
	var timestamps []uint64
	for _, ts := range []uint64{10, 20, 10, 20} {
		if e := filterReplayRows(newReplayTestEvent(ts, 1), replayed, nil); e != nil {
			timestamps = append(timestamps, e.CommitTs)
		}
	}
	require.Equal(t, []uint64{10, 20}, timestamps)

	table := newReplayTestTableInfo(mysql.TypeLonglong, "id")
	makeEvent := func(ts uint64, types ...common.RowType) *commonEvent.DMLEvent {
		e := commonEvent.NewDMLEvent(common.DispatcherID{}, 1, ts-1, ts, table)
		e.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), len(types))
		for i, typ := range types {
			e.Rows.AppendRow(chunk.MutRowFromValues(int64(1)).ToRow())
			e.RowTypes = append(e.RowTypes, typ)
			e.RowKeys = append(e.RowKeys, []byte{byte(i)})
		}
		for i := 0; i < len(types); i++ {
			e.Length++
			e.Checksum = append(e.Checksum, &integrity.Checksum{Current: uint32(i + 1)})
			if types[i] == common.RowTypeUpdate {
				i++
			}
		}
		return e
	}
	seen := make(map[replayKey]struct{})
	// Different operations at the same timestamp and handle must survive.
	original := makeEvent(100, common.RowTypeDelete, common.RowTypeInsert, common.RowTypeDelete, common.RowTypeInsert)
	original.ReplicatingTs = 200
	original.TableInfoVersion = 99
	filtered := filterReplayRows(original, seen, nil)
	require.Equal(t, original.ReplicatingTs, filtered.ReplicatingTs)
	require.Equal(t, original.TableInfoVersion, filtered.TableInfoVersion)
	require.Equal(t, int32(2), filtered.Len())
	require.Equal(t, original.RowTypes[:2], filtered.RowTypes)
	require.Equal(t, original.RowKeys[:2], filtered.RowKeys)
	require.Equal(t, original.Checksum[:2], filtered.Checksum)
	newer := makeEvent(101, common.RowTypeUpdate, common.RowTypeUpdate, common.RowTypeUpdate, common.RowTypeUpdate)
	filtered = filterReplayRows(newer, seen, nil)
	require.Equal(t, int32(1), filtered.Len())
	require.Equal(t, newer.RowTypes[:2], filtered.RowTypes)
	require.Equal(t, newer.RowKeys[:2], filtered.RowKeys)
	require.Equal(t, newer.Checksum[:1], filtered.Checksum)
	require.Equal(t, 2, filtered.Rows.NumRows())
	laterInsert := makeEvent(102, common.RowTypeInsert)
	require.Same(t, laterInsert, filterReplayRows(laterInsert, seen, nil))
	otherTable := makeEvent(100, common.RowTypeInsert)
	otherTable.PhysicalTableID++
	require.Same(t, otherTable, filterReplayRows(otherTable, seen, nil))
}

func TestReplayHandleIdentity(t *testing.T) {
	table := newReplayTestTableInfo(mysql.TypeBlob, "a", "b")
	// NUL-separated encoding would collide for these two different keys.
	a := replayHandle(chunk.MutRowFromValues([]byte("a\x00"), []byte("b")).ToRow(), table)
	b := replayHandle(chunk.MutRowFromValues([]byte("a"), []byte("\x00b")).ToRow(), table)
	require.NotEmpty(t, a)
	require.NotEqual(t, a, b)
	noHandle := common.NewTableInfo4Decoder("test", &model.TableInfo{
		ID: 2, Name: ast.NewCIStr("no_handle"),
		Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
	})
	e := commonEvent.NewDMLEvent(common.DispatcherID{}, 2, 99, 100, noHandle)
	e.Rows = chunk.NewChunkWithCapacity(noHandle.GetFieldSlice(), 2)
	for range 2 {
		e.Rows.AppendRow(chunk.MutRowFromValues(int64(1)).ToRow())
	}
	e.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
	e.Length = 2
	require.Same(t, e, filterReplayRows(e, make(map[replayKey]struct{}), nil))
}
