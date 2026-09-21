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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/util"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
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

func newReplayTestEvent(ts uint64, ids ...int64) *event.DMLEvent {
	table := newReplayTestTableInfo(mysql.TypeLonglong, "id")
	e := event.NewDMLEvent(common.DispatcherID{}, 1, ts-1, ts, table)
	e.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), len(ids))
	for _, id := range ids {
		e.Rows.AppendInt64(0, id)
		e.RowTypes = append(e.RowTypes, common.RowTypeInsert)
	}
	e.Length = int32(len(ids))
	return e
}

func TestReplayWatermarkBoundary(t *testing.T) {
	s := sinkmock.NewMockSink(gomock.NewController(t))
	var handles []int64
	s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(e *event.DMLEvent) {
		for row, ok := e.GetNextRow(); ok; row, ok = e.GetNextRow() {
			handles = append(handles, row.Row.GetInt64(0))
		}
		e.PostFlush()
	}).AnyTimes()
	w := &writer{mysqlSink: s, progresses: []*partitionProgress{{watermark: 100}}}
	flush := func(ids ...int64) {
		e := newReplayTestEvent(100, ids...)
		callbacks := 0
		e.AddPostFlushFunc(func() { callbacks++ })
		require.NoError(t, w.flushDMLBatch(t.Context(), []*event.DMLEvent{e}))
		require.Equal(t, 1, callbacks)
	}
	flush(1, 2, 1, 2)
	require.Equal(t, []int64{1, 2}, handles)
	flush(1, 2) // Entirely replayed, without a sink submission.
	require.Equal(t, []int64{1, 2}, handles)
	flush(1, 3, 2) // An unseen handle at the same timestamp must survive.
	require.Equal(t, []int64{1, 2, 3}, handles)
	require.Len(t, w.replayBoundary, 3)
	w.progresses[0].watermark = 101
	// An unrelated batch must not discard boundary state while a previously
	// buffered replay is still waiting in another group.
	require.NoError(t, w.flushDMLBatch(t.Context(), nil))
	flush(1, 2)
	require.Equal(t, []int64{1, 2, 3}, handles)
	w.spillStore = util.NewSpillStore()
	t.Cleanup(func() { require.NoError(t, w.spillStore.Cleanup()) })
	require.NoError(t, w.flushDMLEventsByWatermark(t.Context()))
	require.Nil(t, w.replayBoundary)
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
	makeEvent := func(ts uint64, types ...common.RowType) *event.DMLEvent {
		e := event.NewDMLEvent(common.DispatcherID{}, 1, ts-1, ts, table)
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

func TestReplayFailedFlush(t *testing.T) {
	events := []*event.DMLEvent{newReplayTestEvent(100, 1), newReplayTestEvent(100, 1)}
	callbacks := 0
	for _, e := range events {
		e.AddPostFlushFunc(func() { callbacks++ })
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := sinkmock.NewMockSink(gomock.NewController(t))
	s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(*event.DMLEvent) { cancel() })
	w := &writer{mysqlSink: s, progresses: []*partitionProgress{{watermark: 100}}}
	require.ErrorIs(t, w.flushDMLBatch(ctx, events), context.Canceled)
	require.Empty(t, w.replayBoundary)
	require.Zero(t, callbacks)
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
	e := event.NewDMLEvent(common.DispatcherID{}, 2, 99, 100, noHandle)
	e.Rows = chunk.NewChunkWithCapacity(noHandle.GetFieldSlice(), 2)
	for range 2 {
		e.Rows.AppendRow(chunk.MutRowFromValues(int64(1)).ToRow())
	}
	e.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
	e.Length = 2
	require.Same(t, e, filterReplayRows(e, make(map[replayKey]struct{}), nil))
}
