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
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestAssemblerCSVTransactionBatches(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic, FieldType: *field},
			{ID: 2, Name: ast.NewCIStr("uk"), Offset: 1, State: timodel.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	})
	w := &writer{memory: newMemoryUsage(), mutations: make(map[mutationKey]*writeBatch)}
	callbacks := 0
	for _, commitTs := range []uint64{100, 200} {
		for id := int64(1); id <= 2; id++ {
			dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, commitTs, table)
			dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 2)
			if commitTs == 100 {
				dml.Rows.AppendRow(chunk.MutRowFromValues(id, id).ToRow())
				dml.RowTypes, dml.Length = []common.RowType{common.RowTypeInsert}, 1
			} else {
				dml.Rows.AppendRow(chunk.MutRowFromValues(id, nil).ToRow())
				dml.Rows.AppendRow(chunk.MutRowFromValues(id, 3-id).ToRow())
				dml.RowTypes, dml.Length = []common.RowType{common.RowTypeDelete, common.RowTypeInsert}, 2
			}
			dml.AddPostFlushFunc(func() {
				callbacks++
			})
			w.pendingDML = append(w.pendingDML, &writeEvent{dml: dml})
		}
	}
	a := &assembler{memory: w.memory, mergeRows: true}
	w.pendingDML = a.prepare(w.pendingDML)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var first *event.DMLEvent
	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			first = dml
			require.EqualValues(t, 100, dml.CommitTs)
			require.EqualValues(t, 2, dml.Len())
			cancel()
		}),
		downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			require.Equal(t, 2, callbacks)
			require.EqualValues(t, 200, dml.CommitTs)
			require.Equal(t, []common.RowType{common.RowTypeDelete, common.RowTypeInsert, common.RowTypeDelete, common.RowTypeInsert}, dml.RowTypes)
			dml.PostFlush()
		}),
	)
	w.downstream = downstream
	require.ErrorIs(t, w.flushDML(ctx), context.Canceled)
	require.Len(t, w.inFlight[0].events, 1)
	require.Len(t, w.pendingDML, 1)
	require.Zero(t, callbacks)
	first.PostFlush()
	require.NoError(t, w.flushDML(t.Context()))
	require.Equal(t, 4, callbacks)
	w.consume(&writeEvent{watermark: 201, hasWatermark: true})
	w.finishBatches()
	require.Zero(t, w.memory.used())
}

func TestAssemblerCSVDeletesBeforeInsertsAcrossBatches(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic, FieldType: *field}},
	})
	w := &writer{memory: newMemoryUsage(), mutations: make(map[mutationKey]*writeBatch)}
	w.memory.reserve(2 * batchBytes)
	callbacks := 0
	for index, rowType := range []common.RowType{common.RowTypeDelete, common.RowTypeInsert, common.RowTypeDelete, common.RowTypeInsert} {
		id := int64(index/2 + 1)
		if rowType == common.RowTypeInsert {
			id = 3 - id
		}
		dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, 100, table)
		dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
		dml.Rows.AppendInt64(0, id)
		dml.RowTypes, dml.Length = []common.RowType{rowType}, 1
		dml.AddPostFlushFunc(func() {
			callbacks++
		})
		w.pendingDML = append(w.pendingDML, &writeEvent{dml: dml, bytes: batchBytes / 2})
	}
	a := &assembler{memory: w.memory, mergeRows: true, sortCSVRows: true}
	w.pendingDML = a.prepare(w.pendingDML)
	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			require.Equal(t, []common.RowType{common.RowTypeDelete, common.RowTypeDelete}, dml.RowTypes)
			require.EqualValues(t, 1, dml.Rows.GetRow(0).GetInt64(0))
			require.EqualValues(t, 2, dml.Rows.GetRow(1).GetInt64(0))
			dml.PostFlush()
		}),
		downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			require.Equal(t, 2, callbacks)
			require.Equal(t, []common.RowType{common.RowTypeInsert, common.RowTypeInsert}, dml.RowTypes)
			require.EqualValues(t, 2, dml.Rows.GetRow(0).GetInt64(0))
			require.EqualValues(t, 1, dml.Rows.GetRow(1).GetInt64(0))
			dml.PostFlush()
		}),
	)
	w.downstream = downstream
	require.NoError(t, w.flushDML(t.Context()))
	require.Equal(t, 4, callbacks)
	require.Empty(t, w.pendingDML)
	require.Empty(t, w.inFlight)
	w.consume(&writeEvent{watermark: 101, hasWatermark: true})
	w.finishBatches()
	require.Zero(t, w.memory.used())
}

func TestAssemblerPreservesControlBoundaries(t *testing.T) {
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
	})
	var items []*writeEvent
	for _, commitTs := range []uint64{100, 200} {
		for _, rowType := range []common.RowType{common.RowTypeInsert, common.RowTypeDelete} {
			dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, commitTs, table)
			dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
			dml.Rows.AppendInt64(0, 1)
			dml.RowTypes, dml.Length = []common.RowType{rowType}, 1
			items = append(items, &writeEvent{dml: dml})
		}
		items = append(items, &writeEvent{watermark: commitTs, hasWatermark: true})
	}
	a := &assembler{memory: newMemoryUsage(), mergeRows: true, sortCSVRows: true}
	prepared := a.prepare(items)
	require.Len(t, prepared, 4)
	for index, commitTs := range []uint64{100, 200} {
		result := prepared[index*2]
		require.Equal(t, []common.RowType{common.RowTypeDelete, common.RowTypeInsert}, result.dml.RowTypes)
		require.Equal(t, commitTs, result.dml.CommitTs)
		a.memory.release(result.bytes)
		result = prepared[index*2+1]
		require.True(t, result.hasWatermark)
		require.Equal(t, commitTs, result.watermark)
	}
	require.Zero(t, a.memory.used())
}

func TestAssemblerGroupBoundary(t *testing.T) {
	first := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 1, CommitTs: 20, RowTypes: []common.RowType{common.RowTypeInsert}}}
	second := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10, RowTypes: []common.RowType{common.RowTypeInsert}}}
	third := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10, RowTypes: []common.RowType{common.RowTypeDelete}}}
	r := &storageReader{
		checkpoint:  20,
		scanned:     true,
		sortCSVRows: true,
	}
	group := &readGroup{
		tableID:  1,
		compare:  r.compareRows,
		boundary: &readBoundary{},
	}
	for _, item := range []*writeEvent{first, second, third} {
		item.boundary = group.boundary
	}
	unrelated := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 2, CommitTs: 5}}
	a := &assembler{pendingDML: []*writeEvent{unrelated, first, second, third}, sortCSVRows: true}
	for result, err := range a.decode(t.Context(), &readData{group: group, groupEnd: true}) {
		require.NoError(t, err)
		require.NoError(t, a.accept(t.Context(), result))
		r.Advance([]readUpdate{{data: result.data, decoded: result.decoded, progress: result.watermark}})
		a.collectWatermarks()
	}
	for _, expected := range []*writeEvent{third, second, first} {
		result := a.next()
		require.Same(t, expected, result)
	}
	batch, err := r.Read(t.Context())
	require.NoError(t, err)
	data := batch[0]
	require.NotNil(t, data.control)
	for result, err := range a.decode(t.Context(), data) {
		require.NoError(t, err)
		require.NoError(t, a.accept(t.Context(), result))
		r.Advance([]readUpdate{{data: result.data, decoded: result.decoded, progress: result.watermark}})
		a.collectWatermarks()
	}
	// The completed scan's checkpoint also covers the earlier table.
	unrelated.boundary = &readBoundary{reached: true}
	result := a.next()
	require.Same(t, unrelated, result)
	result = a.next()
	require.True(t, result.hasWatermark)
	require.EqualValues(t, 20, result.watermark)
	require.Empty(t, a.pendingDML)
	record := newAck(newMemoryUsage(), 256, nil)
	require.NoError(t, a.accept(t.Context(), decodedData{
		data:   &readData{format: rowFormat},
		dml:    &event.DMLEvent{CommitTs: 19},
		record: record,
	}))
	require.Empty(t, a.pendingDML)
}
