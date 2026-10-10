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

func TestWriterReplayBoundary(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, State: timodel.StatePublic, FieldType: *field}},
	})
	var handles []int64
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
		for row, ok := dml.GetNextRow(); ok; row, ok = dml.GetNextRow() {
			handles = append(handles, row.Row.GetInt64(0))
		}
		dml.PostFlush()
	}).Times(2)
	w := &writer{downstream: downstream, memory: &memoryUsage{}, mutations: make(map[mutationKey]*writeBatch)}
	c := &consumer{writer: w}
	for _, ids := range [][]int64{{1, 2, 1, 2}, {1, 2}, {1, 3, 2}} {
		dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 99, 100, table)
		dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), len(ids))
		for _, id := range ids {
			dml.Rows.AppendInt64(0, id)
			dml.RowTypes = append(dml.RowTypes, common.RowTypeInsert)
		}
		dml.Length = int32(len(ids))
		callbacks := 0
		dml.AddPostFlushFunc(func() { callbacks++ })
		w.pendingDML = append(w.pendingDML, &readResult{dml: dml})
		require.NoError(t, c.flushDML(t.Context(), nil))
		require.Equal(t, 1, callbacks)
		w.advanceReplay(100, 0)
	}
	require.Equal(t, []int64{1, 2, 3}, handles)
	require.Len(t, w.mutations, 3)
	w.advanceReplay(101, 0)
	require.Empty(t, w.mutations)
	require.Zero(t, w.memory.bytes.Load())
	filtered, err := c.filterRows(t.Context(), &event.DMLEvent{CommitTs: 100}, &writeBatch{})
	require.NoError(t, err)
	require.Nil(t, filtered)
}

func TestWriterReplayConfirmationWaitsForFlush(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, State: timodel.StatePublic, FieldType: *field}},
	})
	memory := &memoryUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	input := &storageReader{buffer: buffer, records: []*ack{record}}
	var retained *event.DMLEvent
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) { retained = dml })
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	c := &consumer{reader: input, writer: w}
	for range 2 {
		dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 99, 100, table)
		dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
		dml.Rows.AppendInt64(0, 1)
		dml.RowTypes = []common.RowType{common.RowTypeInsert}
		dml.Length = 1
		require.NoError(t, buffer.queueDML(t.Context(), dml, []*ack{record}, nil))
		result := buffer.nextReady(100)
		w.pendingDML = append(w.pendingDML, result)
	}
	record.refs.Add(-1)
	require.NoError(t, c.flushDML(t.Context(), nil))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.waitBatch(ctx, w.inFlight[0]), context.Canceled)
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Len(t, input.records, 1)
	require.EqualValues(t, 2, record.refs.Load())
	retained.PostFlush()
	w.finishBatches()
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, input.records)
}

func TestWriterReplayKeepsEarlierMutationsUntilWatermark(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, State: timodel.StatePublic, FieldType: *field},
			{ID: 2, Name: ast.NewCIStr("v"), Offset: 1, State: timodel.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	})
	value := int64(0)
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
		for row, ok := dml.GetNextRow(); ok; row, ok = dml.GetNextRow() {
			value = row.Row.GetInt64(1)
		}
		dml.PostFlush()
	}).Times(2)
	memory := &memoryUsage{}
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	c := &consumer{reader: &storageReader{buffer: &readBuffer{memory: memory}}, writer: w, watermarks: map[int64]uint64{0: 90}}
	// A table move can replay the INSERT after a later UPDATE is durable.
	for index, commitTs := range []uint64{100, 200, 100, 200} {
		dml := event.NewDMLEvent(common.NewDispatcherID(), 1, commitTs-1, commitTs, table)
		dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 2)
		dml.Rows.AppendRow(chunk.MutRowFromValues(int64(1), int64(10)).ToRow())
		dml.RowTypes = []common.RowType{common.RowTypeInsert}
		if index%2 == 1 {
			dml.Rows.AppendRow(chunk.MutRowFromValues(int64(1), int64(20)).ToRow())
			dml.RowTypes = []common.RowType{common.RowTypeUpdate, common.RowTypeUpdate}
		}
		dml.Length = 1
		callbacks := 0
		dml.AddPostFlushFunc(func() { callbacks++ })
		require.NoError(t, c.consume(t.Context(), &readResult{dml: dml}))
		require.NoError(t, c.flushDML(t.Context(), nil))
		require.NoError(t, c.confirmCompleted(t.Context()))
		require.Equal(t, 1, callbacks)
		if index >= 1 {
			require.EqualValues(t, 20, value)
			require.Len(t, w.mutations, 2)
		}
	}
	require.NoError(t, c.consume(t.Context(), &readResult{watermark: 201, hasWatermark: true}))
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, w.mutations)
	require.Zero(t, memory.bytes.Load())
}

func TestWriterReplayPreservesUpdateRows(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, State: timodel.StatePublic, FieldType: *field}},
	})
	dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 99, 100, table)
	dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 4)
	for _, id := range []int64{1, 11, 2, 22} {
		dml.Rows.AppendInt64(0, id)
		dml.RowTypes = append(dml.RowTypes, common.RowTypeUpdate)
		dml.RowKeys = append(dml.RowKeys, []byte{byte(id)})
	}
	dml.Length = 2
	batch := &writeBatch{}
	w := &writer{memory: &memoryUsage{}, mutations: map[mutationKey]*writeBatch{
		{tableID: 1, commitTs: 100, rowType: common.RowTypeUpdate, handle: string([]byte{1, '1'})}: nil,
	}}
	c := &consumer{writer: w}
	callbacks := 0
	dml.AddPostFlushFunc(func() { callbacks++ })
	filtered, err := c.filterRows(t.Context(), dml, batch)
	require.NoError(t, err)
	require.EqualValues(t, 1, filtered.Len())
	require.Equal(t, []common.RowType{common.RowTypeUpdate, common.RowTypeUpdate}, filtered.RowTypes)
	require.Equal(t, [][]byte{{2}, {22}}, filtered.RowKeys)
	row, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.EqualValues(t, 2, row.PreRow.GetInt64(0))
	require.EqualValues(t, 22, row.Row.GetInt64(0))
	filtered.PostFlush()
	require.Equal(t, 1, callbacks)
}

func TestWriterDDLOnlyFlushesAffectedTable(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	tableA := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("a"),
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *field}},
	})
	tableB := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 2, Name: ast.NewCIStr("b"),
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *field}},
	})
	a := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, 10, tableA)
	b := event.NewDMLEvent(common.NewDispatcherID(), 2, 0, 10, tableB)
	for _, dml := range []*event.DMLEvent{a, b} {
		dml.Rows = chunk.NewChunkWithCapacity(dml.TableInfo.GetFieldSlice(), 1)
		dml.Rows.AppendInt64(0, 1)
		dml.RowTypes, dml.Length = []common.RowType{common.RowTypeInsert}, 1
	}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "a", FinishedTs: 20}
	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().AddDMLEvent(a).Do(func(dml *event.DMLEvent) { dml.PostFlush() }),
		downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil),
		downstream.EXPECT().WriteBlockEvent(ddl).Return(nil),
	)
	other := &writeBatch{items: []*readResult{{dml: b}}, done: make(chan bool)}
	w := &writer{
		downstream: downstream, memory: &memoryUsage{}, pendingDML: []*readResult{{dml: b}, {dml: a}},
		inFlight: []*writeBatch{other}, inFlightEvents: 1,
	}
	c := &consumer{writer: w}
	require.NoError(t, c.writeDDL(t.Context(), &readResult{ddl: ddl}))
	require.Len(t, w.pendingDML, 1)
	require.Same(t, b, w.pendingDML[0].dml)
	require.Equal(t, []*writeBatch{other}, w.inFlight)
}

func TestWriterSeparatesUnversionedSchemas(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	oldTable := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *field}},
	})
	newTable := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), FieldType: *field},
			{ID: 2, Name: ast.NewCIStr("v"), Offset: 1, FieldType: *field},
		},
	})
	first := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, 10, oldTable)
	second := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, 20, newTable)
	for _, dml := range []*event.DMLEvent{first, second} {
		dml.Rows = chunk.NewChunkWithCapacity(dml.TableInfo.GetFieldSlice(), 1)
		for column := range dml.TableInfo.GetColumns() {
			dml.Rows.AppendInt64(column, 1)
		}
		dml.RowTypes, dml.Length = []common.RowType{common.RowTypeInsert}, 1
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().AddDMLEvent(first).Do(func(*event.DMLEvent) { cancel() }),
		downstream.EXPECT().AddDMLEvent(second).Do(func(dml *event.DMLEvent) { dml.PostFlush() }),
	)
	w := &writer{downstream: downstream, memory: &memoryUsage{}, pendingDML: []*readResult{{dml: first}, {dml: second}}}
	c := &consumer{writer: w}
	require.ErrorIs(t, c.flushDML(ctx, nil), context.Canceled)
	require.Len(t, w.inFlight, 1)
	require.Len(t, w.inFlight[0].items, 1)
	require.Len(t, w.pendingDML, 1)
	first.PostFlush()
	require.NoError(t, c.flushDML(t.Context(), nil))
	require.Empty(t, w.inFlight)
	require.Empty(t, w.pendingDML)
}

func TestWriterCSVTransactionBatches(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic, FieldType: *field},
			{ID: 2, Name: ast.NewCIStr("uk"), Offset: 1, State: timodel.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	})
	w := &writer{memory: &memoryUsage{}, mutations: make(map[mutationKey]*writeBatch), serialDML: true}
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
			dml.AddPostFlushFunc(func() { callbacks++ })
			w.pendingDML = append(w.pendingDML, &readResult{dml: dml})
		}
	}
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
	c := &consumer{writer: w}
	require.ErrorIs(t, c.flushDML(ctx, nil), context.Canceled)
	require.Len(t, w.inFlight[0].items, 2)
	require.Len(t, w.pendingDML, 2)
	require.Zero(t, callbacks)
	first.PostFlush()
	require.NoError(t, c.flushDML(t.Context(), nil))
	require.Equal(t, 4, callbacks)
	w.advanceReplay(201, 0)
	require.Zero(t, w.memory.used())
}

func TestWriterCSVDeletesBeforeInsertsAcrossBatches(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic, FieldType: *field}},
	})
	w := &writer{memory: &memoryUsage{}, mutations: make(map[mutationKey]*writeBatch), serialDML: true, sortCSVRows: true}
	require.NoError(t, w.memory.reserve(t.Context(), 2*batchBytes))
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
		dml.AddPostFlushFunc(func() { callbacks++ })
		w.pendingDML = append(w.pendingDML, &readResult{dml: dml, bytes: batchBytes / 2})
	}
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
	c := &consumer{writer: w}
	require.NoError(t, c.flushDML(t.Context(), nil))
	require.Equal(t, 4, callbacks)
	require.Empty(t, w.pendingDML)
	require.Empty(t, w.inFlight)
	w.advanceReplay(101, 0)
	require.Zero(t, w.memory.used())
}
