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
	"sync"
	"testing"
	"time"

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
	w := &writer{downstream: downstream, memory: newMemoryUsage(), mutations: make(map[mutationKey]*writeBatch)}
	for _, ids := range [][]int64{{1, 2, 1, 2}, {1, 2}, {1, 3, 2}} {
		dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 99, 100, table)
		dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), len(ids))
		for _, id := range ids {
			dml.Rows.AppendInt64(0, id)
			dml.RowTypes = append(dml.RowTypes, common.RowTypeInsert)
		}
		dml.Length = int32(len(ids))
		callbacks := 0
		dml.AddPostFlushFunc(func() {
			callbacks++
		})
		w.pendingDML = append(w.pendingDML, &writeEvent{dml: dml})
		require.NoError(t, w.flushDML(t.Context()))
		require.Equal(t, 1, callbacks)
		w.consume(&writeEvent{watermark: 100, hasWatermark: true})
		w.finishBatches()
	}
	require.Equal(t, []int64{1, 2, 3}, handles)
	require.Len(t, w.mutations, 3)
	w.consume(&writeEvent{watermark: 101, hasWatermark: true})
	w.finishBatches()
	require.Empty(t, w.mutations)
	require.Zero(t, w.memory.bytes.Load())
	filtered, err := w.filterRows(t.Context(), &event.DMLEvent{CommitTs: 100}, &writeBatch{})
	require.NoError(t, err)
	require.Nil(t, filtered)
}

func TestWriterWatermarksFollowWrites(t *testing.T) {
	first := mutationKey{tableID: 1, commitTs: 10, handle: "a"}
	second := mutationKey{tableID: 2, commitTs: 10, handle: "b"}
	batch := &writeBatch{
		items: []*writeEvent{{dml: &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10}}},
		keys:  []mutationKey{first}, done: make(chan struct{}),
	}
	w := &writer{
		memory: newMemoryUsage(), inFlight: []*writeBatch{batch}, inFlightEvents: 1,
		mutations: map[mutationKey]*writeBatch{first: batch, second: nil},
	}
	w.consume(&writeEvent{watermark: 20, hasWatermark: true})
	w.finishBatches()
	require.Contains(t, w.mutations, first)
	require.Contains(t, w.mutations, second)
	require.EqualValues(t, 10, w.writtenBefore)
	// Completion advances the global boundary without a newer watermark.
	close(batch.done)
	w.finishBatches()
	require.Empty(t, w.mutations)
	require.Zero(t, w.memory.used())
}

func TestWriterReplayConfirmationWaitsForFlush(t *testing.T) {
	field := types.NewFieldType(mysql.TypeLonglong)
	field.AddFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, State: timodel.StatePublic, FieldType: *field}},
	})
	memory := newMemoryUsage()
	buffer := &assembler{
		memory: memory,
	}
	record := newAck(memory, 128, nil)
	input := &storageReader{memory: buffer.memory, records: []*ack{record}}
	var retained *event.DMLEvent
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
		retained = dml
	})
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	for range 2 {
		dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 99, 100, table)
		dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
		dml.Rows.AppendInt64(0, 1)
		dml.RowTypes = []common.RowType{common.RowTypeInsert}
		dml.Length = 1
		buffer.queueDML(&writeEvent{dml: dml}, record, nil)
		result := buffer.nextReady(100)
		w.pendingDML = append(w.pendingDML, result)
	}
	record.release()
	require.NoError(t, w.flushDML(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, w.waitBatch(ctx, w.inFlight[0]), context.Canceled)
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
	require.Len(t, input.records, 1)
	require.EqualValues(t, 2, record.refs.Load())
	retained.PostFlush()
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
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
	var value int64
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
		for row, ok := dml.GetNextRow(); ok; row, ok = dml.GetNextRow() {
			value = row.Row.GetInt64(1)
		}
		dml.PostFlush()
	}).Times(2)
	memory := newMemoryUsage()
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	input := &storageReader{memory: memory}
	w.consume(&writeEvent{watermark: 90, hasWatermark: true})
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
		dml.AddPostFlushFunc(func() {
			callbacks++
		})
		w.consume(&writeEvent{dml: dml})
		require.NoError(t, w.flushDML(t.Context()))
		w.finishBatches()
		require.NoError(t, input.Confirm(t.Context()))
		require.Equal(t, 1, callbacks)
		if index >= 1 {
			require.EqualValues(t, 20, value)
			require.Len(t, w.mutations, 2)
		}
		if index == 1 {
			// Another writer's files can replay older mutations in the next group.
			group := &readGroup{tableID: 1, boundary: &readBoundary{}}
			a := &assembler{}
			require.NoError(t, a.accept(t.Context(), decodedData{
				data:    &readData{group: group, groupEnd: true},
				decoded: true,
			}))
			require.True(t, group.boundary.reached)
		}
	}
	w.consume(&writeEvent{watermark: 201, hasWatermark: true})
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
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
	w := &writer{memory: newMemoryUsage(), mutations: map[mutationKey]*writeBatch{
		{tableID: 1, commitTs: 100, rowType: common.RowTypeUpdate, handle: string([]byte{1, '1'})}: nil,
	}}
	callbacks := 0
	dml.AddPostFlushFunc(func() {
		callbacks++
	})
	filtered, err := w.filterRows(t.Context(), dml, batch)
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
	downstream.EXPECT().AddDMLEvent(a).Do(func(dml *event.DMLEvent) {
		dml.PostFlush()
	})
	other := &writeBatch{items: []*writeEvent{{dml: b}}, done: make(chan struct{})}
	control := &writeEvent{ddl: ddl}
	w := &writer{
		downstream: downstream, memory: newMemoryUsage(), pendingDML: []*writeEvent{{dml: a}},
		inFlight: []*writeBatch{other}, inFlightEvents: 1,
		pendingDDL: []*writeEvent{control}, ddlJobs: make(chan *writeEvent, 1),
	}
	require.NoError(t, w.flushDML(t.Context()))
	select {
	case result := <-w.ddlJobs:
		require.Same(t, control, result)
	default:
		t.Fatal("DDL waited for an unrelated table")
	}
	require.Empty(t, w.pendingDML)
	require.Equal(t, []*writeBatch{other}, w.inFlight)
}

func TestWriterDDLReplay(t *testing.T) {
	ctx := t.Context()
	memory := newMemoryUsage()
	a := &assembler{
		memory: memory,
	}
	w := &writer{memory: memory, ddlJobs: make(chan *writeEvent, 1)}
	truncate := &event.DDLEvent{SchemaName: "test", TableName: "table_3", FinishedTs: 100, Query: "TRUNCATE TABLE `test`.`table_3`"}
	drop := &event.DDLEvent{SchemaName: "test", TableName: "table_3", FinishedTs: 200, Query: "DROP TABLE `test`.`table_3`"}
	first := &event.DDLEvent{SchemaName: "test", TableName: "table_3", FinishedTs: 300, Query: "RENAME TABLE `test`.`table_3` TO `test`.`tmp`"}
	second := &event.DDLEvent{SchemaName: "test", TableName: "table_3", FinishedTs: 300, Query: "RENAME TABLE `test`.`other` TO `test`.`table_3`"}
	downstream := mock.NewMockSink(gomock.NewController(t))
	for _, ddl := range []*event.DDLEvent{truncate, drop, first, second} {
		downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil)
		downstream.EXPECT().WriteBlockEvent(ddl).Return(nil)
	}
	w.downstream = downstream
	for index, ddl := range []*event.DDLEvent{truncate, drop, truncate, first, second, first} {
		record := newAck(memory, 128, nil)
		a.queueDDL(ddl, record)
		w.consume(a.nextReady(ddl.GetCommitTs()))
		record.release()
		require.NoError(t, w.flushDML(ctx))
		if index == 2 || index == 5 {
			require.Empty(t, w.ddlJobs, "a replay must not execute again")
			require.Zero(t, record.refs.Load())

			continue
		}
		require.Len(t, w.ddlJobs, 1)
		job := <-w.ddlJobs
		var duplicate *ack
		if index == 0 {
			duplicate = newAck(memory, 128, nil)
			a.queueDDL(ddl, duplicate)
			w.consume(a.nextReady(ddl.GetCommitTs()))
			duplicate.release()
			require.NoError(t, w.flushDML(ctx))
			require.EqualValues(t, 1, duplicate.refs.Load(), "a replay must wait for the original write")
		}
		require.NoError(t, w.writeDDL(ctx, job))
		w.ddlInFlight = nil
		w.progressChanged = true
		require.NoError(t, w.flushDML(ctx))
		require.Empty(t, w.ddlJobs)
		require.Zero(t, record.refs.Load())

		if duplicate != nil {
			require.Zero(t, duplicate.refs.Load())
		}
	}
	// Equality stays protected by the replay identity. Once progress passes it,
	// the identity is retired and the old input is rejected by writtenBefore.
	checkpoint := false
	w.consume(&writeEvent{
		watermark:    300,
		hasWatermark: true,
		onFlush: func() {
			checkpoint = true
		},
	})
	record := newAck(memory, 128, nil)
	a.queueDDL(first, record)
	w.consume(a.nextReady(300))
	record.release()
	require.NoError(t, w.flushDML(ctx))
	require.True(t, checkpoint, "retiring the last replay must unblock the watermark")
	require.Empty(t, w.ddlJobs)
	require.Zero(t, record.refs.Load())

	require.Zero(t, memory.used())
	w.consume(&writeEvent{watermark: 301, hasWatermark: true})
	require.NoError(t, w.flushDML(ctx))
	require.Zero(t, memory.used())
	record = newAck(memory, 128, nil)
	a.queueDDL(truncate, record)
	w.consume(a.nextReady(301))
	record.release()
	require.NoError(t, w.flushDML(ctx))
	require.Empty(t, w.ddlJobs)
	require.Zero(t, record.refs.Load())

	require.Zero(t, memory.used())
}

func TestWriterDDLDoesNotBlockIndependentTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	memory := newMemoryUsage()
	completed := make(chan struct{}, 1)
	var events []*event.DMLEvent
	for index, table := range []string{"a", "a", "b"} {
		info := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
			ID: int64(index + 1), Name: ast.NewCIStr(table),
			Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
		})
		id := int64(1)
		if table == "b" {
			id = 2
		}
		dml := event.NewDMLEvent(common.NewDispatcherID(), id, 0, uint64(10+index*10), info)
		dml.Rows = chunk.NewChunkWithCapacity(info.GetFieldSlice(), 1)
		dml.Rows.AppendInt64(0, 1)
		dml.RowTypes, dml.Length = []common.RowType{common.RowTypeInsert}, 1
		events = append(events, dml)
	}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "a", FinishedTs: 15}
	ddlEntered, releaseDDL, independent := make(chan struct{}), make(chan struct{}), make(chan struct{})
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(events[0])
	downstream.EXPECT().AddDMLEvent(events[2]).Do(func(dml *event.DMLEvent) {
		close(independent)
		dml.PostFlush()
	})
	downstream.EXPECT().AddDMLEvent(events[1]).Do(func(dml *event.DMLEvent) {
		dml.PostFlush()
	})
	downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil)
	downstream.EXPECT().WriteBlockEvent(ddl).DoAndReturn(func(*event.DDLEvent) error {
		close(ddlEntered)
		select {
		case <-releaseDDL:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	w := &writer{
		downstream: downstream,
		memory:     memory,
		completed:  completed,
		mutations:  make(map[mutationKey]*writeBatch),
		ddlJobs:    make(chan *writeEvent, 1),
		ddlDone:    make(chan struct{}, 1),
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = w.executeDDL(ctx)
	})
	defer func() {
		cancel()
		wg.Wait()
	}()
	w.consume(&writeEvent{dml: events[0], sequential: true})
	require.NoError(t, w.flushDML(ctx))
	w.consume(&writeEvent{ddl: ddl})
	require.NoError(t, w.flushDML(ctx))
	select {
	case <-ddlEntered:
		t.Fatal("DDL overtook an unfinished affected write")
	default:
	}
	events[0].PostFlush()
	require.NoError(t, w.flushDML(ctx))
	select {
	case <-ddlEntered:
	case <-ctx.Done():
		t.Fatal("DDL did not start after its affected write completed")
	}
	replayConfirmed := false
	w.consume(&writeEvent{
		ddl: ddl,
		onFlush: func() {
			replayConfirmed = true
		},
	})
	w.consume(&writeEvent{dml: events[1], sequential: true})
	w.consume(&writeEvent{dml: events[2], sequential: true})
	checkpoint := false
	w.consume(&writeEvent{
		hasWatermark: true,
		watermark:    15,
		onFlush: func() {
			checkpoint = true
		},
	})
	require.NoError(t, w.flushDML(ctx))
	select {
	case <-independent:
	case <-ctx.Done():
		t.Fatal("an independent table was blocked by the running DDL")
	}
	require.Len(t, w.pendingDML, 1)
	require.False(t, replayConfirmed, "replay cannot be confirmed while its original DDL is running")
	require.False(t, checkpoint, "watermark cannot pass an unfinished DDL")
	select {
	case <-completed:
	default:
	}
	close(releaseDDL)
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("DDL did not complete")
	}
	require.NoError(t, w.flushDML(ctx))
	require.Empty(t, w.pendingDML)
	require.True(t, checkpoint)
	require.True(t, replayConfirmed)
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
		downstream.EXPECT().AddDMLEvent(first).Do(func(*event.DMLEvent) {
			cancel()
		}),
		downstream.EXPECT().AddDMLEvent(second).Do(func(dml *event.DMLEvent) {
			dml.PostFlush()
		}),
	)
	w := &writer{downstream: downstream, memory: newMemoryUsage(), pendingDML: []*writeEvent{{dml: first}, {dml: second}}}
	require.ErrorIs(t, w.flushDML(ctx), context.Canceled)
	require.Len(t, w.inFlight, 1)
	require.Len(t, w.inFlight[0].items, 1)
	require.Len(t, w.pendingDML, 1)
	first.PostFlush()
	require.NoError(t, w.flushDML(t.Context()))
	require.Empty(t, w.inFlight)
	require.Empty(t, w.pendingDML)
}
