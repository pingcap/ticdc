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
	w := &writer{downstream: downstream, memory: &bufferUsage{}, mutations: make(map[mutationKey]*writeBatch)}
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
		w.pendingDML = append(w.pendingDML, &pendingDML{event: dml})
		require.NoError(t, w.flushDML(t.Context(), t.Context(), 100, true, true))
		require.Equal(t, 1, callbacks)
		w.advanceReplay(100, 0)
	}
	require.Equal(t, []int64{1, 2, 3}, handles)
	require.Len(t, w.mutations, 3)
	w.advanceReplay(101, 0)
	require.Empty(t, w.mutations)
	require.Zero(t, w.memory.bytes.Load())
	filtered, err := w.filterRows(t.Context(), &event.DMLEvent{CommitTs: 100}, &writeBatch{})
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
	memory := &bufferUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.newRecord(128)
	require.NoError(t, err)
	input := &storageReader{buffer: buffer, records: []*inputRecord{record}}
	var retained *event.DMLEvent
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) { retained = dml })
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	c := &consumer{reader: input, writer: w}
	w.confirm = c.confirmCompleted
	for range 2 {
		dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 99, 100, table)
		dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
		dml.Rows.AppendInt64(0, 1)
		dml.RowTypes = []common.RowType{common.RowTypeInsert}
		dml.Length = 1
		require.NoError(t, buffer.queueDML(dml, []*inputRecord{record}, nil))
		result := buffer.nextReady(100)
		w.pendingDML = append(w.pendingDML, &pendingDML{event: result.dml, bytes: result.bytes})
		w.dmlBytes += result.bytes
	}
	record.pending.Add(-1)
	memory.effects.Add(-1)
	require.NoError(t, w.flushDML(t.Context(), t.Context(), 100, true, true))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, w.waitBatch(ctx, w.inFlight[0]), context.Canceled)
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Len(t, input.records, 1)
	require.EqualValues(t, 2, record.pending.Load())
	retained.PostFlush()
	w.finishBatches()
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, input.records)
	require.Zero(t, memory.effects.Load())
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
	w := &writer{memory: &bufferUsage{}, mutations: map[mutationKey]*writeBatch{
		{tableID: 1, commitTs: 100, rowType: common.RowTypeUpdate, handle: string([]byte{1, '1'})}: nil,
	}}
	callbacks := 0
	dml.AddPostFlushFunc(func() { callbacks++ })
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
