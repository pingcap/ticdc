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

package main

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/util"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
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
	require.Equal(t, 3, w.getReplayFilter().Len())
	w.progresses[0].watermark = 101
	// An unrelated batch must not discard boundary state while a previously
	// buffered replay is still waiting in another group.
	require.NoError(t, w.flushDMLBatch(t.Context(), nil))
	flush(1, 2)
	require.Equal(t, []int64{1, 2, 3}, handles)
	w.spillStore = util.NewSpillStore()
	t.Cleanup(func() { require.NoError(t, w.spillStore.Cleanup()) })
	require.NoError(t, w.flushDMLEventsByWatermark(t.Context()))
	require.Zero(t, w.getReplayFilter().Len())
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
	require.Zero(t, w.getReplayFilter().Len())
	require.Zero(t, callbacks)
}
