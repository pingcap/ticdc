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
	"os"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func newTestDMLMessage(commitTs uint64) *codeccommon.DMLMessage {
	return codeccommon.NewDMLMessageFromEvent(newTestDMLEvent(commitTs, common.RowTypeInsert))
}

func newTestDMLEvent(commitTs uint64, rowTypes ...common.RowType) *commonEvent.DMLEvent {
	return &commonEvent.DMLEvent{
		PhysicalTableID: 1,
		CommitTs:        commitTs,
		Length:          int32(len(rowTypes)),
		RowTypes:        rowTypes,
		Rows:            chunk.NewChunkWithCapacity(nil, 0),
	}
}

func TestEventsGroupResolveIntoAppendsAndCleansResolvedSpillRecords(t *testing.T) {
	// Scenario: A consumer resolves events by watermark/commit-ts and appends them into a downstream
	// batch slice. Buffered messages are held only by a spill file, and the file must be cleaned once
	// all of its records have been resolved.
	//
	// Steps:
	//  1. Append 3 events with increasing CommitTs.
	//  2. Call ResolveInto with resolve=2 and a nil dst.
	//  3. Verify (a) returned events are correct, (b) group keeps only the remaining event,
	//     (c) the file survives the partial resolve.
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	m3 := newTestDMLMessage(3)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	spillPath := group.spillFile.Path()

	var dst []*codeccommon.DMLMessage
	dst, err := group.ResolveInto(2, dst)
	require.NoError(t, err)

	require.Len(t, dst, 2)
	require.Equal(t, m1.GetCommitTs(), dst[0].GetCommitTs())
	require.Equal(t, m2.GetCommitTs(), dst[1].GetCommitTs())

	require.Len(t, group.messages, 1)
	require.Equal(t, m3.GetCommitTs(), group.messages[0].commitTs)
	require.FileExists(t, spillPath)

	_, err = group.GetAllMessages()
	require.NoError(t, err)
	require.Nil(t, group.spillFile)
	_, err = os.Stat(spillPath)
	require.True(t, os.IsNotExist(err))
}

func TestEventsGroupResolveIntoNoopWhenNothingResolved(t *testing.T) {
	// Scenario: resolveTs is behind all buffered events.
	// Expectation: ResolveInto should be a no-op (dst unchanged, group unchanged).
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(10)
	m2 := newTestDMLMessage(20)
	group.AppendMessage(m1)
	group.AppendMessage(m2)

	dst := make([]*codeccommon.DMLMessage, 0, 1)
	dst, err := group.ResolveInto(5, dst)
	require.NoError(t, err)

	require.Len(t, dst, 0)
	require.Len(t, group.messages, 2)
	require.Equal(t, m1.GetCommitTs(), group.messages[0].commitTs)
	require.Equal(t, m2.GetCommitTs(), group.messages[1].commitTs)
}

func TestEventsGroupResolveIntoClearsAllWhenFullyResolved(t *testing.T) {
	// Scenario: resolveTs advances beyond all buffered events.
	// Expectation: group is emptied and all backing-array pointers for resolved events are cleared.
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	group.AppendMessage(m1)
	group.AppendMessage(m2)

	spillPath := group.spillFile.Path()
	var dst []*codeccommon.DMLMessage
	dst, err := group.ResolveInto(100, dst)
	require.NoError(t, err)

	require.Len(t, dst, 2)
	require.Equal(t, m1.GetCommitTs(), dst[0].GetCommitTs())
	require.Equal(t, m2.GetCommitTs(), dst[1].GetCommitTs())

	require.Len(t, group.messages, 0)
	require.Nil(t, group.spillFile)
	_, err = os.Stat(spillPath)
	require.True(t, os.IsNotExist(err))
}

func TestEventsGroupResolveIntoSortsOutOfOrderResolvedMessages(t *testing.T) {
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(20)
	m2 := newTestDMLMessage(10)
	m3 := newTestDMLMessage(30)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	var dst []*codeccommon.DMLMessage
	dst, err := group.ResolveInto(25, dst)
	require.NoError(t, err)

	require.Len(t, dst, 2)
	require.Equal(t, m2.GetCommitTs(), dst[0].GetCommitTs())
	require.Equal(t, m1.GetCommitTs(), dst[1].GetCommitTs())

	require.Len(t, group.messages, 1)
	require.Equal(t, m3.GetCommitTs(), group.messages[0].commitTs)
}

func TestEventsGroupResolveIntoKeepsSameCommitTsStable(t *testing.T) {
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(20)
	m2 := newTestDMLMessage(10)
	m3 := newTestDMLMessage(20)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	var dst []*codeccommon.DMLMessage
	dst, err := group.ResolveInto(20, dst)
	require.NoError(t, err)

	require.Len(t, dst, 3)
	require.Equal(t, m2.GetCommitTs(), dst[0].GetCommitTs())
	require.Equal(t, m1.GetCommitTs(), dst[1].GetCommitTs())
	require.Equal(t, m3.GetCommitTs(), dst[2].GetCommitTs())
	require.Empty(t, group.messages)
}

func TestEventsGroupGetAllMessagesSortsOutOfOrderMessages(t *testing.T) {
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(20)
	m2 := newTestDMLMessage(10)
	m3 := newTestDMLMessage(30)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	messages, err := group.GetAllMessages()
	require.NoError(t, err)

	require.Len(t, messages, 3)
	require.Equal(t, m2.GetCommitTs(), messages[0].GetCommitTs())
	require.Equal(t, m1.GetCommitTs(), messages[1].GetCommitTs())
	require.Equal(t, m3.GetCommitTs(), messages[2].GetCommitTs())
	require.Empty(t, group.messages)
}

func TestEventsGroupRestoresSpilledEventRowsAndTableInfo(t *testing.T) {
	tableInfo := common.WrapTableInfo("test", &model.TableInfo{
		ID:   1,
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
		},
	})
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	rows.AppendInt64(0, 42)
	event := commonEvent.NewDMLEvent(common.NewDispatcherID(), 1, 90, 100, tableInfo)
	event.Rows = rows
	event.RowTypes = []common.RowType{common.RowTypeInsert}
	event.Length = 1
	event.TableInfoVersion = 88
	event.ReplicatingTs = 99
	event.Checksum = []*integrity.Checksum{{Current: 1, Previous: 2, Corrupted: true, Version: 3}}

	group := NewEventsGroup(0, 1)
	group.AppendMessage(codeccommon.NewDMLMessageFromEvent(event))

	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Len(t, messages, 1)
	restored := messages[0].ToDMLEvent()
	require.Equal(t, uint64(100), restored.CommitTs)
	require.Equal(t, uint64(88), restored.TableInfoVersion)
	require.Equal(t, uint64(99), restored.ReplicatingTs)
	require.Equal(t, event.Checksum, restored.Checksum)
	require.Equal(t, "test", restored.TableInfo.GetSchemaName())
	require.Equal(t, "t", restored.TableInfo.GetTableName())
	require.Equal(t, int64(42), restored.Rows.GetRow(0).GetInt64(0))
}

func TestEventsGroupRestoresRowsFromSharedChunk(t *testing.T) {
	tableInfo := common.WrapTableInfo("test", &model.TableInfo{
		ID:   1,
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)},
			{ID: 2, Name: ast.NewCIStr("v1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	})
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 4)
	for _, value := range []struct {
		null  bool
		value int64
	}{
		{null: true}, {value: 1}, {null: true}, {value: 2},
	} {
		rows.AppendInt64(0, 42)
		if value.null {
			rows.AppendNull(1)
		} else {
			rows.AppendInt64(1, value.value)
		}
	}

	group := NewEventsGroup(0, 1)
	for _, offset := range []int{0, 2} {
		event := commonEvent.NewDMLEvent(common.NewDispatcherID(), 1, 90, 100, tableInfo)
		event.Rows = rows
		event.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
		event.Length = 2
		event.PreviousTotalOffset = offset
		require.NoError(t, group.AppendMessage(codeccommon.NewDMLMessageFromEvent(event)))
	}

	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Len(t, messages, 2)
	for _, message := range messages {
		restored := message.ToDMLEvent()
		require.Zero(t, restored.PreviousTotalOffset)
		require.Len(t, restored.RowTypes, restored.Rows.NumRows())
	}

	second := messages[1].ToDMLEvent()
	require.True(t, second.Rows.GetRow(0).IsNull(1))
	require.Equal(t, int64(2), second.Rows.GetRow(1).GetInt64(1))

	var events []*commonEvent.DMLEvent
	for _, message := range messages {
		events = AppendOrMergeDMLEvent(events, message.ToDMLEvent())
	}
	require.Len(t, events, 1)
	require.Len(t, events[0].RowTypes, events[0].Rows.NumRows())
	require.True(t, events[0].Rows.GetRow(0).IsNull(1))
	require.Equal(t, int64(1), events[0].Rows.GetRow(1).GetInt64(1))
	require.True(t, events[0].Rows.GetRow(2).IsNull(1))
	require.Equal(t, int64(2), events[0].Rows.GetRow(3).GetInt64(1))
}

func TestEventsGroupSpillDoesNotSignalDownstreamCallbacks(t *testing.T) {
	event := newTestDMLEvent(100, common.RowTypeInsert)
	var enqueued, flushed int
	event.AddPostEnqueueFunc(func() { enqueued++ })
	event.AddPostFlushFunc(func() { flushed++ })

	group := NewEventsGroup(0, 1)
	defer func() { require.NoError(t, group.Cleanup()) }()
	require.NoError(t, group.AppendMessage(codeccommon.NewDMLMessageFromEvent(event)))
	require.Zero(t, enqueued)
	require.Zero(t, flushed)
}

func BenchmarkEventsGroupResolveInto(b *testing.B) {
	const messageCount = 16 * 1024

	messages := make([]*codeccommon.DMLMessage, messageCount)
	for i := range messages {
		messages[i] = newTestDMLMessage(uint64(i + 1))
	}

	benchmarks := []struct {
		name       string
		resolveTs  uint64
		outOfOrder bool
	}{
		{name: "ordered/noop", resolveTs: 0},
		{name: "ordered/half", resolveTs: messageCount / 2},
		{name: "ordered/all", resolveTs: messageCount},
		{name: "out-of-order/all", resolveTs: messageCount, outOfOrder: true},
	}

	oldLogLevel := log.GetLevel()
	log.SetLevel(zapcore.FatalLevel)
	b.Cleanup(func() { log.SetLevel(oldLogLevel) })

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			source := messages
			if benchmark.outOfOrder {
				source = append([]*codeccommon.DMLMessage(nil), messages...)
				lastIndex := len(source) - 1
				source[lastIndex-1], source[lastIndex] = source[lastIndex], source[lastIndex-1]
			}
			dst := make([]*codeccommon.DMLMessage, 0, messageCount)

			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for b.Loop() {
				group := NewEventsGroup(0, 1)
				for _, message := range source {
					if err := group.AppendMessage(message); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				var err error
				dst, err = group.ResolveInto(benchmark.resolveTs, dst[:0])
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if err := group.Cleanup(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestAppendOrMergeDMLEventMergesSameCommitTs(t *testing.T) {
	var flushed []int
	e1 := newTestDMLEvent(10, common.RowTypeInsert)
	e1.AddPostFlushFunc(func() { flushed = append(flushed, 1) })
	e2 := newTestDMLEvent(10, common.RowTypeDelete)
	e2.AddPostFlushFunc(func() { flushed = append(flushed, 2) })

	events := AppendOrMergeDMLEvent(nil, e1)
	events = AppendOrMergeDMLEvent(events, e2)

	require.Len(t, events, 1)
	require.Same(t, e1, events[0])
	require.Equal(t, int32(2), events[0].Length)
	require.Equal(t, []common.RowType{common.RowTypeInsert, common.RowTypeDelete}, events[0].RowTypes)

	events[0].PostFlush()
	require.Equal(t, []int{1, 2}, flushed)
}

func TestAppendOrMergeDMLEventAppendsDifferentCommitTs(t *testing.T) {
	e1 := newTestDMLEvent(10, common.RowTypeInsert)
	e2 := newTestDMLEvent(20, common.RowTypeDelete)

	events := AppendOrMergeDMLEvent(nil, e1)
	events = AppendOrMergeDMLEvent(events, e2)

	require.Len(t, events, 2)
	require.Same(t, e1, events[0])
	require.Same(t, e2, events[1])
}
