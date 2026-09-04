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
	"fmt"
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

func attachTestDMLMessageData(message *codeccommon.DMLMessage) *codeccommon.DMLMessage {
	messageData := codeccommon.NewDMLMessageData(nil, nil,
		func([]byte, uint64) (*codeccommon.DMLMessage, error) {
			return message, nil
		},
	)
	messageData.AttachDMLMessage(message)
	return message
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

func newMergeTestTableInfo(tableID int64, updateTS uint64, columnCount int) *common.TableInfo {
	columns := make([]*model.ColumnInfo, columnCount)
	for i := range columns {
		columns[i] = &model.ColumnInfo{
			ID:        int64(i + 1),
			Offset:    i,
			Name:      ast.NewCIStr(fmt.Sprintf("c%d", i)),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}
	}
	return common.WrapTableInfo("test", &model.TableInfo{
		ID:       tableID,
		Name:     ast.NewCIStr("t"),
		UpdateTS: updateTS,
		Columns:  columns,
	})
}

func newMergeTestDMLEvent(commitTs uint64, tableInfo *common.TableInfo, value int64) *commonEvent.DMLEvent {
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	for column := range tableInfo.GetFieldSlice() {
		rows.AppendInt64(column, value)
	}
	return &commonEvent.DMLEvent{
		DispatcherID:     common.DispatcherID{Low: 1},
		PhysicalTableID:  tableInfo.TableName.TableID,
		StartTs:          commitTs - 1,
		CommitTs:         commitTs,
		Length:           1,
		RowTypes:         []common.RowType{common.RowTypeInsert},
		Rows:             rows,
		TableInfo:        tableInfo,
		TableInfoVersion: tableInfo.GetUpdateTS(),
	}
}

func newMergeTestDMLMessage(event *commonEvent.DMLEvent) *codeccommon.DMLMessage {
	return codeccommon.NewDMLMessage(event.GetTableID(), event.TableInfo.GetSchemaName(), event.TableInfo.GetTableName(),
		event.GetCommitTs(), event.RowTypes[0], func() *commonEvent.DMLEvent { return event })
}

func TestAppendOrMergeDMLEvent(t *testing.T) {
	t.Run("merge compatible events", func(t *testing.T) {
		tableInfo := newMergeTestTableInfo(1, 10, 1)
		first := newMergeTestDMLEvent(100, tableInfo, 1)
		second := newMergeTestDMLEvent(100, tableInfo, 2)
		first.RowKeys = [][]byte{[]byte("first")}
		second.RowKeys = [][]byte{[]byte("second")}
		first.Checksum = []*integrity.Checksum{{Current: 1}}
		second.Checksum = []*integrity.Checksum{{Current: 2}}
		var flushed []int
		first.AddPostFlushFunc(func() { flushed = append(flushed, 1) })
		second.AddPostFlushFunc(func() { flushed = append(flushed, 2) })

		events := DMLMessagesToEvents([]*codeccommon.DMLMessage{
			newMergeTestDMLMessage(first),
			newMergeTestDMLMessage(second),
		})

		require.Len(t, events, 1)
		require.Same(t, first, events[0])
		require.Equal(t, int32(2), first.Length)
		require.Equal(t, 2, first.Rows.NumRows())
		require.Equal(t, []byte("second"), first.RowKeys[1])
		require.Equal(t, uint32(2), first.Checksum[1].Current)
		first.PostFlush()
		require.Equal(t, []int{1, 2}, flushed)
	})

	t.Run("keep different commit timestamps separate", func(t *testing.T) {
		tableInfo := newMergeTestTableInfo(1, 10, 1)
		first := newMergeTestDMLEvent(100, tableInfo, 1)
		second := newMergeTestDMLEvent(101, tableInfo, 2)

		events := DMLMessagesToEvents([]*codeccommon.DMLMessage{
			newMergeTestDMLMessage(first),
			newMergeTestDMLMessage(second),
		})

		require.Len(t, events, 2)
	})

	t.Run("merge compatible events restored from spill", func(t *testing.T) {
		tableInfo := newMergeTestTableInfo(1, 10, 1)
		group := NewEventsGroup(0, 1)
		first := newMergeTestDMLEvent(100, tableInfo, 1)
		second := newMergeTestDMLEvent(100, tableInfo, 2)
		firstMessage := newMergeTestDMLMessage(first)
		secondMessage := newMergeTestDMLMessage(second)
		require.NoError(t, group.AppendMessage(attachTestDMLMessageData(firstMessage)))
		require.NoError(t, group.AppendMessage(attachTestDMLMessageData(secondMessage)))

		messages, err := group.GetAllMessages()
		require.NoError(t, err)
		require.Len(t, messages, 2)
		events := DMLMessagesToEvents(messages)

		require.Len(t, events, 1)
		require.Equal(t, 2, events[0].Rows.NumRows())
	})
}

func TestEventsGroupSharesRawMessageData(t *testing.T) {
	first := newTestDMLMessage(10)
	second := newTestDMLMessage(10)
	messageData := codeccommon.NewDMLMessageData(nil, []byte("raw message"),
		func(_ []byte, dmlIndex uint64) (*codeccommon.DMLMessage, error) {
			return []*codeccommon.DMLMessage{first, second}[dmlIndex], nil
		},
	)

	group := NewEventsGroup(0, 1)
	messageData.AttachDMLMessage(first)
	require.NoError(t, group.AppendMessage(first))
	messageData.AttachDMLMessage(second)
	require.NoError(t, group.AppendMessage(second))
	require.Len(t, group.messages, 2)
	require.Equal(t, group.messages[0].handle, group.messages[1].handle)
	require.Equal(t, uint64(0), group.messages[0].dmlIndex)
	require.Equal(t, uint64(1), group.messages[1].dmlIndex)

	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Same(t, first, messages[0])
	require.Same(t, second, messages[1])
}

func TestEventsGroupRestoresSharedSpillInputOnce(t *testing.T) {
	// A single canal-json input can contain thousands of DML messages. Restoring
	// each ordinal must not re-decode the complete input.
	inputMessages := []*codeccommon.DMLMessage{
		newTestDMLMessage(30),
		newTestDMLMessage(10),
		newTestDMLMessage(20),
	}
	var decoderCount int
	messageData := NewDMLMessageDataWithDecoderFactory(nil, []byte("raw message"),
		func(_, _ []byte) (codeccommon.Decoder, error) {
			decoderCount++
			return &dmlMessageDecoderStub{messages: []*codeccommon.DMLMessage{
				newTestDMLMessage(30),
				newTestDMLMessage(10),
				newTestDMLMessage(20),
			}}, nil
		})

	group := NewEventsGroup(0, 1)
	for _, message := range inputMessages {
		messageData.AttachDMLMessage(message)
		require.NoError(t, group.AppendMessage(message))
	}

	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Equal(t, 1, decoderCount)
	require.Equal(t, []uint64{10, 20, 30}, []uint64{
		messages[0].GetCommitTs(), messages[1].GetCommitTs(), messages[2].GetCommitTs(),
	})
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
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m1)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m2)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m3)))

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
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m1)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m2)))

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
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m1)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m2)))

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
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m1)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m2)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m3)))

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
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m1)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m2)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m3)))

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
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m1)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m2)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m3)))

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
	message := codeccommon.NewDMLMessageFromEvent(event)
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(message)))

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
		},
	})
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 4)
	for i := range int64(4) {
		rows.AppendInt64(0, i)
	}

	group := NewEventsGroup(0, 1)
	for _, offset := range []int{0, 2} {
		event := commonEvent.NewDMLEvent(common.NewDispatcherID(), 1, 90, 100, tableInfo)
		event.Rows = rows
		// A decoded update occupies two RowTypes entries, matching its before
		// and after rows in the shared chunk.
		event.RowTypes = []common.RowType{common.RowTypeUpdate, common.RowTypeUpdate}
		event.Length = 1
		event.PreviousTotalOffset = offset
		message := codeccommon.NewDMLMessageFromEvent(event)
		require.NoError(t, group.AppendMessage(attachTestDMLMessageData(message)))
	}

	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Zero(t, messages[0].ToDMLEvent().PreviousTotalOffset)
	require.Equal(t, 2, messages[1].ToDMLEvent().PreviousTotalOffset)
	for _, message := range messages {
		require.Equal(t, 4, message.ToDMLEvent().Rows.NumRows())
	}

	second := messages[1].ToDMLEvent()
	row, ok := second.GetNextRow()
	require.True(t, ok)
	require.Equal(t, int64(2), row.PreRow.GetInt64(0))
	require.Equal(t, int64(3), row.Row.GetInt64(0))
	_, ok = second.GetNextRow()
	require.False(t, ok)
}

func TestEventsGroupRestoresCompactUpdateRows(t *testing.T) {
	tableInfo := common.WrapTableInfo("test", &model.TableInfo{
		ID:   1,
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	})
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	rows.AppendInt64(0, 1)
	rows.AppendInt64(0, 2)

	// The Avro decoder represents an update with one RowType even though the
	// chunk still contains both before and after rows.
	event := commonEvent.NewDMLEvent(common.NewDispatcherID(), 1, 90, 100, tableInfo)
	event.Rows = rows
	event.RowTypes = []common.RowType{common.RowTypeUpdate}
	event.Length = 1

	group := NewEventsGroup(0, 1)
	message := codeccommon.NewDMLMessageFromEvent(event)
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(message)))
	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Len(t, messages, 1)

	restored := messages[0].ToDMLEvent()
	require.Equal(t, 2, restored.Rows.NumRows())
	row, ok := restored.GetNextRow()
	require.True(t, ok)
	require.Equal(t, int64(1), row.PreRow.GetInt64(0))
	require.Equal(t, int64(2), row.Row.GetInt64(0))
	_, ok = restored.GetNextRow()
	require.False(t, ok)
}

func TestEventsGroupSpillDoesNotSignalDownstreamCallbacks(t *testing.T) {
	event := newTestDMLEvent(100, common.RowTypeInsert)
	var enqueued, flushed int
	event.AddPostEnqueueFunc(func() { enqueued++ })
	event.AddPostFlushFunc(func() { flushed++ })

	group := NewEventsGroup(0, 1)
	defer func() { require.NoError(t, group.Cleanup()) }()
	message := codeccommon.NewDMLMessageFromEvent(event)
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(message)))
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
					if err := group.AppendMessage(attachTestDMLMessageData(message)); err != nil {
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
