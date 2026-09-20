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
	"math"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/integrity"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/spill"
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
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			return []*codeccommon.DMLMessage{message}, nil
		},
	)
	messageData.AttachDMLMessage(message)
	return message
}

func attachTestDMLMessageDataWithPayload(
	message *codeccommon.DMLMessage, key, value []byte,
) *codeccommon.DMLMessage {
	messageData := codeccommon.NewDMLMessageData(key, value,
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			return []*codeccommon.DMLMessage{message}, nil
		},
	)
	messageData.AttachDMLMessage(message)
	return message
}

func readGroupIndex(t *testing.T, group *EventsGroup) []spilledMessage {
	return readGroupIndexFrom(t, group, group.indexCursor)
}

func readGroupIndexFrom(t *testing.T, group *EventsGroup, lower []byte) []spilledMessage {
	t.Helper()
	require.NoError(t, group.store.flushEventIndex())
	_, upper := eventIndexBounds(group.id)
	iterator, err := group.store.index.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	require.NoError(t, err)
	defer func() { require.NoError(t, iterator.Close()) }()

	entries := make([]spilledMessage, 0)
	for valid := iterator.First(); valid; valid = iterator.Next() {
		entry, err := decodeSpilledMessage(iterator.Key(), iterator.Value())
		require.NoError(t, err)
		entries = append(entries, entry)
	}
	require.NoError(t, iterator.Error())
	return entries
}

func TestEventsGroupResolvesFromAppliedIndexCursor(t *testing.T) {
	config := defaultSpillConfig()
	config.resolveBatchMessages = 1
	config.indexCleanupMessages = 1000
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)

	const messageCount = 129
	for commitTs := uint64(1); commitTs <= messageCount; commitTs++ {
		require.NoError(t, group.AppendMessage(
			attachTestDMLMessageData(newTestDMLMessage(commitTs))))
	}
	for resolveTs := uint64(1); resolveTs < messageCount; resolveTs++ {
		batch, _, err := group.PrepareResolve(resolveTs, store.ResolveLimit())
		require.NoError(t, err)
		require.NoError(t, batch.Ack())
	}

	require.Zero(t, store.indexDeleteRangeCount)
	lower, _ := eventIndexBounds(group.id)
	require.Len(t, readGroupIndexFrom(t, group, lower), messageCount)
	entries := readGroupIndex(t, group)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(messageCount), entries[0].commitTs)
}

func TestEventsGroupCoalescesAppliedIndexCleanup(t *testing.T) {
	config := defaultSpillConfig()
	config.resolveBatchMessages = 1
	config.indexCleanupMessages = 64
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)

	const messageCount = 257
	for commitTs := uint64(1); commitTs <= messageCount; commitTs++ {
		require.NoError(t, group.AppendMessage(
			attachTestDMLMessageData(newTestDMLMessage(commitTs))))
	}

	for resolveTs := uint64(1); resolveTs < messageCount; resolveTs++ {
		batch, _, err := group.PrepareResolve(resolveTs, store.ResolveLimit())
		require.NoError(t, err)
		require.NotNil(t, batch)
		require.Len(t, batch.Messages, 1)
		require.Equal(t, resolveTs, batch.Messages[0].GetCommitTs())
		require.NoError(t, batch.Ack())
	}

	require.Equal(t, int64(4), store.indexDeleteRangeCount)
	require.Zero(t, group.indexCleanupCount)
	require.Equal(t, int64(1), group.pendingCount)
	entries := readGroupIndex(t, group)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(messageCount), entries[0].commitTs)
}

func TestEventsGroupRestoresLateEventBehindAppliedCursor(t *testing.T) {
	config := defaultSpillConfig()
	config.indexCleanupMessages = 100
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)

	for _, commitTs := range []uint64{10, 20, 30} {
		require.NoError(t, group.AppendMessage(
			attachTestDMLMessageData(newTestDMLMessage(commitTs))))
	}
	resolved, err := group.ResolveInto(20, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, []uint64{
		resolved[0].GetCommitTs(), resolved[1].GetCommitTs(),
	})
	require.Zero(t, store.indexDeleteRangeCount)

	require.NoError(t, group.AppendMessage(
		attachTestDMLMessageData(newTestDMLMessage(15))))
	require.Equal(t, int64(1), store.indexDeleteRangeCount)
	resolved, err = group.GetAllMessages()
	require.NoError(t, err)
	require.Equal(t, []uint64{15, 30}, []uint64{
		resolved[0].GetCommitTs(), resolved[1].GetCommitTs(),
	})
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

func TestEventsGroupMessagesToEventsMerge(t *testing.T) {
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

		events, err := mergeFragments(t, first, second)
		require.NoError(t, err)

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

		events, err := mergeFragments(t, first, second)
		require.NoError(t, err)

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
		events, err := mergeFragments(t, first, second)
		require.NoError(t, err)

		require.Len(t, events, 1)
		require.Equal(t, 2, events[0].Rows.NumRows())
	})
}

// newDedupTestTableInfo builds a two column table whose primary key is the
// handle key, so row mutations can be identified by it.
func newDedupTestTableInfo() *common.TableInfo {
	idFieldType := types.NewFieldType(mysql.TypeLonglong)
	idFieldType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	valueFieldType := types.NewFieldType(mysql.TypeLonglong)
	return common.WrapTableInfo("test", &model.TableInfo{
		ID:         1,
		Name:       ast.NewCIStr("t"),
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Offset: 0, Name: ast.NewCIStr("id"), State: model.StatePublic, FieldType: *idFieldType},
			{ID: 2, Offset: 1, Name: ast.NewCIStr("c"), State: model.StatePublic, FieldType: *valueFieldType},
		},
	})
}

// dedupTestRow is one physical row image: the handle key value and a payload
// value.
type dedupTestRow struct {
	key   int64
	value int64
}

// newDedupTestFragment builds one Kafka fragment of one transaction. For an
// update every pair of rows is the pre-image and the row image.
func newDedupTestFragment(
	commitTs uint64, tableInfo *common.TableInfo, rowType common.RowType, rows ...dedupTestRow,
) *commonEvent.DMLEvent {
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), len(rows))
	rowTypes := make([]common.RowType, 0, len(rows))
	for _, row := range rows {
		chk.AppendInt64(0, row.key)
		chk.AppendInt64(1, row.value)
		rowTypes = append(rowTypes, rowType)
	}
	// Length counts mutations, while RowTypes has one entry per physical row.
	mutations := len(rows)
	if rowType == common.RowTypeUpdate {
		mutations /= 2
	}
	return &commonEvent.DMLEvent{
		DispatcherID:     common.DispatcherID{Low: 1},
		PhysicalTableID:  tableInfo.TableName.TableID,
		StartTs:          commitTs - 1,
		CommitTs:         commitTs,
		Length:           int32(mutations),
		RowTypes:         rowTypes,
		Rows:             chk,
		TableInfo:        tableInfo,
		TableInfoVersion: tableInfo.GetUpdateTS(),
	}
}

// dedupRowMutation is one row mutation read back from a merged event.
type dedupRowMutation struct {
	rowType common.RowType
	key     int64
	value   int64
}

// readDedupRowMutations reads every mutation through GetNextRow, the API the sink
// uses, so the test also covers RowTypes and Checksum staying aligned with the
// rows that were kept.
func readDedupRowMutations(event *commonEvent.DMLEvent) []dedupRowMutation {
	mutations := make([]dedupRowMutation, 0, event.Len())
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		image := row.Row
		if row.RowType == common.RowTypeDelete {
			image = row.PreRow
		}
		mutations = append(mutations, dedupRowMutation{
			rowType: row.RowType,
			key:     image.GetInt64(0),
			value:   image.GetInt64(1),
		})
	}
	return mutations
}

// mergeFragments merges fragments through one events group, the way a
// consumer merges the messages of one table.
func mergeFragments(t *testing.T, fragments ...*commonEvent.DMLEvent) ([]*commonEvent.DMLEvent, error) {
	t.Helper()
	messages := make([]*codeccommon.DMLMessage, 0, len(fragments))
	for _, fragment := range fragments {
		messages = append(messages, newMergeTestDMLMessage(fragment))
	}
	return NewEventsGroup(0, 1).MessagesToEvents(messages)
}

func TestEventsGroupMessagesToEventsDropReplayedRows(t *testing.T) {
	tableInfo := newDedupTestTableInfo()

	t.Run("drop replay and keep its callbacks", func(t *testing.T) {
		first := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		replay := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		var enqueued, flushed []int
		first.AddPostEnqueueFunc(func() { enqueued = append(enqueued, 1) })
		replay.AddPostEnqueueFunc(func() { enqueued = append(enqueued, 2) })
		first.AddPostFlushFunc(func() { flushed = append(flushed, 1) })
		replay.AddPostFlushFunc(func() { flushed = append(flushed, 2) })

		events, err := mergeFragments(t, first, replay)
		require.NoError(t, err)

		require.Len(t, events, 1)
		require.Same(t, first, events[0])
		require.Equal(t, []dedupRowMutation{{rowType: common.RowTypeInsert, key: 1, value: 1}},
			readDedupRowMutations(events[0]))
		require.Equal(t, int32(1), events[0].Length)

		events[0].PostFlush()
		require.Equal(t, []int{1, 2}, enqueued)
		require.Equal(t, []int{1, 2}, flushed)
	})

	t.Run("drop only the replayed mutations of a fragment", func(t *testing.T) {
		first := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		partial := newDedupTestFragment(100, tableInfo, common.RowTypeInsert,
			dedupTestRow{key: 1, value: 1}, dedupTestRow{key: 2, value: 2})

		events, err := mergeFragments(t, first, partial)
		require.NoError(t, err)

		require.Len(t, events, 1)
		require.Equal(t, []dedupRowMutation{
			{rowType: common.RowTypeInsert, key: 1, value: 1},
			{rowType: common.RowTypeInsert, key: 2, value: 2},
		}, readDedupRowMutations(events[0]))
		require.Equal(t, int32(2), events[0].Length)
	})

	t.Run("drop a replayed update", func(t *testing.T) {
		first := newDedupTestFragment(100, tableInfo, common.RowTypeUpdate,
			dedupTestRow{key: 1, value: 1}, dedupTestRow{key: 1, value: 2})
		replay := newDedupTestFragment(100, tableInfo, common.RowTypeUpdate,
			dedupTestRow{key: 1, value: 1}, dedupTestRow{key: 1, value: 2})

		events, err := mergeFragments(t, first, replay)
		require.NoError(t, err)

		require.Len(t, events, 1)
		require.Equal(t, []dedupRowMutation{{rowType: common.RowTypeUpdate, key: 1, value: 2}},
			readDedupRowMutations(events[0]))
		require.Equal(t, int32(1), events[0].Length)
		require.Equal(t, 2, events[0].Rows.NumRows())
	})

	t.Run("keep every row of a multi-row transaction", func(t *testing.T) {
		first := newDedupTestFragment(100, tableInfo, common.RowTypeInsert,
			dedupTestRow{key: 1, value: 1}, dedupTestRow{key: 2, value: 2})
		second := newDedupTestFragment(100, tableInfo, common.RowTypeInsert,
			dedupTestRow{key: 3, value: 3})

		events, err := mergeFragments(t, first, second)
		require.NoError(t, err)

		require.Len(t, events, 1)
		require.Equal(t, []dedupRowMutation{
			{rowType: common.RowTypeInsert, key: 1, value: 1},
			{rowType: common.RowTypeInsert, key: 2, value: 2},
			{rowType: common.RowTypeInsert, key: 3, value: 3},
		}, readDedupRowMutations(events[0]))
	})

	t.Run("keep delete and insert of the same handle key", func(t *testing.T) {
		deleted := newDedupTestFragment(100, tableInfo, common.RowTypeDelete, dedupTestRow{key: 1, value: 1})
		inserted := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		replayedDelete := newDedupTestFragment(100, tableInfo, common.RowTypeDelete, dedupTestRow{key: 1, value: 1})
		replayedInsert := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})

		events, err := mergeFragments(t, deleted, inserted, replayedDelete, replayedInsert)
		require.NoError(t, err)

		require.Len(t, events, 1)
		require.Equal(t, []dedupRowMutation{
			{rowType: common.RowTypeDelete, key: 1, value: 1},
			{rowType: common.RowTypeInsert, key: 1, value: 1},
		}, readDedupRowMutations(events[0]))
	})

	t.Run("keep the same mutation of a later transaction", func(t *testing.T) {
		first := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		second := newDedupTestFragment(101, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})

		events, err := mergeFragments(t, first, second)
		require.NoError(t, err)

		require.Len(t, events, 2)
	})

	t.Run("keep a lower commit-ts arriving after a higher one", func(t *testing.T) {
		// Out of order transactions are still applied: de-duplication must not
		// filter anything by a commit-ts watermark.
		higher := newDedupTestFragment(101, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		lower := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})

		events, err := mergeFragments(t, higher, lower)
		require.NoError(t, err)

		require.Len(t, events, 2)
		for _, event := range events {
			require.Equal(t, []dedupRowMutation{{rowType: common.RowTypeInsert, key: 1, value: 1}},
				readDedupRowMutations(event))
		}
	})

	t.Run("keep rows when the table has no handle key", func(t *testing.T) {
		noHandleKey := newMergeTestTableInfo(1, 10, 2)
		first := newDedupTestFragment(100, noHandleKey, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		second := newDedupTestFragment(100, noHandleKey, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})

		events, err := mergeFragments(t, first, second)
		require.NoError(t, err)

		require.Len(t, events, 1)
		require.Equal(t, 2, events[0].Rows.NumRows())
		require.Equal(t, int32(2), events[0].Length)
	})
}

func TestEventsGroupMessagesToEventsRejectConflictingReplay(t *testing.T) {
	tableInfo := newDedupTestTableInfo()

	t.Run("insert", func(t *testing.T) {
		first := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 1})
		conflict := newDedupTestFragment(100, tableInfo, common.RowTypeInsert, dedupTestRow{key: 1, value: 2})

		events, err := mergeFragments(t, first, conflict)
		require.ErrorContains(t, err, "carries a different row image")
		require.Empty(t, events)
	})

	t.Run("update pre-image", func(t *testing.T) {
		first := newDedupTestFragment(100, tableInfo, common.RowTypeUpdate,
			dedupTestRow{key: 1, value: 1}, dedupTestRow{key: 1, value: 2})
		conflict := newDedupTestFragment(100, tableInfo, common.RowTypeUpdate,
			dedupTestRow{key: 1, value: 9}, dedupTestRow{key: 1, value: 2})

		events, err := mergeFragments(t, first, conflict)
		require.ErrorContains(t, err, "carries a different row image")
		require.Empty(t, events)
	})
}

func TestEventsGroupSharesRawMessageData(t *testing.T) {
	first := newTestDMLMessage(10)
	second := newTestDMLMessage(10)
	messageData := codeccommon.NewDMLMessageData(nil, []byte("raw message"),
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			return []*codeccommon.DMLMessage{first, second}, nil
		},
	)

	group := NewEventsGroup(0, 1)
	messageData.AttachDMLMessage(first)
	require.NoError(t, group.AppendMessage(first))
	messageData.AttachDMLMessage(second)
	require.NoError(t, group.AppendMessage(second))
	entries := readGroupIndex(t, group)
	require.Len(t, entries, 2)
	require.Equal(t, entries[0].location, entries[1].location)
	require.Equal(t, uint64(0), entries[0].dmlIndex)
	require.Equal(t, uint64(1), entries[1].dmlIndex)

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
	key := []byte("raw key")
	value := []byte("raw message")
	messageData := NewDMLMessageDataWithDecoderFactory(key, value,
		func(restoredKey, restoredValue []byte) (codeccommon.Decoder, error) {
			decoderCount++
			require.Equal(t, key, restoredKey)
			require.Equal(t, value, restoredValue)
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

func TestEventsGroupReadsLargeSharedPayloadOnceAcrossBatches(t *testing.T) {
	const messageCount = 27020

	config := defaultSpillConfig()
	config.resolveBatchMessages = 10000
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)
	originalMessages := make([]*codeccommon.DMLMessage, messageCount)
	restoredMessages := make([]*codeccommon.DMLMessage, messageCount)
	for i := range messageCount {
		commitTs := uint64(i + 1)
		originalMessages[i] = newTestDMLMessage(commitTs)
		restoredMessages[i] = newTestDMLMessage(commitTs)
	}
	decodeCount := 0
	messageData := codeccommon.NewDMLMessageData(nil, []byte("one large object payload"),
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			decodeCount++
			return restoredMessages, nil
		})
	for _, message := range originalMessages {
		messageData.AttachDMLMessage(message)
		require.NoError(t, group.AppendMessage(message))
	}

	readRecord := store.readRecord
	readCount := 0
	store.readRecord = func(file *spill.RecordFile, handle spill.Handle) ([]byte, error) {
		readCount++
		return readRecord(file, handle)
	}

	resolved := 0
	batchCount := 0
	for {
		batch, hasMore, err := group.PrepareResolve(math.MaxUint64, store.ResolveLimit())
		require.NoError(t, err)
		require.NotNil(t, batch)
		batchCount++
		resolved += len(batch.Messages)
		if batchCount == 1 {
			require.Positive(t, batch.ResolvedBytes)
		} else {
			require.Zero(t, batch.ResolvedBytes)
		}
		require.NoError(t, batch.Ack())
		if !hasMore {
			break
		}
	}

	require.Equal(t, messageCount, resolved)
	require.Equal(t, 3, batchCount)
	require.Equal(t, 1, readCount)
	require.Equal(t, 1, decodeCount)
	require.Equal(t, int64(1), store.Stats().PayloadWriteCount)
	require.Equal(t, int64(1), store.Stats().PayloadReadCount)
	require.Equal(t, int64(1), store.Stats().PayloadDecodeCount)
	require.Equal(t, store.Stats().PayloadWriteBytes, store.Stats().PayloadReadBytes)
	require.Zero(t, store.PendingBytes())
	require.Empty(t, store.restorers)
	require.Empty(t, store.segments)
}

func TestEventsGroupsSharePayloadUntilEveryGroupAcks(t *testing.T) {
	store := NewSpillStore()
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	firstGroup := NewEventsGroup(0, 1, store)
	secondGroup := NewEventsGroup(0, 2, store)
	first := newTestDMLMessage(1)
	second := newTestDMLMessage(2)
	decodeCount := 0
	messageData := codeccommon.NewDMLMessageData(nil, []byte("shared across groups"),
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			decodeCount++
			return []*codeccommon.DMLMessage{first, second}, nil
		})
	messageData.AttachDMLMessage(first)
	require.NoError(t, firstGroup.AppendMessage(first))
	messageData.AttachDMLMessage(second)
	require.NoError(t, secondGroup.AppendMessage(second))

	readRecord := store.readRecord
	readCount := 0
	store.readRecord = func(file *spill.RecordFile, handle spill.Handle) ([]byte, error) {
		readCount++
		return readRecord(file, handle)
	}

	firstBatch, _, err := firstGroup.PrepareResolve(math.MaxUint64, store.ResolveLimit())
	require.NoError(t, err)
	secondBatch, _, err := secondGroup.PrepareResolve(math.MaxUint64, store.ResolveLimit())
	require.NoError(t, err)
	require.Equal(t, 1, readCount)
	require.Equal(t, 1, decodeCount)
	require.Equal(t, 1, store.Stats().LivePayloads)
	require.Len(t, store.segments, 1)
	entry := readGroupIndex(t, firstGroup)[0]
	spillPath := store.segments[entry.location.segmentID].file.Path()

	require.NoError(t, firstBatch.Ack())
	require.Equal(t, 1, store.Stats().LivePayloads)
	require.FileExists(t, spillPath)
	require.NoError(t, secondBatch.Ack())
	require.Zero(t, store.Stats().LivePayloads)
	require.Empty(t, store.segments)
	_, err = os.Stat(spillPath)
	require.True(t, os.IsNotExist(err))
}

func TestEventsGroupPrepareDoesNotReleaseBeforeAck(t *testing.T) {
	store := NewSpillStore()
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)
	message := attachTestDMLMessageDataWithPayload(newTestDMLMessage(1), nil, []byte("payload"))
	require.NoError(t, group.AppendMessage(message))
	entry := readGroupIndex(t, group)[0]
	spillPath := store.segments[entry.location.segmentID].file.Path()

	batch, _, err := group.PrepareResolve(math.MaxUint64, store.ResolveLimit())
	require.NoError(t, err)
	require.Len(t, batch.Messages, 1)
	require.Equal(t, int64(1), group.pendingCount)
	require.FileExists(t, spillPath)
	_, _, err = group.PrepareResolve(math.MaxUint64, store.ResolveLimit())
	require.Error(t, err)

	require.NoError(t, batch.Ack())
	require.Zero(t, group.pendingCount)
	_, err = os.Stat(spillPath)
	require.True(t, os.IsNotExist(err))
}

func TestEventsGroupSegmentsAndPendingWatermarks(t *testing.T) {
	config := defaultSpillConfig()
	config.segmentBytes = 64
	config.pendingHighBytes = 82
	config.pendingLowBytes = 41
	config.messageMetadataBytes = 1
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)

	first := attachTestDMLMessageDataWithPayload(newTestDMLMessage(1), nil, make([]byte, 16))
	second := attachTestDMLMessageDataWithPayload(newTestDMLMessage(2), nil, make([]byte, 16))
	require.NoError(t, group.AppendMessage(first))
	firstEntry := readGroupIndex(t, group)[0]
	firstSegment := store.segments[firstEntry.location.segmentID]
	firstPath := firstSegment.file.Path()
	require.NoError(t, group.AppendMessage(second))
	entries := readGroupIndex(t, group)
	secondPath := store.segments[entries[1].location.segmentID].file.Path()

	require.Len(t, store.segments, 2)
	require.Equal(t, int64(82), store.PendingBytes())
	require.True(t, store.ShouldDrain())

	messages, hasMore, _, err := group.ResolveIntoBatch(1, nil, ResolveLimit{MaxMessages: 1})
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, messages, 1)
	require.Equal(t, int64(41), store.PendingBytes())
	require.False(t, store.ShouldDrain())
	require.Len(t, store.segments, 1)
	_, err = os.Stat(firstPath)
	require.True(t, os.IsNotExist(err))
	require.FileExists(t, secondPath)

	require.NoError(t, group.Cleanup())
	require.Zero(t, store.PendingBytes())
	_, err = os.Stat(secondPath)
	require.True(t, os.IsNotExist(err))
}

func TestEventsGroupKeepsSharedPayloadInOneSegment(t *testing.T) {
	config := defaultSpillConfig()
	config.segmentMessages = 1
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)
	first := newTestDMLMessage(1)
	second := newTestDMLMessage(2)
	messageData := codeccommon.NewDMLMessageData(nil, []byte("shared payload"),
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			return []*codeccommon.DMLMessage{first, second}, nil
		})

	messageData.AttachDMLMessage(first)
	require.NoError(t, group.AppendMessage(first))
	messageData.AttachDMLMessage(second)
	require.NoError(t, group.AppendMessage(second))

	entries := readGroupIndex(t, group)
	require.Len(t, store.segments, 1)
	require.Equal(t, entries[0].location, entries[1].location)
	require.NoError(t, group.Cleanup())
}

func TestEventsGroupAllowsOversizeSegment(t *testing.T) {
	config := defaultSpillConfig()
	config.segmentBytes = 16
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)

	message := attachTestDMLMessageDataWithPayload(
		newTestDMLMessage(1), []byte("oversize-key"), []byte("oversize-value"))
	require.NoError(t, group.AppendMessage(message))
	require.Len(t, store.segments, 1)
	require.Nil(t, store.activeSegment)
	require.Greater(t, store.PendingBytes(), config.segmentBytes)

	messages, hasMore, _, err := group.ResolveIntoBatch(
		math.MaxUint64, nil, ResolveLimit{MaxBytes: 1, MaxMessages: 1})
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, messages, 1)
	require.Zero(t, store.PendingBytes())
}

func TestEventsGroupRestoreErrorDoesNotReleasePendingData(t *testing.T) {
	config := defaultSpillConfig()
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)
	wantErr := errors.New("restore failed")
	messageData := codeccommon.NewDMLMessageData([]byte("key"), []byte("value"),
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			return nil, wantErr
		})
	message := newTestDMLMessage(1)
	messageData.AttachDMLMessage(message)
	require.NoError(t, group.AppendMessage(message))
	pendingBytes := store.PendingBytes()

	_, _, _, err := group.ResolveIntoBatch(math.MaxUint64, nil, store.ResolveLimit())
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, int64(1), group.pendingCount)
	require.Equal(t, pendingBytes, store.PendingBytes())
	require.NoError(t, group.Cleanup())
	require.Zero(t, store.PendingBytes())
}

func TestEventsGroupResolveIntoBatchBounds(t *testing.T) {
	group := NewEventsGroup(0, 1)
	for _, commitTs := range []uint64{1, 2, 2, 3} {
		require.NoError(t, group.AppendMessage(attachTestDMLMessageData(newTestDMLMessage(commitTs))))
	}

	messages, hasMore, _, err := group.ResolveIntoBatch(
		math.MaxUint64, nil, ResolveLimit{MaxMessages: 2})
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, messages, 3)
	require.Equal(t, []uint64{1, 2, 2}, []uint64{
		messages[0].GetCommitTs(), messages[1].GetCommitTs(), messages[2].GetCommitTs(),
	})

	messages, hasMore, _, err = group.ResolveIntoBatch(
		math.MaxUint64, nil, ResolveLimit{MaxBytes: 1})
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, messages, 1)
	require.Equal(t, uint64(3), messages[0].GetCommitTs())
}

func TestSpillStoreAllowsPendingAboveHighWatermark(t *testing.T) {
	config := defaultSpillConfig()
	config.pendingHighBytes = 10
	config.pendingLowBytes = 5
	store := newSpillStore(config)

	store.addPending(11)
	require.True(t, store.ShouldDrain())
	store.addPending(100)
	require.Equal(t, int64(111), store.PendingBytes())
	require.True(t, store.ShouldDrain())

	store.releasePending(106)
	require.Equal(t, int64(5), store.PendingBytes())
	require.False(t, store.ShouldDrain())
}

func TestSpillStoreDefaults(t *testing.T) {
	store := NewSpillStore()
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	require.Equal(t, int64(128*1024*1024), store.config.segmentBytes)
	require.Equal(t, int64(1024*1024*1024), store.config.pendingHighBytes)
	require.Equal(t, int64(512*1024*1024), store.config.pendingLowBytes)
	require.Equal(t, ResolveLimit{MaxBytes: 64 * 1024 * 1024, MaxMessages: 10000}, store.ResolveLimit())
	require.Equal(t, int64(100000), store.config.indexCleanupMessages)
	require.Equal(t, 0.90, store.config.diskUsageLimit)
}

func TestSpillStoreRejectsWriteAboveDiskUsageLimit(t *testing.T) {
	config := defaultSpillConfig()
	config.diskCheckBytes = 1
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	store.diskUsage = func(string) (filesystemUsage, error) {
		return filesystemUsage{usedBytes: 91, totalBytes: 100}, nil
	}
	group := NewEventsGroup(0, 1, store)

	err := group.AppendMessage(attachTestDMLMessageData(newTestDMLMessage(1)))
	require.ErrorContains(t, err, "spill filesystem usage")
	require.ErrorContains(t, err, "90.00% limit")
	require.Same(t, store.terminalErr, err)
	require.Zero(t, group.pendingCount)
	require.Empty(t, store.segments)
	require.NoError(t, store.Cleanup())
}

func TestEventsGroupTracksResolvedAndAppliedFrontiers(t *testing.T) {
	store := NewSpillStore()
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(newTestDMLMessage(1))))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(newTestDMLMessage(2))))

	batch, hasMore, err := group.PrepareResolve(1, store.ResolveLimit())
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, uint64(2), group.HighWatermark)
	require.Equal(t, uint64(1), group.resolvedTs)
	require.Zero(t, group.appliedTs)
	require.Equal(t, int64(2), group.pendingCount)

	require.NoError(t, batch.Ack())
	require.Equal(t, uint64(1), group.appliedTs)
	require.Equal(t, int64(1), group.pendingCount)
	require.Equal(t, int64(1), store.Stats().AppliedEventCount)
	require.NoError(t, group.Cleanup())
	require.NoError(t, store.Cleanup())
}

func TestEventsGroupRestoresPersistedSourcePosition(t *testing.T) {
	group := NewEventsGroup(3, 1)
	message := newTestDMLMessage(10)
	messageData := codeccommon.NewDMLMessageData(nil, nil,
		func([]byte) ([]*codeccommon.DMLMessage, error) {
			return []*codeccommon.DMLMessage{message}, nil
		})
	messageData.SourcePosition = 42
	messageData.AttachDMLMessage(message)
	var restoredPosition int64
	group.SetPostRestore(func(message *codeccommon.DMLMessage, position int64) *codeccommon.DMLMessage {
		restoredPosition = position
		return message
	})

	require.NoError(t, group.AppendMessage(message))
	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, int64(42), restoredPosition)
}

func TestSpillStoreRestoreCacheHasByteAndEntryBounds(t *testing.T) {
	config := defaultSpillConfig()
	config.resolveBatchBytes = 1 << 20
	config.resolveBatchMessages = 2
	store := newSpillStore(config)
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)
	for commitTs := uint64(1); commitTs <= 4; commitTs++ {
		require.NoError(t, group.AppendMessage(
			attachTestDMLMessageDataWithPayload(newTestDMLMessage(commitTs), nil, []byte{byte(commitTs)})))
	}

	for commitTs := uint64(1); commitTs <= 3; commitTs++ {
		batch, hasMore, err := group.PrepareResolve(4, ResolveLimit{MaxMessages: 1})
		require.NoError(t, err)
		require.True(t, hasMore)
		require.Equal(t, commitTs, batch.Messages[0].GetCommitTs())
		require.NoError(t, batch.Ack())
		require.LessOrEqual(t, len(store.cache), 2)
		require.LessOrEqual(t, store.cacheBytes, config.resolveBatchBytes)
	}

	batch, hasMore, err := group.PrepareResolve(4, ResolveLimit{MaxMessages: 1})
	require.NoError(t, err)
	require.False(t, hasMore)
	require.NoError(t, batch.Ack())
	require.Empty(t, store.cache)
}

func TestEventsGroupKeepsPerEventMetadataOnDisk(t *testing.T) {
	group := NewEventsGroup(0, 1)
	const messageCount = 2048
	messages := make([]*codeccommon.DMLMessage, 0, messageCount)
	for i := 1; i <= messageCount; i++ {
		messages = append(messages, newTestDMLMessage(uint64(i)))
	}
	messageData := codeccommon.NewDMLMessageData(nil, nil,
		func([]byte) ([]*codeccommon.DMLMessage, error) { return messages, nil })
	for _, message := range messages {
		messageData.AttachDMLMessage(message)
		require.NoError(t, group.AppendMessage(message))
	}
	require.Equal(t, int64(messageCount), group.pendingCount)
	require.Len(t, group.segmentRefs, 1)
	require.Len(t, group.restorerRefs, 1)

	resolved, hasMore, _, err := group.ResolveIntoBatch(1536, nil, ResolveLimit{})
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, resolved, 1536)
	require.Equal(t, int64(512), group.pendingCount)
	require.Len(t, readGroupIndex(t, group), 512)
	require.NoError(t, group.Cleanup())
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

	spillPath := group.store.activeSegment.file.Path()

	var dst []*codeccommon.DMLMessage
	dst, err := group.ResolveInto(2, dst)
	require.NoError(t, err)

	require.Len(t, dst, 2)
	require.Equal(t, m1.GetCommitTs(), dst[0].GetCommitTs())
	require.Equal(t, m2.GetCommitTs(), dst[1].GetCommitTs())

	require.Equal(t, int64(1), group.pendingCount)
	entries := readGroupIndex(t, group)
	require.Len(t, entries, 1)
	require.Equal(t, m3.GetCommitTs(), entries[0].commitTs)
	require.FileExists(t, spillPath)

	_, err = group.GetAllMessages()
	require.NoError(t, err)
	require.Nil(t, group.store.activeSegment)
	require.Empty(t, group.store.segments)
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
	require.Equal(t, int64(2), group.pendingCount)
	entries := readGroupIndex(t, group)
	require.Equal(t, m1.GetCommitTs(), entries[0].commitTs)
	require.Equal(t, m2.GetCommitTs(), entries[1].commitTs)
}

func TestEventsGroupResolveIntoClearsAllWhenFullyResolved(t *testing.T) {
	// Scenario: resolveTs advances beyond all buffered events.
	// Expectation: group is emptied and all backing-array pointers for resolved events are cleared.
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m1)))
	require.NoError(t, group.AppendMessage(attachTestDMLMessageData(m2)))

	spillPath := group.store.activeSegment.file.Path()
	var dst []*codeccommon.DMLMessage
	dst, err := group.ResolveInto(100, dst)
	require.NoError(t, err)

	require.Len(t, dst, 2)
	require.Equal(t, m1.GetCommitTs(), dst[0].GetCommitTs())
	require.Equal(t, m2.GetCommitTs(), dst[1].GetCommitTs())

	require.Zero(t, group.pendingCount)
	require.Nil(t, group.store.activeSegment)
	require.Empty(t, group.store.segments)
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

	require.Equal(t, int64(1), group.pendingCount)
	entries := readGroupIndex(t, group)
	require.Equal(t, m3.GetCommitTs(), entries[0].commitTs)
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
	require.Zero(t, group.pendingCount)
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
	require.Zero(t, group.pendingCount)
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
			for range b.N {
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

func BenchmarkEventsGroupResolveIncrementally(b *testing.B) {
	const (
		messageCount  = 32 * 1024
		batchMessages = 16
	)

	oldLogLevel := log.GetLevel()
	log.SetLevel(zapcore.FatalLevel)
	b.Cleanup(func() { log.SetLevel(oldLogLevel) })

	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		config := defaultSpillConfig()
		config.resolveBatchMessages = batchMessages
		store := newSpillStore(config)
		group := NewEventsGroup(0, 1, store)
		for i := 1; i <= messageCount; i++ {
			if err := group.AppendMessage(
				attachTestDMLMessageData(newTestDMLMessage(uint64(i)))); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		for {
			batch, hasMore, err := group.PrepareResolve(math.MaxUint64, store.ResolveLimit())
			if err != nil {
				b.Fatal(err)
			}
			if err := batch.Ack(); err != nil {
				b.Fatal(err)
			}
			if !hasMore {
				break
			}
		}
		b.StopTimer()
		if err := store.Cleanup(); err != nil {
			b.Fatal(err)
		}
	}
}
