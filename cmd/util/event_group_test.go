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
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newTestDMLMessage(commitTs uint64) *codeccommon.DMLMessage {
	return codeccommon.NewDMLMessage(1, "test", "t", commitTs, common.RowTypeInsert, nil)
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

<<<<<<< HEAD
func TestEventsGroupResolveIntoAppendsAndClearsResolvedMessages(t *testing.T) {
=======
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

func TestAppendOrMergeDMLEventSparseChecksums(t *testing.T) {
	for _, tc := range []struct {
		name      string
		checksums []*integrity.Checksum
	}{
		{name: "historical update between checksummed rows", checksums: []*integrity.Checksum{{Current: 1}, nil, {Current: 3}}},
		{name: "historical update before first checksum", checksums: []*integrity.Checksum{nil, nil, {Current: 3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tableInfo := newMergeTestTableInfo(1, 10, 1)
			first := newMergeTestDMLEvent(100, tableInfo, 1)
			update := newMergeTestDMLEvent(100, tableInfo, 2)
			last := newMergeTestDMLEvent(100, tableInfo, 3)
			update.RowTypes = []common.RowType{common.RowTypeUpdate, common.RowTypeUpdate}
			update.Rows.AppendInt64(0, 20)
			first.RowKeys = [][]byte{[]byte("first")}
			last.RowKeys = [][]byte{[]byte("last")}
			inputs := []*commonEvent.DMLEvent{first, update, last}
			messages := make([]*codeccommon.DMLMessage, len(inputs))
			for i, input := range inputs {
				input.Version = commonEvent.DMLEventVersion1
				if tc.checksums[i] != nil {
					input.Checksum = []*integrity.Checksum{tc.checksums[i]}
				}
				messages[i] = newMergeTestDMLMessage(input)
			}

			events := DMLMessagesToEvents(messages)
			require.Len(t, events, 1)
			merged := events[0]
			require.Equal(t, int32(3), merged.Length)
			require.Len(t, merged.RowTypes, 4)
			require.Equal(t, 4, merged.Rows.NumRows())
			require.Equal(t, tc.checksums, merged.Checksum)
			require.Equal(t, [][]byte{[]byte("first"), nil, nil, []byte("last")}, merged.RowKeys)

			for i, rowType := range []common.RowType{common.RowTypeInsert, common.RowTypeUpdate, common.RowTypeInsert} {
				row, ok := merged.GetNextRow()
				require.True(t, ok, "logical row %d must not be skipped", i)
				require.Equal(t, rowType, row.RowType)
				require.Equal(t, tc.checksums[i], row.Checksum)
				if rowType == common.RowTypeUpdate {
					require.Equal(t, int64(2), row.PreRow.GetInt64(0))
					require.Equal(t, int64(20), row.Row.GetInt64(0))
				} else {
					require.Equal(t, int64(i+1), row.Row.GetInt64(0))
				}
			}
			_, ok := merged.GetNextRow()
			require.False(t, ok)

			data, err := merged.Marshal()
			require.NoError(t, err)
			var decoded commonEvent.DMLEvent
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, merged.Length, decoded.Length)
			require.Equal(t, tc.checksums, decoded.Checksum)
		})
	}
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
>>>>>>> 90a0a9aa4 (event: serialize row checksum in dml event (#6343))
	// Scenario: A consumer resolves events by watermark/commit-ts and appends them into a downstream
	// batch slice. We must clear resolved messages in the group's backing array to avoid retaining
	// already-flushed events and causing unbounded memory growth.
	//
	// Steps:
	//  1. Append 3 events with increasing CommitTs.
	//  2. Call ResolveInto with resolve=2 and a nil dst.
	//  3. Verify (a) returned events are correct, (b) group keeps only the remaining event,
	//     (c) resolved messages in the original backing slice are cleared (nil'd).
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	m3 := newTestDMLMessage(3)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	// Keep a reference to the original slice header so we can validate that ResolveInto clears
	// resolved messages in-place (this is what prevents GC retention of flushed events).
	original := group.messages

	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(2, dst)

	require.Len(t, dst, 2)
	require.Same(t, m1, dst[0])
	require.Same(t, m2, dst[1])

	require.Len(t, group.messages, 1)
	require.Same(t, m3, group.messages[0])

	// The unresolved event is compacted to the front, and the tail is cleared so the group
	// doesn't keep flushed events alive via its backing array.
	require.Same(t, m3, original[0])
	require.Nil(t, original[1])
	require.Nil(t, original[2])
}

func TestEventsGroupResolveIntoNoopWhenNothingResolved(t *testing.T) {
	// Scenario: resolveTs is behind all buffered events.
	// Expectation: ResolveInto should be a no-op (dst unchanged, group unchanged).
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(10)
	m2 := newTestDMLMessage(20)
	group.AppendMessage(m1)
	group.AppendMessage(m2)

	original := group.messages
	dst := make([]*codeccommon.DMLMessage, 0, 1)
	dst = group.ResolveInto(5, dst)

	require.Len(t, dst, 0)
	require.Len(t, group.messages, 2)
	require.Same(t, m1, group.messages[0])
	require.Same(t, m2, group.messages[1])

	// No prefix should be cleared because nothing was resolved.
	require.Same(t, m1, original[0])
	require.Same(t, m2, original[1])
}

func TestEventsGroupResolveIntoClearsAllWhenFullyResolved(t *testing.T) {
	// Scenario: resolveTs advances beyond all buffered events.
	// Expectation: group is emptied and all backing-array pointers for resolved events are cleared.
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	group.AppendMessage(m1)
	group.AppendMessage(m2)

	original := group.messages
	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(100, dst)

	require.Len(t, dst, 2)
	require.Same(t, m1, dst[0])
	require.Same(t, m2, dst[1])

	require.Len(t, group.messages, 0)
	require.Nil(t, original[0])
	require.Nil(t, original[1])
}

func TestEventsGroupResolveIntoSortsOutOfOrderResolvedMessages(t *testing.T) {
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(20)
	m2 := newTestDMLMessage(10)
	m3 := newTestDMLMessage(30)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	original := group.messages
	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(25, dst)

	require.Len(t, dst, 2)
	require.Same(t, m2, dst[0])
	require.Same(t, m1, dst[1])

	require.Len(t, group.messages, 1)
	require.Same(t, m3, group.messages[0])
	require.Same(t, m3, original[0])
	require.Nil(t, original[1])
	require.Nil(t, original[2])
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
	dst = group.ResolveInto(20, dst)

	require.Len(t, dst, 3)
	require.Same(t, m2, dst[0])
	require.Same(t, m1, dst[1])
	require.Same(t, m3, dst[2])
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

	messages := group.GetAllMessages()

	require.Len(t, messages, 3)
	require.Same(t, m2, messages[0])
	require.Same(t, m1, messages[1])
	require.Same(t, m3, messages[2])
	require.Empty(t, group.messages)
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
