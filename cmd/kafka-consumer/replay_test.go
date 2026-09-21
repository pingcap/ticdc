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
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/util"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	mysqlsink "github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func replayDecoder(t *testing.T) func(int, uint64) *codeccommon.DMLMessage {
	t.Helper()
	cfg := codeccommon.NewConfig(config.ProtocolCanalJSON)
	cfg.EnableTiDBExtension = true
	decoder, err := canal.NewDecoder(t.Context(), cfg, nil)
	require.NoError(t, err)
	return func(id int, ts uint64) *codeccommon.DMLMessage {
		decoder.AddKeyValue(nil, []byte(fmt.Sprintf(`{"database":"test","table":"t","pkNames":["id"],"isDdl":false,"type":"INSERT","sqlType":{"id":4},"mysqlType":{"id":"int"},"data":[{"id":"%d"}],"_tidb":{"commitTs":%d}}`, id, ts)))
		typ, ok := decoder.HasNext()
		require.True(t, ok)
		require.Equal(t, codeccommon.MessageTypeRow, typ)
		return decoder.NextDMLMessage()
	}
}

func TestReplayBatch(t *testing.T) {
	for _, safeMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("safeMode=%t", safeMode), func(t *testing.T) {
			decode := replayDecoder(t)
			// A(10), B(20), A(10), B(20); spill restoration sorts by commit-ts.
			messages := []*codeccommon.DMLMessage{decode(1, 10), decode(2, 20), decode(1, 10), decode(2, 20)}
			slices.SortStableFunc(messages, func(a, b *codeccommon.DMLMessage) int { return cmp.Compare(a.GetCommitTs(), b.GetCommitTs()) })
			events := util.DMLMessagesToEvents(messages)
			cfg := mysqlsink.New()
			cfg.SafeMode = safeMode
			cfg.DryRun = true
			id := common.NewChangeFeedIDWithName("replay-test", common.DefaultKeyspaceName)
			stats := metrics.NewStatistics(id, common.DefaultKeyspaceID, "mysql")
			defer stats.Close()
			sqlWriter := mysqlsink.NewWriter(t.Context(), -1, nil, cfg, id, stats, nil)
			defer sqlWriter.Close()
			s := sinkmock.NewMockSink(gomock.NewController(t))
			var captured []*event.DMLEvent
			s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(e *event.DMLEvent) {
				captured = append(captured, e)
				if len(captured) == 2 {
					require.NoError(t, sqlWriter.Flush(captured))
				}
			}).Times(2)
			callbacks := 0
			for _, e := range events {
				e.AddPostFlushFunc(func() { callbacks++ })
			}
			w := &writer{mysqlSink: s, progresses: []*partitionProgress{{watermark: 20}}}
			require.NoError(t, w.flushDMLBatch(t.Context(), events))
			require.Equal(t, 2, callbacks)
			require.Equal(t, []uint64{10, 20}, []uint64{captured[0].CommitTs, captured[1].CommitTs})
			require.Equal(t, int32(1), captured[0].Len())
			require.Equal(t, int32(1), captured[1].Len())
		})
	}
}

func TestReplayWatermarkBoundary(t *testing.T) {
	decode := replayDecoder(t)
	s := sinkmock.NewMockSink(gomock.NewController(t))
	var handles []int64
	s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(e *event.DMLEvent) {
		for row, ok := e.GetNextRow(); ok; row, ok = e.GetNextRow() {
			handles = append(handles, row.Row.GetInt64(0))
		}
		e.PostFlush()
	}).AnyTimes()
	w := &writer{mysqlSink: s, progresses: []*partitionProgress{{watermark: 100}}}
	flush := func(ids ...int) {
		var messages []*codeccommon.DMLMessage
		for _, id := range ids {
			messages = append(messages, decode(id, 100))
		}
		events := util.DMLMessagesToEvents(messages)
		callbacks := 0
		for _, e := range events {
			e.AddPostFlushFunc(func() { callbacks++ })
		}
		require.NoError(t, w.flushDMLBatch(t.Context(), events))
		require.Equal(t, len(events), callbacks)
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
	table := replayDecoder(t)(1, 100).ToDMLEvent().TableInfo
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
	// Avro's synthetic DELETE + INSERT is one logical UPDATE. Retain both
	// operations, and remove only their repeated copies.
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
	decode := replayDecoder(t)
	events := []*event.DMLEvent{decode(1, 100).ToDMLEvent(), decode(1, 100).ToDMLEvent()}
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
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	table := helper.DDL2Event("create table test.t (a varbinary(32), b varbinary(32), primary key(a,b))").TableInfo
	// NUL-separated encoding would collide for these two different keys.
	a := replayHandle(chunk.MutRowFromValues([]byte("a\x00"), []byte("b")).ToRow(), table)
	b := replayHandle(chunk.MutRowFromValues([]byte("a"), []byte("\x00b")).ToRow(), table)
	require.NotEmpty(t, a)
	require.NotEqual(t, a, b)
	noHandle := helper.DDL2Event("create table test.no_handle (a int)").TableInfo
	e := event.NewDMLEvent(common.DispatcherID{}, 2, 99, 100, noHandle)
	e.Rows = chunk.NewChunkWithCapacity(noHandle.GetFieldSlice(), 2)
	for range 2 {
		e.Rows.AppendRow(chunk.MutRowFromValues(int64(1)).ToRow())
	}
	e.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
	e.Length = 2
	require.Same(t, e, filterReplayRows(e, make(map[replayKey]struct{}), nil))
}

func TestReplaySpillRecovery(t *testing.T) {
	decode := replayDecoder(t)
	store := util.NewSpillStore()
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := util.NewEventsGroup(0, 1, store)
	for _, item := range []struct {
		id int
		ts uint64
	}{{1, 10}, {2, 20}, {1, 10}, {2, 20}} {
		require.NoError(t, group.AppendMessage(attachDMLMessageDataForWriterTest(decode(item.id, item.ts))))
	}
	s := sinkmock.NewMockSink(gomock.NewController(t))
	var timestamps []uint64
	s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(e *event.DMLEvent) {
		require.Equal(t, int32(1), e.Len())
		timestamps = append(timestamps, e.CommitTs)
		e.PostFlush()
	}).Times(2)
	w := &writer{mysqlSink: s, spillStore: store, progresses: []*partitionProgress{{watermark: 20}}}
	_, err := w.flushEventsFromGroups(t.Context(), []*util.EventsGroup{group}, 20)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, timestamps)
	batch, hasMore, err := group.PrepareResolve(20, store.ResolveLimit())
	require.NoError(t, err)
	require.Nil(t, batch)
	require.False(t, hasMore)
}
