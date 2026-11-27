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

package writer

import (
	"slices"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestWriterAdvanceCheckpointNotifiesTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	gomock.InOrder(
		target.EXPECT().AddCheckpointTs(uint64(100)).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(149)).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
	}

	w.Advance(Progress{Watermark: 100})
	w.advanceCheckpoint()
	// Calling again without progress must not re-emit the same checkpoint.
	w.advanceCheckpoint()

	w.Advance(Progress{Watermark: 200, Pending: 150})
	w.advanceCheckpoint()

	require.Equal(t, uint64(149), w.Checkpoint())
}

func TestWriterReplayDedupAcrossCheckpointAndOutOfOrder(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.t(id int primary key, v int)")
	dml200 := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	dml200.CommitTs = 200
	helper.ExecuteDeleteDml("test", "t", "delete from test.t where id = 1")
	dml200Replay := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	dml200Replay.CommitTs = 200
	dml150 := helper.DML2Event("test", "t", "insert into test.t values (2, 20)")
	dml150.CommitTs = 150
	dml100Replay := helper.DML2Event("test", "t", "insert into test.t values (3, 30)")
	dml100Replay.CommitTs = 100

	var replayed200Flushed, replayed100Flushed bool
	dml200Replay.AddPostFlushFunc(func() { replayed200Flushed = true })
	dml100Replay.AddPostFlushFunc(func() { replayed100Flushed = true })

	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	var written []uint64
	gomock.InOrder(
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			written = append(written, dml.CommitTs)
			dml.PostFlush()
		}).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(100)).Times(1),
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			written = append(written, dml.CommitTs)
			dml.PostFlush()
		}).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(200)).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
	}

	w.Advance(Progress{Watermark: 100})
	w.AddDML(dml200)
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(100), w.Checkpoint())

	// An earlier commit above checkpoint (> 100, < 200) must still be written.
	w.AddDML(dml150)
	require.NoError(t, w.Flush(t.Context()))

	// Same-handle replay in (checkpoint, ...] is dropped by mutations; <= checkpoint is dropped directly.
	w.AddDML(dml200Replay)
	w.AddDML(dml100Replay)
	require.NoError(t, w.Flush(t.Context()))
	require.True(t, replayed200Flushed)
	require.True(t, replayed100Flushed)
	require.Equal(t, []uint64{200, 150}, written)

	w.Advance(Progress{Watermark: 200})
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(200), w.Checkpoint())
	require.Empty(t, w.mutations)
}

func TestWriterFlushOrdersDMLBeforeDDLAndRetainsDDLWrites(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.t(id int primary key, v int)")
	dml100 := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	dml100.CommitTs = 100
	ddlEvent := helper.DDL2Event("alter table test.t add column c int")
	ddlEvent.FinishedTs = 150

	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	gomock.InOrder(
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			dml.PostFlush()
		}).Times(1),
		target.EXPECT().FlushDMLBeforeBlock(ddlEvent).Return(nil).Times(1),
		target.EXPECT().WriteBlockEvent(ddlEvent).Return(nil).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(200)).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
	}

	w.AddDML(dml100)
	require.NoError(t, w.WriteDDL(t.Context(), ddlEvent))

	w.Advance(Progress{Watermark: 200})
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(200), w.Checkpoint())
	require.True(t, w.ddlWrites[ddl.KeyOf(ddlEvent)])

	// Historical DDL replay after checkpoint advancement is still deduplicated.
	require.NoError(t, w.WriteDDL(t.Context(), ddlEvent))
}

func TestWriterFilterRowsPartialMultiRowWithUpdate(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.t(id int primary key, v int)")
	ins1Seen := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	ins1Seen.CommitTs = 200

	helper.ExecuteDeleteDml("test", "t", "delete from test.t where id = 1")
	ins1Dup := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	upd2, _ := helper.DML2UpdateEvent(
		"test", "t",
		"insert into test.t values (2, 20)",
		"update test.t set v = 21 where id = 2",
	)
	ins3 := helper.DML2Event("test", "t", "insert into test.t values (3, 30)")

	batch := event.NewDMLEvent(ins1Dup.DispatcherID, ins1Dup.PhysicalTableID, 190, 200, ins1Dup.TableInfo)
	batch.Rows = chunk.NewChunkWithCapacity(ins1Dup.TableInfo.GetFieldSlice(), 4)
	batch.Rows.Append(ins1Dup.Rows, 0, 1)
	batch.Rows.Append(upd2.Rows, 0, 2)
	batch.Rows.Append(ins3.Rows, 0, 1)
	batch.RowTypes = []common.RowType{
		common.RowTypeInsert,
		common.RowTypeUpdate, common.RowTypeUpdate,
		common.RowTypeInsert,
	}
	batch.RowKeys = [][]byte{[]byte("k1"), []byte("k2-pre"), []byte("k2-post"), []byte("k3")}
	batch.Checksum = []*integrity.Checksum{{Current: 1}, {Previous: 2, Current: 22}, {Current: 3}}
	batch.Length = 3

	var batchFlushed bool
	batch.AddPostFlushFunc(func() { batchFlushed = true })

	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	var filtered *event.DMLEvent
	gomock.InOrder(
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			dml.PostFlush()
		}).Times(1),
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			filtered = dml
			dml.PostFlush()
		}).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
	}

	w.AddDML(ins1Seen)
	require.NoError(t, w.Flush(t.Context()))

	w.AddDML(batch)
	require.NoError(t, w.Flush(t.Context()))

	require.True(t, batchFlushed)
	require.NotNil(t, filtered)
	require.NotSame(t, batch, filtered)
	require.Equal(t, int32(2), filtered.Len())
	require.Equal(t, []common.RowType{common.RowTypeUpdate, common.RowTypeUpdate, common.RowTypeInsert}, filtered.RowTypes)
	require.Equal(t, [][]byte{[]byte("k2-pre"), []byte("k2-post"), []byte("k3")}, filtered.RowKeys)
	require.Equal(t, []*integrity.Checksum{{Previous: 2, Current: 22}, {Current: 3}}, filtered.Checksum)

	row0, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, common.RowTypeUpdate, row0.RowType)
	require.Equal(t, int64(2), row0.PreRow.GetInt64(0))
	require.Equal(t, int64(20), row0.PreRow.GetInt64(1))
	require.Equal(t, int64(2), row0.Row.GetInt64(0))
	require.Equal(t, int64(21), row0.Row.GetInt64(1))

	row1, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, common.RowTypeInsert, row1.RowType)
	require.Equal(t, int64(3), row1.Row.GetInt64(0))
	require.Equal(t, int64(30), row1.Row.GetInt64(1))

	_, ok = filtered.GetNextRow()
	require.False(t, ok)
}

func TestWriterAddDMLMergesSameTableCommitTsAndSetsDispatcherID(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.t1(id int primary key, v int)")
	_ = helper.DDL2Event("create table test.t2(id int primary key, v int)")

	ins1 := helper.DML2Event("test", "t1", "insert into test.t1 values (1, 10)")
	ins1.DispatcherID = common.DispatcherID{}
	ins1.CommitTs = 300
	ins1.RowKeys = [][]byte{[]byte("t1-k1")}
	ins1.Checksum = []*integrity.Checksum{{Current: 11}}
	ins1.ApproximateSize = 16

	upd1, _ := helper.DML2UpdateEvent(
		"test", "t1",
		"insert into test.t1 values (2, 20)",
		"update test.t1 set v = 21 where id = 2",
	)
	upd1.DispatcherID = common.DispatcherID{}
	upd1.CommitTs = 300
	upd1.RowKeys = [][]byte{[]byte("t1-u2-pre"), []byte("t1-u2-post")}
	upd1.Checksum = []*integrity.Checksum{{Previous: 20, Current: 21}}
	upd1.ApproximateSize = 32

	t2Ins := helper.DML2Event("test", "t2", "insert into test.t2 values (1, 100)")
	t2Ins.DispatcherID = common.DispatcherID{}
	t2Ins.CommitTs = 300
	t2Ins.RowKeys = [][]byte{[]byte("t2-k1")}
	t2Ins.ApproximateSize = 20

	t1Second := helper.DML2Event("test", "t1", "insert into test.t1 values (3, 30)")
	t1Second.DispatcherID = common.DispatcherID{}
	t1Second.CommitTs = 300
	t1Second.RowKeys = [][]byte{[]byte("t1-k3")}
	t1Second.Checksum = []*integrity.Checksum{{Current: 33}}
	t1Second.ApproximateSize = 18

	t2Second := helper.DML2Event("test", "t2", "insert into test.t2 values (2, 200)")
	t2Second.DispatcherID = common.DispatcherID{}
	t2Second.CommitTs = 300
	t2Second.RowKeys = nil
	t2Second.ApproximateSize = 22

	var events []string
	ins1.AddPostEnqueueFunc(func() { events = append(events, "t1-1:enq") })
	ins1.AddPostFlushFunc(func() { events = append(events, "t1-1:flush") })
	upd1.AddPostEnqueueFunc(func() { events = append(events, "t1-u2:enq") })
	upd1.AddPostFlushFunc(func() { events = append(events, "t1-u2:flush") })
	t1Second.AddPostEnqueueFunc(func() { events = append(events, "t1-3:enq") })
	t1Second.AddPostFlushFunc(func() { events = append(events, "t1-3:flush") })

	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	flushedByTable := make(map[int64]*event.DMLEvent)
	target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
		flushedByTable[dml.PhysicalTableID] = dml
		dml.PostEnqueue()
		dml.PostFlush()
	}).Times(2)
	target.EXPECT().AddCheckpointTs(uint64(300)).Times(1)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
	}
	w.Advance(Progress{Watermark: 300})

	// Interleave t1 and t2 events at the same CommitTs=300.
	w.AddDML(ins1)
	w.AddDML(t2Ins)
	w.AddDML(upd1)
	w.AddDML(t2Second)
	w.AddDML(t1Second)

	require.Len(t, w.pendingDML, 2)
	require.NoError(t, w.Flush(t.Context()))
	require.Len(t, flushedByTable, 2)

	t1Merged := flushedByTable[ins1.PhysicalTableID]
	require.NotNil(t, t1Merged)
	require.Equal(t, int32(3), t1Merged.Len())
	require.Equal(t, 4, t1Merged.Rows.NumRows())
	require.Equal(t, []common.RowType{
		common.RowTypeInsert,
		common.RowTypeUpdate, common.RowTypeUpdate,
		common.RowTypeInsert,
	}, t1Merged.RowTypes)
	require.Equal(t, [][]byte{
		[]byte("t1-k1"), []byte("t1-u2-pre"), []byte("t1-u2-post"), []byte("t1-k3"),
	}, t1Merged.RowKeys)
	require.Equal(t, []*integrity.Checksum{
		{Current: 11}, {Previous: 20, Current: 21}, {Current: 33},
	}, t1Merged.Checksum)
	require.Equal(t, int64(16+32+18), t1Merged.ApproximateSize)
	require.Equal(t, common.DispatcherID{Low: uint64(ins1.PhysicalTableID)}, t1Merged.DispatcherID)
	require.Equal(t, []string{
		"t1-1:enq", "t1-u2:enq", "t1-3:enq",
		"t1-1:flush", "t1-u2:flush", "t1-3:flush",
	}, events)

	t2Merged := flushedByTable[t2Ins.PhysicalTableID]
	require.NotNil(t, t2Merged)
	require.Equal(t, int32(2), t2Merged.Len())
	require.Equal(t, [][]byte{[]byte("t2-k1"), nil}, t2Merged.RowKeys)
	require.Equal(t, int64(42), t2Merged.ApproximateSize)
	require.Equal(t, common.DispatcherID{Low: uint64(t2Ins.PhysicalTableID)}, t2Merged.DispatcherID)
	require.NotEqual(t, t1Merged.DispatcherID, t2Merged.DispatcherID)
}

func TestWriterAddDMLMergeBoundaries(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.t(id int primary key, v int)")
	row0A := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	row0A.DispatcherID = common.DispatcherID{}
	row0A.CommitTs = 0
	row0B := helper.DML2Event("test", "t", "insert into test.t values (2, 20)")
	row0B.DispatcherID = common.DispatcherID{}
	row0B.CommitTs = 0

	w := &Writer{
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
	}

	// Zero CommitTs rows must never merge.
	w.AddDML(row0A)
	w.AddDML(row0B)
	require.Len(t, w.pendingDML, 2)
	w.pendingDML = nil

	// Different CommitTs or older CommitTs arriving after newer CommitTs must not merge across it.
	ts100A := helper.DML2Event("test", "t", "insert into test.t values (3, 30)")
	ts100A.DispatcherID = common.DispatcherID{}
	ts100A.CommitTs = 100
	ts200 := helper.DML2Event("test", "t", "insert into test.t values (4, 40)")
	ts200.DispatcherID = common.DispatcherID{}
	ts200.CommitTs = 200
	ts100B := helper.DML2Event("test", "t", "insert into test.t values (5, 50)")
	ts100B.DispatcherID = common.DispatcherID{}
	ts100B.CommitTs = 100
	w.AddDML(ts100A)
	w.AddDML(ts200)
	w.AddDML(ts100B)
	require.Len(t, w.pendingDML, 3)
	w.pendingDML = nil

	// Schema mismatch at the same CommitTs must not merge.
	oldSchema := helper.DML2Event("test", "t", "insert into test.t values (6, 60)")
	oldSchema.DispatcherID = common.DispatcherID{}
	oldSchema.CommitTs = 300
	ddl := helper.DDL2Event("alter table test.t add column extra int default 0")
	newSchema := helper.DML2Event("test", "t", "insert into test.t values (7, 70, 1)")
	newSchema.DispatcherID = common.DispatcherID{}
	newSchema.CommitTs = 300
	newSchema.TableInfo = ddl.TableInfo
	w.AddDML(oldSchema)
	w.AddDML(newSchema)
	require.Len(t, w.pendingDML, 2)
	w.pendingDML = nil

	// A merged CommitTs event exceeding batchRows is still kept whole by selectBatch.
	base := helper.DML2Event("test", "t", "insert into test.t values (8, 80, 1)")
	for range batchRows + 5 {
		dml := event.NewDMLEvent(common.DispatcherID{}, base.PhysicalTableID, base.StartTs, 400, base.TableInfo)
		dml.Rows = chunk.NewChunkWithCapacity(base.TableInfo.GetFieldSlice(), 1)
		dml.Rows.Append(base.Rows, 0, 1)
		dml.RowTypes = slices.Clone(base.RowTypes)
		dml.Length = 1
		w.AddDML(dml)
	}
	require.Len(t, w.pendingDML, 1)
	require.Equal(t, int32(batchRows+5), w.pendingDML[0].Len())
	selected := w.selectBatch()
	require.Len(t, selected, 1)
	require.Equal(t, int32(batchRows+5), selected[0].Len())
	w.pendingDML = nil

	// Merging into an event whose Rows is nil must preserve src.Rows.
	dstNilRows := event.NewDMLEvent(common.DispatcherID{}, base.PhysicalTableID, base.StartTs, 500, base.TableInfo)
	srcWithRows := event.NewDMLEvent(common.DispatcherID{}, base.PhysicalTableID, base.StartTs, 500, base.TableInfo)
	srcWithRows.Rows = chunk.NewChunkWithCapacity(base.TableInfo.GetFieldSlice(), 1)
	srcWithRows.Rows.Append(base.Rows, 0, 1)
	srcWithRows.RowTypes = slices.Clone(base.RowTypes)
	srcWithRows.Length = 1
	w.AddDML(dstNilRows)
	w.AddDML(srcWithRows)
	require.Len(t, w.pendingDML, 1)
	require.Equal(t, int32(1), w.pendingDML[0].Len())
	require.NotNil(t, w.pendingDML[0].Rows)
	require.Equal(t, 1, w.pendingDML[0].Rows.NumRows())
}
