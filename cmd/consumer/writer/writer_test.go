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

package writer

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type recordingSink struct {
	sink.Sink
	hold   bool
	dmls   []*event.DMLEvent
	blocks []event.BlockEvent
}

func (s *recordingSink) AddDMLEvent(dml *event.DMLEvent) {
	s.dmls = append(s.dmls, dml)
	if !s.hold {
		dml.PostFlush()
	}
}

func (s *recordingSink) FlushDMLBeforeBlock(event.BlockEvent) error { return nil }

func (s *recordingSink) WriteBlockEvent(block event.BlockEvent) error {
	s.blocks = append(s.blocks, block)
	block.PostFlush()
	return nil
}

func (s *recordingSink) AddCheckpointTs(uint64) {}
func (s *recordingSink) Close()                 {}
func (s *recordingSink) BatchCount() int        { return batchRows }
func (s *recordingSink) BatchBytes() int        { return batchBytes }

func (s *recordingSink) releaseHeld() {
	for _, dml := range s.dmls {
		dml.PostFlush()
	}
}

func newWriterForTest(target sink.Sink, memory *budget.Budget, cfg *codecCommon.Config) *Writer {
	return &Writer{
		target:    target,
		memory:    memory,
		merger:    newRowMerger(cfg, memory),
		completed: make(chan struct{}, 1),
		mutations: make(map[mutationKey]*writeBatch),
	}
}

func testTableInfo(updateTS uint64) *common.TableInfo {
	fieldType := types.NewFieldType(mysql.TypeLong)
	fieldType.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	return common.WrapTableInfo("test", &model.TableInfo{
		ID:         20,
		Name:       ast.NewCIStr("t"),
		UpdateTS:   updateTS,
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), FieldType: *fieldType, State: model.StatePublic, Offset: 0},
		},
	})
}

func testInsert(tableInfo *common.TableInfo, id int64, commitTs uint64) *event.DMLEvent {
	dml := event.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, commitTs-1, commitTs, tableInfo)
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	rows.AppendRow(chunk.MutRowFromValues(id).ToRow())
	dml.Rows = rows
	dml.RowTypes = []common.RowType{common.RowTypeInsert}
	dml.Length = 1
	dml.ApproximateSize = rows.MemoryUsage()
	return dml
}

func testDDL(commitTs uint64, release func()) *event.DDLEvent {
	return &event.DDLEvent{
		Type:       byte(model.ActionCreateTable),
		SchemaName: "test",
		TableName:  "t",
		FinishedTs: commitTs,
		Query:      "create table test.t (id bigint primary key)",
		BlockedTables: &event.InfluencedTables{
			InfluenceType: event.InfluenceTypeNormal,
			TableIDs:      []int64{20},
		},
	}
}

func TestAddDMLHonorsInFlightLimit(t *testing.T) {
	memory := budget.New()
	w := newWriterForTest(&recordingSink{}, memory, codecCommon.NewConfig(config.ProtocolOpen))
	dml := testInsert(testTableInfo(100), 1, 100)
	memory.ForceAcquire(dml.Rows.MemoryUsage())

	w.inFlightEvents = maxInFlightEvents
	require.False(t, w.AddDML(dml), "the in-flight limit must stop admission")

	w.inFlightEvents = 0
	require.True(t, w.AddDML(dml))
}

func TestDDLFencesRowsAndReleasesOnce(t *testing.T) {
	memory := budget.New()
	target := &recordingSink{}
	w := newWriterForTest(target, memory, codecCommon.NewConfig(config.ProtocolOpen))
	tableInfo := testTableInfo(100)

	var releases int
	ddl := testDDL(100, func() { releases++ })
	w.AddDDL(ddl, func() { releases++ })

	after := testInsert(tableInfo, 1, 110)
	require.False(t, w.AddDML(after), "a pending DDL must block later rows")

	before := testInsert(tableInfo, 1, 90)
	memory.ForceAcquire(before.Rows.MemoryUsage())
	require.True(t, w.AddDML(before))
	require.NoError(t, w.Flush(t.Context()))

	require.Len(t, target.dmls, 1)
	require.Len(t, target.blocks, 1)
	require.Equal(t, uint64(90), target.dmls[0].CommitTs)
	require.Same(t, ddl, target.blocks[0])
	require.Equal(t, 1, releases)

	// A replayed DDL is retired without reaching the sink a second time.
	w.AddDDL(ddl, func() { releases++ })
	require.NoError(t, w.Flush(t.Context()))
	require.Len(t, target.blocks, 1)
	require.Equal(t, 2, releases)
}

func TestCheckpointWaitsForPendingRows(t *testing.T) {
	memory := budget.New()
	target := &recordingSink{}
	w := newWriterForTest(target, memory, codecCommon.NewConfig(config.ProtocolOpen))
	tableInfo := testTableInfo(100)

	dml := testInsert(tableInfo, 1, 150)
	memory.ForceAcquire(dml.Rows.MemoryUsage())
	require.True(t, w.AddDML(dml))
	w.Advance(Progress{Watermark: 200})
	w.advanceCheckpoint()
	require.Equal(t, uint64(149), w.Checkpoint(), "a pending row caps the checkpoint below its commit ts")

	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(200), w.Checkpoint())
}

func TestDuplicateRowIsDropped(t *testing.T) {
	memory := budget.New()
	target := &recordingSink{}
	w := newWriterForTest(target, memory, codecCommon.NewConfig(config.ProtocolOpen))
	tableInfo := testTableInfo(100)

	first := testInsert(tableInfo, 7, 100)
	second := testInsert(tableInfo, 7, 100)
	memory.ForceAcquire(first.Rows.MemoryUsage())
	memory.ForceAcquire(second.Rows.MemoryUsage())
	require.True(t, w.AddDML(first))
	require.True(t, w.AddDML(second))

	require.NoError(t, w.Flush(t.Context()))
	require.Len(t, target.dmls, 1)
	require.Equal(t, int64(7), target.dmls[0].Rows.GetRow(0).GetInt64(0))
}

func TestCSVMergerCombinesSplitUpdate(t *testing.T) {
	memory := budget.New()
	target := &recordingSink{}
	w := newWriterForTest(target, memory, codecCommon.NewConfig(config.ProtocolCsv))
	tableInfo := testTableInfo(100)

	insert := testInsert(tableInfo, 1, 100)
	deleteRow := testInsert(tableInfo, 1, 100)
	deleteRow.RowTypes = []common.RowType{common.RowTypeDelete}
	memory.ForceAcquire(insert.Rows.MemoryUsage())
	memory.ForceAcquire(deleteRow.Rows.MemoryUsage())

	require.True(t, w.AddDML(insert))
	require.True(t, w.AddDML(deleteRow))
	require.NoError(t, w.Flush(t.Context()))

	require.Len(t, target.dmls, 1)
	require.Len(t, target.dmls[0].RowTypes, 2)
	require.Zero(t, memory.Used())
}

func TestDDLReleaseIsCalled(t *testing.T) {
	memory := budget.New()
	target := &recordingSink{}
	w := newWriterForTest(target, memory, codecCommon.NewConfig(config.ProtocolOpen))
	called := false
	w.AddDDL(testDDL(100, nil), func() { called = true })
	w.Advance(Progress{Watermark: 200})
	require.NoError(t, w.Flush(context.Background()))
	require.True(t, called)
	require.Len(t, target.blocks, 1, "a DDL accepted after progress passed it must still be written")
}

func TestAddDMLChecksLatestInFlightTable(t *testing.T) {
	memory := budget.New()
	w := newWriterForTest(&recordingSink{hold: true}, memory, codecCommon.NewConfig(config.ProtocolOpen))

	first := testInsert(testTableInfo(100), 1, 100)
	memory.ForceAcquire(first.Rows.MemoryUsage())
	require.True(t, w.AddDML(first))
	require.NoError(t, w.Flush(t.Context()))

	changed := testInsert(testTableInfo(200), 1, 110)
	memory.ForceAcquire(changed.Rows.MemoryUsage())
	require.False(t, w.AddDML(changed), "a new schema for an in-flight table must wait")
	memory.Release(changed.Rows.MemoryUsage())
}
