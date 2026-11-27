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

package conductor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	"github.com/pingcap/ticdc/cmd/consumer/decode"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/cmd/consumer/writer"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type stubReader struct {
	partitions int32
}

func (s *stubReader) Read(context.Context) ([]*source.Payload, error) { return nil, nil }
func (s *stubReader) Partitions() int32                               { return s.partitions }
func (s *stubReader) Close()                                          {}

type stubSchemaNotifierReader struct {
	stubReader
	processed []struct {
		tables    []commonEvent.SchemaTableName
		timestamp uint64
	}
}

func (s *stubSchemaNotifierReader) MarkProcessed(tables []commonEvent.SchemaTableName, timestamp uint64) {
	s.processed = append(s.processed, struct {
		tables    []commonEvent.SchemaTableName
		timestamp uint64
	}{
		tables:    tables,
		timestamp: timestamp,
	})
}

func TestConductorFlushDDLWithoutDML(t *testing.T) {
	c := New(&stubReader{partitions: 0}, codecCommon.NewTableInfoProvider())
	w := writer.New(t.Context(), writer.Options{
		DownstreamURI: "blackhole://",
		ReplicaConfig: config.GetDefaultReplicaConfig(),
	})

	var flushed []string
	for i, q := range []string{"CREATE TABLE test.t1 (id INT PRIMARY KEY)", "CREATE TABLE test.t2 (id INT PRIMARY KEY)"} {
		ddl := &commonEvent.DDLEvent{
			FinishedTs: uint64(100 + i),
			Type:       byte(timodel.ActionCreateTable),
			SchemaName: "test",
			TableName:  "t" + string(rune('1'+i)),
			Query:      q,
		}
		query := q
		ddl.AddPostFlushFunc(func() {
			flushed = append(flushed, query)
		})
		require.NoError(t, c.accept(ddl, &source.Payload{Partition: 0}))
	}

	require.NoError(t, c.flush(t.Context(), w))
	require.Empty(t, flushed)

	c.advanceWatermark(101, 0)
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{
		"CREATE TABLE test.t1 (id INT PRIMARY KEY)",
		"CREATE TABLE test.t2 (id INT PRIMARY KEY)",
	}, flushed)
	require.Empty(t, c.pendingDDL)
}

func TestConductorPositionsNonMonotonicIdleAndBoundedMemory(t *testing.T) {
	c := New(&stubReader{partitions: 2}, codecCommon.NewTableInfoProvider())

	// Partition 0: larger CommitTs (500) followed by smaller CommitTs (150).
	var acked0 atomic.Int64

	dmlHigh := &commonEvent.DMLEvent{CommitTs: 500, PhysicalTableID: 1}
	require.NoError(t, c.accept(dmlHigh, &source.Payload{Partition: 0, Offset: 1}))
	require.NoError(t, c.accept(nil, &source.Payload{Partition: 0, Offset: 1, Ack: func(context.Context) error {
		acked0.Store(1)
		return nil
	}}))

	dmlLow := &commonEvent.DMLEvent{CommitTs: 150, PhysicalTableID: 1}
	require.NoError(t, c.accept(dmlLow, &source.Payload{Partition: 0, Offset: 2}))
	require.NoError(t, c.accept(nil, &source.Payload{Partition: 0, Offset: 2, Ack: func(context.Context) error {
		acked0.Store(2)
		return nil
	}}))

	// Partition 1: idle partition receives only ResolvedEvent.
	var acked1 atomic.Int64
	require.NoError(t, c.accept(&commonEvent.ResolvedEvent{ResolvedTs: 200}, &source.Payload{Partition: 1, Offset: 10}))
	require.NoError(t, c.accept(nil, &source.Payload{Partition: 1, Offset: 10, Ack: func(context.Context) error {
		acked1.Store(10)
		return nil
	}}))

	// At checkpoint 200, Partition 0 must NOT skip past through=500 to ack offset 2.
	// Partition 1's idle resolved payload (through=0 <= 200) must ack offset 10.
	c.confirmPositions(t.Context(), 200)
	require.Equal(t, int64(0), acked0.Load())
	require.Equal(t, int64(10), acked1.Load())

	// At checkpoint 500, Partition 0 acks offset 2 and clears its slice.
	c.confirmPositions(t.Context(), 500)
	require.Equal(t, int64(2), acked0.Load())
	require.Empty(t, c.positions[0])

	// Repeated enqueue + partial confirm keeps slice capacity bounded.
	for i := uint64(1); i <= 2000; i++ {
		require.NoError(t, c.accept(&commonEvent.DMLEvent{CommitTs: 1000 + i, PhysicalTableID: 1}, &source.Payload{Partition: 0}))
		require.NoError(t, c.accept(nil, &source.Payload{Partition: 0, Ack: func(context.Context) error { return nil }}))
		if i%10 == 0 {
			c.confirmPositions(t.Context(), 1000+i-1)
			require.LessOrEqual(t, cap(c.positions[0]), 64)
		}
	}
	c.confirmPositions(t.Context(), 5000)
	require.Empty(t, c.positions[0])
}

func TestDDLBarrierScopeRenameExchangeAndCaseInsensitive(t *testing.T) {
	alterRename := &commonEvent.DDLEvent{
		Type:       byte(timodel.ActionRenameTable),
		SchemaName: "test",
		TableName:  "t2",
		Query:      "ALTER TABLE test.t1 RENAME TO test.t2",
	}
	require.NoError(t, setDDLTableNames(alterRename))
	require.ElementsMatch(t, []commonEvent.SchemaTableName{
		{SchemaName: "test", TableName: "t2"},
		{SchemaName: "test", TableName: "t1"},
	}, ddlProcessedTables(alterRename))

	renameTables := &commonEvent.DDLEvent{
		Type:       byte(timodel.ActionRenameTables),
		SchemaName: "test",
		TableName:  "t3",
		Query:      "RENAME TABLE test.t1 TO test.t3, test.t2 TO test.t4",
	}
	require.NoError(t, setDDLTableNames(renameTables))
	require.ElementsMatch(t, []commonEvent.SchemaTableName{
		{SchemaName: "test", TableName: "t3"},
		{SchemaName: "test", TableName: "t1"},
		{SchemaName: "test", TableName: "t2"},
		{SchemaName: "test", TableName: "t4"},
	}, ddlProcessedTables(renameTables))

	exchange := &commonEvent.DDLEvent{
		Type:       byte(timodel.ActionExchangeTablePartition),
		SchemaName: "test",
		TableName:  "p_tbl",
		Query:      "ALTER TABLE test.p_tbl EXCHANGE PARTITION p0 WITH TABLE test.n_tbl",
	}
	require.NoError(t, setDDLTableNames(exchange))
	require.ElementsMatch(t, []commonEvent.SchemaTableName{
		{SchemaName: "test", TableName: "p_tbl"},
		{SchemaName: "test", TableName: "n_tbl"},
	}, ddlProcessedTables(exchange))

	dml := &commonEvent.DMLEvent{
		PhysicalTableID: 99,
		TableInfo:       commonType.WrapTableInfo("TEST", &timodel.TableInfo{Name: ast.NewCIStr("T1")}),
	}
	require.True(t, ddl.BlocksTable(alterRename, dml))
	require.True(t, ddl.BlocksTable(renameTables, dml))
}

func TestConductorUnifiedWatermarkGateRowOrderAndHeapBlocking(t *testing.T) {
	c := New(&stubReader{partitions: 0}, codecCommon.NewTableInfoProvider())
	w := writer.New(t.Context(), writer.Options{DownstreamURI: "blackhole://", ReplicaConfig: config.GetDefaultReplicaConfig()})

	writerCtx, cancel := context.WithCancel(t.Context())
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- w.Start(writerCtx)
	}()
	defer func() {
		cancel()
		<-writerDone
	}()

	t1Info := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t1")})
	t2Info := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("t2")})

	var mu sync.Mutex
	var flushed []string
	record := func(label string) func() {
		return func() {
			mu.Lock()
			flushed = append(flushed, label)
			mu.Unlock()
		}
	}
	newDML := func(tableID int64, info *commonType.TableInfo, commitTs uint64, rt commonType.RowType, label string) *commonEvent.DMLEvent {
		dml := &commonEvent.DMLEvent{
			PhysicalTableID: tableID,
			CommitTs:        commitTs,
			TableInfo:       info,
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			RowTypes:        []commonType.RowType{rt},
			Length:          1,
		}
		dml.AddPostFlushFunc(record(label))
		return dml
	}

	require.NoError(t, c.accept(newDML(1, t1Info, 150, commonType.RowTypeInsert, "t1:insert@150"), &source.Payload{}))
	require.NoError(t, c.accept(newDML(1, t1Info, 150, commonType.RowTypeDelete, "t1:delete@150"), &source.Payload{}))
	ddl150 := &commonEvent.DDLEvent{
		FinishedTs: 150,
		Type:       byte(timodel.ActionAddColumn),
		SchemaName: "test",
		TableName:  "t1",
		Query:      "ALTER TABLE test.t1 ADD COLUMN c INT",
	}
	ddl150.AddPostFlushFunc(record("t1:ddl@150"))
	require.NoError(t, c.accept(ddl150, &source.Payload{}))
	require.NoError(t, c.accept(newDML(1, t1Info, 160, commonType.RowTypeInsert, "t1:insert@160"), &source.Payload{}))
	require.NoError(t, c.accept(newDML(2, t2Info, 130, commonType.RowTypeInsert, "t2:insert@130"), &source.Payload{}))

	c.advanceWatermark(140, 0)
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{"t2:insert@130"}, flushed)
	require.Equal(t, uint64(140), w.Checkpoint())
	require.Len(t, c.readyHeap, 1)
	require.Empty(t, c.blockedHeap)

	c.advanceWatermark(150, 0)
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{"t2:insert@130", "t1:delete@150", "t1:insert@150", "t1:ddl@150"}, flushed)
	require.Equal(t, uint64(150), w.Checkpoint())
	require.Len(t, c.readyHeap, 1)
	require.Empty(t, c.blockedHeap)

	c.advanceWatermark(160, 0)
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{"t2:insert@130", "t1:delete@150", "t1:insert@150", "t1:ddl@150", "t1:insert@160"}, flushed)
	require.Equal(t, uint64(160), w.Checkpoint())
	require.Empty(t, c.readyHeap)
	require.Empty(t, c.blockedHeap)
	require.Empty(t, c.pendingDML)
}

func TestConductorEffectiveWatermarkPendingDecodeAndInvertedOrder(t *testing.T) {
	schemas := codecCommon.NewTableInfoProvider()
	c := New(&stubReader{partitions: 2}, schemas)
	w := writer.New(t.Context(), writer.Options{DownstreamURI: "blackhole://", ReplicaConfig: config.GetDefaultReplicaConfig()})

	writerCtx, cancel := context.WithCancel(t.Context())
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- w.Start(writerCtx)
	}()
	defer func() {
		cancel()
		<-writerDone
	}()

	t1Info := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t1"), UpdateTS: 200})
	schemas.WriteTableInfo(t1Info, 0)

	var mu sync.Mutex
	var flushed []string
	record := func(label string) func() {
		return func() {
			mu.Lock()
			flushed = append(flushed, label)
			mu.Unlock()
		}
	}
	newDML := func(commitTs uint64, label string) *commonEvent.DMLEvent {
		dml := &commonEvent.DMLEvent{
			PhysicalTableID: 1,
			CommitTs:        commitTs,
			TableInfo:       t1Info,
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			RowTypes:        []commonType.RowType{commonType.RowTypeInsert},
			Length:          1,
		}
		dml.AddPostFlushFunc(record(label))
		return dml
	}

	// Pending placeholder at CommitTs=200 must cap effectiveW at 199 even when W=300.
	require.NoError(t, c.acceptFuture(&decode.Future{
		Payload: &source.Payload{Partition: 0, Offset: 1},
		Pending: []decode.PendingDecode{{ID: 1, CommitTs: 200}},
		Events:  []commonEvent.Event{nil},
	}))
	ddl220 := &commonEvent.DDLEvent{
		FinishedTs: 220,
		Type:       byte(timodel.ActionAddColumn),
		SchemaName: "test",
		TableName:  "t1",
		Query:      "ALTER TABLE test.t1 ADD COLUMN c INT",
	}
	ddl220.AddPostFlushFunc(record("t1:ddl@220"))
	require.NoError(t, c.acceptFuture(&decode.Future{
		Payload: &source.Payload{Partition: 0, Offset: 2},
		Events: []commonEvent.Event{
			ddl220,
			newDML(250, "t1:dml@250"),
			&commonEvent.ResolvedEvent{ResolvedTs: 300},
			nil,
		},
	}))
	require.NoError(t, c.acceptFuture(&decode.Future{
		Payload: &source.Payload{Partition: 1, Offset: 1},
		Events:  []commonEvent.Event{&commonEvent.ResolvedEvent{ResolvedTs: 300}, nil},
	}))

	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, uint64(199), c.effectiveWatermark())
	require.Equal(t, uint64(199), schemas.ReleasedWatermark())
	require.Empty(t, flushed)
	require.Equal(t, uint64(199), w.Checkpoint())

	// Resolving placeholder 1 releases effectiveW to 300, allowing 200 -> DDL 220 -> 250.
	require.NoError(t, c.acceptFuture(&decode.Future{
		Payload:  &source.Payload{Partition: 1, Offset: 2},
		Resolved: []decode.ResolvedDML{{ID: 1, DML: newDML(200, "t1:dml@200"), Partition: 0, Offset: 1}},
		Events:   []commonEvent.Event{nil},
	}))
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, uint64(300), c.effectiveWatermark())
	require.Equal(t, uint64(300), schemas.ReleasedWatermark())
	require.Equal(t, []string{"t1:dml@200", "t1:ddl@220", "t1:dml@250"}, flushed)
	require.Equal(t, uint64(300), w.Checkpoint())

	// Inverted arrival: ResolvedDML arrives before PendingDecode must clear cleanly without leaking.
	require.NoError(t, c.acceptFuture(&decode.Future{
		Payload:  &source.Payload{Partition: 1, Offset: 3},
		Resolved: []decode.ResolvedDML{{ID: 2, DML: newDML(305, "t1:dml@305"), Partition: 0, Offset: 3}},
		Events:   []commonEvent.Event{nil},
	}))
	require.NoError(t, c.acceptFuture(&decode.Future{
		Payload: &source.Payload{Partition: 0, Offset: 3},
		Pending: []decode.PendingDecode{{ID: 2, CommitTs: 305}},
		Events:  []commonEvent.Event{nil},
	}))
	require.Empty(t, c.pendingDecodes)
	require.Empty(t, c.resolvedDecodes)
	require.Equal(t, uint64(300), c.effectiveWatermark())
}

func TestConductorBlockedTableReentersReadyHeapAndCreateTableLike(t *testing.T) {
	c := New(&stubReader{partitions: 0}, codecCommon.NewTableInfoProvider())
	w := writer.New(t.Context(), writer.Options{DownstreamURI: "blackhole://", ReplicaConfig: config.GetDefaultReplicaConfig()})

	writerCtx, cancel := context.WithCancel(t.Context())
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- w.Start(writerCtx)
	}()
	defer func() {
		cancel()
		<-writerDone
	}()

	t1Info := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t1")})
	var mu sync.Mutex
	var flushed []string
	record := func(label string) func() {
		return func() {
			mu.Lock()
			flushed = append(flushed, label)
			mu.Unlock()
		}
	}
	newDML := func(commitTs uint64, label string) *commonEvent.DMLEvent {
		dml := &commonEvent.DMLEvent{
			PhysicalTableID: 1,
			CommitTs:        commitTs,
			TableInfo:       t1Info,
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			RowTypes:        []commonType.RowType{commonType.RowTypeInsert},
			Length:          1,
		}
		dml.AddPostFlushFunc(record(label))
		return dml
	}

	// Queue ALTER@150 and DML@160 first so t1 sits in blockedHeap, then deliver an earlier DML@140.
	ddl150 := &commonEvent.DDLEvent{
		FinishedTs: 150,
		Type:       byte(timodel.ActionAddColumn),
		SchemaName: "test",
		TableName:  "t1",
		Query:      "ALTER TABLE test.t1 ADD COLUMN c INT",
	}
	ddl150.AddPostFlushFunc(record("t1:ddl@150"))
	require.NoError(t, c.accept(ddl150, &source.Payload{}))
	require.NoError(t, c.accept(newDML(160, "t1:dml@160"), &source.Payload{}))
	require.Empty(t, c.readyHeap)
	require.Len(t, c.blockedHeap, 1)

	require.NoError(t, c.accept(newDML(140, "t1:dml@140"), &source.Payload{}))
	require.Len(t, c.readyHeap, 1)
	require.Empty(t, c.blockedHeap)

	c.advanceWatermark(150, 0)
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{"t1:dml@140", "t1:ddl@150"}, flushed)

	// CREATE TABLE ... LIKE ... references a source table, so it must wait for W >= FinishedTs.
	createLike := &commonEvent.DDLEvent{
		FinishedTs: 200,
		Type:       byte(timodel.ActionCreateTable),
		SchemaName: "test",
		TableName:  "t2",
		Query:      "CREATE TABLE test.t2 LIKE test.t1",
	}
	createLike.AddPostFlushFunc(record("t2:create_like@200"))
	require.NoError(t, c.accept(createLike, &source.Payload{}))
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{"t1:dml@140", "t1:ddl@150"}, flushed)

	c.advanceWatermark(200, 0)
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{"t1:dml@140", "t1:ddl@150", "t1:dml@160", "t2:create_like@200"}, flushed)
}

func TestConductorCreateTableLikeMarksOnlyCreatedTableProcessed(t *testing.T) {
	reader := &stubSchemaNotifierReader{}
	c := New(reader, codecCommon.NewTableInfoProvider())

	createLike := &commonEvent.DDLEvent{
		FinishedTs: 200,
		Type:       byte(timodel.ActionCreateTable),
		SchemaName: "test",
		TableName:  "t2",
		Query:      "CREATE TABLE test.t2 LIKE test.t1",
	}
	require.NoError(t, c.accept(createLike, &source.Payload{}))
	require.Len(t, reader.processed, 1)
	require.Equal(t, uint64(200), reader.processed[0].timestamp)
	require.Equal(t, []commonEvent.SchemaTableName{{SchemaName: "test", TableName: "t2"}}, reader.processed[0].tables)
	require.Equal(t, []commonEvent.SchemaTableName{
		{SchemaName: "test", TableName: "t2"},
		{SchemaName: "test", TableName: "t1"},
	}, createLike.BlockedTableNames)
}

func TestConductorWaitsWatermarkForOutOfOrderCreateDDLs(t *testing.T) {
	c := New(&stubReader{partitions: 2}, codecCommon.NewTableInfoProvider())
	w := writer.New(t.Context(), writer.Options{
		DownstreamURI: "blackhole://",
		ReplicaConfig: config.GetDefaultReplicaConfig(),
	})

	var flushed []string
	createTable := &commonEvent.DDLEvent{
		FinishedTs: 120,
		Type:       byte(timodel.ActionCreateTable),
		SchemaName: "test",
		TableName:  "t",
		Query:      "CREATE TABLE test.t (id INT PRIMARY KEY)",
	}
	createTable.AddPostFlushFunc(func() {
		flushed = append(flushed, "create_table@120")
	})
	createDB := &commonEvent.DDLEvent{
		FinishedTs: 100,
		Type:       byte(timodel.ActionCreateSchema),
		SchemaName: "test",
		Query:      "CREATE DATABASE test",
	}
	createDB.AddPostFlushFunc(func() {
		flushed = append(flushed, "create_db@100")
	})

	// Partition 0 delivers CREATE TABLE@120 before partition 1 delivers CREATE DATABASE@100.
	require.NoError(t, c.accept(createTable, &source.Payload{Partition: 0}))
	c.advanceWatermark(120, 0)
	require.NoError(t, c.flush(t.Context(), w))
	require.Empty(t, flushed)

	require.NoError(t, c.accept(createDB, &source.Payload{Partition: 1}))
	c.advanceWatermark(120, 1)
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, []string{"create_db@100", "create_table@120"}, flushed)
}

type stubValidatorReader struct {
	stubReader
	onCheck func(dml *commonEvent.DMLEvent)
}

func (s *stubValidatorReader) CheckDML(dml *commonEvent.DMLEvent, _ int32, _ int64) {
	if s.onCheck != nil {
		s.onCheck(dml)
	}
}

func TestConductorFlushDrainsWholeCommitTsGroupAndPreservesRoundBoundForZeroTs(t *testing.T) {
	var flushedCount atomic.Int32
	var drainedOrder []int64
	var observedFlushedDuringT1 []int32

	reader := &stubValidatorReader{}
	c := New(reader, codecCommon.NewTableInfoProvider())
	w := writer.New(t.Context(), writer.Options{
		DownstreamURI: "blackhole://",
		ReplicaConfig: config.GetDefaultReplicaConfig(),
	})

	writerCtx, cancel := context.WithCancel(t.Context())
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- w.Start(writerCtx)
	}()
	defer func() {
		cancel()
		<-writerDone
	}()

	reader.onCheck = func(dml *commonEvent.DMLEvent) {
		if dml.CommitTs == 100 {
			drainedOrder = append(drainedOrder, dml.PhysicalTableID)
			if dml.PhysicalTableID == 1 {
				observedFlushedDuringT1 = append(observedFlushedDuringT1, flushedCount.Load())
			}
			return
		}
		if dml.CommitTs == 0 {
			observedFlushedDuringT1 = append(observedFlushedDuringT1, flushedCount.Load())
		}
	}

	t1Info := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t1")})
	t2Info := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("t2")})
	newDML := func(tableID int64, info *commonType.TableInfo, commitTs uint64, rt commonType.RowType) *commonEvent.DMLEvent {
		dml := &commonEvent.DMLEvent{
			PhysicalTableID: tableID,
			CommitTs:        commitTs,
			TableInfo:       info,
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			RowTypes:        []commonType.RowType{rt},
			Length:          1,
		}
		dml.AddPostFlushFunc(func() {
			flushedCount.Add(1)
		})
		return dml
	}

	// Queue roundRows+5 rows for t1@100 and 2 rows for t2@100 (both with Delete & Insert).
	t1GroupRows := roundRows + 5
	require.NoError(t, c.accept(newDML(1, t1Info, 100, commonType.RowTypeDelete), &source.Payload{}))
	for i := 1; i < t1GroupRows; i++ {
		require.NoError(t, c.accept(newDML(1, t1Info, 100, commonType.RowTypeInsert), &source.Payload{}))
	}
	require.NoError(t, c.accept(newDML(2, t2Info, 100, commonType.RowTypeInsert), &source.Payload{}))
	require.NoError(t, c.accept(newDML(2, t2Info, 100, commonType.RowTypeDelete), &source.Payload{}))

	c.advanceWatermark(100, 0)
	require.NoError(t, c.flush(t.Context(), w))

	// Entire t1@100 group (including rows past roundRows) must drain contiguously in a single
	// flush round before t2@100 begins.
	require.Len(t, drainedOrder, t1GroupRows+2)
	for i := 0; i < t1GroupRows; i++ {
		require.Equal(t, int64(1), drainedOrder[i])
		require.Equal(t, int32(0), observedFlushedDuringT1[i])
	}
	require.Equal(t, int64(2), drainedOrder[t1GroupRows])
	require.Equal(t, int64(2), drainedOrder[t1GroupRows+1])
	require.Equal(t, int32(t1GroupRows+2), flushedCount.Load())

	// CommitTs == 0 rows must still honor roundRows instead of draining unboundedly in one round.
	flushedCount.Store(0)
	observedFlushedDuringT1 = nil
	zeroRows := roundRows + 10
	for i := 0; i < zeroRows; i++ {
		require.NoError(t, c.accept(newDML(1, t1Info, 0, commonType.RowTypeInsert), &source.Payload{}))
	}
	require.NoError(t, c.flush(t.Context(), w))
	require.Len(t, observedFlushedDuringT1, zeroRows)
	require.Equal(t, int32(0), observedFlushedDuringT1[roundRows-1])
	require.Equal(t, int32(roundRows), observedFlushedDuringT1[roundRows])
	require.Equal(t, int32(zeroRows), flushedCount.Load())
}
