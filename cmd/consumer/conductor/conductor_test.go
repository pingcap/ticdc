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
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/cmd/consumer/writer"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

type stubReader struct {
	partitions int32
}

func (s *stubReader) Read(context.Context) ([]*source.Payload, error) { return nil, nil }
func (s *stubReader) Partitions() int32                               { return s.partitions }
func (s *stubReader) Close()                                          {}

func TestConductorFlushDDLWithoutDML(t *testing.T) {
	c := New(&stubReader{partitions: 0}, codecCommon.NewTableInfoProvider())
	w := writer.New(t.Context(), writer.NewOptions(
		"blackhole://",
		config.GetDefaultReplicaConfig(),
	))

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
