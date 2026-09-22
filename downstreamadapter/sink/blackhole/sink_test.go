// Copyright 2024 PingCAP, Inc.
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

package blackhole

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// newInsertEventForTest builds an insert event by hand: bootstrapping the TiDB
// mock store just to get a table info dominates this package's test time.
func newInsertEventForTest(t *testing.T) *commonEvent.DMLEvent {
	idFieldType := types.NewFieldType(mysql.TypeLong)
	idFieldType.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	nameFieldType := types.NewFieldType(mysql.TypeVarchar)
	nameFieldType.SetFlen(32)

	tableInfo := commonType.WrapTableInfo("test", &model.TableInfo{
		ID:       20,
		Name:     ast.NewCIStr("t"),
		UpdateTS: 100,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), FieldType: *idFieldType, State: model.StatePublic, Offset: 0},
			{ID: 2, Name: ast.NewCIStr("name"), FieldType: *nameFieldType, State: model.StatePublic, Offset: 1},
		},
	})
	require.NotNil(t, tableInfo)

	event := commonEvent.NewDMLEvent(
		commonType.NewDispatcherID(),
		tableInfo.TableName.TableID,
		1,
		2,
		tableInfo,
	)
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	rows.AppendRow(chunk.MutRowFromValues(int64(1), "test").ToRow())
	rows.AppendRow(chunk.MutRowFromValues(int64(2), "test2").ToRow())
	event.SetRows(rows)
	event.RowTypes = append(event.RowTypes, commonType.RowTypeInsert)
	event.Length = 2
	event.TableInfoVersion = tableInfo.GetUpdateTS()
	return event
}

// Test callback and tableProgress works as expected after AddDMLEvent
func TestBlacHoleSinkBasicFunctionality(t *testing.T) {
	sink, err := New(commonType.NewChangefeedID(commonType.DefaultKeyspaceName), commonType.DefaultKeyspaceID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.Run(ctx)
	require.NoError(t, err)

	var count atomic.Int32
	count.Swap(0)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      "create table t (id int primary key, name varchar(32))",
		SchemaName: "test",
		TableName:  "t",
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	ddlEvent2 := &commonEvent.DDLEvent{
		Query:      "create table t (id int primary key, name varchar(32))",
		SchemaName: "test",
		TableName:  "t",
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	dmlEvent := newInsertEventForTest(t)
	dmlEvent.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}
	dmlEvent.CommitTs = 2

	err = sink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	sink.AddDMLEvent(dmlEvent)
	require.Eventually(t, func() bool {
		return count.Load() == 2
	}, 5*time.Second, 10*time.Millisecond, "the DML event should be flushed")

	ddlEvent2.PostFlush()

	require.Equal(t, count.Load(), int32(3))
}

func TestBlackHoleSinkBatchConfig(t *testing.T) {
	sink, err := New(commonType.NewChangefeedID(commonType.DefaultKeyspaceName), commonType.DefaultKeyspaceID)
	require.NoError(t, err)
	require.Equal(t, 4096, sink.BatchCount())
	require.Zero(t, sink.BatchBytes())
}
