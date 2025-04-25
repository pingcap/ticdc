// Copyright 2022 PingCAP, Inc.
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

package partition

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func genRow(t *testing.T, helper *event.EventTestHelper, schema string, table string, dml ...string) *event.RowChange {
	event := helper.DML2Event(schema, table, dml...)
	row, exist := event.GetNextRow()
	require.True(t, exist)
	helper.DDL2Job("TRUNCATE TABLE " + table)
	return &row
}

func TestIndexValueDispatcher(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	job1 := helper.DDL2Job("create table t1(a int primary key, b int)")
	require.NotNil(t, job1)
	job2 := helper.DDL2Job("create table t2(a int, b int, primary key(a,b))")
	require.NotNil(t, job2)
	tableInfoWithSinglePK := helper.GetTableInfo(job1)
	tableInfoWithCompositePK := helper.GetTableInfo(job2)

	testCases := []struct {
		row             *event.RowChange
		tableInfo       *common.TableInfo
		expectPartition int32
	}{
		{row: genRow(t, helper, "test", "t1", "insert into t1 values(11, 12)"), tableInfo: tableInfoWithSinglePK, expectPartition: 2},
		{row: genRow(t, helper, "test", "t1", "insert into t1 values(22, 22)"), tableInfo: tableInfoWithSinglePK, expectPartition: 11},
		{row: genRow(t, helper, "test", "t1", "insert into t1 values(11, 33)"), tableInfo: tableInfoWithSinglePK, expectPartition: 2},
		{row: genRow(t, helper, "test", "t2", "insert into t2 values(11, 22)"), tableInfo: tableInfoWithCompositePK, expectPartition: 5},
		{row: genRow(t, helper, "test", "t2", "insert into t2 (b, a) values(22, 11)"), tableInfo: tableInfoWithCompositePK, expectPartition: 5},
		{row: genRow(t, helper, "test", "t2", "insert into t2 values(11, 0)"), tableInfo: tableInfoWithCompositePK, expectPartition: 14},
		{row: genRow(t, helper, "test", "t2", "insert into t2 values(11, 33)"), tableInfo: tableInfoWithCompositePK, expectPartition: 2},
	}
	p := newIndexValuePartitionGenerator("")
	for _, tc := range testCases {
		index, _, err := p.GeneratePartitionIndexAndKey(tc.row, 16, tc.tableInfo, 1)
		require.Equal(t, tc.expectPartition, index)
		require.NoError(t, err)
	}
}

func TestIndexValueDispatcherWithIndexName(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t1(a int, INDEX index1(a))")
	require.NotNil(t, job)
	tableInfo := helper.GetTableInfo(job)
	dml := helper.DML2Event("test", "t1", "insert into t1 values(11)")
	row, exist := dml.GetNextRow()
	require.True(t, exist)

	p := newIndexValuePartitionGenerator("index2")
	_, _, err := p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 33)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = newIndexValuePartitionGenerator("index1")
	index, _, err := p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 33)
	require.NoError(t, err)
	require.Equal(t, int32(2), index)

	p = newIndexValuePartitionGenerator("INDEX1")
	index, _, err = p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 33)
	require.NoError(t, err)
	require.Equal(t, int32(2), index)

	p = newIndexValuePartitionGenerator("")
	index, _, err = p.GeneratePartitionIndexAndKey(&row, 3, tableInfo, 33)
	require.NoError(t, err)
	require.Equal(t, int32(0), index)
}

func TestXXX(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	ctx := context.Background()
	codecConfig := codecCommon.NewConfig(config.ProtocolCanalJSON)

	encoder, err := canal.NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := canal.NewCanalJSONDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	createDB := helper.DDL2Event(`CREATE DATABASE dispatcher`)

	m, err := encoder.EncodeDDLEvent(createDB)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	_, _ = decoder.HasNext()

	_ = decoder.NextDDLEvent()

	createTable := helper.DDL2Event(`CREATE TABLE dispatcher.index (a int primary key, b int)`)

	m, err = encoder.EncodeDDLEvent(createTable)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)
	_, _ = decoder.HasNext()
	_ = decoder.NextDDLEvent()

	dispatcher := newIndexValuePartitionGenerator("")

	insert := helper.DML2Event("dispatcher", "index", "INSERT INTO dispatcher.index values (1, 2);")
	require.NotNil(t, insert)
	insertRow, ok := insert.GetNextRow()
	require.True(t, ok)

	origin, _, err := dispatcher.GeneratePartitionIndexAndKey(&insertRow, 3, insert.TableInfo, insert.GetCommitTs())
	require.NoError(t, err)

	columnSelector := columnselector.NewDefaultColumnSelector()
	insertEvent := &commonEvent.RowEvent{
		TableInfo:      insert.TableInfo,
		CommitTs:       insert.GetCommitTs(),
		Event:          insertRow,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	err = encoder.AppendRowChangedEvent(ctx, "", insertEvent)
	require.NoError(t, err)

	messages := encoder.Build()
	require.Len(t, messages, 1)

	decoder.AddKeyValue(messages[0].Key, messages[0].Value)
	_, _ = decoder.HasNext()
	decodedInsert := decoder.NextDMLEvent()

	decodedRow, ok := decodedInsert.GetNextRow()
	require.True(t, ok)

	obtained, _, err := dispatcher.GeneratePartitionIndexAndKey(&decodedRow, 3, insert.TableInfo, insert.GetCommitTs())
	require.NoError(t, err)

	require.Equal(t, origin, obtained)
}
