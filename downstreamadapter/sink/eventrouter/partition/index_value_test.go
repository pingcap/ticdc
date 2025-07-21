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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
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
	job := helper.DDL2Job("create table t1(COL2 int not null, Col1 int not null, col3 int, CONSTRAINT index1 UNIQUE KEY `index1`(COL2,Col1))")
	require.NotNil(t, job)
	tableInfo := helper.GetTableInfo(job)
	dml := helper.DML2Event("test", "t1", "insert into t1 values(22, 11, 33)")
	row, exist := dml.GetNextRow()
	require.True(t, exist)

	p := newIndexValuePartitionGenerator("index2")
	_, _, err := p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 33)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = newIndexValuePartitionGenerator("index1")
	index, _, err := p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 33)
	require.NoError(t, err)
	require.Equal(t, int32(5), index)

	p = newIndexValuePartitionGenerator("INDEX1")
	index, _, err = p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 33)
	require.NoError(t, err)
	require.Equal(t, int32(5), index)

	p = newIndexValuePartitionGenerator("")
	index, _, err = p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 33)
	require.NoError(t, err)
	require.Equal(t, int32(5), index)
}
