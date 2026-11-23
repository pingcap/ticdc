// Copyright 2023 PingCAP, Inc.
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

func TestColumnsDispatcher(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t1(COL2 int, Col1 int, col3 int)")
	require.NotNil(t, job)
	tableInfo := helper.GetTableInfo(job)
	dml := helper.DML2Event("test", "t1", "insert into t1 values(22, 11, 33)")
	row, exist := dml.GetNextRow()
	require.True(t, exist)

	p := newColumnsPartitionGenerator([]string{"col-2", "col-not-found"})
	_, _, err := p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 1)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = newColumnsPartitionGenerator([]string{"col2", "col1"})
	index, _, err := p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 1)
	require.NoError(t, err)
	require.Equal(t, int32(5), index)

	p = newColumnsPartitionGenerator([]string{"COL2", "Col1"})
	index, _, err = p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 1)
	require.NoError(t, err)
	require.Equal(t, int32(5), index)
}

func TestPartitionGeneratorWithNullableColumns(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t(id bigint not null primary key, k bigint null, unique key idx_k(k))")
	tableInfo := helper.GetTableInfo(job)
	nullRow := genRow(t, helper, "test", "t", "insert into t values(1, NULL)")
	nonNullRow := genRow(t, helper, "test", "t", "insert into t values(1, 9)")

	for _, rule := range []struct {
		name      string
		generator Generator
	}{
		{name: "columns", generator: newColumnsPartitionGenerator([]string{"k"})},
		{name: "index-value", generator: newIndexValuePartitionGenerator("idx_k")},
	} {
		t.Run(rule.name, func(t *testing.T) {
			for _, tc := range []struct {
				name            string
				row             event.RowChange
				expectPartition int32
				expectKey       string
			}{
				{name: "insert null", row: *nullRow, expectPartition: 15, expectKey: "2689334255"},
				{name: "insert non-null", row: *nonNullRow, expectPartition: 6, expectKey: "349258086"},
				{
					name:            "update to null",
					row:             event.RowChange{PreRow: nonNullRow.Row, Row: nullRow.Row, RowType: common.RowTypeUpdate},
					expectPartition: 15, expectKey: "2689334255",
				},
				{
					name:            "update to non-null",
					row:             event.RowChange{PreRow: nullRow.Row, Row: nonNullRow.Row, RowType: common.RowTypeUpdate},
					expectPartition: 6, expectKey: "349258086",
				},
				{
					name:            "delete null",
					row:             event.RowChange{PreRow: nullRow.Row, RowType: common.RowTypeDelete},
					expectPartition: 15, expectKey: "2689334255",
				},
				{
					name:            "delete non-null",
					row:             event.RowChange{PreRow: nonNullRow.Row, RowType: common.RowTypeDelete},
					expectPartition: 6, expectKey: "349258086",
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					index, key, err := rule.generator.GeneratePartitionIndexAndKey(&tc.row, 16, tableInfo, 1)
					require.NoError(t, err)
					// These partitions and keys are produced by the legacy architecture.
					require.Equal(t, tc.expectPartition, index)
					require.Equal(t, tc.expectKey, key)
				})
			}
		})
	}
}
