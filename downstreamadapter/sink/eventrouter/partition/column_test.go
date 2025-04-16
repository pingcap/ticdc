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

	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestColumnsDispatcher(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t1(col1 int, col2 int, col3 int)")
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
	require.Equal(t, int32(15), index)

	p = newColumnsPartitionGenerator([]string{"COL2", "Col1"})
	index, _, err = p.GeneratePartitionIndexAndKey(&row, 16, tableInfo, 1)
	require.NoError(t, err)
	require.Equal(t, int32(15), index)
}
