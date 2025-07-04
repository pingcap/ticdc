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

package schemastore

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestIsSplitable(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQLWithPK := "create table t1 (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQLWithPK)
	tableInfo := helper.GetModelTableInfo(job)
	require.True(t, isSplitable(tableInfo))

	createTableSQLWithPKAndUK := "CREATE TABLE t2 (student_id INT PRIMARY KEY, first_name VARCHAR(50) NOT NULL,last_name VARCHAR(50) NOT NULL,email VARCHAR(100) UNIQUE);"
	job = helper.DDL2Job(createTableSQLWithPKAndUK)
	tableInfo = helper.GetModelTableInfo(job)
	require.False(t, isSplitable(tableInfo))

	createTableSQLWithNoPK := "create table t3 (id int, name varchar(32));"
	job = helper.DDL2Job(createTableSQLWithNoPK)
	tableInfo = helper.GetModelTableInfo(job)
	require.False(t, isSplitable(tableInfo))

	createTableSQLWithVarcharPK := "create table t4 (id varchar(32) primary key, name varchar(32));"
	job = helper.DDL2Job(createTableSQLWithVarcharPK)
	tableInfo = helper.GetModelTableInfo(job)
	require.True(t, isSplitable(tableInfo))

	createTableSQLWithVarcharPKNONCLUSTERED := "create table t5 (a varchar(200), b int, primary key(a) NONCLUSTERED);"
	job = helper.DDL2Job(createTableSQLWithVarcharPKNONCLUSTERED)
	tableInfo = helper.GetModelTableInfo(job)
	require.True(t, isSplitable(tableInfo))

	createTableSQLWithMultiPK := "create table t6 (a int, b int, primary key(a, b));"
	job = helper.DDL2Job(createTableSQLWithMultiPK)
	tableInfo = helper.GetModelTableInfo(job)
	require.True(t, isSplitable(tableInfo))

	createTableSQLWithUK := "create table t7 (a int, b int, unique key(a, b));"
	job = helper.DDL2Job(createTableSQLWithUK)
	tableInfo = helper.GetModelTableInfo(job)
	require.False(t, isSplitable(tableInfo))
}
