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
	"github.com/pingcap/tidb/pkg/meta/model"
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

func TestBuildPersistedDDLEventForMultiSchemaChangeContainsIndexIDs(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, c1 int)")

	job := helper.DDL2Job("alter table t add column c2 int, add index (c1)")
	require.Equal(t, model.ActionMultiSchemaChange, job.Type)

	args := buildPersistedDDLEventFuncArgs{
		job: job,
		databaseMap: map[int64]*BasicDatabaseInfo{
			job.SchemaID: {
				Name: "test",
				Tables: map[int64]bool{
					job.TableID: true,
				},
			},
		},
		tableMap: map[int64]*BasicTableInfo{
			job.TableID: {
				SchemaID: job.SchemaID,
				Name:     "t",
			},
		},
	}

	event := buildPersistedDDLEventForMultiSchemaChange(args)
	expectedIndexIDs := getIndexIDs(job)
	require.Len(t, expectedIndexIDs, 1)
	require.Equal(t, expectedIndexIDs, event.IndexIDs)
	require.Equal(t, "test", event.SchemaName)
	require.Equal(t, "t", event.TableName)
}

func TestGetIndexIDsReturnsAllAddIndexIDsInOrder(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, c1 int)")

	job := helper.DDL2Job("alter table t add index idx_c1(c1), add index (c1)")
	tableInfo := helper.GetModelTableInfo(job)
	require.NotNil(t, tableInfo)

	var namedIndexID int64
	var anonymousIndexID int64
	for _, index := range tableInfo.Indices {
		if index == nil {
			continue
		}
		if index.Name.O == "idx_c1" {
			namedIndexID = index.ID
			continue
		}
		if len(index.Columns) == 1 && index.Columns[0].Name.L == "c1" {
			anonymousIndexID = index.ID
		}
	}
	require.NotZero(t, namedIndexID)
	require.NotZero(t, anonymousIndexID)
	require.Equal(t, []int64{namedIndexID, anonymousIndexID}, getIndexIDs(job))
}

func TestGetIndexIDsReturnsAllAddIndexIDsInOrderForMultiSchemaChange(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, c1 int)")

	job := helper.DDL2Job("alter table t add column c2 int, add index idx_c1(c1), add index (c1)")
	require.Equal(t, model.ActionMultiSchemaChange, job.Type)
	tableInfo := helper.GetModelTableInfo(job)
	require.NotNil(t, tableInfo)

	var namedIndexID int64
	var anonymousIndexID int64
	for _, index := range tableInfo.Indices {
		if index == nil {
			continue
		}
		if index.Name.O == "idx_c1" {
			namedIndexID = index.ID
			continue
		}
		if len(index.Columns) == 1 && index.Columns[0].Name.L == "c1" {
			anonymousIndexID = index.ID
		}
	}
	require.NotZero(t, namedIndexID)
	require.NotZero(t, anonymousIndexID)
	require.Equal(t, []int64{namedIndexID, anonymousIndexID}, getIndexIDs(job))
}

func TestGetIndexIDsIgnoresDropIndexSubJobsForMultiSchemaChange(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, a int, key idx_old(id))")

	job := helper.DDL2Job("alter table t drop index idx_old, add index (a)")
	require.Equal(t, model.ActionMultiSchemaChange, job.Type)

	tableInfo := helper.GetModelTableInfo(job)
	require.NotNil(t, tableInfo)

	var anonymousIndexID int64
	for _, index := range tableInfo.Indices {
		if index == nil || index.Primary || len(index.Columns) != 1 {
			continue
		}
		if index.Columns[0].Name.L == "a" {
			anonymousIndexID = index.ID
			break
		}
	}
	require.NotZero(t, anonymousIndexID)
	require.Equal(t, []int64{anonymousIndexID}, getIndexIDs(job))
}

func TestGetIndexIDsIgnoresAddPrimaryKeySubJobsForMultiSchemaChange(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int, a int)")

	job := helper.DDL2Job("alter table t add primary key(id), add index (a)")
	require.Equal(t, model.ActionMultiSchemaChange, job.Type)

	tableInfo := helper.GetModelTableInfo(job)
	require.NotNil(t, tableInfo)

	var anonymousIndexID int64
	for _, index := range tableInfo.Indices {
		if index == nil || index.Primary || len(index.Columns) != 1 {
			continue
		}
		if index.Columns[0].Name.L == "a" {
			anonymousIndexID = index.ID
			break
		}
	}
	require.NotZero(t, anonymousIndexID)
	require.Equal(t, []int64{anonymousIndexID}, getIndexIDs(job))
}
