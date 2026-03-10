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

package mysql

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func getIndexIDsFromJob(t *testing.T, job *timodel.Job) []int64 {
	idxArgs, err := timodel.GetModifyIndexArgs(job)
	if idxArgs != nil && err == nil {
		indexIDs := make([]int64, 0, len(idxArgs.IndexArgs))
		for _, indexArg := range idxArgs.IndexArgs {
			indexIDs = append(indexIDs, indexArg.IndexID)
		}
		return indexIDs
	}

	indexIDs := make([]int64, 0)
	require.NotNil(t, job.MultiSchemaInfo)
	for idx, subJob := range job.MultiSchemaInfo.SubJobs {
		proxyJob := subJob.ToProxyJob(job, idx)
		subIdxArgs, subErr := timodel.GetModifyIndexArgs(&proxyJob)
		if subIdxArgs == nil || subErr != nil {
			continue
		}
		for _, indexArg := range subIdxArgs.IndexArgs {
			indexIDs = append(indexIDs, indexArg.IndexID)
		}
	}
	return indexIDs
}

func getIndexNameByID(t *testing.T, tableInfo *common.TableInfo, indexID int64) string {
	for _, index := range tableInfo.GetIndices() {
		if index != nil && index.ID == indexID {
			return index.Name.O
		}
	}
	require.FailNow(t, "index id not found", "index id: %d", indexID)
	return ""
}

func parseAddIndexConstraintNames(t *testing.T, query string) []string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	require.NoError(t, err)

	alterStmt, ok := stmt.(*ast.AlterTableStmt)
	require.True(t, ok)

	names := make([]string, 0)
	for _, spec := range alterStmt.Specs {
		if spec == nil || spec.Tp != ast.AlterTableAddConstraint || spec.Constraint == nil {
			continue
		}
		if !isIndexConstraint(spec.Constraint) {
			continue
		}
		names = append(names, spec.Constraint.Name)
	}
	return names
}

func TestExecDDL_RestoreAnonymousIndexToNamedIndex(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, name varchar(32), index name(id))")

	job := helper.DDL2Job("alter table t add index (name)")
	require.Equal(t, timodel.ActionAddIndex, job.Type)

	tableInfo := helper.GetTableInfo(job)
	require.NotNil(t, tableInfo)

	indexIDs := getIndexIDsFromJob(t, job)
	require.Len(t, indexIDs, 1)
	expectedIndexName := getIndexNameByID(t, tableInfo, indexIDs[0])

	anonymousQuery := "ALTER TABLE `t` ADD INDEX (`name`)"

	restoredQuery, changed, err := restoreAnonymousIndexToNamedIndex(anonymousQuery, tableInfo, indexIDs)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, []string{expectedIndexName}, parseAddIndexConstraintNames(t, restoredQuery))

	ddlEvent := &commonEvent.DDLEvent{
		Type:       byte(job.Type),
		Query:      anonymousQuery,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		TableInfo:  tableInfo,
		IndexIDs:   indexIDs,
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(restoredQuery).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.execDDL(ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestoreAnonymousIndexToNamedIndexMultipleAnonymousIndexes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, name varchar(32), age int)")

	job := helper.DDL2Job("alter table t add index (name), add unique (age)")

	tableInfo := helper.GetTableInfo(job)
	require.NotNil(t, tableInfo)

	indexIDs := getIndexIDsFromJob(t, job)
	require.Len(t, indexIDs, 2)

	expectedNames := []string{
		getIndexNameByID(t, tableInfo, indexIDs[0]),
		getIndexNameByID(t, tableInfo, indexIDs[1]),
	}

	anonymousQuery := "ALTER TABLE `t` ADD INDEX (`name`), ADD UNIQUE (`age`)"
	restoredQuery, changed, err := restoreAnonymousIndexToNamedIndex(anonymousQuery, tableInfo, indexIDs)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, expectedNames, parseAddIndexConstraintNames(t, restoredQuery))
}

func TestRestoreAnonymousIndexToNamedIndexWithNamedAndAnonymousIndexes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, name varchar(32), age int)")

	job := helper.DDL2Job("alter table t add index idx_name(name), add index (age)")

	tableInfo := helper.GetTableInfo(job)
	require.NotNil(t, tableInfo)
	indexIDs := getIndexIDsFromJob(t, job)
	require.Len(t, indexIDs, 2)

	expectedAnonymousName := ""
	for _, index := range tableInfo.GetIndices() {
		if index == nil || len(index.Columns) != 1 {
			continue
		}
		if index.Columns[0].Name.L == "age" {
			expectedAnonymousName = index.Name.O
			break
		}
	}
	require.NotEmpty(t, expectedAnonymousName)

	mixedQuery := "ALTER TABLE `t` ADD INDEX `idx_name` (`name`), ADD INDEX (`age`)"
	restoredQuery, changed, err := restoreAnonymousIndexToNamedIndex(mixedQuery, tableInfo, indexIDs)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, []string{"idx_name", expectedAnonymousName}, parseAddIndexConstraintNames(t, restoredQuery))

	unchangedQuery, unchanged, err := restoreAnonymousIndexToNamedIndex(mixedQuery, tableInfo, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, mixedQuery, unchangedQuery)
}

func TestExecDDL_RestoreAnonymousIndexToNamedIndexForMultiSchemaChange(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, name varchar(32))")

	job := helper.DDL2Job("alter table t add column age int, add index (name)")
	require.Equal(t, timodel.ActionMultiSchemaChange, job.Type)

	tableInfo := helper.GetTableInfo(job)
	require.NotNil(t, tableInfo)

	indexIDs := getIndexIDsFromJob(t, job)
	require.Len(t, indexIDs, 1)
	expectedIndexName := getIndexNameByID(t, tableInfo, indexIDs[0])

	anonymousQuery := "ALTER TABLE `t` ADD COLUMN `age` INT, ADD INDEX (`name`)"
	restoredQuery, changed, err := restoreAnonymousIndexToNamedIndex(anonymousQuery, tableInfo, indexIDs)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, []string{expectedIndexName}, parseAddIndexConstraintNames(t, restoredQuery))

	ddlEvent := &commonEvent.DDLEvent{
		Type:       byte(job.Type),
		Query:      anonymousQuery,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		TableInfo:  tableInfo,
		IndexIDs:   indexIDs,
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(restoredQuery).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.execDDL(ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
