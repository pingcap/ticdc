package mysql

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

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

	expectedIndexName := ""
	for _, index := range tableInfo.GetIndices() {
		if index == nil || index.Primary || index.Unique {
			continue
		}
		if len(index.Columns) != 1 {
			continue
		}
		if index.Columns[0].Name.L == "name" {
			expectedIndexName = index.Name.O
			break
		}
	}
	require.NotEmpty(t, expectedIndexName)

	anonymousQuery := "ALTER TABLE `t` ADD INDEX (`name`)"

	restoredQuery, changed, err := restoreAnonymousIndexToNamedIndex(anonymousQuery, tableInfo)
	require.NoError(t, err)
	require.True(t, changed)

	p := parser.New()
	stmt, err := p.ParseOneStmt(restoredQuery, "", "")
	require.NoError(t, err)
	alterStmt, ok := stmt.(*ast.AlterTableStmt)
	require.True(t, ok)

	restoredName := ""
	for _, spec := range alterStmt.Specs {
		if spec != nil && spec.Tp == ast.AlterTableAddConstraint && spec.Constraint != nil {
			restoredName = spec.Constraint.Name
			break
		}
	}
	require.Equal(t, expectedIndexName, restoredName)

	ddlEvent := &commonEvent.DDLEvent{
		Type:       byte(job.Type),
		Query:      anonymousQuery,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		TableInfo:  tableInfo,
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
