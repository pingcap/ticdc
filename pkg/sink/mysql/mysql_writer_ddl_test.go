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

package mysql

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func newBatchCreateTablesEvent() (*commonEvent.DDLEvent, []string) {
	event := &commonEvent.DDLEvent{
		Type:       byte(model.ActionCreateTables),
		SchemaName: "test",
		StartTs:    1,
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
	}
	queries := make([]string, 0, 3)
	for i := 1; i <= 3; i++ {
		tableName := fmt.Sprintf("t%d", i)
		queries = append(queries, fmt.Sprintf("CREATE TABLE `%s` (`id` INT);", tableName))
		event.MultipleTableInfos = append(event.MultipleTableInfos, &common.TableInfo{
			TableName: common.TableName{Schema: "test", Table: tableName, TableID: int64(i)},
		})
		event.NeedAddedTables = append(event.NeedAddedTables, commonEvent.Table{SchemaID: 1, TableID: int64(i)})
	}
	event.Query = strings.Join(queries, "")
	return event, queries
}

func TestFlushDDLEventCreateTables(t *testing.T) {
	tableExists := &mysql.MySQLError{Number: 1050, Message: "Table already exists"}
	accessDenied := &mysql.MySQLError{Number: 1045, Message: "Access denied"}
	for _, tc := range []struct {
		name          string
		queryErrors   [][]error
		expectedError error
		replayBatch   bool
	}{
		{name: "successful batch", queryErrors: [][]error{{nil}, {nil}, {nil}}},
		{name: "first table exists", queryErrors: [][]error{{tableExists}, {nil}, {nil}}},
		{name: "middle table exists", queryErrors: [][]error{{nil}, {tableExists}, {nil}}},
		{name: "retry after partial execution", queryErrors: [][]error{{nil}, {mysql.ErrInvalidConn, tableExists}, {nil}}},
		{name: "later create fails", queryErrors: [][]error{{tableExists}, {accessDenied}}, expectedError: accessDenied},
		{name: "replay whole batch after partial success", queryErrors: [][]error{{nil}, {accessDenied}}, expectedError: accessDenied, replayBatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer, db, mock := newTestMysqlWriter(t)
			defer db.Close()
			writer.ddlTsTableInit = true
			event, queries := newBatchCreateTablesEvent()
			original := *event

			ddlTsQuery := "INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES " +
				"('default', 'test/test', '2', 0, %d, 0), ('default', 'test/test', '2', 1, %d, 0), " +
				"('default', 'test/test', '2', 2, %d, 0), ('default', 'test/test', '2', 3, %d, 0) " +
				"ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);"
			mock.ExpectBegin()
			mock.ExpectExec(fmt.Sprintf(ddlTsQuery, 0, 0, 0, 0)).WillReturnResult(sqlmock.NewResult(1, 4))
			mock.ExpectCommit()

			for i, queryErrors := range tc.queryErrors {
				mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").
					WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}))
				for _, queryError := range queryErrors {
					mock.ExpectBegin()
					mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(0, 0))
					queryExpectation := mock.ExpectExec(queries[i])
					if queryError != nil {
						queryExpectation.WillReturnError(queryError)
						mock.ExpectRollback()
					} else {
						queryExpectation.WillReturnResult(sqlmock.NewResult(0, 0))
						mock.ExpectCommit()
					}
				}
			}
			if tc.expectedError == nil {
				// The original event's progress is completed only after every CREATE.
				mock.ExpectBegin()
				mock.ExpectExec(fmt.Sprintf(ddlTsQuery, 1, 1, 1, 1)).WillReturnResult(sqlmock.NewResult(1, 4))
				mock.ExpectCommit()
			}

			err := writer.FlushDDLEvent(event)
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, original, *event)
			require.NoError(t, mock.ExpectationsWereMet())
			if !tc.replayBatch {
				return
			}

			// Replaying the original batch must accept the table created by the
			// first attempt and complete the remaining CREATEs before finishing.
			mock.ExpectBegin()
			mock.ExpectExec(fmt.Sprintf(ddlTsQuery, 0, 0, 0, 0)).WillReturnResult(sqlmock.NewResult(1, 4))
			mock.ExpectCommit()
			for i, query := range queries {
				mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").
					WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}))
				mock.ExpectBegin()
				mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(0, 0))
				if i == 0 {
					mock.ExpectExec(query).WillReturnError(tableExists)
					mock.ExpectRollback()
				} else {
					mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectCommit()
				}
			}
			mock.ExpectBegin()
			mock.ExpectExec(fmt.Sprintf(ddlTsQuery, 1, 1, 1, 1)).WillReturnResult(sqlmock.NewResult(1, 4))
			mock.ExpectCommit()

			require.NoError(t, writer.FlushDDLEvent(event))
			require.Equal(t, original, *event)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestFlushDDLEventCreateTablesWaitsForCurrentStatement(t *testing.T) {
	writer, db, mock := newTestMysqlWriterForTiDB(t)
	defer db.Close()
	writer.cfg.EnableDDLTs = false
	writer.ddlPollInterval = time.Millisecond
	event, queries := newBatchCreateTablesEvent()
	createTime := "2026-10-27 16:47:22.856000"

	for i, query := range queries {
		mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").
			WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow(createTime))
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(0, 0))
		if i == 1 {
			mock.ExpectExec(query).WillReturnError(mysql.ErrInvalidConn)
			mock.ExpectRollback()
			mock.ExpectQuery(checkRunningSQL).WithArgs(createTime, query).
				WillReturnRows(sqlmock.NewRows([]string{"QUERY", "STATE", "JOB_ID", "JOB_TYPE", "SCHEMA_STATE"}).
					AddRow(query, "synced", "1", "create table", "public"))
		} else {
			mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit()
		}
	}

	require.NoError(t, writer.FlushDDLEvent(event))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFlushDDLEventPreservesRenameTables(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()
	writer.cfg.EnableDDLTs = false
	event := &commonEvent.DDLEvent{
		Type:       byte(model.ActionRenameTables),
		SchemaName: "test",
		Query:      "RENAME TABLE `t1` TO `t2`, `t3` TO `t4`;",
	}
	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").
		WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(0, 0))
	expectDDLExec(mock, event, writer.cfg.Timezone)
	mock.ExpectCommit()

	require.NoError(t, writer.FlushDDLEvent(event))
	require.NoError(t, mock.ExpectationsWereMet())
}
