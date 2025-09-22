// Copyright 2021 PingCAP, Inc.
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

package applier

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/phayes/freeport"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo/reader"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

var _ reader.RedoLogReader = &MockReader{}

// MockReader is a mock redo log reader that implements LogReader interface
type MockReader struct {
	checkpointTs uint64
	resolvedTs   uint64
	redoLogCh    chan *commonEvent.RedoDMLEvent
	ddlEventCh   chan *commonEvent.RedoDDLEvent
}

// NewMockReader creates a new MockReader
func NewMockReader(
	checkpointTs uint64,
	resolvedTs uint64,
	redoLogCh chan *commonEvent.RedoDMLEvent,
	ddlEventCh chan *commonEvent.RedoDDLEvent,
) *MockReader {
	return &MockReader{
		checkpointTs: checkpointTs,
		resolvedTs:   resolvedTs,
		redoLogCh:    redoLogCh,
		ddlEventCh:   ddlEventCh,
	}
}

// ResetReader implements LogReader.ReadLog
func (br *MockReader) Run(ctx context.Context) error {
	return nil
}

// ReadNextRow implements LogReader.ReadNextRow
func (br *MockReader) ReadNextRow(ctx context.Context) (row *commonEvent.RedoDMLEvent, ok bool, err error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case row, ok = <-br.redoLogCh:
	}
	return
}

// ReadNextDDL implements LogReader.ReadNextDDL
func (br *MockReader) ReadNextDDL(ctx context.Context) (ddl *commonEvent.RedoDDLEvent, ok bool, err error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case ddl, ok = <-br.ddlEventCh:
	}
	return
}

// ReadMeta implements LogReader.ReadMeta
func (br *MockReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error) {
	return br.checkpointTs, br.resolvedTs, nil
}

func newFieldType(tp byte, flag uint) *types.FieldType {
	ft := types.NewFieldType(tp)
	ft.SetFlag(flag)
	return ft
}

func TestApply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkpointTs := uint64(1000)
	resolvedTs := uint64(2000)
	redoLogCh := make(chan *commonEvent.RedoDMLEvent, 1024)
	ddlEventCh := make(chan *commonEvent.RedoDDLEvent, 1024)
	createMockReader := func(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
		return NewMockReader(checkpointTs, resolvedTs, redoLogCh, ddlEventCh), nil
	}

	// DML sink and DDL sink share the same db
	// db := getMockDB(t)
	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
	}()

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		Name:  ast.NewCIStr("t"),
		State: timodel.StatePublic,
		Columns: []*timodel.ColumnInfo{
			{
				Name:      ast.NewCIStr("a"),
				FieldType: *newFieldType(pmysql.TypeLong, pmysql.PriKeyFlag),
			}, {
				Name:      ast.NewCIStr("b"),
				FieldType: *newFieldType(pmysql.TypeString, 0),
			},
		},
	})
	dmls := []*commonEvent.RedoDMLEvent{
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1100,
				CommitTs: 1200,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: 1,
				}, {
					Value: "2",
				},
			},
		},
		// update event which doesn't modify handle key
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1120,
				CommitTs: 1220,
				Table:    &common.TableName{},
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: 1,
				}, {
					Value: "3",
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: 1,
				}, {
					Value: "2",
				},
			},
		},
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1150,
				CommitTs: 1250,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: 10,
				}, {
					Value: "20",
				},
			},
		},
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1150,
				CommitTs: 1250,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: 100,
				}, {
					Value: "200",
				},
			},
		},
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1200,
				CommitTs: resolvedTs,
				Table:    &tableInfo.TableName,
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: 10,
				}, {
					Value: "20",
				},
			},
		},
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1200,
				CommitTs: resolvedTs,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: 2,
				}, {
					Value: "3",
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: 1,
				}, {
					Value: "3",
				},
			},
		},
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1200,
				CommitTs: resolvedTs,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: 200,
				}, {
					Value: "300",
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: 100,
				}, {
					Value: "200",
				},
			},
		},
	}
	for _, dml := range dmls {
		redoLogCh <- dml
	}
	ddls := []*commonEvent.RedoDDLEvent{
		{
			DDL: &commonEvent.DDLEventInRedoLog{
				CommitTs: checkpointTs,
				Query:    "create table checkpoint(id int)",
			},
			TableName: common.TableName{
				Schema: "test", Table: "checkpoint",
			},
			Type: byte(timodel.ActionCreateTable),
		},
		{
			DDL: &commonEvent.DDLEventInRedoLog{
				CommitTs: resolvedTs,
				Query:    "create table resolved(id int not null unique key)",
			},
			TableName: common.TableName{
				Schema: "test", Table: "resolved",
			},
			Type: byte(timodel.ActionCreateTable),
		},
	}
	for _, ddl := range ddls {
		ddlEventCh <- ddl
	}
	close(redoLogCh)
	close(ddlEventCh)

	dir, err := os.Getwd()
	require.Nil(t, err)
	cfg := &RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?worker-count=1&max-txn-row=1" +
			"&tidb_placement_mode=ignore&safe-mode=true&cache-prep-stmts=false" +
			"&multi-stmt-enable=false",
		Dir: dir,
	}
	ap := NewRedoApplier(cfg)
	err = ap.Apply(ctx)
	require.Nil(t, err)
}

func TestApplyBigTxn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkpointTs := uint64(1000)
	resolvedTs := uint64(2000)
	redoLogCh := make(chan *commonEvent.RedoDMLEvent, 1024)
	ddlEventCh := make(chan *commonEvent.RedoDDLEvent, 1024)
	createMockReader := func(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
		return NewMockReader(checkpointTs, resolvedTs, redoLogCh, ddlEventCh), nil
	}

	// DML sink and DDL sink share the same db

	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
	}()

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		Name:  ast.NewCIStr("t1"),
		State: timodel.StatePublic,
		Columns: []*timodel.ColumnInfo{
			{
				Name:      ast.NewCIStr("a"),
				FieldType: *newFieldType(pmysql.TypeLong, pmysql.PriKeyFlag),
			}, {
				Name:      ast.NewCIStr("b"),
				FieldType: *newFieldType(pmysql.TypeString, 0),
			},
		},
	})

	dmls := make([]*commonEvent.RedoDMLEvent, 0)
	// insert some rows
	for i := 1; i <= 100; i++ {
		dml := &commonEvent.RedoDMLEvent{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1100,
				CommitTs: 1200,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: i,
				}, {
					Value: fmt.Sprintf("%d", i+1),
				},
			},
		}
		dmls = append(dmls, dml)
	}
	// update
	for i := 1; i <= 100; i++ {
		dml := &commonEvent.RedoDMLEvent{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1200,
				CommitTs: 1300,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: i * 10,
				}, {
					Value: fmt.Sprintf("%d", i*10+1),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: i,
				}, {
					Value: fmt.Sprintf("%d", i+1),
				},
			},
		}
		dmls = append(dmls, dml)
	}
	// delete and update
	for i := 1; i <= 50; i++ {
		dml := &commonEvent.RedoDMLEvent{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1300,
				CommitTs: resolvedTs,
				Table:    &tableInfo.TableName,
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: i * 10,
				}, {
					Value: fmt.Sprintf("%d", i*10+1),
				},
			},
		}
		dmls = append(dmls, dml)
	}
	for i := 51; i <= 100; i++ {
		dml := &commonEvent.RedoDMLEvent{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1300,
				CommitTs: resolvedTs,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a"},
					{Name: "b"},
				},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: i * 100,
				}, {
					Value: fmt.Sprintf("%d", i*100+1),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: i * 10,
				}, {
					Value: fmt.Sprintf("%d", i*10+1),
				},
			},
		}
		dmls = append(dmls, dml)
	}
	for _, dml := range dmls {
		redoLogCh <- dml
	}
	ddls := []*commonEvent.RedoDDLEvent{
		{
			DDL: &commonEvent.DDLEventInRedoLog{
				CommitTs: checkpointTs,
				Query:    "create table checkpoint(id int)",
			},
			TableName: common.TableName{
				Schema: "test", Table: "checkpoint",
			},
			Type: byte(timodel.ActionCreateTable),
		},
		{
			DDL: &commonEvent.DDLEventInRedoLog{
				CommitTs: resolvedTs,
				Query:    "create table resolved(id int not null unique key)",
			},
			TableName: common.TableName{
				Schema: "test", Table: "resolved",
			},
			Type: byte(timodel.ActionCreateTable),
		},
	}
	for _, ddl := range ddls {
		ddlEventCh <- ddl
	}
	close(redoLogCh)
	close(ddlEventCh)

	dir, err := os.Getwd()
	require.Nil(t, err)
	cfg := &RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?worker-count=1&max-txn-row=1" +
			"&tidb_placement_mode=ignore&safe-mode=true&cache-prep-stmts=false" +
			"&multi-stmt-enable=false",
		Dir: dir,
	}
	ap := NewRedoApplier(cfg)
	err = ap.Apply(ctx)
	require.Nil(t, err)
}

func TestApplyMeetSinkError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	cfg := &RedoApplierConfig{
		Storage: "blackhole://",
		SinkURI: fmt.Sprintf("mysql://127.0.0.1:%d/?read-timeout=1s&timeout=1s", port),
	}
	ap := NewRedoApplier(cfg)
	err = ap.Apply(ctx)
	require.Regexp(t, "CDC:ErrMySQLConnectionError", err)
}

func getMockDB(t *testing.T) *sql.DB {
	// normal db
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)

	// Before we write data to downstream, we need to check whether the downstream is TiDB.
	// So we mock a select tidb_version() query.
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table checkpoint(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(1, "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? LIMIT 1").
		WithArgs(1, "3", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(10, "20").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(100, "200").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// First, apply row which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a` = ?)").
		WithArgs(10).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a` = ?)").
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a` = ?)").
		WithArgs(100).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(2, "3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(200, "300").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Then, apply ddl which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table resolved(id int not null unique key)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectClose()
	return db
}

func getMockDBForBigTxn(t *testing.T) *sql.DB {
	// normal db
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)

	// Before we write data to downstream, we need to check whether the downstream is TiDB.
	// So we mock a select tidb_version() query.
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table checkpoint(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i, fmt.Sprintf("%d", i+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	mock.ExpectBegin()
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a` = ?)").
			WithArgs(i).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i*10, fmt.Sprintf("%d", i*10+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	// First, apply row which commitTs equal to resolvedTs
	mock.ExpectBegin()
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a` = ?)").
			WithArgs(i * 10).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	for i := 51; i <= 100; i++ {
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i*100, fmt.Sprintf("%d", i*100+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	// Then, apply ddl which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table resolved(id int not null unique key)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectClose()
	return db
}
