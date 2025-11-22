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
	"net/url"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/phayes/freeport"
	dmysql "github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	misc "github.com/pingcap/ticdc/pkg/redo/common"
	"github.com/pingcap/ticdc/pkg/redo/reader"
	pkgMysql "github.com/pingcap/ticdc/pkg/sink/mysql"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
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
func (br *MockReader) ReadNextRow(ctx context.Context) (row *commonEvent.RedoDMLEvent, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case row = <-br.redoLogCh:
	}
	return
}

// ReadNextDDL implements LogReader.ReadNextDDL
func (br *MockReader) ReadNextDDL(ctx context.Context) (ddl *commonEvent.RedoDDLEvent, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ddl = <-br.ddlEventCh:
	}
	return
}

// ReadMeta implements LogReader.ReadMeta
func (br *MockReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, version int, err error) {
	return br.checkpointTs, br.resolvedTs, misc.Version, nil
}

// GetChangefeedID implements LogReader.GetChangefeedID
func (br *MockReader) GetChangefeedID() commonType.ChangeFeedID {
	return commonType.ChangeFeedID{}
}

// GetVersion implements LogReader.GetVersion
func (br *MockReader) GetVersion() int {
	return misc.Version
}

func newFlag(flag uint) uint64 {
	var result commonType.ColumnFlagType
	if flag == pmysql.PriKeyFlag {
		result.SetIsHandleKey()
		result.SetIsPrimaryKey()
	}
	return uint64(result)
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
	db := getMockDB(t)

	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
	}()

	tableInfo := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		Name:  ast.NewCIStr("t1"),
		State: timodel.StatePublic,
	})
	dmls := []*commonEvent.RedoDMLEvent{
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1100,
				CommitTs: 1200,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(1),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: "2",
				},
			},
		},
		// update event which doesn't modify handle key
		// split into delete+insert when safe-mode is true
		{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs:  1120,
				CommitTs: 1220,
				Table:    &tableInfo.TableName,
				Columns: []*commonEvent.RedoColumn{
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(1),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: "3",
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(1),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(10),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(100),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(10),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(2),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: "3",
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(1),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(200),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: "300",
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(100),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
	applyCfg := &RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?worker-count=1&max-txn-row=1" +
			"&tidb_placement_mode=ignore&safe-mode=false&cache-prep-stmts=false" +
			"&multi-stmt-enable=false&enable-ddl-ts=false&batch-dml-enable=false&enable-ddl-ts=false",
		Dir: dir,
	}
	ap := NewRedoApplier(applyCfg)
	// use mock db init sink
	cfg := &config.ChangefeedConfig{
		ChangefeedID: common.NewChangefeedID4Test(common.DefaultKeyspace.Name, "test"),
		SinkURI:      applyCfg.SinkURI,
		SinkConfig:   config.GetDefaultReplicaConfig().Sink,
	}
	mysqlCfg := pkgMysql.New()
	sinkURI, err := url.Parse(cfg.SinkURI)
	require.NoError(t, err)
	mysqlCfg.Apply(sinkURI, cfg.ChangefeedID, cfg)
	ap.mysqlSink = dmysql.NewMySQLSink(ctx, cfg.ChangefeedID, mysqlCfg, db, false)
	ap.needRecoveryInfo = false
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
	db := getMockDBForBigTxn(t)

	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
	}()

	tableInfo := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		Name:  ast.NewCIStr("t1"),
		State: timodel.StatePublic,
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i * 10),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: fmt.Sprintf("%d", i*10+1),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i * 10),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				PreColumns: []*commonEvent.RedoColumn{
					{Name: "a", Type: pmysql.TypeLong},
					{Name: "b", Type: pmysql.TypeString},
				},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i * 100),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: fmt.Sprintf("%d", i*100+1),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i * 10),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
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
	applyCfg := &RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?worker-count=1&max-txn-row=1" +
			"&tidb_placement_mode=ignore&safe-mode=false&cache-prep-stmts=false" +
			"&multi-stmt-enable=false&enable-ddl-ts=false&batch-dml-enable=false&enable-ddl-ts=false",
		Dir: dir,
	}
	ap := NewRedoApplier(applyCfg)
	// use mock db init sink
	cfg := &config.ChangefeedConfig{
		ChangefeedID: common.NewChangefeedID4Test(common.DefaultKeyspace.Name, "test"),
		SinkURI:      applyCfg.SinkURI,
		SinkConfig:   config.GetDefaultReplicaConfig().Sink,
	}
	mysqlCfg := pkgMysql.New()
	sinkURI, err := url.Parse(cfg.SinkURI)
	require.NoError(t, err)
	mysqlCfg.Apply(sinkURI, cfg.ChangefeedID, cfg)
	ap.mysqlSink = dmysql.NewMySQLSink(ctx, cfg.ChangefeedID, mysqlCfg, db, false)
	ap.needRecoveryInfo = false
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

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table checkpoint(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(1, []byte("2")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test`.`t1` SET `a` = ?,`b` = ? WHERE `a` = ? LIMIT 1").
		WithArgs(1, []byte("3"), 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(10, []byte("20")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(100, []byte("200")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// First, apply row which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1").
		WithArgs(10).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1").
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1").
		WithArgs(100).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(2, []byte("3")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(200, []byte("300")).
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

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table checkpoint(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i, []byte(fmt.Sprintf("%d", i+1))).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	mock.ExpectBegin()
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1").
			WithArgs(i).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i*10, []byte(fmt.Sprintf("%d", i*10+1))).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	// First, apply row which commitTs equal to resolvedTs
	mock.ExpectBegin()
	for i := 1; i <= 100; i++ {
		mock.ExpectExec("DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1").
			WithArgs(i * 10).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	for i := 51; i <= 100; i++ {
		mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i*100, []byte(fmt.Sprintf("%d", i*100+1))).
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
