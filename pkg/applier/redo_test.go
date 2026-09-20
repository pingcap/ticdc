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
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/phayes/freeport"
	dmysql "github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
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

func TestApplyReplicationKeyLossRecovery(t *testing.T) {
	for _, completed := range []bool{true, false} {
		t.Run(fmt.Sprintf("completed=%t", completed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			changefeedID := common.NewChangefeedID4Test("default", "test")
			mysqlCfg := pkgMysql.New()
			// Recovery queries remain enabled in the applier. Metadata writes are
			// exercised explicitly below; only reapplying the unfinished ALTER
			// omits their repetition, to keep that control case focused on replay.
			mysqlCfg.EnableDDLTs = completed
			mysqlCfg.IsTiDB = false
			stat := metrics.NewStatistics(changefeedID, common.DefaultKeyspaceID, "mysqlSink")
			metadataWriter := pkgMysql.NewWriter(ctx, 0, db, mysqlCfg, changefeedID, stat, nil)
			defer metadataWriter.Close()
			ddl := &commonEvent.DDLEvent{
				Type: byte(timodel.ActionDropColumn), SchemaName: "test", TableName: "t",
				Query: "ALTER TABLE test.t DROP COLUMN id", FinishedTs: 120,
				BlockedTables: &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      []int64{42, common.DDLSpanTableID},
				},
				NeedDroppedTables: &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal, TableIDs: []int64{42},
				},
			}
			// Persist the actual key-loss payload: before execution both records
			// are unfinished, and after execution both must survive as finished.
			insertSQL := "INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES " +
				"('default', 'default/test', '120', 42, %d, 0), ('default', 'default/test', '120', 0, %d, 0) " +
				"ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);"
			mock.ExpectBegin()
			mock.ExpectExec(fmt.Sprintf(insertSQL, 0, 0)).WillReturnResult(sqlmock.NewResult(0, 2))
			mock.ExpectCommit()
			require.NoError(t, metadataWriter.SendDDLTsPre(ddl))
			if completed {
				mock.ExpectBegin()
				mock.ExpectExec(fmt.Sprintf(insertSQL, 1, 1)).WillReturnResult(sqlmock.NewResult(0, 2))
				// No DELETE is allowed: it would discard the table's recovery bound.
				mock.ExpectCommit()
				require.NoError(t, metadataWriter.SendDDLTs(ddl))
			}
			for _, tableID := range []int64{42, common.DDLSpanTableID} {
				query := fmt.Sprintf("SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 "+
					"WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'default/test', %d), ('default', 'default/test', -1))", tableID)
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"table_id", "ddl_ts", "finished", "is_syncpoint"}).
					AddRow(tableID, 120, completed, false))
			}
			if !completed {
				mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").
					WillReturnRows(sqlmock.NewRows([]string{"ts"}).AddRow("2026-01-01 00:00:00"))
				mock.ExpectBegin()
				mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(ddl.Query).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			}

			var keyFlag common.ColumnFlagType
			keyFlag.SetIsHandleKey()
			keyFlag.SetIsUniqueKey()
			rows := make(chan *commonEvent.RedoDMLEvent, 1)
			rows <- &commonEvent.RedoDMLEvent{
				Row: &commonEvent.DMLEventInRedoLog{
					StartTs: 105, CommitTs: 110,
					Table: &common.TableName{Schema: "test", Table: "t", TableID: 42},
					Columns: []*commonEvent.RedoColumn{
						{Name: "id", Type: pmysql.TypeLonglong}, {Name: "v", Type: pmysql.TypeLong},
					},
					IndexColumns: [][]int{{0}},
				},
				Columns: []commonEvent.RedoColumnValue{{Value: int64(1), Flag: uint64(keyFlag)}, {Value: int64(10)}},
			}
			close(rows)
			ddls := make(chan *commonEvent.RedoDDLEvent, 1)
			ddls <- &commonEvent.RedoDDLEvent{
				Type: ddl.Type, TableName: common.TableName{Schema: "test", Table: "t", TableID: 42},
				DDL: &commonEvent.DDLEventInRedoLog{
					CommitTs: ddl.FinishedTs, Query: ddl.Query,
					Columns:       []*commonEvent.ColumnInfo{{Name: "v", Type: pmysql.TypeLong}},
					BlockedTables: ddl.BlockedTables, NeedDroppedTables: ddl.NeedDroppedTables,
				},
			}
			close(ddls)
			ap := NewRedoApplier(&RedoApplierConfig{Dir: t.TempDir()})
			ap.rd = NewMockReader(100, 130, rows, ddls)
			ap.updateSplitter = newUpdateEventSplitter(ap.rd, ap.cfg.Dir)
			ap.mysqlSink = dmysql.NewMySQLSink(ctx, changefeedID, mysqlCfg, db, false, false, time.Second, common.DefaultKeyspaceID)
			defer ap.mysqlSink.Close()
			require.True(t, ap.needRecoveryInfo)
			require.ErrorIs(t, ap.consumeLogs(ctx), errApplyFinished)
			require.Zero(t, ap.appliedLogCount, "old-schema INSERT must not be replayed")
			if completed {
				require.Zero(t, ap.appliedDDLCount, "completed key-loss ALTER must not be replayed")
			} else {
				require.Equal(t, uint64(1), ap.appliedDDLCount, "unfinished ALTER must still be replayed")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
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
					Value: []byte("20"),
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
					Value: []byte("3"),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(1),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: []byte("20"),
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
					Value: []byte("20"),
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
					Value: []byte("200"),
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
					Value: []byte("20"),
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
					Value: []byte("3"),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(1),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: []byte("3"),
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
					Value: []byte("300"),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(100),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: []byte("200"),
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
	ap.mysqlSink = dmysql.NewMySQLSink(ctx, cfg.ChangefeedID, mysqlCfg, db, false, false, 1*time.Second, common.DefaultKeyspaceID)
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
					Value: []byte(fmt.Sprintf("%d", i+1)),
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
					Value: []byte(fmt.Sprintf("%d", i*10+1)),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: []byte(fmt.Sprintf("%d", i+1)),
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
					Value: []byte(fmt.Sprintf("%d", i*10+1)),
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
					Value: []byte(fmt.Sprintf("%d", i*100+1)),
				},
			},
			PreColumns: []commonEvent.RedoColumnValue{
				{
					Value: int64(i * 10),
					Flag:  newFlag(pmysql.PriKeyFlag),
				},
				{
					Value: []byte(fmt.Sprintf("%d", i*10+1)),
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
	ap.mysqlSink = dmysql.NewMySQLSink(ctx, cfg.ChangefeedID, mysqlCfg, db, false, false, 1*time.Second, common.DefaultKeyspaceID)
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
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table checkpoint(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(1, []byte([]byte("20"))).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test`.`t1` SET `a` = ?,`b` = ? WHERE `a` = ? LIMIT 1").
		WithArgs(1, []byte("3"), 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(10, []byte([]byte("20"))).
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
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(1, 1))
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
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(1, 1))
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
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table resolved(id int not null unique key)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectClose()
	return db
}
