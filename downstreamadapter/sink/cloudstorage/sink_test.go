// Copyright 2025 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newSinkForTest(
	ctx context.Context,
	replicaConfig *config.ReplicaConfig,
	sinkURI *url.URL,
	cleanUpJobs []func(),
) (*sink, error) {
	changefeedID := common.NewChangefeedID4Test("test", "test")
	result, err := New(ctx, changefeedID, sinkURI, replicaConfig.Sink, true, cleanUpJobs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func TestBasicFunctionality(t *testing.T) {
	uri := fmt.Sprintf("file:///%s?protocol=csv&flush-interval=3600s&file-size=1024", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())
	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	runDone := runSinkInBackground(t, ctx, cloudStorageSink)
	defer cancelAndWaitSink(t, cancel, runDone)

	var count atomic.Int64

	createTableSQL := "create table t (id int primary key, name varchar(2048));"
	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   1,
		Name: ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
				State:     timodel.StatePublic,
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("name"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
				State:     timodel.StatePublic,
			},
		},
	})

	ddlEvent := &commonEvent.DDLEvent{
		Query:      createTableSQL,
		SchemaName: "test",
		TableName:  "t",
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo:       tableInfo,
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	ddlEvent2 := &commonEvent.DDLEvent{
		Query:      createTableSQL,
		SchemaName: "test",
		TableName:  "t",
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo:       tableInfo,
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	rows.AppendInt64(0, 1)
	rows.AppendString(1, strings.Repeat("x", 1024))
	rows.AppendInt64(0, 2)
	rows.AppendString(1, strings.Repeat("y", 1024))
	dmlEvent := commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, 2, 3, tableInfo)
	dmlEvent.TableInfoVersion = ddlEvent.FinishedTs
	dmlEvent.SetRows(rows)
	dmlEvent.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
	dmlEvent.Length = 2
	dmlEvent.ApproximateSize = 2
	dmlEvent.PostTxnFlushed = []func(){
		func() {
			count.Add(1)
		},
	}

	err = cloudStorageSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	cloudStorageSink.AddDMLEvent(dmlEvent)
	require.Eventually(t, func() bool {
		return count.Load() == 2
	}, testEventuallyTimeout, testEventuallyTick)

	ddlEvent2.PostFlush()

	require.Equal(t, count.Load(), int64(3))
}

func TestCloudStorageSinkWithColumnSelector(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv&flush-interval=3600s&file-size=1024", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{Matcher: []string{"test.table1"}, Columns: []string{"c1"}},
	}
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)
	replicaConfig.Sink.DateSeparator = util.AddressOf(config.DateSeparatorNone.String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())
	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	runDone := runSinkInBackground(t, ctx, cloudStorageSink)
	defer cancelAndWaitSink(t, cancel, runDone)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table table1(c1 int primary key, c2 varchar(255))")
	require.NotNil(t, job)
	helper.ApplyJob(job)

	dispatcherID := common.NewDispatcherID()
	event := helper.DML2Event(job.SchemaName, job.TableName, `insert into table1 values (1, "filtered")`)
	event.TableInfoVersion = job.BinlogInfo.FinishedTS
	event.DispatcherID = dispatcherID

	var flushed atomic.Uint64
	event.AddPostFlushFunc(func() {
		flushed.Add(1)
	})

	cloudStorageSink.AddDMLEvent(event)
	err = cloudStorageSink.FlushDMLBeforeBlock(&commonEvent.DDLEvent{
		DispatcherID: dispatcherID,
		FinishedTs:   event.CommitTs + 1,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), flushed.Load())

	tableDir := path.Join(parentDir, job.SchemaName, job.TableName, fmt.Sprint(event.TableInfoVersion))
	var content []byte
	require.Eventually(t, func() bool {
		files, err := os.ReadDir(tableDir)
		if err != nil {
			return false
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".csv") {
				continue
			}
			content, err = os.ReadFile(path.Join(tableDir, file.Name()))
			return err == nil
		}
		return false
	}, testEventuallyTimeout, testEventuallyTick)
	require.Contains(t, string(content), "1")
	require.NotContains(t, string(content), "filtered")
}

func TestIgnoreCallsAfterRunError(t *testing.T) {
	uri := fmt.Sprintf("file:///%s?protocol=csv", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)
	cloudStorageSink.cfg.FileCleanupCronSpec = "invalid cron spec"

	runDone := make(chan error, 1)
	go func() {
		runDone <- cloudStorageSink.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return !cloudStorageSink.IsNormal()
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case err = <-runDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("sink.Run did not return after fatal error")
	}

	tableInfo := &common.TableInfo{
		TableName: common.TableName{
			Schema:  "test",
			Table:   "t_ignore_after_error",
			TableID: 100,
		},
	}
	event := commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, 1, 1, tableInfo)
	event.TableInfoVersion = 1
	event.Length = 1
	event.ApproximateSize = 1

	require.Zero(t, cloudStorageSink.dmlWriters.msgCh.Len())
	cloudStorageSink.AddDMLEvent(event)
	require.Zero(t, cloudStorageSink.dmlWriters.msgCh.Len())

	ddlEvent := &commonEvent.DDLEvent{
		DispatcherID: common.NewDispatcherID(),
		FinishedTs:   1,
	}
	err = cloudStorageSink.FlushDMLBeforeBlock(ddlEvent)
	require.Error(t, err)
	err = cloudStorageSink.WriteBlockEvent(ddlEvent)
	require.Error(t, err)
}

func TestCloudStorageSinkBatchConfig(t *testing.T) {
	sink := &sink{
		cfg: &cloudstorage.Config{
			FileSize: 2048,
		},
	}
	require.Equal(t, 4096, sink.BatchCount())
	require.Equal(t, 2048, sink.BatchBytes())
}

func TestWriteDDLEvent(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)
	defer cloudStorageSink.Close()

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				Name:      ast.NewCIStr("col2"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})
	ddlEvent := &commonEvent.DDLEvent{
		Query:      "alter table test.table1 add col2 varchar(64)",
		Type:       byte(timodel.ActionAddColumn),
		SchemaName: "test",
		TableName:  "table1",
		FinishedTs: 100,
		TableInfo:  tableInfo,
	}

	tableDir := path.Join(parentDir, "test/table1/meta/")
	err = cloudStorageSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	schemaContent, err := os.ReadFile(path.Join(tableDir, "schema_100_4192708364.json"))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"Table": "table1",
		"Schema": "test",
		"Version": 1,
		"TableVersion": 100,
		"Query": "alter table test.table1 add col2 varchar(64)",
		"Type": 5,
		"TableColumns": [
			{
				"ColumnName": "col1",
				"ColumnType": "INT",
				"ColumnPrecision": "11"
			},
			{
				"ColumnName": "col2",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "5"
			}
		],
		"TableColumnsTotal": 2
	}`, string(schemaContent))
	t.Run("flush dml before write ddl", verifyWriteDDLEventFlushDMLBeforeBlock)
}

func verifyWriteDDLEventFlushDMLBeforeBlock(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv&flush-interval=3600s", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	runDone := runSinkInBackground(t, ctx, cloudStorageSink)
	defer cancelAndWaitSink(t, cancel, runDone)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_flush_before_ddl (id int primary key, v int)")
	require.NotNil(t, job)
	helper.ApplyJob(job)

	dispatcherID := common.NewDispatcherID()
	dmlEvent := helper.DML2Event(job.SchemaName, job.TableName, "insert into t_flush_before_ddl values (1, 1)")
	dmlEvent.TableInfoVersion = job.BinlogInfo.FinishedTS
	dmlEvent.DispatcherID = dispatcherID

	var dmlFlushed atomic.Int64
	dmlEvent.AddPostFlushFunc(func() {
		dmlFlushed.Add(1)
	})

	cloudStorageSink.AddDMLEvent(dmlEvent)

	ddlEvent := &commonEvent.DDLEvent{
		Query:        "alter table t_flush_before_ddl add column c2 int",
		Type:         byte(timodel.ActionAddColumn),
		SchemaName:   job.SchemaName,
		TableName:    job.TableName,
		FinishedTs:   dmlEvent.CommitTs + 10,
		TableInfo:    helper.GetTableInfo(job),
		DispatcherID: dispatcherID,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{dmlEvent.PhysicalTableID},
		},
	}

	err = cloudStorageSink.FlushDMLBeforeBlock(ddlEvent)
	require.NoError(t, err)
	require.Equal(t, int64(1), dmlFlushed.Load())

	err = cloudStorageSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)
}

func TestWriteDDLEventWithTableIDAsPath(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv&use-table-id-as-path=true", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)
	defer cloudStorageSink.Close()

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				Name:      ast.NewCIStr("col2"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})
	ddlEvent := &commonEvent.DDLEvent{
		Query:      "alter table test.table1 add col2 varchar(64)",
		Type:       byte(timodel.ActionAddColumn),
		SchemaName: "test",
		TableName:  "table1",
		FinishedTs: 100,
		TableInfo:  tableInfo,
	}

	err = cloudStorageSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	tableDir := path.Join(parentDir, "20/meta/")
	schemaContent, err := os.ReadFile(path.Join(tableDir, "schema_100_4192708364.json"))
	require.NoError(t, err)
	require.Contains(t, string(schemaContent), `"Table": "table1"`)
}

func TestSkipDatabaseSchemaWithTableIDAsPath(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv&use-table-id-as-path=true", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	runDone := runSinkInBackground(t, ctx, cloudStorageSink)
	defer cancelAndWaitSink(t, cancel, runDone)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      "create database test_db",
		Type:       byte(timodel.ActionCreateSchema),
		SchemaName: "test_db",
		TableName:  "",
		FinishedTs: 100,
		TableInfo:  nil,
	}

	err = cloudStorageSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	_, err = os.Stat(path.Join(parentDir, "test_db"))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func TestWriteDDLEventWithInvalidExchangePartitionEvent(t *testing.T) {
	testCases := []struct {
		name               string
		multipleTableInfos []*common.TableInfo
	}{
		{
			name:               "nil source table info",
			multipleTableInfos: []*common.TableInfo{nil},
		},
		{
			name:               "short table infos",
			multipleTableInfos: nil,
		},
	}

	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv&use-table-id-as-path=true", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			setPDClockForTest(t, pdutil.NewClock4Test())

			cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
			require.NoError(t, err)

			ddlEvent := &commonEvent.DDLEvent{
				Query:           "alter table test.table1 exchange partition p0 with table test.table2",
				Type:            byte(timodel.ActionExchangeTablePartition),
				SchemaName:      "test",
				TableName:       "table1",
				ExtraSchemaName: "test",
				ExtraTableName:  "table2",
				FinishedTs:      100,
				TableInfo:       tableInfo,
			}
			ddlEvent.MultipleTableInfos = append([]*common.TableInfo{tableInfo}, tc.multipleTableInfos...)

			err = cloudStorageSink.WriteBlockEvent(ddlEvent)
			require.ErrorContains(t, err, "invalid exchange partition ddl event, source table info is missing")
		})
	}
}

func readSchemaFileForTest(t *testing.T, parentDir, schema, table string) cloudstorage.SchemaFile {
	t.Helper()

	files, err := os.ReadDir(filepath.Join(parentDir, schema, table, "meta"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	content, err := os.ReadFile(filepath.Join(parentDir, schema, table, "meta", files[0].Name()))
	require.NoError(t, err)

	var schemaFile cloudstorage.SchemaFile
	require.NoError(t, json.Unmarshal(content, &schemaFile))
	return schemaFile
}

func TestWriteExchangePartitionDDLEventUsesTargetNames(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	idColumn := &timodel.ColumnInfo{
		ID:        1,
		Name:      ast.NewCIStr("id"),
		FieldType: *types.NewFieldType(mysql.TypeLong),
		State:     timodel.StatePublic,
	}
	partitionedTableInfo := common.WrapTableInfo("source_db", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("partitioned"),
		Columns: []*timodel.ColumnInfo{
			idColumn,
			{
				ID:        2,
				Name:      ast.NewCIStr("partition_value"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
				State:     timodel.StatePublic,
			},
		},
	}).CloneWithRouting("target_db", "partitioned_routed")
	exchangeTableInfo := common.WrapTableInfo("source_db", &timodel.TableInfo{
		ID:   21,
		Name: ast.NewCIStr("exchange_table"),
		Columns: []*timodel.ColumnInfo{
			idColumn,
			{
				ID:        2,
				Name:      ast.NewCIStr("exchange_value"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
				State:     timodel.StatePublic,
			},
		},
	}).CloneWithRouting("target_db", "exchange_table_routed")

	baseEvent := &commonEvent.DDLEvent{
		Query:           "alter table source_db.partitioned exchange partition p0 with table source_db.exchange_table",
		Type:            byte(timodel.ActionExchangeTablePartition),
		SchemaName:      "source_db",
		TableName:       "partitioned",
		ExtraSchemaName: "source_db",
		ExtraTableName:  "exchange_table",
		FinishedTs:      100,
		TableInfo:       partitionedTableInfo,
	}
	routedEvent := commonEvent.NewRoutedDDLEvent(
		baseEvent,
		"alter table target_db.partitioned_routed exchange partition p0 with table target_db.exchange_table_routed",
		"target_db",
		"partitioned_routed",
		"target_db",
		"exchange_table_routed",
		partitionedTableInfo,
		[]*common.TableInfo{partitionedTableInfo, exchangeTableInfo},
		nil,
	)

	err = cloudStorageSink.WriteBlockEvent(routedEvent)
	require.NoError(t, err)

	exchangeSchemaFile := readSchemaFileForTest(t, parentDir, "target_db", "exchange_table_routed")
	require.Equal(t, "target_db", exchangeSchemaFile.Schema)
	require.Equal(t, "exchange_table_routed", exchangeSchemaFile.Table)
	require.Equal(t, routedEvent.Query, exchangeSchemaFile.Query)
	require.Equal(t, byte(timodel.ActionExchangeTablePartition), exchangeSchemaFile.Type)
	require.Equal(t, "partition_value", exchangeSchemaFile.Columns[1].Name)

	partitionedSchemaFile := readSchemaFileForTest(t, parentDir, "target_db", "partitioned_routed")
	require.Equal(t, "target_db", partitionedSchemaFile.Schema)
	require.Equal(t, "partitioned_routed", partitionedSchemaFile.Table)
	require.Empty(t, partitionedSchemaFile.Query)
	require.Zero(t, partitionedSchemaFile.Type)
	require.Equal(t, "exchange_value", partitionedSchemaFile.Columns[1].Name)

	_, err = os.Stat(filepath.Join(parentDir, "source_db"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWriteCheckpointEvent(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	runDone := runSinkInBackground(t, ctx, cloudStorageSink)
	defer cancelAndWaitSink(t, cancel, runDone)

	cloudStorageSink.lastSendCheckpointTsTime = time.Now().Add(-2 * time.Second)
	cloudStorageSink.AddCheckpointTs(100)

	metadata := readFileEventually(t, path.Join(parentDir, "metadata"))
	require.JSONEq(t, `{"checkpoint-ts":100}`, string(metadata))
}

func TestCloseBeforeRunDoesNotPanicAndCleansSpool(t *testing.T) {
	spoolBaseDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv&spool-base-dir=%s", t.TempDir(), url.QueryEscape(spoolBaseDir))
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setPDClockForTest(t, pdutil.NewClock4Test())

	changefeedID := common.NewChangefeedID4Test("test", "close-before-run")
	cloudStorageSink, err := New(ctx, changefeedID, sinkURI, replicaConfig.Sink, true, nil)
	require.NoError(t, err)

	spoolDir := filepath.Join(spoolBaseDir, changefeedID.Keyspace(), changefeedID.Name())
	_, err = os.Stat(spoolDir)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		cloudStorageSink.Close()
	})

	_, err = os.Stat(spoolDir)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func TestCleanupExpiredFiles(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.CloudStorageConfig = &config.CloudStorageConfig{
		FileExpirationDays:  util.AddressOf(1),
		FileCleanupCronSpec: util.AddressOf("@every 1ms"),
	}
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	var count atomic.Int64
	cleanupDone := make(chan struct{}, 1)
	cleanupJobs := []func(){
		func() {
			count.Add(1)
			select {
			case cleanupDone <- struct{}{}:
			default:
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cloudStorageSink := &sink{
		changefeedID: common.NewChangefeedID4Test("test", "test"),
		cfg: &cloudstorage.Config{
			DateSeparator:       config.DateSeparatorDay.String(),
			FileExpirationDays:  1,
			FileCleanupCronSpec: util.GetOrZero(replicaConfig.Sink.CloudStorageConfig.FileCleanupCronSpec),
		},
	}
	require.NoError(t, cloudStorageSink.initCron(ctx, sinkURI, cleanupJobs))

	require.Len(t, cloudStorageSink.cron.Entries(), len(cleanupJobs))
	cleanupJobs[0]()
	require.Equal(t, int64(1), count.Load())

	cleanupDoneCh := make(chan struct{}, 1)
	go func() {
		cloudStorageSink.bgCleanup(ctx)
		cleanupDoneCh <- struct{}{}
	}()

	cancel()
	select {
	case <-cleanupDoneCh:
	case <-time.After(testEventuallyTimeout):
		t.Fatal("background cleanup did not exit after context cancel")
	}

	select {
	case <-cleanupDone:
	default:
		t.Fatal("cleanup job did not run")
	}
}

func TestRemoveEmptyDirsCleanupJobCanRunMultipleTimes(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx := context.Background()
	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	cleanupJobs := cloudStorageSink.genCleanupJob(ctx, sinkURI)
	require.NotEmpty(t, cleanupJobs)

	firstEmptyDir := filepath.Join(parentDir, "first")
	require.NoError(t, os.MkdirAll(firstEmptyDir, 0o755))
	cleanupJobs[0]()
	_, err = os.Stat(firstEmptyDir)
	require.ErrorIs(t, err, os.ErrNotExist)

	secondEmptyDir := filepath.Join(parentDir, "second")
	require.NoError(t, os.MkdirAll(secondEmptyDir, 0o755))
	cleanupJobs[0]()
	_, err = os.Stat(secondEmptyDir)
	require.ErrorIs(t, err, os.ErrNotExist)
}
