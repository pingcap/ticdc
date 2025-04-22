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
	"fmt"
	"net/url"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func newSinkForTest(
	ctx context.Context,
	replicaConfig *config.ReplicaConfig,
	sinkURI *url.URL,
	cleanUpJobs []func(),
) (*sink, error) {
	changefeedID := common.NewChangefeedID4Test("test", "test")
	result, err := New(ctx, changefeedID, sinkURI, replicaConfig.Sink, cleanUpJobs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func TestBasicFunctionality(t *testing.T) {
	uri := fmt.Sprintf("file:///%s?protocol=csv", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	go cloudStorageSink.Run(ctx)

	var count atomic.Int64

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	helper.ApplyJob(job)

	tableInfo := helper.GetTableInfo(job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
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
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
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

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() {
			count.Add(1)
		},
	}
	dmlEvent.TableInfoVersion = 1

	err = cloudStorageSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	cloudStorageSink.AddDMLEvent(dmlEvent)

	time.Sleep(5 * time.Second)

	ddlEvent2.PostFlush()

	require.Equal(t, count.Load(), int64(3))
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

	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	go cloudStorageSink.Run(ctx)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: pmodel.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				Name:      pmodel.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				Name:      pmodel.NewCIStr("col2"),
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

	tableSchema, err := os.ReadFile(path.Join(tableDir, "schema_100_4192708364.json"))
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
	}`, string(tableSchema))
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

	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	go cloudStorageSink.Run(ctx)
	time.Sleep(3 * time.Second)

	cloudStorageSink.AddCheckpointTs(100)

	time.Sleep(2 * time.Second)
	metadata, err := os.ReadFile(path.Join(parentDir, "metadata"))
	require.NoError(t, err)
	require.JSONEq(t, `{"checkpoint-ts":100}`, string(metadata))
}

func TestCleanupExpiredFiles(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.CloudStorageConfig = &config.CloudStorageConfig{
		FileExpirationDays:  util.AddressOf(1),
		FileCleanupCronSpec: util.AddressOf("* * * * * *"),
	}
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	var count atomic.Int64
	cleanupJobs := []func(){
		func() {
			count.Add(1)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, cleanupJobs)
	go cloudStorageSink.Run(ctx)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	require.LessOrEqual(t, int64(1), count.Load())
}
