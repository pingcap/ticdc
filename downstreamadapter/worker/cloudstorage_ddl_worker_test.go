// Copyright 2022 PingCAP, Inc.
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

package worker

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
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func newCloudStorageDDLWorkerForTest(parentDir string) (*CloudStorageDDLWorker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	if err != nil {
		return nil, err
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")

	csvProtocol := "csv"
	sinkConfig := &config.SinkConfig{Protocol: &csvProtocol}
	cfg := cloudstorage.NewConfig()
	err = cfg.Apply(ctx, sinkURI, sinkConfig)
	if err != nil {
		return nil, err
	}
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}
	sink := NewCloudStorageDDLWorker(changefeedID, sinkURI, cfg, nil, storage, metrics.NewStatistics(changefeedID, "CloudStorageSink"))
	go sink.Run(ctx)
	return sink, nil
}

func TestCloudStorageWriteDDLEvent(t *testing.T) {
	parentDir := t.TempDir()
	sink, err := newCloudStorageDDLWorkerForTest(parentDir)
	require.NoError(t, err)

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
	err = sink.WriteBlockEvent(ddlEvent)
	require.Nil(t, err)

	tableSchema, err := os.ReadFile(path.Join(tableDir, "schema_100_4192708364.json"))
	require.Nil(t, err)
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

func TestCloudStorageWriteCheckpointTs(t *testing.T) {
	parentDir := t.TempDir()
	sink, err := newCloudStorageDDLWorkerForTest(parentDir)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	sink.AddCheckpointTs(100)
	metadata, err := os.ReadFile(path.Join(parentDir, "metadata"))
	require.Nil(t, err)
	require.JSONEq(t, `{"checkpoint-ts":100}`, string(metadata))
}

func TestCleanupExpiredFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=csv", parentDir)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.CloudStorageConfig = &config.CloudStorageConfig{
		FileExpirationDays:  util.AddressOf(1),
		FileCleanupCronSpec: util.AddressOf("* * * * * *"),
	}
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.Nil(t, err)

	cnt := atomic.Int64{}
	cleanupJobs := []func(){
		func() {
			cnt.Add(1)
		},
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")
	cfg := cloudstorage.NewConfig()
	err = cfg.Apply(ctx, sinkURI, replicaConfig.Sink)
	require.Nil(t, err)
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, sinkURI.String())
	require.Nil(t, err)

	sink := NewCloudStorageDDLWorker(changefeedID, sinkURI, cfg, cleanupJobs, storage, metrics.NewStatistics(changefeedID, "CloudStorageSink"))
	go sink.Run(ctx)
	require.Nil(t, err)

	_ = sink
	time.Sleep(5 * time.Second)
	require.LessOrEqual(t, int64(1), cnt.Load())
}
