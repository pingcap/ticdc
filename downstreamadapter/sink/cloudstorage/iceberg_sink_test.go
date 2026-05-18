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

package cloudstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestStorageSinkIcebergAppendBasic(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=iceberg&namespace=ns&commit-interval=200ms", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	go func() {
		_ = cloudStorageSink.Run(ctx)
	}()

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:         100,
		Name:       ast.NewCIStr("t_iceberg"),
		PKIsHandle: true,
		UpdateTS:   1,
		Columns: []*timodel.ColumnInfo{
			func() *timodel.ColumnInfo {
				col := &timodel.ColumnInfo{
					ID:        1,
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLong),
				}
				col.AddFlag(mysql.PriKeyFlag)
				col.AddFlag(mysql.NotNullFlag)
				return col
			}(),
			{
				ID:        2,
				Name:      ast.NewCIStr("v"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	rows.AppendInt64(0, 1)
	rows.AppendString(1, "a")
	rows.AppendInt64(0, 2)
	rows.AppendString(1, "b")

	dmlEvent := commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, 1, 2, tableInfo)
	dmlEvent.SetRows(rows)
	dmlEvent.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
	dmlEvent.Length = 2
	dmlEvent.TableInfoVersion = 1
	cloudStorageSink.AddDMLEvent(dmlEvent)

	metadataGlob := filepath.Join(parentDir, "ns", "test", "t_iceberg", "metadata", "v*.metadata.json")
	dataGlob := filepath.Join(parentDir, "ns", "test", "t_iceberg", "data", "snap-*.parquet")

	require.Eventually(t, func() bool {
		metas, _ := filepath.Glob(metadataGlob)
		dataFiles, _ := filepath.Glob(dataGlob)
		return len(metas) > 0 && len(dataFiles) > 0
	}, 20*time.Second, 200*time.Millisecond)

	metas, err := filepath.Glob(metadataGlob)
	require.NoError(t, err)
	require.NotEmpty(t, metas)

	metadataBytes, err := os.ReadFile(metas[len(metas)-1])
	require.NoError(t, err)
	require.Contains(t, string(metadataBytes), "\"format-version\":2")
	require.Contains(t, string(metadataBytes), "\"tidb.committed_resolved_ts\"")
}

func TestStorageSinkIcebergWritesPhase0ControlManifests(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=iceberg&namespace=ns&commit-interval=200ms", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	go func() {
		_ = cloudStorageSink.Run(ctx)
	}()

	time.Sleep(3 * time.Second)

	tableVersion := oracle.GoTimeToTS(time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC))
	resolvedTs := oracle.GoTimeToTS(time.Date(2026, 4, 16, 10, 0, 5, 0, time.UTC))

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:         100,
		Name:       ast.NewCIStr("t_iceberg"),
		PKIsHandle: true,
		UpdateTS:   tableVersion,
		Columns: []*timodel.ColumnInfo{
			func() *timodel.ColumnInfo {
				col := &timodel.ColumnInfo{
					ID:        1,
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLong),
				}
				col.AddFlag(mysql.PriKeyFlag)
				col.AddFlag(mysql.NotNullFlag)
				return col
			}(),
			{
				ID:        2,
				Name:      ast.NewCIStr("v"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})

	ddlEvent := &commonEvent.DDLEvent{
		Version:    commonEvent.DDLEventVersion1,
		Type:       byte(timodel.ActionCreateTable),
		SchemaName: "test",
		TableName:  "t_iceberg",
		Query:      "create table `test`.`t_iceberg` (`id` int primary key, `v` varchar(32))",
		FinishedTs: tableVersion,
		Seq:        7,
		TableInfo:  tableInfo,
	}

	require.NoError(t, cloudStorageSink.WriteBlockEvent(ddlEvent))
	cloudStorageSink.AddCheckpointTs(resolvedTs)

	ddlGlob := filepath.Join(parentDir, "ns", "control", "ddl", "date=*", "commit_ts=*", "seq=*", "*.json")
	checkpointGlob := filepath.Join(parentDir, "ns", "control", "checkpoint", "global", "date=*", "*.json")
	schemaPath := filepath.Join(parentDir, "ns", "control", "schema", "test", "t_iceberg", fmt.Sprintf("schema_%d.json", tableVersion))

	require.Eventually(t, func() bool {
		ddlFiles, _ := filepath.Glob(ddlGlob)
		checkpointFiles, _ := filepath.Glob(checkpointGlob)
		_, schemaErr := os.Stat(schemaPath)
		return len(ddlFiles) == 1 && len(checkpointFiles) == 1 && schemaErr == nil
	}, 20*time.Second, 200*time.Millisecond)

	ddlFiles, err := filepath.Glob(ddlGlob)
	require.NoError(t, err)
	require.Len(t, ddlFiles, 1)

	ddlBytes, err := os.ReadFile(ddlFiles[0])
	require.NoError(t, err)
	var ddlManifest map[string]any
	require.NoError(t, json.Unmarshal(ddlBytes, &ddlManifest))
	require.Equal(t, "direct_replay", ddlManifest["ddl_apply_class"])
	require.Equal(t, false, ddlManifest["need_rebuild"])
	require.Equal(t, "test/test", ddlManifest["changefeed_id"])
	require.EqualValues(t, float64(tableVersion), ddlManifest["table_version_after"])
	require.Equal(t, "test", ddlManifest["source_db"])
	require.Equal(t, "t_iceberg", ddlManifest["source_table"])

	schemaBytes, err := os.ReadFile(schemaPath)
	require.NoError(t, err)
	var schemaManifest map[string]any
	require.NoError(t, json.Unmarshal(schemaBytes, &schemaManifest))
	require.EqualValues(t, float64(tableVersion), schemaManifest["table_version"])
	require.Equal(t, "test", schemaManifest["source_db"])
	require.Equal(t, "t_iceberg", schemaManifest["source_table"])
	require.Equal(t, []any{"id"}, schemaManifest["handle_key_columns"])

	checkpointFiles, err := filepath.Glob(checkpointGlob)
	require.NoError(t, err)
	require.Len(t, checkpointFiles, 1)

	checkpointBytes, err := os.ReadFile(checkpointFiles[0])
	require.NoError(t, err)
	var checkpointManifest map[string]any
	require.NoError(t, json.Unmarshal(checkpointBytes, &checkpointManifest))
	require.Equal(t, "test/test", checkpointManifest["changefeed_id"])
	require.EqualValues(t, float64(resolvedTs), checkpointManifest["resolved_ts"])
}

func TestStorageSinkIcebergDoesNotFailOnPartitionDDL(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=iceberg&namespace=ns&commit-interval=200ms", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	tableVersion := oracle.GoTimeToTS(time.Date(2026, 4, 16, 11, 0, 0, 0, time.UTC))
	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:       100,
		Name:     ast.NewCIStr("t_partitioned"),
		UpdateTS: tableVersion,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})

	ddlEvent := &commonEvent.DDLEvent{
		Version:    commonEvent.DDLEventVersion1,
		Type:       byte(timodel.ActionAddTablePartition),
		SchemaName: "test",
		TableName:  "t_partitioned",
		Query:      "alter table `test`.`t_partitioned` add partition (partition p1 values less than (10))",
		FinishedTs: tableVersion,
		Seq:        9,
		TableInfo:  tableInfo,
	}

	require.NoError(t, cloudStorageSink.WriteBlockEvent(ddlEvent))

	ddlGlob := filepath.Join(parentDir, "ns", "control", "ddl", "date=*", "commit_ts=*", "seq=*", "*.json")
	require.Eventually(t, func() bool {
		ddlFiles, _ := filepath.Glob(ddlGlob)
		return len(ddlFiles) == 1
	}, 20*time.Second, 200*time.Millisecond)

	ddlFiles, err := filepath.Glob(ddlGlob)
	require.NoError(t, err)
	require.Len(t, ddlFiles, 1)

	ddlBytes, err := os.ReadFile(ddlFiles[0])
	require.NoError(t, err)
	var ddlManifest map[string]any
	require.NoError(t, json.Unmarshal(ddlBytes, &ddlManifest))
	require.Equal(t, "rebuild_required", ddlManifest["ddl_apply_class"])
	require.Equal(t, true, ddlManifest["need_rebuild"])
}
