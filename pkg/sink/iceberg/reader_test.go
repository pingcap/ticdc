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

package iceberg

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestListHadoopTablesAndLoadTableVersion(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"

	tableWriter := NewTableWriter(cfg, extStorage)
	cfID := common.NewChangefeedID4Test("default", "cf")

	tableInfoV1 := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})
	require.NoError(t, tableWriter.EnsureTable(ctx, cfID, tableInfoV1))

	v1 := "1"
	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfoV1, 20, []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "101",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"id": &v1,
			},
		},
	}, 101)
	require.NoError(t, err)

	tableInfoV2 := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("name"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})
	require.NoError(t, tableWriter.EnsureTable(ctx, cfID, tableInfoV2))

	tables, err := ListHadoopTables(ctx, cfg, extStorage)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, "test", tables[0].SchemaName)
	require.Equal(t, "table1", tables[0].TableName)
	require.Equal(t, 3, tables[0].LatestMetadataVersion)

	version1, err := LoadTableVersion(ctx, cfg, extStorage, "test", "table1", 1)
	require.NoError(t, err)
	require.Equal(t, 1, version1.MetadataVersion)
	require.Empty(t, version1.DataFiles)
	require.Len(t, version1.Columns, 1)

	version2, err := LoadTableVersion(ctx, cfg, extStorage, "test", "table1", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(101), version2.CommittedResolvedTs)
	require.Len(t, version2.DataFiles, 1)
	require.Len(t, version2.Columns, 1)

	version3, err := LoadTableVersion(ctx, cfg, extStorage, "test", "table1", 3)
	require.NoError(t, err)
	require.Empty(t, version3.DataFiles)
	require.Len(t, version3.Columns, 2)
}

func TestDecodeParquetFile(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"

	tableWriter := NewTableWriter(cfg, extStorage)
	cfID := common.NewChangefeedID4Test("default", "cf")

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("name"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})

	id := "1"
	name := "alice"
	tableVersion := "9"
	rowIdentity := "[\"1\"]"
	oldRowIdentity := "[\"0\"]"
	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, []ChangeRow{
		{
			Op:             "I",
			CommitTs:       "101",
			CommitTime:     "2026-01-01T00:00:00Z",
			TableVersion:   tableVersion,
			RowIdentity:    rowIdentity,
			OldRowIdentity: &oldRowIdentity,
			IdentityKind:   "pk",
			Columns: map[string]*string{
				"id":   &id,
				"name": &name,
			},
		},
	}, 101)
	require.NoError(t, err)

	version, err := LoadTableVersion(ctx, cfg, extStorage, "test", "table1", 1)
	require.NoError(t, err)
	require.Len(t, version.DataFiles, 1)

	rows, err := DecodeParquetFile(ctx, extStorage, version.DataFiles[0])
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "I", rows[0].Op)
	require.Equal(t, "101", rows[0].CommitTs)
	require.Equal(t, tableVersion, rows[0].TableVersion)
	require.Equal(t, rowIdentity, rows[0].RowIdentity)
	require.NotNil(t, rows[0].OldRowIdentity)
	require.Equal(t, oldRowIdentity, *rows[0].OldRowIdentity)
	require.Equal(t, "pk", rows[0].IdentityKind)
	require.NotNil(t, rows[0].Columns["id"])
	require.Equal(t, "1", *rows[0].Columns["id"])
	require.NotNil(t, rows[0].Columns["name"])
	require.Equal(t, "alice", *rows[0].Columns["name"])
}

func TestGlueCatalogReaderUsesWarehouseLayout(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	writeCfg := NewConfig()
	writeCfg.WarehouseURI = warehouseURL.String()
	writeCfg.WarehouseLocation = warehouseURL.String()
	writeCfg.Namespace = "ns"

	tableWriter := NewTableWriter(writeCfg, extStorage)
	cfID := common.NewChangefeedID4Test("default", "cf")

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})
	require.NoError(t, tableWriter.EnsureTable(ctx, cfID, tableInfo))

	id := "1"
	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "101",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"id": &id,
			},
		},
	}, 101)
	require.NoError(t, err)

	readCfg := NewConfig()
	readCfg.WarehouseURI = warehouseURL.String()
	readCfg.WarehouseLocation = warehouseURL.String()
	readCfg.Namespace = "ns"
	readCfg.Catalog = CatalogGlue

	tables, err := ListHadoopTables(ctx, readCfg, extStorage)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, 2, tables[0].LatestMetadataVersion)

	version, err := LoadTableVersion(ctx, readCfg, extStorage, "test", "table1", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(101), version.CommittedResolvedTs)
	require.Len(t, version.DataFiles, 1)
}
