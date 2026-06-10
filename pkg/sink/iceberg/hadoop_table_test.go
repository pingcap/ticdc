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

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAppendChangelogCreatesIcebergTableFiles(t *testing.T) {
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

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})

	v := "1"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v,
			},
		},
	}

	cfID := common.NewChangefeedID4Test("default", "cf")
	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 100)
	require.NoError(t, err)

	hintPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", versionHintFile)
	hint, err := os.ReadFile(hintPath)
	require.NoError(t, err)
	require.Equal(t, "1", string(hint))

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	_, err = os.Stat(metadataPath)
	require.NoError(t, err)
}

func TestEnsureTableSchemaEvolutionBumpsMetadataVersion(t *testing.T) {
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
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})
	require.NoError(t, tableWriter.EnsureTable(ctx, cfID, tableInfoV1))

	hintPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", versionHintFile)
	hint, err := os.ReadFile(hintPath)
	require.NoError(t, err)
	require.Equal(t, "1", string(hint))

	tableInfoV2 := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("col2"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})
	require.NoError(t, tableWriter.EnsureTable(ctx, cfID, tableInfoV2))

	hint, err = os.ReadFile(hintPath)
	require.NoError(t, err)
	require.Equal(t, "2", string(hint))

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v2.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	require.Len(t, m.Schemas, 2)
	require.Equal(t, 1, m.CurrentSchemaID)
	require.Equal(t, "struct", m.Schemas[0].Type)
	require.Equal(t, "struct", m.Schemas[1].Type)
}

func TestGetLastCommittedResolvedTsReadsFromMetadata(t *testing.T) {
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

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})

	v := "1"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v,
			},
		},
	}
	cfID := common.NewChangefeedID4Test("default", "cf")
	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 123)
	require.NoError(t, err)

	got, err := tableWriter.GetLastCommittedResolvedTs(ctx, tableInfo)
	require.NoError(t, err)
	require.Equal(t, uint64(123), got)
}

func TestAppendChangelogTracksParentSnapshotAndProperties(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"
	cfg.TargetFileSizeBytes = 123

	tableWriter := NewTableWriter(cfg, extStorage)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})

	v := "1"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v,
			},
		},
	}
	cfID := common.NewChangefeedID4Test("default", "cf")

	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 10)
	require.NoError(t, err)

	metadataPathV1 := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPathV1)
	require.NoError(t, err)

	var mV1 tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &mV1))
	require.Equal(t, "parquet", mV1.Properties["write.format.default"])
	require.Equal(t, "zstd", mV1.Properties["write.parquet.compression-codec"])
	require.Equal(t, "merge-on-read", mV1.Properties["write.update.mode"])
	require.Equal(t, "merge-on-read", mV1.Properties["write.delete.mode"])
	require.Equal(t, "123", mV1.Properties["write.target-file-size-bytes"])
	require.NotNil(t, mV1.CurrentSnapshotID)
	require.NotNil(t, mV1.currentSnapshot())
	require.Nil(t, mV1.currentSnapshot().ParentSnapshotID)
	firstSnapshotID := *mV1.CurrentSnapshotID

	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 20)
	require.NoError(t, err)

	metadataPathV2 := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v2.metadata.json")
	metadataBytes, err = os.ReadFile(metadataPathV2)
	require.NoError(t, err)

	var mV2 tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &mV2))
	require.NotNil(t, mV2.CurrentSnapshotID)
	require.NotNil(t, mV2.currentSnapshot())
	require.NotNil(t, mV2.currentSnapshot().ParentSnapshotID)
	require.Equal(t, firstSnapshotID, *mV2.currentSnapshot().ParentSnapshotID)
}

func TestUpsertWritesEqualityDeletesWithSequenceBounds(t *testing.T) {
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

	idType := types.NewFieldType(mysql.TypeLong)
	idType.AddFlag(mysql.PriKeyFlag)
	idType.AddFlag(mysql.NotNullFlag)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:         20,
		Name:       ast.NewCIStr("table1"),
		PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *idType,
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("v"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})
	equalityFieldIDs, err := GetEqualityFieldIDs(tableInfo)
	require.NoError(t, err)

	id := "1"
	val := "a"
	dataRows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"id": &id,
				"v":  &val,
			},
		},
	}
	deleteRows := []ChangeRow{
		{
			Op:         "D",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"id": &id,
			},
		},
	}

	cfID := common.NewChangefeedID4Test("default", "cf")
	_, err = tableWriter.Upsert(ctx, cfID, tableInfo, 20, dataRows, deleteRows, equalityFieldIDs, 100)
	require.NoError(t, err)

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)
	require.Equal(t, "delta", s.Summary["operation"])
	require.Equal(t, "100", s.Summary[summaryKeyCommittedResolvedTs])

	manifestListRel, err := tableWriter.relativePathFromLocation(s.ManifestList)
	require.NoError(t, err)
	manifestListBytes, err := extStorage.ReadFile(ctx, manifestListRel)
	require.NoError(t, err)

	reader, err := goavro.NewOCFReader(bytes.NewReader(manifestListBytes))
	require.NoError(t, err)

	var records []map[string]any
	for reader.Scan() {
		r, err := reader.Read()
		require.NoError(t, err)
		records = append(records, r.(map[string]any))
	}
	require.Len(t, records, 2)

	require.Equal(t, int32(0), records[0]["content"])
	require.Equal(t, int64(1), records[0]["sequence_number"])
	require.Equal(t, int64(1), records[0]["min_sequence_number"])

	require.Equal(t, int32(1), records[1]["content"])
	require.Equal(t, int64(1), records[1]["sequence_number"])
	require.Equal(t, int64(0), records[1]["min_sequence_number"])
}

func TestMetadataVersionFromFileName(t *testing.T) {
	version, ok := metadataVersionFromFileName("v1.metadata.json")
	require.True(t, ok)
	require.Equal(t, 1, version)

	version, ok = metadataVersionFromFileName("v99.metadata.json")
	require.True(t, ok)
	require.Equal(t, 99, version)

	_, ok = metadataVersionFromFileName("v0.metadata.json")
	require.False(t, ok)

	_, ok = metadataVersionFromFileName("metadata.json")
	require.False(t, ok)
}

func TestRecordCheckpointAppendsToCheckpointTable(t *testing.T) {
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

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})

	v := "1"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v,
			},
		},
	}
	cfID := common.NewChangefeedID4Test("default", "cf")

	commitResult, err := tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 10)
	require.NoError(t, err)
	require.NotNil(t, commitResult)

	require.NoError(t, tableWriter.RecordCheckpoint(ctx, cfID, tableInfo, 20, 10, commitResult))

	hintPath := filepath.Join(tmpDir, "ns", checkpointSchemaName, checkpointTableName, "metadata", versionHintFile)
	hint, err := os.ReadFile(hintPath)
	require.NoError(t, err)
	require.Equal(t, "1", string(hint))

	metadataPath := filepath.Join(tmpDir, "ns", checkpointSchemaName, checkpointTableName, "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)
	require.Equal(t, "10", s.Summary[summaryKeyCommittedResolvedTs])
}

func TestTruncateTableCommitsOverwriteSnapshot(t *testing.T) {
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

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("col1"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
	})
	cfID := common.NewChangefeedID4Test("default", "cf")

	v := "1"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v,
			},
		},
	}
	_, err = tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 10)
	require.NoError(t, err)

	commitResult, err := tableWriter.TruncateTable(ctx, cfID, tableInfo, 20, 20)
	require.NoError(t, err)
	require.NotNil(t, commitResult)

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v2.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)
	require.Equal(t, "overwrite", s.Summary["operation"])
	require.Equal(t, "20", s.Summary[summaryKeyCommittedResolvedTs])
}
