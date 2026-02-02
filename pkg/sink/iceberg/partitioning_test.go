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
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func readManifestEntries(
	t *testing.T,
	ctx context.Context,
	extStorage storage.ExternalStorage,
	tableWriter *TableWriter,
	manifestListRecords []map[string]any,
) []map[string]any {
	t.Helper()

	var entries []map[string]any
	for _, rec := range manifestListRecords {
		manifestPath := rec["manifest_path"].(string)
		manifestRel, err := tableWriter.relativePathFromLocation(manifestPath)
		require.NoError(t, err)
		manifestBytes, err := extStorage.ReadFile(ctx, manifestRel)
		require.NoError(t, err)

		manifestReader, err := goavro.NewOCFReader(bytes.NewReader(manifestBytes))
		require.NoError(t, err)
		for manifestReader.Scan() {
			entryAny, err := manifestReader.Read()
			require.NoError(t, err)
			entries = append(entries, entryAny.(map[string]any))
		}
	}
	return entries
}

func TestAppendChangelogPartitionsByCommitTimeDay(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"
	cfg.Partitioning = "days(_tidb_commit_time)"

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

	v1 := "1"
	v2 := "2"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v1,
			},
		},
		{
			Op:         "I",
			CommitTs:   "2",
			CommitTime: "2026-01-02T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v2,
			},
		},
	}

	cfID := common.NewChangefeedID4Test("default", "cf")
	commitResult, err := tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 100)
	require.NoError(t, err)
	require.NotNil(t, commitResult)
	require.Equal(t, 2, commitResult.DataFilesWritten)

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)

	manifestListRel, err := tableWriter.relativePathFromLocation(s.ManifestList)
	require.NoError(t, err)
	manifestListBytes, err := extStorage.ReadFile(ctx, manifestListRel)
	require.NoError(t, err)

	manifestListReader, err := goavro.NewOCFReader(bytes.NewReader(manifestListBytes))
	require.NoError(t, err)
	var manifestListRecords []map[string]any
	for manifestListReader.Scan() {
		r, err := manifestListReader.Read()
		require.NoError(t, err)
		manifestListRecords = append(manifestListRecords, r.(map[string]any))
	}
	require.NotEmpty(t, manifestListRecords)

	epochDays := make(map[int32]struct{})
	entries := readManifestEntries(t, ctx, extStorage, tableWriter, manifestListRecords)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		dataFile := entry["data_file"].(map[string]any)
		partition := dataFile["partition"].(map[string]any)
		require.Contains(t, partition, "_tidb_commit_time_day")

		union := partition["_tidb_commit_time_day"].(map[string]any)
		days := union["int"].(int32)
		epochDays[days] = struct{}{}
	}

	day1 := int32(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	day2 := int32(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	_, ok1 := epochDays[day1]
	_, ok2 := epochDays[day2]
	require.True(t, ok1)
	require.True(t, ok2)
}

func TestUpsertWritesUnpartitionedDeletesWithPartitionedData(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"
	cfg.Partitioning = "days(_tidb_commit_time)"

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
	commitResult, err := tableWriter.Upsert(ctx, cfID, tableInfo, 20, dataRows, deleteRows, equalityFieldIDs, 100)
	require.Error(t, err)
	require.Nil(t, commitResult)
	require.Contains(t, err.Error(), "upsert requires partitioning")
}

func TestUpsertPartitionsDeletesWhenDerivedFromEqualityFields(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"
	cfg.Partitioning = "bucket(id, 16)"

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
	commitResult, err := tableWriter.Upsert(ctx, cfID, tableInfo, 20, dataRows, deleteRows, equalityFieldIDs, 100)
	require.NoError(t, err)
	require.NotNil(t, commitResult)
	require.Equal(t, 1, commitResult.DataFilesWritten)
	require.Equal(t, 1, commitResult.DeleteFilesWritten)

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)

	manifestListRel, err := tableWriter.relativePathFromLocation(s.ManifestList)
	require.NoError(t, err)
	manifestListBytes, err := extStorage.ReadFile(ctx, manifestListRel)
	require.NoError(t, err)

	manifestListReader, err := goavro.NewOCFReader(bytes.NewReader(manifestListBytes))
	require.NoError(t, err)
	var manifestListRecords []map[string]any
	for manifestListReader.Scan() {
		r, err := manifestListReader.Read()
		require.NoError(t, err)
		manifestListRecords = append(manifestListRecords, r.(map[string]any))
	}
	// Manifest entries can be batched, so avoid asserting an exact count.
	require.NotEmpty(t, manifestListRecords)

	bucketBytes, err := bucketHashBytes("int", id)
	require.NoError(t, err)
	expectedBucket := int32(((int(int32(murmur3Sum32(bucketBytes))) % 16) + 16) % 16)

	var sawData, sawDeletes bool
	entries := readManifestEntries(t, ctx, extStorage, tableWriter, manifestListRecords)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		dataFile := entry["data_file"].(map[string]any)
		content := dataFile["content"].(int32)

		partition := dataFile["partition"].(map[string]any)
		union := partition["id_bucket_16"].(map[string]any)
		actualBucket := union["int"].(int32)

		switch content {
		case dataFileContentData:
			sawData = true
			require.Equal(t, expectedBucket, actualBucket)
		case dataFileContentEqualityDeletes:
			sawDeletes = true
			require.Equal(t, expectedBucket, actualBucket)
		default:
			t.Fatalf("unexpected manifest data file content: %d", content)
		}
	}
	require.True(t, sawData)
	require.True(t, sawDeletes)
}

func TestAppendChangelogPartitionsByIdentityInt(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"
	cfg.Partitioning = "identity(col1)"

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

	v1 := "1"
	v2 := "2"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v1,
			},
		},
		{
			Op:         "I",
			CommitTs:   "2",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"col1": &v2,
			},
		},
	}

	cfID := common.NewChangefeedID4Test("default", "cf")
	commitResult, err := tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 100)
	require.NoError(t, err)
	require.NotNil(t, commitResult)
	require.Equal(t, 2, commitResult.DataFilesWritten)

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)

	manifestListRel, err := tableWriter.relativePathFromLocation(s.ManifestList)
	require.NoError(t, err)
	manifestListBytes, err := extStorage.ReadFile(ctx, manifestListRel)
	require.NoError(t, err)

	manifestListReader, err := goavro.NewOCFReader(bytes.NewReader(manifestListBytes))
	require.NoError(t, err)
	var manifestListRecords []map[string]any
	for manifestListReader.Scan() {
		r, err := manifestListReader.Read()
		require.NoError(t, err)
		manifestListRecords = append(manifestListRecords, r.(map[string]any))
	}
	require.NotEmpty(t, manifestListRecords)

	values := make(map[int32]struct{})
	entries := readManifestEntries(t, ctx, extStorage, tableWriter, manifestListRecords)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		dataFile := entry["data_file"].(map[string]any)
		partition := dataFile["partition"].(map[string]any)
		union := partition["col1_identity"].(map[string]any)
		values[union["int"].(int32)] = struct{}{}
	}

	_, ok1 := values[1]
	_, ok2 := values[2]
	require.True(t, ok1)
	require.True(t, ok2)
}

func TestAppendChangelogPartitionsByTruncateString(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"
	cfg.Partitioning = "truncate(s,2)"

	tableWriter := NewTableWriter(cfg, extStorage)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("s"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})

	s1 := "abcd"
	s2 := "xyz"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"s": &s1,
			},
		},
		{
			Op:         "I",
			CommitTs:   "2",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"s": &s2,
			},
		},
	}

	cfID := common.NewChangefeedID4Test("default", "cf")
	commitResult, err := tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 100)
	require.NoError(t, err)
	require.NotNil(t, commitResult)
	require.Equal(t, 2, commitResult.DataFilesWritten)

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)

	manifestListRel, err := tableWriter.relativePathFromLocation(s.ManifestList)
	require.NoError(t, err)
	manifestListBytes, err := extStorage.ReadFile(ctx, manifestListRel)
	require.NoError(t, err)

	manifestListReader, err := goavro.NewOCFReader(bytes.NewReader(manifestListBytes))
	require.NoError(t, err)
	var manifestListRecords []map[string]any
	for manifestListReader.Scan() {
		r, err := manifestListReader.Read()
		require.NoError(t, err)
		manifestListRecords = append(manifestListRecords, r.(map[string]any))
	}
	require.NotEmpty(t, manifestListRecords)

	values := make(map[string]struct{})
	entries := readManifestEntries(t, ctx, extStorage, tableWriter, manifestListRecords)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		dataFile := entry["data_file"].(map[string]any)
		partition := dataFile["partition"].(map[string]any)
		union := partition["s_truncate_2"].(map[string]any)
		values[union["string"].(string)] = struct{}{}
	}

	_, ok1 := values["ab"]
	_, ok2 := values["xy"]
	require.True(t, ok1)
	require.True(t, ok2)
}

func TestAppendChangelogPartitionsByHour(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"
	cfg.Partitioning = "hours(ts)"

	tableWriter := NewTableWriter(cfg, extStorage)

	tsType := types.NewFieldType(mysql.TypeDatetime)
	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("ts"),
				FieldType: *tsType,
			},
		},
	})

	t1 := "1970-01-01 01:00:00"
	t2 := "1970-01-01 02:00:00"
	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "1",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"ts": &t1,
			},
		},
		{
			Op:         "I",
			CommitTs:   "2",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"ts": &t2,
			},
		},
	}

	cfID := common.NewChangefeedID4Test("default", "cf")
	commitResult, err := tableWriter.AppendChangelog(ctx, cfID, tableInfo, 20, rows, 100)
	require.NoError(t, err)
	require.NotNil(t, commitResult)
	require.Equal(t, 2, commitResult.DataFilesWritten)

	metadataPath := filepath.Join(tmpDir, "ns", "test", "table1", "metadata", "v1.metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var m tableMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &m))
	s := m.currentSnapshot()
	require.NotNil(t, s)

	manifestListRel, err := tableWriter.relativePathFromLocation(s.ManifestList)
	require.NoError(t, err)
	manifestListBytes, err := extStorage.ReadFile(ctx, manifestListRel)
	require.NoError(t, err)

	manifestListReader, err := goavro.NewOCFReader(bytes.NewReader(manifestListBytes))
	require.NoError(t, err)
	var manifestListRecords []map[string]any
	for manifestListReader.Scan() {
		r, err := manifestListReader.Read()
		require.NoError(t, err)
		manifestListRecords = append(manifestListRecords, r.(map[string]any))
	}
	require.NotEmpty(t, manifestListRecords)

	values := make(map[int32]struct{})
	entries := readManifestEntries(t, ctx, extStorage, tableWriter, manifestListRecords)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		dataFile := entry["data_file"].(map[string]any)
		partition := dataFile["partition"].(map[string]any)
		union := partition["ts_hour"].(map[string]any)
		values[union["int"].(int32)] = struct{}{}
	}

	_, ok1 := values[1]
	_, ok2 := values[2]
	require.True(t, ok1)
	require.True(t, ok2)
}
