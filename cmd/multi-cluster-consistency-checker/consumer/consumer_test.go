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

package consumer

import (
	"bytes"
	"context"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestUpdateTableDMLIdxMap(t *testing.T) {
	t.Parallel()

	t.Run("insert new entry", func(t *testing.T) {
		t.Parallel()
		m := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
		dmlKey := cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl", TableVersion: 1},
			Date:          "2026-01-01",
		}
		fileIdx := &cloudstorage.FileIndex{
			FileIndexKey: cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false},
			Idx:          5,
		}

		updateTableDMLIdxMap(m, dmlKey, fileIdx)
		require.Len(t, m, 1)
		require.Equal(t, uint64(5), m[dmlKey][fileIdx.FileIndexKey])
	})

	t.Run("update with higher index", func(t *testing.T) {
		t.Parallel()
		m := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
		dmlKey := cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl", TableVersion: 1},
			Date:          "2026-01-01",
		}
		indexKey := cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}
		fileIdx1 := &cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: 3}
		fileIdx2 := &cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: 7}

		updateTableDMLIdxMap(m, dmlKey, fileIdx1)
		updateTableDMLIdxMap(m, dmlKey, fileIdx2)
		require.Equal(t, uint64(7), m[dmlKey][indexKey])
	})

	t.Run("skip lower index", func(t *testing.T) {
		t.Parallel()
		m := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
		dmlKey := cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl", TableVersion: 1},
			Date:          "2026-01-01",
		}
		indexKey := cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}
		fileIdx1 := &cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: 10}
		fileIdx2 := &cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: 5}

		updateTableDMLIdxMap(m, dmlKey, fileIdx1)
		updateTableDMLIdxMap(m, dmlKey, fileIdx2)
		require.Equal(t, uint64(10), m[dmlKey][indexKey])
	})
}

func TestCurrentTableVersion(t *testing.T) {
	t.Parallel()

	t.Run("get returns zero value for missing key", func(t *testing.T) {
		t.Parallel()
		cvt := NewCurrentTableVersion()
		v := cvt.GetCurrentTableVersion("db", "tbl")
		require.Equal(t, types.VersionKey{}, v)
	})

	t.Run("update and get", func(t *testing.T) {
		t.Parallel()
		cvt := NewCurrentTableVersion()
		vk := types.VersionKey{Version: 100, VersionPath: "db/tbl/meta/schema_100_0000000000.json"}
		cvt.UpdateCurrentTableVersion("db", "tbl", vk)
		got := cvt.GetCurrentTableVersion("db", "tbl")
		require.Equal(t, vk, got)
	})

	t.Run("update overwrites previous value", func(t *testing.T) {
		t.Parallel()
		cvt := NewCurrentTableVersion()
		vk1 := types.VersionKey{Version: 1}
		vk2 := types.VersionKey{Version: 2}
		cvt.UpdateCurrentTableVersion("db", "tbl", vk1)
		cvt.UpdateCurrentTableVersion("db", "tbl", vk2)
		got := cvt.GetCurrentTableVersion("db", "tbl")
		require.Equal(t, vk2, got)
	})

	t.Run("different tables are independent", func(t *testing.T) {
		t.Parallel()
		cvt := NewCurrentTableVersion()
		vk1 := types.VersionKey{Version: 10}
		vk2 := types.VersionKey{Version: 20}
		cvt.UpdateCurrentTableVersion("db", "tbl1", vk1)
		cvt.UpdateCurrentTableVersion("db", "tbl2", vk2)
		require.Equal(t, vk1, cvt.GetCurrentTableVersion("db", "tbl1"))
		require.Equal(t, vk2, cvt.GetCurrentTableVersion("db", "tbl2"))
	})
}

func TestSchemaDefinitions(t *testing.T) {
	t.Parallel()

	t.Run("get returns error for missing key", func(t *testing.T) {
		t.Parallel()
		sp := NewSchemaDefinitions()
		_, err := sp.GetColumnFieldTypes("db", "tbl", 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema definition not found")
	})

	t.Run("set and get empty table definition", func(t *testing.T) {
		t.Parallel()
		sp := NewSchemaDefinitions()
		key := cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl", TableVersion: 1}
		td := &cloudstorage.TableDefinition{}
		err := sp.SetSchemaDefinition(key, "/path/to/schema.json", td)
		require.NoError(t, err)

		got, err := sp.GetColumnFieldTypes("db", "tbl", 1)
		require.NoError(t, err)
		require.Equal(t, map[string]*ptypes.FieldType{}, got)
	})

	t.Run("set and get with columns parses field types correctly", func(t *testing.T) {
		t.Parallel()
		sp := NewSchemaDefinitions()
		key := cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl", TableVersion: 1}
		td := &cloudstorage.TableDefinition{
			Table:  "tbl",
			Schema: "db",
			Columns: []cloudstorage.TableCol{
				{Name: "id", Tp: "INT", IsPK: "true", Precision: "11"},
				{Name: "name", Tp: "VARCHAR", Precision: "255"},
				{Name: "score", Tp: "DECIMAL", Precision: "10", Scale: "2"},
				{Name: "duration", Tp: "TIME", Scale: "3"},
				{Name: "created_at", Tp: "TIMESTAMP", Scale: "6"},
				{Name: "big_id", Tp: "BIGINT UNSIGNED", Precision: "20"},
			},
			TotalColumns: 6,
		}
		err := sp.SetSchemaDefinition(key, "/path/to/schema.json", td)
		require.NoError(t, err)

		got, err := sp.GetColumnFieldTypes("db", "tbl", 1)
		require.NoError(t, err)
		require.Len(t, got, 6)

		// INT PK
		require.Equal(t, mysql.TypeLong, got["id"].GetType())
		require.True(t, mysql.HasPriKeyFlag(got["id"].GetFlag()))
		require.Equal(t, 11, got["id"].GetFlen())

		// VARCHAR(255)
		require.Equal(t, mysql.TypeVarchar, got["name"].GetType())
		require.Equal(t, 255, got["name"].GetFlen())

		// DECIMAL(10,2)
		require.Equal(t, mysql.TypeNewDecimal, got["score"].GetType())
		require.Equal(t, 10, got["score"].GetFlen())
		require.Equal(t, 2, got["score"].GetDecimal())

		// TIME(3) — decimal stores FSP
		require.Equal(t, mysql.TypeDuration, got["duration"].GetType())
		require.Equal(t, 3, got["duration"].GetDecimal())

		// TIMESTAMP(6) — decimal stores FSP
		require.Equal(t, mysql.TypeTimestamp, got["created_at"].GetType())
		require.Equal(t, 6, got["created_at"].GetDecimal())

		// BIGINT UNSIGNED
		require.Equal(t, mysql.TypeLonglong, got["big_id"].GetType())
		require.True(t, mysql.HasUnsignedFlag(got["big_id"].GetFlag()))
		require.Equal(t, 20, got["big_id"].GetFlen())
	})

	t.Run("set returns error for invalid column definition", func(t *testing.T) {
		t.Parallel()
		sp := NewSchemaDefinitions()
		key := cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl", TableVersion: 1}
		td := &cloudstorage.TableDefinition{
			Table:  "tbl",
			Schema: "db",
			Columns: []cloudstorage.TableCol{
				{Name: "id", Tp: "INT", Precision: "not_a_number"},
			},
			TotalColumns: 1,
		}
		err := sp.SetSchemaDefinition(key, "/path/to/schema.json", td)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to convert column id to FieldType")

		// Verify the definition was NOT stored
		_, err = sp.GetColumnFieldTypes("db", "tbl", 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema definition not found")
	})

	t.Run("remove with condition", func(t *testing.T) {
		t.Parallel()
		sp := NewSchemaDefinitions()
		key1 := cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl1", TableVersion: 1}
		key2 := cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl2", TableVersion: 2}
		require.NoError(t, sp.SetSchemaDefinition(key1, "/path1", nil))
		require.NoError(t, sp.SetSchemaDefinition(key2, "/path2", nil))

		// Remove only entries for tbl1
		sp.RemoveSchemaDefinitionWithCondition(func(k cloudstorage.SchemaPathKey) bool {
			return k.Table == "tbl1"
		})

		_, err := sp.GetColumnFieldTypes("db", "tbl1", 1)
		require.Error(t, err)

		_, err = sp.GetColumnFieldTypes("db", "tbl2", 2)
		require.NoError(t, err)
	})

	t.Run("remove with condition matching all", func(t *testing.T) {
		t.Parallel()
		sp := NewSchemaDefinitions()
		key1 := cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl1", TableVersion: 1}
		key2 := cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl2", TableVersion: 2}
		require.NoError(t, sp.SetSchemaDefinition(key1, "/path1", nil))
		require.NoError(t, sp.SetSchemaDefinition(key2, "/path2", nil))

		sp.RemoveSchemaDefinitionWithCondition(func(k cloudstorage.SchemaPathKey) bool {
			return true
		})

		_, err := sp.GetColumnFieldTypes("db", "tbl1", 1)
		require.Error(t, err)
		_, err = sp.GetColumnFieldTypes("db", "tbl2", 2)
		require.Error(t, err)
	})
}

func TestTableDMLIdx_DiffNewTableDMLIdxMap(t *testing.T) {
	t.Parallel()

	indexKey := cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}
	dmlKey := cloudstorage.DmlPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl", TableVersion: 1},
		Date:          "2026-01-01",
	}

	t.Run("new entry starts from 1", func(t *testing.T) {
		t.Parallel()
		idx := NewTableDMLIdx()
		newMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 5},
		}

		result := idx.DiffNewTableDMLIdxMap(newMap)
		require.Len(t, result, 1)
		require.Equal(t, indexRange{start: 1, end: 5}, result[dmlKey][indexKey])
	})

	t.Run("existing entry increments from previous end + 1", func(t *testing.T) {
		t.Parallel()
		idx := NewTableDMLIdx()

		// First call: set initial state
		firstMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 3},
		}
		idx.DiffNewTableDMLIdxMap(firstMap)

		// Second call: new end is 7, should get range [4, 7]
		secondMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 7},
		}
		result := idx.DiffNewTableDMLIdxMap(secondMap)
		require.Len(t, result, 1)
		require.Equal(t, indexRange{start: 4, end: 7}, result[dmlKey][indexKey])
	})

	t.Run("same end value returns no diff", func(t *testing.T) {
		t.Parallel()
		idx := NewTableDMLIdx()

		firstMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 5},
		}
		idx.DiffNewTableDMLIdxMap(firstMap)

		secondMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 5},
		}
		result := idx.DiffNewTableDMLIdxMap(secondMap)
		require.Empty(t, result)
	})

	t.Run("lower end value returns no diff", func(t *testing.T) {
		t.Parallel()
		idx := NewTableDMLIdx()

		firstMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 10},
		}
		idx.DiffNewTableDMLIdxMap(firstMap)

		secondMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 5},
		}
		result := idx.DiffNewTableDMLIdxMap(secondMap)
		require.Empty(t, result)
	})

	t.Run("empty new map returns empty result", func(t *testing.T) {
		t.Parallel()
		idx := NewTableDMLIdx()
		result := idx.DiffNewTableDMLIdxMap(map[cloudstorage.DmlPathKey]fileIndexKeyMap{})
		require.Empty(t, result)
	})

	t.Run("multiple keys", func(t *testing.T) {
		t.Parallel()
		idx := NewTableDMLIdx()
		dmlKey2 := cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "db", Table: "tbl2", TableVersion: 1},
			Date:          "2026-01-02",
		}

		newMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey:  {indexKey: 3},
			dmlKey2: {indexKey: 5},
		}
		result := idx.DiffNewTableDMLIdxMap(newMap)
		require.Len(t, result, 2)
		require.Equal(t, indexRange{start: 1, end: 3}, result[dmlKey][indexKey])
		require.Equal(t, indexRange{start: 1, end: 5}, result[dmlKey2][indexKey])
	})

	t.Run("multiple index keys for same dml path", func(t *testing.T) {
		t.Parallel()
		idx := NewTableDMLIdx()
		indexKey2 := cloudstorage.FileIndexKey{DispatcherID: "dispatcher1", EnableTableAcrossNodes: true}

		newMap := map[cloudstorage.DmlPathKey]fileIndexKeyMap{
			dmlKey: {indexKey: 3, indexKey2: 5},
		}
		result := idx.DiffNewTableDMLIdxMap(newMap)
		require.Len(t, result, 1)
		require.Equal(t, indexRange{start: 1, end: 3}, result[dmlKey][indexKey])
		require.Equal(t, indexRange{start: 1, end: 5}, result[dmlKey][indexKey2])
	})
}

type mockFile struct {
	name    string
	content []byte
}

type mockS3Storage struct {
	storage.ExternalStorage

	fileOffset  map[string]int
	sortedFiles []mockFile
}

func NewMockS3Storage(sortedFiles []mockFile) *mockS3Storage {
	s3Storage := &mockS3Storage{}
	s3Storage.UpdateFiles(sortedFiles)
	return s3Storage
}

func (m *mockS3Storage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return m.sortedFiles[m.fileOffset[name]].content, nil
}

func (m *mockS3Storage) WalkDir(ctx context.Context, opt *storage.WalkOption, fn func(path string, size int64) error) error {
	filenamePrefix := path.Join(opt.SubDir, opt.ObjPrefix)
	for _, file := range m.sortedFiles {
		if strings.HasPrefix(file.name, filenamePrefix) {
			if err := fn(file.name, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *mockS3Storage) UpdateFiles(sortedFiles []mockFile) {
	fileOffset := make(map[string]int)
	for i, file := range sortedFiles {
		fileOffset[file.name] = i
	}
	m.fileOffset = fileOffset
	m.sortedFiles = sortedFiles
}

func TestS3Consumer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	round1Files := []mockFile{
		{name: "test/t1/meta/schema_1_0000000001.json", content: []byte("{}")},
		{name: "test/t1/1/2026-01-01/CDC00000000000000000001.json", content: []byte("1_2026-01-01_1.json")},
	}
	round1TimeWindowData := types.TimeWindowData{
		TimeWindow: types.TimeWindow{LeftBoundary: 1, RightBoundary: 10},
		Data:       map[cloudstorage.DmlPathKey]types.IncrementalData{},
		MaxVersion: map[types.SchemaTableKey]types.VersionKey{
			{Schema: "test", Table: "t1"}: {
				Version:     1,
				VersionPath: "test/t1/meta/schema_1_0000000001.json",
				DataPath:    "test/t1/1/2026-01-01/CDC00000000000000000001.json",
			},
		},
	}
	expectedMaxVersionMap1 := func(maxVersionMap map[types.SchemaTableKey]types.VersionKey) {
		require.Len(t, maxVersionMap, 1)
		require.Equal(t, types.VersionKey{
			Version: 1, VersionPath: "test/t1/meta/schema_1_0000000001.json", DataPath: "test/t1/1/2026-01-01/CDC00000000000000000001.json",
		}, maxVersionMap[types.SchemaTableKey{Schema: "test", Table: "t1"}])
	}
	round2Files := []mockFile{
		{name: "test/t1/meta/schema_1_0000000001.json", content: []byte("{}")},
		{name: "test/t1/1/2026-01-01/CDC00000000000000000001.json", content: []byte("1_2026-01-01_1.json")},
		{name: "test/t1/1/2026-01-01/CDC00000000000000000002.json", content: []byte("1_2026-01-01_2.json")},
		{name: "test/t1/1/2026-01-02/CDC00000000000000000001.json", content: []byte("1_2026-01-02_1.json")},
	}
	round2TimeWindowData := types.TimeWindowData{
		TimeWindow: types.TimeWindow{LeftBoundary: 10, RightBoundary: 20},
		Data:       map[cloudstorage.DmlPathKey]types.IncrementalData{},
		MaxVersion: map[types.SchemaTableKey]types.VersionKey{
			{Schema: "test", Table: "t1"}: {
				Version:     1,
				VersionPath: "test/t1/meta/schema_1_0000000001.json",
				DataPath:    "test/t1/1/2026-01-02/CDC00000000000000000001.json",
			},
		},
	}
	expectedNewData2 := func(newData map[cloudstorage.DmlPathKey]types.IncrementalData) {
		require.Len(t, newData, 2)
		require.Equal(t, types.IncrementalData{
			DataContentSlices: map[cloudstorage.FileIndexKey][][]byte{
				{DispatcherID: "", EnableTableAcrossNodes: false}: {[]byte("1_2026-01-01_2.json")},
			},
			ColumnFieldTypes: map[string]*ptypes.FieldType{},
		}, newData[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 1},
			PartitionNum:  0,
			Date:          "2026-01-01",
		}])
		require.Equal(t, types.IncrementalData{
			DataContentSlices: map[cloudstorage.FileIndexKey][][]byte{
				{DispatcherID: "", EnableTableAcrossNodes: false}: {[]byte("1_2026-01-02_1.json")},
			},
			ColumnFieldTypes: map[string]*ptypes.FieldType{},
		}, newData[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 1},
			PartitionNum:  0,
			Date:          "2026-01-02",
		}])
	}
	expectedMaxVersionMap2 := func(maxVersionMap map[types.SchemaTableKey]types.VersionKey) {
		require.Len(t, maxVersionMap, 1)
		require.Equal(t, types.VersionKey{
			Version: 1, VersionPath: "test/t1/meta/schema_1_0000000001.json", DataPath: "test/t1/1/2026-01-02/CDC00000000000000000001.json",
		}, maxVersionMap[types.SchemaTableKey{Schema: "test", Table: "t1"}])
	}
	round3Files := []mockFile{
		{name: "test/t1/meta/schema_1_0000000001.json", content: []byte("{}")},
		{name: "test/t1/meta/schema_2_0000000001.json", content: []byte("{}")},
		{name: "test/t1/1/2026-01-01/CDC00000000000000000001.json", content: []byte("1_2026-01-01_1.json")},
		{name: "test/t1/1/2026-01-01/CDC00000000000000000002.json", content: []byte("1_2026-01-01_2.json")},
		{name: "test/t1/1/2026-01-02/CDC00000000000000000001.json", content: []byte("1_2026-01-02_1.json")},
		{name: "test/t1/1/2026-01-02/CDC00000000000000000002.json", content: []byte("1_2026-01-02_2.json")},
		{name: "test/t1/2/2026-01-02/CDC00000000000000000001.json", content: []byte("2_2026-01-02_1.json")},
		{name: "test/t1/2/2026-01-03/CDC00000000000000000001.json", content: []byte("2_2026-01-03_1.json")},
		{name: "test/t1/2/2026-01-03/CDC00000000000000000002.json", content: []byte("2_2026-01-03_2.json")},
	}
	round3TimeWindowData := types.TimeWindowData{
		TimeWindow: types.TimeWindow{LeftBoundary: 20, RightBoundary: 30},
		Data:       map[cloudstorage.DmlPathKey]types.IncrementalData{},
		MaxVersion: map[types.SchemaTableKey]types.VersionKey{
			{Schema: "test", Table: "t1"}: {
				Version:     2,
				VersionPath: "test/t1/meta/schema_2_0000000001.json",
				DataPath:    "test/t1/2/2026-01-03/CDC00000000000000000002.json",
			},
		},
	}
	expectedNewData3 := func(newData map[cloudstorage.DmlPathKey]types.IncrementalData) {
		require.Len(t, newData, 3)
		require.Equal(t, types.IncrementalData{
			DataContentSlices: map[cloudstorage.FileIndexKey][][]byte{
				{DispatcherID: "", EnableTableAcrossNodes: false}: {[]byte("1_2026-01-02_2.json")},
			},
			ColumnFieldTypes: map[string]*ptypes.FieldType{},
		}, newData[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 1},
			PartitionNum:  0,
			Date:          "2026-01-02",
		}])
		require.Equal(t, types.IncrementalData{
			DataContentSlices: map[cloudstorage.FileIndexKey][][]byte{
				{DispatcherID: "", EnableTableAcrossNodes: false}: {[]byte("2_2026-01-02_1.json")},
			},
			ColumnFieldTypes: map[string]*ptypes.FieldType{},
		}, newData[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 2},
			PartitionNum:  0,
			Date:          "2026-01-02",
		}])
		newDataContent := newData[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 2},
			PartitionNum:  0,
			Date:          "2026-01-03",
		}]
		require.Len(t, newDataContent.DataContentSlices, 1)
		contents := newDataContent.DataContentSlices[cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}]
		require.Len(t, contents, 2)
		slices.SortFunc(contents, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})
		require.Equal(t, [][]byte{[]byte("2_2026-01-03_1.json"), []byte("2_2026-01-03_2.json")}, contents)
	}
	expectedMaxVersionMap3 := func(maxVersionMap map[types.SchemaTableKey]types.VersionKey) {
		require.Len(t, maxVersionMap, 1)
		require.Equal(t, types.VersionKey{
			Version: 2, VersionPath: "test/t1/meta/schema_2_0000000001.json", DataPath: "test/t1/2/2026-01-03/CDC00000000000000000002.json",
		}, maxVersionMap[types.SchemaTableKey{Schema: "test", Table: "t1"}])
	}
	expectedCheckpoint23 := func(data map[cloudstorage.DmlPathKey]types.IncrementalData) {
		require.Len(t, data, 4)
		require.Equal(t, types.IncrementalData{
			DataContentSlices: map[cloudstorage.FileIndexKey][][]byte{
				{DispatcherID: "", EnableTableAcrossNodes: false}: {[]byte("1_2026-01-01_2.json")},
			},
			ColumnFieldTypes: map[string]*ptypes.FieldType{},
		}, data[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 1},
			PartitionNum:  0,
			Date:          "2026-01-01",
		}])
		dataContent := data[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 1},
			PartitionNum:  0,
			Date:          "2026-01-02",
		}]
		require.Len(t, dataContent.DataContentSlices, 1)
		contents := dataContent.DataContentSlices[cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}]
		require.Len(t, contents, 2)
		slices.SortFunc(contents, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})
		require.Equal(t, [][]byte{[]byte("1_2026-01-02_1.json"), []byte("1_2026-01-02_2.json")}, contents)
		require.Equal(t, types.IncrementalData{
			DataContentSlices: map[cloudstorage.FileIndexKey][][]byte{
				{DispatcherID: "", EnableTableAcrossNodes: false}: {[]byte("2_2026-01-02_1.json")},
			},
			ColumnFieldTypes: map[string]*ptypes.FieldType{},
		}, data[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 2},
			PartitionNum:  0,
			Date:          "2026-01-02",
		}])
		dataContent = data[cloudstorage.DmlPathKey{
			SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 2},
			PartitionNum:  0,
			Date:          "2026-01-03",
		}]
		require.Len(t, dataContent.DataContentSlices, 1)
		contents = dataContent.DataContentSlices[cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}]
		require.Len(t, contents, 2)
		slices.SortFunc(contents, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})
		require.Equal(t, [][]byte{[]byte("2_2026-01-03_1.json"), []byte("2_2026-01-03_2.json")}, contents)
	}

	t.Run("checkpoint with nil items returns nil", func(t *testing.T) {
		t.Parallel()
		s3Storage := NewMockS3Storage(round1Files)
		s3Consumer := NewS3Consumer(s3Storage, map[string][]string{"test": {"t1"}})
		data, err := s3Consumer.InitializeFromCheckpoint(ctx, "test", nil)
		require.NoError(t, err)
		require.Empty(t, data)
		newData, maxVersionMap, err := s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		require.Empty(t, newData)
		expectedMaxVersionMap1(maxVersionMap)
		s3Storage.UpdateFiles(round2Files)
		newData, maxVersionMap, err = s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		expectedNewData2(newData)
		expectedMaxVersionMap2(maxVersionMap)
		s3Storage.UpdateFiles(round3Files)
		newData, maxVersionMap, err = s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		expectedNewData3(newData)
		expectedMaxVersionMap3(maxVersionMap)
	})
	t.Run("checkpoint with empty items returns nil", func(t *testing.T) {
		t.Parallel()
		checkpoint := recorder.NewCheckpoint()
		s3Storage := NewMockS3Storage(round1Files)
		s3Consumer := NewS3Consumer(s3Storage, map[string][]string{"test": {"t1"}})
		data, err := s3Consumer.InitializeFromCheckpoint(ctx, "test", checkpoint)
		require.NoError(t, err)
		require.Empty(t, data)
		newData, maxVersionMap, err := s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		require.Empty(t, newData)
		expectedMaxVersionMap1(maxVersionMap)
		s3Storage.UpdateFiles(round2Files)
		newData, maxVersionMap, err = s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		expectedNewData2(newData)
		expectedMaxVersionMap2(maxVersionMap)
		s3Storage.UpdateFiles(round3Files)
		newData, maxVersionMap, err = s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		expectedNewData3(newData)
		expectedMaxVersionMap3(maxVersionMap)
	})
	t.Run("checkpoint with 1 item", func(t *testing.T) {
		t.Parallel()
		checkpoint := recorder.NewCheckpoint()
		checkpoint.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"clusterX": round1TimeWindowData,
		})
		s3Storage := NewMockS3Storage(round1Files)
		s3Consumer := NewS3Consumer(s3Storage, map[string][]string{"test": {"t1"}})
		data, err := s3Consumer.InitializeFromCheckpoint(ctx, "clusterX", checkpoint)
		require.NoError(t, err)
		require.Empty(t, data)
		s3Storage.UpdateFiles(round2Files)
		newData, maxVersionMap, err := s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		expectedNewData2(newData)
		expectedMaxVersionMap2(maxVersionMap)
		s3Storage.UpdateFiles(round3Files)
		newData, maxVersionMap, err = s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		expectedNewData3(newData)
		expectedMaxVersionMap3(maxVersionMap)
	})
	t.Run("checkpoint with 2 items", func(t *testing.T) {
		t.Parallel()
		checkpoint := recorder.NewCheckpoint()
		checkpoint.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"clusterX": round1TimeWindowData,
		})
		checkpoint.NewTimeWindowData(1, map[string]types.TimeWindowData{
			"clusterX": round2TimeWindowData,
		})
		s3Storage := NewMockS3Storage(round2Files)
		s3Consumer := NewS3Consumer(s3Storage, map[string][]string{"test": {"t1"}})
		data, err := s3Consumer.InitializeFromCheckpoint(ctx, "clusterX", checkpoint)
		require.NoError(t, err)
		expectedNewData2(data)
		s3Storage.UpdateFiles(round3Files)
		newData, maxVersionMap, err := s3Consumer.ConsumeNewFiles(ctx)
		require.NoError(t, err)
		expectedNewData3(newData)
		expectedMaxVersionMap3(maxVersionMap)
	})
	t.Run("checkpoint with 3 items", func(t *testing.T) {
		t.Parallel()
		checkpoint := recorder.NewCheckpoint()
		checkpoint.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"clusterX": round1TimeWindowData,
		})
		checkpoint.NewTimeWindowData(1, map[string]types.TimeWindowData{
			"clusterX": round2TimeWindowData,
		})
		checkpoint.NewTimeWindowData(2, map[string]types.TimeWindowData{
			"clusterX": round3TimeWindowData,
		})
		s3Storage := NewMockS3Storage(round3Files)
		s3Consumer := NewS3Consumer(s3Storage, map[string][]string{"test": {"t1"}})
		data, err := s3Consumer.InitializeFromCheckpoint(ctx, "clusterX", checkpoint)
		require.NoError(t, err)
		expectedCheckpoint23(data)
	})
}
