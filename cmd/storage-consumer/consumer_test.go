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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"path"
	"testing"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestParseSchemaFilePathDispatcherScope(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		order   []int
		wantDDL bool
	}{
		{name: "ddlFirst", order: []int{0, 1, 2}, wantDDL: true},
		{name: "ddlLast", order: []int{1, 2, 0}, wantDDL: true},
		{name: "ddlBetweenDispatchers", order: []int{1, 0, 2}, wantDDL: true},
		{name: "dispatchersOnly", order: []int{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, "file://"+t.TempDir())
			require.NoError(t, err)
			t.Cleanup(storage.Close)
			c := &consumer{
				externalStorage: storage,
				schemaFileMap:   make(map[string]map[uint64]*cloudstorage.SchemaFile),
				tableDMLIdxMap:  make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
			}
			schema := cloudstorage.SchemaFile{
				Schema: "test", Table: "table1", TableVersion: 100,
				Columns: []cloudstorage.TableCol{{Name: "id", Tp: "INT"}}, TotalColumns: 1,
			}
			paths := make([]string, 3)
			for i, dispatcherID := range []string{"", "12345", "67890"} {
				schema.DispatcherID = dispatcherID
				schema.Query = ""
				if i == 0 {
					schema.Query = "CREATE TABLE test.table1 (id INT)"
				}
				paths[i] = schema.Path(false, 0)
				require.NoError(t, storage.WriteFile(ctx, paths[i], schema.Marshal()))
			}
			for _, i := range tc.order {
				c.parseSchemaFilePath(ctx, paths[i])
			}
			key := cloudstorage.SchemaPathKey{Schema: "test", Table: "table1", TableVersion: 100}
			got := c.mustGetSchemaFile(key)
			if tc.wantDDL {
				require.Empty(t, got.DispatcherID)
				require.Equal(t, "CREATE TABLE test.table1 (id INT)", got.Query)
			} else {
				require.Equal(t, "12345", got.DispatcherID)
				require.Empty(t, got.Query)
			}
			require.Len(t, c.schemaFileMap[key.GetKey()], 1)
			require.Len(t, c.tableDMLIdxMap, 1)
		})
	}
}

func TestGetNewFilesDispatcherSchemas(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, "file://"+t.TempDir())
	require.NoError(t, err)
	t.Cleanup(storage.Close)
	c := &consumer{
		externalStorage:    storage,
		dateSeparator:      config.DateSeparatorNone,
		fileExtension:      ".json",
		globalCheckpointTs: 300,
		schemaFileMap:      make(map[string]map[uint64]*cloudstorage.SchemaFile),
		tableDMLIdxMap:     make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
	}
	schema := cloudstorage.SchemaFile{
		Schema: "test", Table: "table1", TableVersion: 50,
		Columns: []cloudstorage.TableCol{{Name: "id", Tp: "INT"}}, TotalColumns: 1,
		Query: "CREATE TABLE test.table1 (id INT)",
	}
	// Legacy/DDL files and dispatcher-scoped files must be discovered together.
	require.NoError(t, storage.WriteFile(ctx, schema.Path(false, 0), schema.Marshal()))
	schema.Query = ""
	for i, dispatcherID := range []string{"12345", "67890"} {
		schema.DispatcherID = dispatcherID
		schema.TableVersion = uint64(100 + i*100)
		require.NoError(t, storage.WriteFile(ctx, schema.Path(false, 0), schema.Marshal()))
		key := cloudstorage.DMLPathKey{SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema: "test", Table: "table1", TableVersion: schema.TableVersion,
		}}
		index := &cloudstorage.FileIndex{FileIndexKey: cloudstorage.FileIndexKey{
			DispatcherID: dispatcherID, EnableTableAcrossNodes: true,
		}, Idx: 1}
		filePath := key.GenerateDMLFilePath(index, ".json", 6)
		require.NoError(t, storage.WriteFile(ctx, filePath, fmt.Appendf(nil, `{"id":%d}`, i+1)))
		require.NoError(t, storage.WriteFile(ctx, key.GenerateIndexFilePath(index.FileIndexKey), []byte(path.Base(filePath)+"\n")))
	}
	files, err := c.getNewFiles(ctx)
	require.NoError(t, err)
	require.Len(t, files, 5) // Three schema markers and two dispatcher data streams.
	require.Len(t, c.schemaFileMap[commonType.QuoteSchema("test", "table1")], 3)
	for i, dispatcherID := range []string{"12345", "67890"} {
		key := cloudstorage.DMLPathKey{SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema: "test", Table: "table1", TableVersion: uint64(100 + i*100),
		}}
		fileIndexKey := cloudstorage.FileIndexKey{DispatcherID: dispatcherID, EnableTableAcrossNodes: true}
		require.Equal(t, indexRange{start: 1, end: 1}, files[key][fileIndexKey])
	}
}

func TestGetNewFilesDiscoversDDLAfterDispatcherSchema(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, "file://"+t.TempDir())
	require.NoError(t, err)
	t.Cleanup(storage.Close)
	c := &consumer{
		externalStorage: storage,
		schemaFileMap:   make(map[string]map[uint64]*cloudstorage.SchemaFile),
		tableDMLIdxMap:  make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
	}
	schema := cloudstorage.SchemaFile{
		Schema: "test", Table: "table1", TableVersion: 100, DispatcherID: "12345",
		Columns: []cloudstorage.TableCol{{Name: "id", Tp: "INT"}}, TotalColumns: 1,
	}
	require.NoError(t, storage.WriteFile(ctx, schema.Path(false, 0), schema.Marshal()))
	files, err := c.getNewFiles(ctx)
	require.NoError(t, err)
	require.Len(t, files, 1)

	schema.DispatcherID = ""
	schema.Query = "CREATE TABLE test.table1 (id INT)"
	require.NoError(t, storage.WriteFile(ctx, schema.Path(false, 0), schema.Marshal()))
	files, err = c.getNewFiles(ctx)
	require.NoError(t, err)
	schemaKey := cloudstorage.SchemaPathKey{Schema: "test", Table: "table1", TableVersion: 100}
	require.Contains(t, files, cloudstorage.NewSchemaFileDMLPathKey(schemaKey))
	require.Equal(t, schema.Query, c.mustGetSchemaFile(schemaKey).Query)

	files, err = c.getNewFiles(ctx)
	require.NoError(t, err)
	require.Empty(t, files)
}
