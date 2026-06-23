// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestSchemaPathKey(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path      string
		schemakey SchemaPathKey
		checksum  uint32
	}{
		// Test for database schema path: <schema>/meta/schema_{tableVersion}_{checksum}.json
		{
			path: "test_schema/meta/schema_1_2.json",
			schemakey: SchemaPathKey{
				Schema:       "test_schema",
				Table:        "",
				TableVersion: 1,
			},
			checksum: 2,
		},
		// Test for table schema path: <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json
		{
			path: "test_schema/test_table/meta/schema_11_22.json",
			schemakey: SchemaPathKey{
				Schema:       "test_schema",
				Table:        "test_table",
				TableVersion: 11,
			},
			checksum: 22,
		},
	}
	for _, tc := range testCases {
		var schemaKey SchemaPathKey
		checksum, err := schemaKey.ParseSchemaFilePath(tc.path)
		require.NoError(t, err)
		require.Equal(t, tc.schemakey, schemaKey)
		require.Equal(t, tc.checksum, checksum)
	}
}

func TestDmlPathKey(t *testing.T) {
	t.Parallel()

	dispatcherID := common.NewDispatcherID()
	testCases := []struct {
		index          uint64
		fileIndexWidth int
		extension      string
		path           string
		indexPath      string
		dmlkey         DmlPathKey
	}{
		{
			index:          10,
			fileIndexWidth: 20,
			extension:      ".csv",
			path:           fmt.Sprintf("schema1/table1/123456/2023-05-09/CDC_%s_00000000000000000010.csv", dispatcherID.String()),
			indexPath:      fmt.Sprintf("schema1/table1/123456/2023-05-09/meta/CDC_%s.index", dispatcherID.String()),
			dmlkey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
				PartitionNum: 0,
				Date:         "2023-05-09",
			},
		},
	}

	for _, tc := range testCases {
		var dmlkey DmlPathKey
		id, err := dmlkey.ParseIndexFilePath("day", tc.indexPath)
		require.NoError(t, err)
		require.Equal(t, tc.dmlkey, dmlkey)
		require.Equal(t, id, dispatcherID.String())

		fileIndex := &FileIndex{
			FileIndexKey: FileIndexKey{
				DispatcherID:           id,
				EnableTableAcrossNodes: id != "",
			},
			Idx: tc.index,
		}
		fileName := dmlkey.GenerateDMLFilePath(fileIndex, tc.extension, tc.fileIndexWidth)
		require.Equal(t, tc.path, fileName)
	}
}

func TestParseDMLFilePath(t *testing.T) {
	t.Parallel()

	dispatcherID := common.NewDispatcherID().String()
	testCases := []struct {
		name           string
		dateSeparator  string
		path           string
		fileIndexWidth int
		dmlkey         DmlPathKey
		fileIndex      FileIndex
	}{
		{
			name:           "no date no partition",
			dateSeparator:  "none",
			path:           "schema1/table1/123456/CDC000010.csv",
			fileIndexWidth: 6,
			dmlkey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
			},
			fileIndex: FileIndex{Idx: 10},
		},
		{
			name:           "no date with partition",
			dateSeparator:  "none",
			path:           "schema1/table1/123456/55/CDC000010.csv",
			fileIndexWidth: 6,
			dmlkey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
				PartitionNum: 55,
			},
			fileIndex: FileIndex{Idx: 10},
		},
		{
			name:           "no date with table id path",
			dateSeparator:  "none",
			path:           "12345/123456/CDC000010.csv",
			fileIndexWidth: 6,
			dmlkey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "12345",
					TableVersion: 123456,
				},
				UseTableIDAsPath: true,
				TableID:          12345,
			},
			fileIndex: FileIndex{Idx: 10},
		},
		{
			name:           "day date no partition",
			dateSeparator:  "day",
			path:           fmt.Sprintf("schema1/table1/123456/2023-05-09/CDC_%s_00000000000000000010.csv", dispatcherID),
			fileIndexWidth: 20,
			dmlkey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
				Date: "2023-05-09",
			},
			fileIndex: FileIndex{
				FileIndexKey: FileIndexKey{
					DispatcherID:           dispatcherID,
					EnableTableAcrossNodes: true,
				},
				Idx: 10,
			},
		},
		{
			name:           "day date with table id path",
			dateSeparator:  "day",
			path:           fmt.Sprintf("12345/123456/2023-05-09/CDC_%s_00000000000000000010.csv", dispatcherID),
			fileIndexWidth: 20,
			dmlkey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "12345",
					TableVersion: 123456,
				},
				UseTableIDAsPath: true,
				TableID:          12345,
				Date:             "2023-05-09",
			},
			fileIndex: FileIndex{
				FileIndexKey: FileIndexKey{
					DispatcherID:           dispatcherID,
					EnableTableAcrossNodes: true,
				},
				Idx: 10,
			},
		},
		{
			name:           "day date with partition",
			dateSeparator:  "day",
			path:           fmt.Sprintf("schema1/table1/123456/55/2023-05-09/CDC_%s_00000000000000000010.csv", dispatcherID),
			fileIndexWidth: 20,
			dmlkey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
				PartitionNum: 55,
				Date:         "2023-05-09",
			},
			fileIndex: FileIndex{
				FileIndexKey: FileIndexKey{
					DispatcherID:           dispatcherID,
					EnableTableAcrossNodes: true,
				},
				Idx: 10,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var dmlkey DmlPathKey
			fileIndex, err := dmlkey.ParseDMLFilePath(tc.dateSeparator, tc.path, ".csv")
			require.NoError(t, err)
			require.Equal(t, tc.dmlkey, dmlkey)
			require.Equal(t, tc.fileIndex, fileIndex)
			require.Equal(t, tc.path, dmlkey.GenerateDMLFilePath(&fileIndex, ".csv", tc.fileIndexWidth))
		})
	}
}

func TestParseDMLFilePathRejectsInvalidPath(t *testing.T) {
	t.Parallel()

	var dmlkey DmlPathKey
	_, err := dmlkey.ParseDMLFilePath("none", "schema1//123456/CDC000010.csv", ".csv")
	require.Error(t, err)
}

func TestParseIndexFilePathWithTableIDAsPath(t *testing.T) {
	t.Parallel()

	dispatcherID := common.NewDispatcherID().String()
	indexPath := fmt.Sprintf("12345/123456/2023-05-09/meta/CDC_%s.index", dispatcherID)

	var dmlkey DmlPathKey
	id, err := dmlkey.ParseIndexFilePath("day", indexPath)
	require.NoError(t, err)
	require.Equal(t, dispatcherID, id)
	require.Equal(t, DmlPathKey{
		SchemaPathKey: SchemaPathKey{
			Schema:       "12345",
			TableVersion: 123456,
		},
		UseTableIDAsPath: true,
		TableID:          12345,
		Date:             "2023-05-09",
	}, dmlkey)
}

func TestSchemaFileDMLPathKeyOrder(t *testing.T) {
	t.Parallel()

	schemaKey := SchemaPathKey{
		Schema:       "schema1",
		Table:        "table1",
		TableVersion: 123456,
	}
	schemaDMLKey := NewSchemaFileDMLPathKey(schemaKey)
	require.True(t, schemaDMLKey.IsSchemaFileDMLPathKey())

	dataDMLKey := DmlPathKey{
		SchemaPathKey: schemaKey,
		Date:          "2023-05-09",
	}
	require.Less(t, CompareDMLPathKey(schemaDMLKey, dataDMLKey), 0)
	require.Greater(t, CompareDMLPathKey(dataDMLKey, schemaDMLKey), 0)
	require.Zero(t, CompareDMLPathKey(schemaDMLKey, NewSchemaFileDMLPathKey(schemaKey)))

	tableIDPathKey := DmlPathKey{
		SchemaPathKey: SchemaPathKey{
			Schema:       "12345",
			TableVersion: schemaKey.TableVersion,
		},
		UseTableIDAsPath: true,
		TableID:          12345,
	}
	require.NotZero(t, CompareDMLPathKey(dataDMLKey, tableIDPathKey))
}
