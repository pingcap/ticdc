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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSchemaPathKey(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path      string
		schemakey SchemaPathKey
	}{
		// Test for database schema path: <schema>/meta/schema_{tableVersion}_{checksum}.json
		{
			path: "test_schema/meta/schema_1_2.json",
			schemakey: SchemaPathKey{
				Schema:       "test_schema",
				Table:        "",
				TableVersion: 1,
			},
		},
		// Test for table-level schema file path: <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json
		{
			path: "test_schema/test_table/meta/schema_11_22.json",
			schemakey: SchemaPathKey{
				Schema:       "test_schema",
				Table:        "test_table",
				TableVersion: 11,
			},
		},
	}
	for _, tc := range testCases {
		var schemaKey SchemaPathKey
		schemaKey.Parse(tc.path)
		require.Equal(t, tc.schemakey, schemaKey)
	}
}

func TestGenerateDMLFilePath(t *testing.T) {
	t.Parallel()

	dispatcherID := common.NewDispatcherID()
	testCases := []struct {
		index          uint64
		fileIndexWidth int
		extension      string
		dateSeparator  string
		path           string
		dmlkey         DMLPathKey
	}{
		{
			index:          10,
			fileIndexWidth: 20,
			extension:      ".csv",
			dateSeparator:  config.DateSeparatorDay.String(),
			path:           fmt.Sprintf("schema1/table1/123456/2023-05-09/CDC_%s_00000000000000000010.csv", dispatcherID.String()),
			dmlkey: DMLPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
				PartitionNum: 0,
				Date:         "2023-05-09",
			},
		},
		{
			index:          10,
			fileIndexWidth: 20,
			extension:      ".csv",
			dateSeparator:  config.DateSeparatorNone.String(),
			path:           fmt.Sprintf("12345/123456/CDC_%s_00000000000000000010.csv", dispatcherID.String()),
			dmlkey: DMLPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "12345",
					TableVersion: 123456,
				},
				UseTableIDAsPath: true,
				TableID:          12345,
			},
		},
		{
			index:          10,
			fileIndexWidth: 20,
			extension:      ".csv",
			dateSeparator:  config.DateSeparatorDay.String(),
			path:           fmt.Sprintf("schema1/table1/123456/55/2023-05-09/CDC_%s_00000000000000000010.csv", dispatcherID.String()),
			dmlkey: DMLPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
				PartitionNum: 55,
				Date:         "2023-05-09",
			},
		},
	}

	for _, tc := range testCases {
		fileIndex := &FileIndex{
			FileIndexKey: FileIndexKey{
				DispatcherID:           dispatcherID.String(),
				EnableTableAcrossNodes: true,
			},
			Idx: tc.index,
		}
		fileName := tc.dmlkey.GenerateDMLFilePath(fileIndex, tc.extension, tc.fileIndexWidth)
		require.Equal(t, tc.path, fileName)
		var pathKey DMLPathKey
		gotFileIndex := pathKey.ParseDMLFilePath(tc.dateSeparator, fileName, tc.extension)
		require.Equal(t, tc.dmlkey, pathKey)
		require.Equal(t, *fileIndex, gotFileIndex)
	}
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

	dataDMLKey := DMLPathKey{
		SchemaPathKey: schemaKey,
		Date:          "2023-05-09",
	}
	require.Less(t, CompareDMLPathKey(schemaDMLKey, dataDMLKey), 0)
	require.Greater(t, CompareDMLPathKey(dataDMLKey, schemaDMLKey), 0)
	require.Zero(t, CompareDMLPathKey(schemaDMLKey, NewSchemaFileDMLPathKey(schemaKey)))

	tableIDPathKey := DMLPathKey{
		SchemaPathKey: SchemaPathKey{
			Schema:       "12345",
			TableVersion: schemaKey.TableVersion,
		},
		UseTableIDAsPath: true,
		TableID:          12345,
	}
	require.NotZero(t, CompareDMLPathKey(dataDMLKey, tableIDPathKey))
}

func TestParseIndexFilePathRejectsUnsupportedPath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		dateSeparator string
		path          string
	}{
		{
			name:          "legacy schema table date index",
			dateSeparator: config.DateSeparatorDay.String(),
			path:          "test/binary_columns_dummy/2026-06-23/meta/CDC.index",
		},
		{
			name:          "invalid index file name",
			dateSeparator: config.DateSeparatorNone.String(),
			path:          "schema1/table1/123456/meta/notCDC.index",
		},
		{
			name:          "date does not match separator",
			dateSeparator: config.DateSeparatorMonth.String(),
			path:          "schema1/table1/123456/2023-05-09/meta/CDC.index",
		},
	}

	for _, tc := range testCases {
		var pathKey DMLPathKey
		require.Error(t, pathKey.ParseIndexFilePath(tc.dateSeparator, tc.path), tc.name)
	}
}
