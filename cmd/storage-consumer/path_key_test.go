// Copyright 2026 PingCAP, Inc.
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

package main

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
		schemaKey SchemaPathKey
		checksum  uint32
	}{
		{
			path: "test_schema/meta/schema_1_0000000002.json",
			schemaKey: SchemaPathKey{
				Schema:       "test_schema",
				Table:        "",
				TableVersion: 1,
			},
			checksum: 2,
		},
		{
			path: "test_schema/test_table/meta/schema_11_0000000022.json",
			schemaKey: SchemaPathKey{
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
		require.Equal(t, tc.schemaKey, schemaKey)
		require.Equal(t, tc.checksum, checksum)
	}
}

func TestDmlPathKey(t *testing.T) {
	t.Parallel()

	dispatcherID := common.NewDispatcherID()
	testCases := []struct {
		name           string
		dateSeparator  string
		dispatcherID   string
		index          uint64
		fileIndexWidth int
		extension      string
		path           string
		indexPath      string
		dmlKey         DmlPathKey
	}{
		{
			name:           "day-with-dispatcher",
			dateSeparator:  "day",
			dispatcherID:   dispatcherID.String(),
			index:          10,
			fileIndexWidth: 20,
			extension:      ".csv",
			path:           fmt.Sprintf("schema1/table1/123456/2023-05-09/CDC_%s_00000000000000000010.csv", dispatcherID.String()),
			indexPath:      fmt.Sprintf("schema1/table1/123456/2023-05-09/meta/CDC_%s.index", dispatcherID.String()),
			dmlKey: DmlPathKey{
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
			name:           "none-without-dispatcher",
			dateSeparator:  "none",
			dispatcherID:   "",
			index:          12,
			fileIndexWidth: 6,
			extension:      ".json",
			path:           "schema1/table1/123456/CDC000012.json",
			indexPath:      "schema1/table1/123456/meta/CDC.index",
			dmlKey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema1",
					Table:        "table1",
					TableVersion: 123456,
				},
				PartitionNum: 0,
				Date:         "",
			},
		},
		{
			name:           "none-with-partition-and-hyphen-dispatcher",
			dateSeparator:  "none",
			dispatcherID:   "dispatcher-1",
			index:          3,
			fileIndexWidth: 6,
			extension:      ".json",
			path:           "schema2/table2/88/6/CDC_dispatcher-1_000003.json",
			indexPath:      "schema2/table2/88/6/meta/CDC_dispatcher-1.index",
			dmlKey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema2",
					Table:        "table2",
					TableVersion: 88,
				},
				PartitionNum: 6,
				Date:         "",
			},
		},
		{
			name:           "year-with-partition",
			dateSeparator:  "year",
			dispatcherID:   "",
			index:          9,
			fileIndexWidth: 6,
			extension:      ".json",
			path:           "schema3/table3/66/4/2025/CDC000009.json",
			indexPath:      "schema3/table3/66/4/2025/meta/CDC.index",
			dmlKey: DmlPathKey{
				SchemaPathKey: SchemaPathKey{
					Schema:       "schema3",
					Table:        "table3",
					TableVersion: 66,
				},
				PartitionNum: 4,
				Date:         "2025",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var dmlKey DmlPathKey
			id, err := dmlKey.ParseIndexFilePath(tc.dateSeparator, tc.indexPath)
			require.NoError(t, err)
			require.Equal(t, tc.dmlKey, dmlKey)
			require.Equal(t, tc.dispatcherID, id)

			fileIndex := &FileIndex{
				FileIndexKey: FileIndexKey{
					DispatcherID:           id,
					EnableTableAcrossNodes: id != "",
				},
				Idx: tc.index,
			}
			fileName := dmlKey.GenerateDMLFilePath(fileIndex, tc.extension, tc.fileIndexWidth)
			require.Equal(t, tc.path, fileName)
		})
	}
}

func TestDmlPathKeyInvalidPath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		dateSeparator string
		indexPath     string
	}{
		{
			name:          "invalid-date-format",
			dateSeparator: "day",
			indexPath:     "schema1/table1/123/2025-02/meta/CDC.index",
		},
		{
			name:          "invalid-index-file-name",
			dateSeparator: "day",
			indexPath:     "schema1/table1/123/2025-02-14/meta/cdc.index",
		},
		{
			name:          "unexpected-extra-segment",
			dateSeparator: "none",
			indexPath:     "schema1/table1/123/1/2025/meta/CDC.index",
		},
	}

	for _, tc := range testCases {
		var dmlKey DmlPathKey
		_, err := dmlKey.ParseIndexFilePath(tc.dateSeparator, tc.indexPath)
		require.Error(t, err, tc.name)
	}
}

func TestFetchIndexFromFileName(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		fileName string
		index    uint64
		wantErr  bool
	}{
		{fileName: "CDC000011.json", index: 11},
		{fileName: "CDC1000000.json", index: 1000000},
		{fileName: "CDC_dispatcher-1_000007.json", index: 7},
		{fileName: "CDC1.json", wantErr: true},
		{fileName: "cdc000001.json", wantErr: true},
		{fileName: "CDC000005.xxx", wantErr: true},
		{fileName: "CDChello.json", wantErr: true},
		{fileName: "CDC_dispatcher_1.json", wantErr: true},
	}

	for _, tc := range testCases {
		index, err := FetchIndexFromFileName(tc.fileName, ".json")
		if tc.wantErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tc.index, index)
	}
}

func TestIsSchemaFile(t *testing.T) {
	t.Parallel()

	require.True(t, IsSchemaFile("test/table/meta/schema_12_0000000123.json"))
	require.False(t, IsSchemaFile("test/table/meta/CDC000001.json"))
}
