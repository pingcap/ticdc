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
	"strings"
	"testing"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestParseSchemaFilePathChecksChecksum(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, fmt.Sprintf("file:///%s", t.TempDir()))
	require.NoError(t, err)
	defer storage.Close()

	schemaFile := cloudstorage.SchemaFile{
		Schema:       "schema1",
		Table:        "table1",
		Version:      1,
		TableVersion: 100,
		Columns: []cloudstorage.TableCol{{
			Name: "id",
			Tp:   "INT",
		}},
		TotalColumns: 1,
	}
	path := schemaFile.Path(false, 0)
	require.NoError(t, storage.WriteFile(ctx, path, schemaFile.Marshal()))

	c := &consumer{
		externalStorage: storage,
		schemaFileMap:   make(map[string]map[uint64]*cloudstorage.SchemaFile),
		tableDMLIdxMap:  make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
	}
	require.NotPanics(t, func() {
		require.NoError(t, c.parseSchemaFilePath(ctx, path))
	})

	badPath := path[:strings.LastIndex(path, "_")+1] + "0000000000.json"
	require.NoError(t, storage.WriteFile(ctx, badPath, schemaFile.Marshal()))
	c = &consumer{
		externalStorage: storage,
		schemaFileMap:   make(map[string]map[uint64]*cloudstorage.SchemaFile),
		tableDMLIdxMap:  make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
	}
	require.Panics(t, func() {
		require.NoError(t, c.parseSchemaFilePath(ctx, badPath))
	})
}
