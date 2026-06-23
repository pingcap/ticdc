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
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestReadTableDefinitionFromSchemaFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage, err := util.GetExternalStorageWithDefaultTimeout(
		ctx, fmt.Sprintf("file:///%s", t.TempDir()))
	require.NoError(t, err)
	defer storage.Close()

	def, _ := generateTableDef()
	schemaFilePath, err := def.GenerateSchemaFilePath(false, 0)
	require.NoError(t, err)
	encodedDef, err := def.MarshalWithQuery()
	require.NoError(t, err)
	require.NoError(t, storage.WriteFile(ctx, schemaFilePath, encodedDef))

	schemaKey, got, err := ReadTableDefinitionFromSchemaFile(ctx, storage, schemaFilePath)
	require.NoError(t, err)
	require.Equal(t, SchemaPathKey{
		Schema:       def.Schema,
		Table:        def.Table,
		TableVersion: def.TableVersion,
	}, schemaKey)
	require.Equal(t, def.Schema, got.Schema)
	require.Equal(t, def.Table, got.Table)
	require.Equal(t, def.Version, got.Version)
	require.Equal(t, def.TableVersion, got.TableVersion)
	require.Equal(t, def.TotalColumns, got.TotalColumns)
	require.Len(t, got.Columns, len(def.Columns))

	expectedChecksum, err := def.Sum32(nil)
	require.NoError(t, err)
	gotChecksum, err := got.Sum32(nil)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, gotChecksum)
}

func TestReadTableDefinitionFromSchemaFileChecksumMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage, err := util.GetExternalStorageWithDefaultTimeout(
		ctx, fmt.Sprintf("file:///%s", t.TempDir()))
	require.NoError(t, err)
	defer storage.Close()

	def, _ := generateTableDef()
	schemaFilePath, err := def.GenerateSchemaFilePath(false, 0)
	require.NoError(t, err)

	def.TableVersion++
	encodedDef, err := def.MarshalWithQuery()
	require.NoError(t, err)
	require.NoError(t, storage.WriteFile(ctx, schemaFilePath, encodedDef))

	_, _, err = ReadTableDefinitionFromSchemaFile(ctx, storage, schemaFilePath)
	require.Error(t, err)
	require.True(t, errors.ErrStorageSinkInvalidFileName.Equal(err))
}
