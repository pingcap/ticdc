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

func TestParse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage, err := util.GetExternalStorageWithDefaultTimeout(
		ctx, fmt.Sprintf("file:///%s", t.TempDir()))
	require.NoError(t, err)
	defer storage.Close()

	schemaFile, _ := generateSchemaFile()
	schemaFilePath, err := schemaFile.GenerateSchemaFilePath(false, 0)
	require.NoError(t, err)
	encodedSchemaFile, err := schemaFile.Marshal()
	require.NoError(t, err)
	require.NoError(t, storage.WriteFile(ctx, schemaFilePath, encodedSchemaFile))

	schemaKey, got, err := Parse(ctx, storage, schemaFilePath)
	require.NoError(t, err)
	require.Equal(t, SchemaPathKey{
		Schema:       schemaFile.Schema,
		Table:        schemaFile.Table,
		TableVersion: schemaFile.TableVersion,
	}, schemaKey)
	require.Equal(t, schemaFile.Schema, got.Schema)
	require.Equal(t, schemaFile.Table, got.Table)
	require.Equal(t, schemaFile.Version, got.Version)
	require.Equal(t, schemaFile.TableVersion, got.TableVersion)
	require.Equal(t, schemaFile.TotalColumns, got.TotalColumns)
	require.Len(t, got.Columns, len(schemaFile.Columns))

	expectedChecksum, err := schemaFile.Sum32(nil)
	require.NoError(t, err)
	gotChecksum, err := got.Sum32(nil)
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, gotChecksum)
}

func TestParseChecksumMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage, err := util.GetExternalStorageWithDefaultTimeout(
		ctx, fmt.Sprintf("file:///%s", t.TempDir()))
	require.NoError(t, err)
	defer storage.Close()

	schemaFile, _ := generateSchemaFile()
	schemaFilePath, err := schemaFile.GenerateSchemaFilePath(false, 0)
	require.NoError(t, err)

	schemaFile.TableVersion++
	encodedSchemaFile, err := schemaFile.Marshal()
	require.NoError(t, err)
	require.NoError(t, storage.WriteFile(ctx, schemaFilePath, encodedSchemaFile))

	_, _, err = Parse(ctx, storage, schemaFilePath)
	require.Error(t, err)
	require.True(t, errors.ErrStorageSinkInvalidFileName.Equal(err))
}
