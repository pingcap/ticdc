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

package main

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBuildIcebergDDLEventsPreservesOriginalStringTypes(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()
	extStorage, warehouseURL, err := util.GetTestExtStorage(ctx, tmpDir)
	require.NoError(t, err)
	defer extStorage.Close()

	cfg := sinkiceberg.NewConfig()
	cfg.WarehouseURI = warehouseURL.String()
	cfg.WarehouseLocation = warehouseURL.String()
	cfg.Namespace = "ns"

	tableWriter := sinkiceberg.NewTableWriter(cfg, extStorage)
	changefeedID := common.NewChangefeedID4Test("default", "cf")

	textType := types.NewFieldType(mysql.TypeBlob)
	textType.SetCharset(charset.CharsetUTF8MB4)

	varcharType := types.NewFieldType(mysql.TypeVarchar)
	varcharType.SetCharset(charset.CharsetUTF8MB4)
	varcharType.SetFlen(32)

	idType := types.NewFieldType(mysql.TypeLonglong)
	idType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)

	tableInfoV1 := common.WrapTableInfo("test", &model.TableInfo{
		ID:         20,
		Name:       ast.NewCIStr("t_iceberg"),
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), FieldType: *idType},
			{ID: 2, Name: ast.NewCIStr("v"), FieldType: *textType},
			{ID: 3, Name: ast.NewCIStr("score"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	})
	require.NoError(t, tableWriter.EnsureTable(ctx, changefeedID, tableInfoV1))

	tableInfoV2 := common.WrapTableInfo("test", &model.TableInfo{
		ID:         20,
		Name:       ast.NewCIStr("t_iceberg"),
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), FieldType: *idType},
			{ID: 2, Name: ast.NewCIStr("v"), FieldType: *textType},
			{ID: 3, Name: ast.NewCIStr("score"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 4, Name: ast.NewCIStr("extra"), FieldType: *varcharType},
		},
	})
	require.NoError(t, tableWriter.EnsureTable(ctx, changefeedID, tableInfoV2))

	version1, err := sinkiceberg.LoadTableVersion(ctx, cfg, extStorage, "test", "t_iceberg", 1)
	require.NoError(t, err)
	version2, err := sinkiceberg.LoadTableVersion(ctx, cfg, extStorage, "test", "t_iceberg", 2)
	require.NoError(t, err)

	createEvents, err := buildIcebergDDLEvents(nil, version1)
	require.NoError(t, err)
	require.Len(t, createEvents, 2)
	require.Contains(t, createEvents[1].Query, "`v` TEXT NULL")
	require.NotContains(t, createEvents[1].Query, "`v` LONGBLOB NULL")
	require.Contains(t, createEvents[1].Query, "PRIMARY KEY (`id`)")

	alterEvents, err := buildIcebergDDLEvents(version1, version2)
	require.NoError(t, err)
	require.Len(t, alterEvents, 1)
	require.Equal(t,
		"ALTER TABLE `test`.`t_iceberg` ADD COLUMN `extra` VARCHAR(32) NULL",
		alterEvents[0].Query,
	)
}
