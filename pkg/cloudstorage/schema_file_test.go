// Copyright 2022 PingCAP, Inc.
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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func generateSchemaFile() (SchemaFile, *common.TableInfo) {
	var columns []*timodel.ColumnInfo
	ft := types.NewFieldType(mysql.TypeLong)
	ft.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	col := &timodel.ColumnInfo{
		Name:         ast.NewCIStr("Id"),
		FieldType:    *ft,
		DefaultValue: 10,
	}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlag(mysql.NotNullFlag)
	ft.SetFlen(128)
	col = &timodel.ColumnInfo{
		Name:         ast.NewCIStr("LastName"),
		FieldType:    *ft,
		DefaultValue: "Default LastName",
	}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(64)
	col = &timodel.ColumnInfo{
		Name:         ast.NewCIStr("FirstName"),
		FieldType:    *ft,
		DefaultValue: "Default FirstName",
	}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeDatetime)
	col = &timodel.ColumnInfo{
		Name:         ast.NewCIStr("Birthday"),
		FieldType:    *ft,
		DefaultValue: 12345678,
	}
	columns = append(columns, col)

	tableInfo := common.WrapTableInfo("schema1", &timodel.TableInfo{
		ID:       20,
		Name:     ast.NewCIStr("table1"),
		Columns:  columns,
		UpdateTS: 100,
	})
	event := &commonEvent.DDLEvent{
		SchemaName: tableInfo.GetSchemaName(),
		TableName:  tableInfo.GetTableName(),
		TableInfo:  tableInfo,
		FinishedTs: tableInfo.GetUpdateTS(),
	}
	var schemaFile SchemaFile
	schemaFile.Build(event, false)
	return schemaFile, tableInfo
}

func TestBuildUsesTargetNames(t *testing.T) {
	t.Parallel()

	idFieldType := types.NewFieldType(mysql.TypeLong)
	idFieldType.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	routedTableInfo := common.WrapTableInfo("source_db", &timodel.TableInfo{
		ID:       20,
		Name:     ast.NewCIStr("source_table"),
		UpdateTS: 100,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *idFieldType,
				State:     timodel.StatePublic,
			},
		},
	}).CloneWithRouting("target_db", "target_table")
	sourceDDL := &commonEvent.DDLEvent{
		Version:    commonEvent.DDLEventVersion1,
		Type:       byte(timodel.ActionCreateTable),
		SchemaName: "source_db",
		TableName:  "source_table",
		Query:      "CREATE TABLE `source_db`.`source_table` (`id` INT PRIMARY KEY)",
		TableInfo:  routedTableInfo,
		FinishedTs: 100,
	}

	routedDDL := commonEvent.NewRoutedDDLEvent(
		sourceDDL,
		"CREATE TABLE `target_db`.`target_table` (`id` INT PRIMARY KEY)",
		"target_db",
		"target_table",
		"",
		"",
		routedTableInfo,
		nil,
		nil,
	)

	var schemaFile SchemaFile
	schemaFile.Build(routedDDL, false)
	require.Equal(t, "target_db", schemaFile.Schema)
	require.Equal(t, "target_table", schemaFile.Table)
	require.Contains(t, schemaFile.Query, "`target_db`.`target_table`")
}

func TestTableCol(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		filedType byte
		flen      int
		decimal   int
		flag      uint
		charset   string
		expected  string
	}{
		{
			name:      "time",
			filedType: mysql.TypeDuration,
			flen:      math.MinInt,
			decimal:   5,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"TIME","ColumnScale":"5"}`,
		},
		{
			name:      "int(5) UNSIGNED",
			filedType: mysql.TypeLong,
			flen:      5,
			decimal:   math.MinInt,
			flag:      mysql.UnsignedFlag,
			expected:  `{"ColumnName":"","ColumnType":"INT UNSIGNED","ColumnPrecision":"5"}`,
		},
		{
			name:      "float(12,3)",
			filedType: mysql.TypeFloat,
			flen:      12,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"12","ColumnScale":"3"}`,
		},
		{
			name:      "float",
			filedType: mysql.TypeFloat,
			flen:      12,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"12"}`,
		},
		{
			name:      "float",
			filedType: mysql.TypeFloat,
			flen:      5,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"5"}`,
		},
		{
			name:      "float(7,3)",
			filedType: mysql.TypeFloat,
			flen:      7,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"7","ColumnScale":"3"}`,
		},
		{
			name:      "double(12,3)",
			filedType: mysql.TypeDouble,
			flen:      12,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"12","ColumnScale":"3"}`,
		},
		{
			name:      "double",
			filedType: mysql.TypeDouble,
			flen:      12,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"12"}`,
		},
		{
			name:      "double",
			filedType: mysql.TypeDouble,
			flen:      5,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"5"}`,
		},
		{
			name:      "double(7,3)",
			filedType: mysql.TypeDouble,
			flen:      7,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"7","ColumnScale":"3"}`,
		},
		{
			name:      "tinyint(5)",
			filedType: mysql.TypeTiny,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"TINYINT","ColumnPrecision":"5"}`,
		},
		{
			name:      "smallint(5)",
			filedType: mysql.TypeShort,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"SMALLINT","ColumnPrecision":"5"}`,
		},
		{
			name:      "mediumint(10)",
			filedType: mysql.TypeInt24,
			flen:      10,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"MEDIUMINT","ColumnPrecision":"10"}`,
		},
		{
			name:      "int(11)",
			filedType: mysql.TypeLong,
			flen:      math.MinInt,
			decimal:   math.MinInt,
			flag:      mysql.PriKeyFlag,
			expected:  `{"ColumnIsPk":"true", "ColumnName":"", "ColumnPrecision":"11", "ColumnType":"INT"}`,
		},
		{
			name:      "bigint(20)",
			filedType: mysql.TypeLonglong,
			flen:      20,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"BIGINT","ColumnPrecision":"20"}`,
		},
		{
			name:      "bit(5)",
			filedType: mysql.TypeBit,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"BIT","ColumnPrecision":"5"}`,
		},
		{
			name:      "varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			filedType: mysql.TypeVarchar,
			flen:      128,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"VARCHAR","ColumnPrecision":"128"}`,
		},
		{
			name:      "char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			filedType: mysql.TypeString,
			flen:      32,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"CHAR","ColumnPrecision":"32"}`,
		},
		{
			name:      "var_string(64)",
			filedType: mysql.TypeVarString,
			flen:      64,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"VAR_STRING","ColumnPrecision":"64"}`,
		},
		{
			name:      "blob",
			filedType: mysql.TypeBlob,
			flen:      100,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"BLOB","ColumnPrecision":"100"}`,
		},
		{
			name:      "text",
			filedType: mysql.TypeBlob,
			flen:      100,
			decimal:   math.MinInt,
			flag:      0,
			charset:   charset.CharsetUTF8MB4,
			expected:  `{"ColumnName":"","ColumnType":"TEXT","ColumnPrecision":"100"}`,
		},
		{
			name:      "tinyblob",
			filedType: mysql.TypeTinyBlob,
			flen:      120,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"TINYBLOB","ColumnPrecision":"120"}`,
		},
		{
			name:      "mediumblob",
			filedType: mysql.TypeMediumBlob,
			flen:      100,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"MEDIUMBLOB","ColumnPrecision":"100"}`,
		},
		{
			name:      "longblob",
			filedType: mysql.TypeLongBlob,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"LONGBLOB","ColumnPrecision":"5"}`,
		},
		{
			name:      "enum",
			filedType: mysql.TypeEnum,
			expected:  `{"ColumnName":"","ColumnType":"ENUM"}`,
		},
		{
			name:      "set",
			filedType: mysql.TypeSet,
			expected:  `{"ColumnName":"","ColumnType":"SET"}`,
		},
		{
			name:      "timestamp(2)",
			filedType: mysql.TypeTimestamp,
			flen:      8,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"TIMESTAMP","ColumnScale":"2"}`,
		},
		{
			name:      "timestamp",
			filedType: mysql.TypeTimestamp,
			flen:      8,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"TIMESTAMP"}`,
		},
		{
			name:      "datetime(2)",
			filedType: mysql.TypeDatetime,
			flen:      8,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"DATETIME","ColumnScale":"2"}`,
		},
		{
			name:      "datetime",
			filedType: mysql.TypeDatetime,
			flen:      8,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"DATETIME"}`,
		},
		{
			name:      "date",
			filedType: mysql.TypeDate,
			flen:      8,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"DATE"}`,
		},
		{
			name:      "date",
			filedType: mysql.TypeDate,
			flen:      8,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"DATE"}`,
		},
		{
			name:      "year(4)",
			filedType: mysql.TypeYear,
			flen:      4,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"YEAR","ColumnPrecision":"4"}`,
		},
		{
			name:      "year(2)",
			filedType: mysql.TypeYear,
			flen:      2,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"YEAR","ColumnPrecision":"2"}`,
		},
	}

	for _, tc := range testCases {
		ft := types.NewFieldType(tc.filedType)
		if tc.flen != math.MinInt {
			ft.SetFlen(tc.flen)
		}
		if tc.decimal != math.MinInt {
			ft.SetDecimal(tc.decimal)
		}
		if tc.flag != 0 {
			ft.SetFlag(tc.flag)
		}
		if len(tc.charset) != 0 {
			ft.SetCharset(tc.charset)
		}
		col := &timodel.ColumnInfo{FieldType: *ft}
		var tableCol TableCol
		tableCol.FromTiColumnInfo(col, false)
		encodedCol, err := json.Marshal(tableCol)
		require.Nil(t, err, tc.name)
		require.JSONEq(t, tc.expected, string(encodedCol), tc.name)

		_, err = tableCol.ToTiColumnInfo(100)
		require.NoError(t, err)
	}
}

func TestSchemaFile(t *testing.T) {
	t.Parallel()

	schemaFile, tableInfo := generateSchemaFile()
	encodedSchemaFile, err := json.MarshalIndent(schemaFile, "", "    ")
	require.NoError(t, err)
	require.JSONEq(t, `{
		"Table": "table1",
		"Schema": "schema1",
		"Version": 1,
		"TableVersion": 100,
		"Query": "",
		"Type": 0,
		"TableColumns": [
			{
				"ColumnName": "Id",
				"ColumnType": "INT",
				"ColumnPrecision": "11",
				"ColumnDefault":10,
				"ColumnNullable": "false",
				"ColumnIsPk": "true"
			},
			{
				"ColumnName": "LastName",
				"ColumnType": "VARCHAR",
				"ColumnDefault":"Default LastName",
				"ColumnPrecision": "128",
				"ColumnNullable": "false"
			},
			{
				"ColumnName": "FirstName",
				"ColumnDefault":"Default FirstName",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "64"
			},
			{
				"ColumnName": "Birthday",
				"ColumnDefault":1.2345678e+07,
				"ColumnType": "DATETIME"
			}
		],
		"TableColumnsTotal": 4
	}`, string(encodedSchemaFile))

	schemaFile = SchemaFile{}
	event := &commonEvent.DDLEvent{
		FinishedTs: tableInfo.GetUpdateTS(),
		Type:       byte(timodel.ActionAddColumn),
		Query:      "alter table schema1.table1 add Birthday date",
		TableInfo:  tableInfo,
		SchemaName: "schema1",
		TableName:  "table1",
	}
	schemaFile.Build(event, false)
	encodedSchemaFile, err = json.MarshalIndent(schemaFile, "", "    ")
	require.NoError(t, err)
	require.JSONEq(t, `{
		"Table": "table1",
		"Schema": "schema1",
		"Version": 1,
		"TableVersion": 100,
		"Query": "alter table schema1.table1 add Birthday date",
		"Type": 5,
		"TableColumns": [
			{
				"ColumnName": "Id",
				"ColumnType": "INT",
				"ColumnPrecision": "11",
				"ColumnDefault":10,
				"ColumnNullable": "false",
				"ColumnIsPk": "true"
			},
			{
				"ColumnName": "LastName",
				"ColumnType": "VARCHAR",
				"ColumnDefault":"Default LastName",
				"ColumnPrecision": "128",
				"ColumnNullable": "false"
			},
			{
				"ColumnName": "FirstName",
				"ColumnDefault":"Default FirstName",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "64"
			},
			{
				"ColumnName": "Birthday",
				"ColumnDefault":1.2345678e+07,
				"ColumnType": "DATETIME"
			}
		],
		"TableColumnsTotal": 4
	}`, string(encodedSchemaFile))

	tableInfo, err = schemaFile.ToTableInfo()
	require.NoError(t, err)
	require.Len(t, tableInfo.GetColumns(), 4)

	event, err = schemaFile.ToDDLEvent()
	require.NoError(t, err)
	require.Equal(t, byte(timodel.ActionAddColumn), event.Type)
	require.Equal(t, uint64(100), event.FinishedTs)
}

func TestSchemaFileGenFilePath(t *testing.T) {
	t.Parallel()

	dbSchemaFile := &SchemaFile{
		Schema:       "schema1",
		Version:      defaultSchemaFileVersion,
		TableVersion: 100,
	}
	schemaPath, err := dbSchemaFile.GenerateSchemaFilePath(false, 0)
	require.NoError(t, err)
	require.Equal(t, "schema1/meta/schema_100_3233644819.json", schemaPath)

	schemaPath, err = dbSchemaFile.GenerateSchemaFilePath(true, 0)
	require.NoError(t, err)
	require.Equal(t, "schema1/meta/schema_100_3233644819.json", schemaPath)

	schemaFile, _ := generateSchemaFile()
	tablePath, err := schemaFile.GenerateSchemaFilePath(false, 0)
	require.NoError(t, err)
	require.Equal(t, "schema1/table1/meta/schema_100_3752767265.json", tablePath)

	tablePath, err = schemaFile.GenerateSchemaFilePath(true, 12345)
	require.NoError(t, err)
	require.Equal(t, "12345/meta/schema_100_3752767265.json", tablePath)
}

func TestParseSchemaFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, fmt.Sprintf("file:///%s", t.TempDir()))
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

	schemaFile.TableVersion++
	encodedSchemaFile, err = schemaFile.Marshal()
	require.NoError(t, err)
	require.NoError(t, storage.WriteFile(ctx, schemaFilePath, encodedSchemaFile))

	_, _, err = Parse(ctx, storage, schemaFilePath)
	require.Error(t, err)
	require.True(t, errors.ErrStorageSinkInvalidFileName.Equal(err))
}

func TestGenerateSchemaFilePathValidation(t *testing.T) {
	t.Parallel()

	schemaFile, _ := generateSchemaFile()

	// empty schema
	emptySchemaFile := &SchemaFile{Schema: "", Table: "t1", TableVersion: 100, TotalColumns: 1, Columns: []TableCol{{}}}
	_, err := emptySchemaFile.GenerateSchemaFilePath(false, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema cannot be empty")

	// zero table version
	zeroVersionSchemaFile := &SchemaFile{Schema: "s1", Table: "t1", TableVersion: 0, TotalColumns: 1, Columns: []TableCol{{}}}
	_, err = zeroVersionSchemaFile.GenerateSchemaFilePath(false, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table version cannot be zero")

	// use-table-id-as-path with invalid tableID
	_, err = schemaFile.GenerateSchemaFilePath(true, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid table id for table-id path")
	_, err = schemaFile.GenerateSchemaFilePath(true, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid table id for table-id path")

	invalidSchemaFile := &SchemaFile{Schema: "s1", Table: "t1", TableVersion: 100, TotalColumns: 1, Columns: nil}
	_, err = invalidSchemaFile.GenerateSchemaFilePath(false, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid schema file")
}

func TestSchemaFileSum32(t *testing.T) {
	t.Parallel()

	schemaFile, _ := generateSchemaFile()
	checksum1, err := schemaFile.Sum32(nil)
	require.NoError(t, err)
	checksum2, err := schemaFile.Sum32(nil)
	require.NoError(t, err)
	require.Equal(t, checksum1, checksum2)

	n := len(schemaFile.Columns)
	newCol := make([]TableCol, n)
	copy(newCol, schemaFile.Columns)
	newSchemaFile := schemaFile
	newSchemaFile.Columns = newCol

	for i := 0; i < n; i++ {
		target := rand.Intn(n)
		newSchemaFile.Columns[i], newSchemaFile.Columns[target] = newSchemaFile.Columns[target], newSchemaFile.Columns[i]
		newChecksum, err := newSchemaFile.Sum32(nil)
		require.NoError(t, err)
		require.Equal(t, checksum1, newChecksum)
	}
}
