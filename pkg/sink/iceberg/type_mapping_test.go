// Copyright 2025 PingCAP, Inc.
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

package iceberg

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBuildChangelogSchemasMapsTiDBTypesToIceberg(t *testing.T) {
	intType := types.NewFieldType(mysql.TypeLong)

	bigType := types.NewFieldType(mysql.TypeLonglong)

	unsignedBigType := types.NewFieldType(mysql.TypeLonglong)
	unsignedBigType.AddFlag(mysql.UnsignedFlag)

	decimalType := types.NewFieldType(mysql.TypeNewDecimal)
	decimalType.SetFlen(10)
	decimalType.SetDecimal(2)

	dateType := types.NewFieldType(mysql.TypeDate)
	datetimeType := types.NewFieldType(mysql.TypeDatetime)
	blobType := types.NewFieldType(mysql.TypeBlob)
	stringType := types.NewFieldType(mysql.TypeVarchar)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("i"), FieldType: *intType},
			{ID: 2, Name: ast.NewCIStr("big"), FieldType: *bigType},
			{ID: 3, Name: ast.NewCIStr("ubig"), FieldType: *unsignedBigType},
			{ID: 4, Name: ast.NewCIStr("dec"), FieldType: *decimalType},
			{ID: 5, Name: ast.NewCIStr("d"), FieldType: *dateType},
			{ID: 6, Name: ast.NewCIStr("ts"), FieldType: *datetimeType},
			{ID: 7, Name: ast.NewCIStr("b"), FieldType: *blobType},
			{ID: 8, Name: ast.NewCIStr("s"), FieldType: *stringType},
		},
	})

	_, schema, _, err := buildChangelogSchemas(tableInfo, true)
	require.NoError(t, err)

	typesByName := make(map[string]string, len(schema.Fields))
	for _, f := range schema.Fields {
		typesByName[f.Name] = f.Type
	}

	require.Equal(t, "string", typesByName["_tidb_op"])
	require.Equal(t, "long", typesByName["_tidb_commit_ts"])
	require.Equal(t, "timestamp", typesByName["_tidb_commit_time"])

	require.Equal(t, "int", typesByName["i"])
	require.Equal(t, "long", typesByName["big"])
	require.Equal(t, "decimal(20,0)", typesByName["ubig"])
	require.Equal(t, "decimal(10,2)", typesByName["dec"])
	require.Equal(t, "date", typesByName["d"])
	require.Equal(t, "timestamp", typesByName["ts"])
	require.Equal(t, "binary", typesByName["b"])
	require.Equal(t, "string", typesByName["s"])
}

func TestEncodeParquetRowsWritesTypedColumns(t *testing.T) {
	ctx := context.Background()

	intType := types.NewFieldType(mysql.TypeLong)

	bigType := types.NewFieldType(mysql.TypeLonglong)

	unsignedBigType := types.NewFieldType(mysql.TypeLonglong)
	unsignedBigType.AddFlag(mysql.UnsignedFlag)

	decimalType := types.NewFieldType(mysql.TypeNewDecimal)
	decimalType.SetFlen(10)
	decimalType.SetDecimal(2)

	dateType := types.NewFieldType(mysql.TypeDate)
	datetimeType := types.NewFieldType(mysql.TypeDatetime)
	blobType := types.NewFieldType(mysql.TypeBlob)
	stringType := types.NewFieldType(mysql.TypeVarchar)

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:   20,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("i"), FieldType: *intType},
			{ID: 2, Name: ast.NewCIStr("big"), FieldType: *bigType},
			{ID: 3, Name: ast.NewCIStr("ubig"), FieldType: *unsignedBigType},
			{ID: 4, Name: ast.NewCIStr("dec"), FieldType: *decimalType},
			{ID: 5, Name: ast.NewCIStr("d"), FieldType: *dateType},
			{ID: 6, Name: ast.NewCIStr("ts"), FieldType: *datetimeType},
			{ID: 7, Name: ast.NewCIStr("b"), FieldType: *blobType},
			{ID: 8, Name: ast.NewCIStr("s"), FieldType: *stringType},
		},
	})

	i := "1"
	big := "2"
	ubig := "18446744073709551615"
	dec := "123.45"
	d := "2026-01-02"
	ts := "2026-01-02 03:04:05.123456"
	rawBytes := []byte{1, 2, 3}
	b := base64.StdEncoding.EncodeToString(rawBytes)
	s := "abc"

	rows := []ChangeRow{
		{
			Op:         "I",
			CommitTs:   "123",
			CommitTime: "2026-01-01T00:00:00Z",
			Columns: map[string]*string{
				"i":    &i,
				"big":  &big,
				"ubig": &ubig,
				"dec":  &dec,
				"d":    &d,
				"ts":   &ts,
				"b":    &b,
				"s":    &s,
			},
		},
	}

	data, err := encodeParquetRows(tableInfo, true, rows)
	require.NoError(t, err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	reader, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	tbl, err := arrowReader.ReadTable(ctx)
	require.NoError(t, err)
	defer tbl.Release()

	schema := tbl.Schema()

	fieldTypeByName := make(map[string]arrow.DataType, len(schema.Fields()))
	for _, f := range schema.Fields() {
		fieldTypeByName[f.Name] = f.Type
	}

	require.Equal(t, arrow.STRING, fieldTypeByName["_tidb_op"].ID())
	require.Equal(t, arrow.INT64, fieldTypeByName["_tidb_commit_ts"].ID())
	require.Equal(t, arrow.TIMESTAMP, fieldTypeByName["_tidb_commit_time"].ID())

	require.Equal(t, arrow.INT32, fieldTypeByName["i"].ID())
	require.Equal(t, arrow.INT64, fieldTypeByName["big"].ID())
	require.Equal(t, arrow.DECIMAL128, fieldTypeByName["ubig"].ID())
	require.Equal(t, arrow.DECIMAL128, fieldTypeByName["dec"].ID())
	require.Equal(t, arrow.DATE32, fieldTypeByName["d"].ID())
	require.Equal(t, arrow.TIMESTAMP, fieldTypeByName["ts"].ID())
	require.Equal(t, arrow.BINARY, fieldTypeByName["b"].ID())
	require.Equal(t, arrow.STRING, fieldTypeByName["s"].ID())

	// Validate a couple of values to ensure parsing matches types.
	requireColumnValue(t, tbl, "_tidb_commit_ts", int64(123))
	requireDecimalColumnValue(t, tbl, "ubig", ubig, 20, 0)
	requireDecimalColumnValue(t, tbl, "dec", dec, 10, 2)
	requireBinaryColumnValue(t, tbl, "b", rawBytes)
	requireTimestampMicrosColumnValue(t, tbl, "ts", ts)
	requireDate32ColumnValue(t, tbl, "d", d)
}

func requireColumnValue(t *testing.T, tbl arrow.Table, name string, expected int64) {
	t.Helper()
	chunk := firstChunkByName(t, tbl, name)
	arr, ok := chunk.(*array.Int64)
	require.True(t, ok)
	require.Equal(t, expected, arr.Value(0))
}

func requireDecimalColumnValue(t *testing.T, tbl arrow.Table, name string, raw string, precision int32, scale int32) {
	t.Helper()
	chunk := firstChunkByName(t, tbl, name)
	arr, ok := chunk.(*array.Decimal128)
	require.True(t, ok)
	dt, ok := arr.DataType().(*arrow.Decimal128Type)
	require.True(t, ok)
	require.Equal(t, precision, dt.Precision)
	require.Equal(t, scale, dt.Scale)

	expected, err := decimal128.FromString(raw, precision, scale)
	require.NoError(t, err)
	require.Equal(t, expected, arr.Value(0))
}

func requireBinaryColumnValue(t *testing.T, tbl arrow.Table, name string, expected []byte) {
	t.Helper()
	chunk := firstChunkByName(t, tbl, name)
	arr, ok := chunk.(*array.Binary)
	require.True(t, ok)
	require.Equal(t, expected, arr.Value(0))
}

func requireTimestampMicrosColumnValue(t *testing.T, tbl arrow.Table, name string, raw string) {
	t.Helper()
	chunk := firstChunkByName(t, tbl, name)
	arr, ok := chunk.(*array.Timestamp)
	require.True(t, ok)

	parsed, err := parseMySQLTimestampString(raw)
	require.NoError(t, err)
	require.Equal(t, arrow.Timestamp(parsed.UTC().UnixNano()/1000), arr.Value(0))
}

func requireDate32ColumnValue(t *testing.T, tbl arrow.Table, name string, raw string) {
	t.Helper()
	chunk := firstChunkByName(t, tbl, name)
	arr, ok := chunk.(*array.Date32)
	require.True(t, ok)

	parsed, err := time.ParseInLocation("2006-01-02", raw, time.UTC)
	require.NoError(t, err)
	require.Equal(t, arrow.Date32(parsed.UTC().Unix()/86400), arr.Value(0))
}

func firstChunkByName(t *testing.T, tbl arrow.Table, name string) arrow.Array {
	t.Helper()
	schema := tbl.Schema()
	idx := -1
	for i, f := range schema.Fields() {
		if f.Name == name {
			idx = i
			break
		}
	}
	require.GreaterOrEqual(t, idx, 0)
	col := tbl.Column(idx)
	require.Greater(t, col.Len(), 0)
	chunks := col.Data().Chunks()
	require.NotEmpty(t, chunks)
	return chunks[0]
}
