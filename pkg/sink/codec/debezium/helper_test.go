// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetColumns(t *testing.T) {
	sql := "CREATE TABLE test (id INT PRIMARY KEY, val1 datetime default current_timestamp, val2 time(2) default 0,  val3 timestamp(3) default now(), val4 YEAR(4) default 1970 comment 'first');"
	columnInfos := []*timodel.ColumnInfo{
		{
			Name:      parser_model.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
		},
		{
			Name:      parser_model.NewCIStr("val1"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
		{
			Name:      parser_model.NewCIStr("val2"),
			FieldType: *types.NewFieldType(mysql.TypeDuration),
		},
		{
			Name:      parser_model.NewCIStr("val3"),
			FieldType: *types.NewFieldType(mysql.TypeTimestamp),
		},
		{
			Name:      parser_model.NewCIStr("val4"),
			FieldType: *types.NewFieldType(mysql.TypeYear),
		},
	}
	columnInfos = parseColumns(sql, columnInfos)
	require.Equal(t, columnInfos[1].GetDefaultValue(), "CURRENT_TIMESTAMP")
	require.Equal(t, columnInfos[2].GetDecimal(), 2)
	require.Equal(t, columnInfos[2].GetDefaultValue(), "0")
	require.Equal(t, columnInfos[3].GetDecimal(), 3)
	require.Equal(t, columnInfos[3].GetDefaultValue(), "CURRENT_TIMESTAMP")
	require.Equal(t, columnInfos[4].GetFlen(), 4)
	require.Equal(t, columnInfos[4].GetDefaultValue(), "1970")
	require.Equal(t, columnInfos[4].Comment, "")
}

func TestParseColumnsDoesNotMutateInput(t *testing.T) {
	sql := "CREATE TABLE test (id INT PRIMARY KEY, val1 time(2) default 0, val2 timestamp(3) default now());"
	columnInfos := []*timodel.ColumnInfo{
		{
			Name:      parser_model.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
		},
		{
			Name:      parser_model.NewCIStr("val1"),
			FieldType: *types.NewFieldType(mysql.TypeDuration),
		},
		{
			Name:      parser_model.NewCIStr("val2"),
			FieldType: *types.NewFieldType(mysql.TypeTimestamp),
		},
	}
	originalVal1Decimal := columnInfos[1].GetDecimal()
	originalVal2Decimal := columnInfos[2].GetDecimal()

	parsed := parseColumns(sql, columnInfos)

	require.NotSame(t, columnInfos[1], parsed[1])
	require.NotSame(t, columnInfos[2], parsed[2])
	require.Equal(t, originalVal1Decimal, columnInfos[1].GetDecimal())
	require.Equal(t, originalVal2Decimal, columnInfos[2].GetDecimal())
	require.Nil(t, columnInfos[1].GetDefaultValue())
	require.Nil(t, columnInfos[2].GetDefaultValue())
	require.Equal(t, 2, parsed[1].GetDecimal())
	require.Equal(t, "0", parsed[1].GetDefaultValue())
	require.Equal(t, 3, parsed[2].GetDecimal())
	require.Equal(t, "CURRENT_TIMESTAMP", parsed[2].GetDefaultValue())
}

func TestParseColumnsReturnsClonedOnParseFailure(t *testing.T) {
	columnInfos := []*timodel.ColumnInfo{
		{
			Name:      parser_model.NewCIStr("val1"),
			FieldType: *types.NewFieldType(mysql.TypeDuration),
		},
	}
	originalDecimal := columnInfos[0].GetDecimal()

	var parsed []*timodel.ColumnInfo
	require.NotPanics(t, func() {
		parsed = parseColumns("CREATE TABLE test (", columnInfos)
	})

	require.Len(t, parsed, 1)
	require.NotSame(t, columnInfos[0], parsed[0])
	require.Equal(t, originalDecimal, parsed[0].GetDecimal())
	require.Equal(t, originalDecimal, columnInfos[0].GetDecimal())
}

func TestGetSchemaTopicName(t *testing.T) {
	namespace := "default"
	schema := "1A.B"
	table := "columnNameWith中文"
	name := getSchemaTopicName(namespace, schema, table)
	require.Equal(t, name, "default._1A_B.columnNameWith__")
}
