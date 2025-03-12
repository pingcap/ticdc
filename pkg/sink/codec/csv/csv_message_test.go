// Copyright 2022 PingCAP, Inc.
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

package csv

import (
	"fmt"
	"strings"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type csvTestColumnTuple struct {
	col                  commonType.Column
	colInfo              model.ColumnInfo
	want                 interface{}
	BinaryEncodingMethod string
}

var csvTestColumnsGroup = [][]*csvTestColumnTuple{
	{
		{
			commonType.Column{Name: "tiny", Value: int64(1), Type: mysql.TypeTiny},
			model.ColumnInfo{
				ID:        1,
				FieldType: *types.NewFieldType(mysql.TypeTiny),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "short", Value: int64(1), Type: mysql.TypeShort},
			model.ColumnInfo{
				ID:        2,
				FieldType: *types.NewFieldType(mysql.TypeShort),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "int24", Value: int64(1), Type: mysql.TypeInt24},
			model.ColumnInfo{
				ID:        3,
				FieldType: *types.NewFieldType(mysql.TypeInt24),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "long", Value: int64(1), Type: mysql.TypeLong},
			model.ColumnInfo{
				ID:        4,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "longlong", Value: int64(1), Type: mysql.TypeLonglong},
			model.ColumnInfo{
				ID:        5,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "tinyunsigned",
				Value: uint64(1),
				Type:  mysql.TypeTiny,
				Flag:  commonType.UnsignedFlag,
			},
			model.ColumnInfo{
				ID:        6,
				FieldType: *setFlag(types.NewFieldType(mysql.TypeTiny), uint(mysql.UnsignedFlag)),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "shortunsigned",
				Value: uint64(1),
				Type:  mysql.TypeShort,
				Flag:  commonType.UnsignedFlag,
			},
			model.ColumnInfo{
				ID:        7,
				FieldType: *setFlag(types.NewFieldType(mysql.TypeShort), mysql.UnsignedFlag),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "int24unsigned",
				Value: uint64(1),
				Type:  mysql.TypeInt24,
				Flag:  commonType.UnsignedFlag,
			},
			model.ColumnInfo{
				ID:        8,
				FieldType: *setFlag(types.NewFieldType(mysql.TypeInt24), mysql.UnsignedFlag),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "longunsigned",
				Value: uint64(1),
				Type:  mysql.TypeLong,
				Flag:  commonType.UnsignedFlag,
			},
			model.ColumnInfo{
				ID:        9,
				FieldType: *setFlag(types.NewFieldType(mysql.TypeLong), mysql.UnsignedFlag),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "longlongunsigned",
				Value: uint64(1),
				Type:  mysql.TypeLonglong,
				Flag:  commonType.UnsignedFlag,
			},
			model.ColumnInfo{
				ID: 10,
				FieldType: *setFlag(
					types.NewFieldType(mysql.TypeLonglong),
					mysql.UnsignedFlag,
				),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{Name: "float", Value: float32(3.14), Type: mysql.TypeFloat},
			model.ColumnInfo{
				ID:        11,
				FieldType: *types.NewFieldType(mysql.TypeFloat),
			},
			float32(3.14),
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "double", Value: float64(3.14), Type: mysql.TypeDouble},
			model.ColumnInfo{
				ID:        12,
				FieldType: *types.NewFieldType(mysql.TypeDouble),
			},
			float64(3.14),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{Name: "bit", Value: types.NewBinaryLiteralFromUint(683, 3), Type: mysql.TypeBit},
			model.ColumnInfo{
				ID:        13,
				FieldType: *types.NewFieldType(mysql.TypeBit),
			},
			uint64(683),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{Name: "decimal", Value: types.NewDecFromStringForTest("129012.1230000"), Type: mysql.TypeNewDecimal},
			model.ColumnInfo{
				ID:        14,
				FieldType: *types.NewFieldType(mysql.TypeNewDecimal),
			},
			"129012.1230000",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{Name: "tinytext", Value: []byte("hello world"), Type: mysql.TypeTinyBlob},
			model.ColumnInfo{
				ID:        15,
				FieldType: *types.NewFieldType(mysql.TypeBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "mediumtext", Value: []byte("hello world"), Type: mysql.TypeMediumBlob},
			model.ColumnInfo{
				ID:        16,
				FieldType: *types.NewFieldType(mysql.TypeMediumBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "text", Value: []byte("hello world"), Type: mysql.TypeBlob},
			model.ColumnInfo{
				ID:        17,
				FieldType: *types.NewFieldType(mysql.TypeBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "longtext", Value: []byte("hello world"), Type: mysql.TypeLongBlob},
			model.ColumnInfo{
				ID:        18,
				FieldType: *types.NewFieldType(mysql.TypeLongBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "varchar", Value: []byte("hello world"), Type: mysql.TypeVarchar},
			model.ColumnInfo{
				ID:        19,
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "varstring", Value: []byte("hello world"), Type: mysql.TypeVarString},
			model.ColumnInfo{
				ID:        20,
				FieldType: *types.NewFieldType(mysql.TypeVarString),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "string", Value: []byte("hello world"), Type: mysql.TypeString},
			model.ColumnInfo{
				ID:        21,
				FieldType: *types.NewFieldType(mysql.TypeString),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{Name: "json", Value: types.CreateBinaryJSON(`{"key": "value"}`), Type: mysql.TypeJSON},
			model.ColumnInfo{
				ID:        31,
				FieldType: *types.NewFieldType(mysql.TypeJSON),
			},
			types.CreateBinaryJSON(`{"key": "value"}`).String(),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{
				Name:  "tinyblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeTinyBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        22,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "mediumblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeMediumBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        23,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "blob",
				Value: []byte("hello world"),
				Type:  mysql.TypeBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        24,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "longblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeLongBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        25,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "varbinary",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarchar,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        26,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "varbinary1",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarString,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        27,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeVarString)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "binary",
				Value: []byte("hello world"),
				Type:  mysql.TypeString,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        28,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeString)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{
				Name:  "tinyblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeTinyBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        22,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			commonType.Column{
				Name:  "mediumblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeMediumBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        23,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			commonType.Column{
				Name:  "blob",
				Value: []byte("hello world"),
				Type:  mysql.TypeBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        24,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			commonType.Column{
				Name:  "longblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeLongBlob,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        25,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			commonType.Column{
				Name:  "varbinary",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarchar,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        26,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			commonType.Column{
				Name:  "varbinary1",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarString,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        27,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeVarString)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			commonType.Column{
				Name:  "binary",
				Value: []byte("hello world"),
				Type:  mysql.TypeString,
				Flag:  commonType.BinaryFlag,
			},
			model.ColumnInfo{
				ID:        28,
				FieldType: *setBinChsClnFlag(types.NewFieldType(mysql.TypeString)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
	},
	{
		{
			commonType.Column{Name: "enum", Value: types.Enum{Value: 1}, Type: mysql.TypeEnum},
			model.ColumnInfo{
				ID:        29,
				FieldType: *setElems(types.NewFieldType(mysql.TypeEnum), []string{"a,", "b"}),
			},
			"a,",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{Name: "set", Value: types.Set{Value: 9}, Type: mysql.TypeSet},
			model.ColumnInfo{
				ID:        30,
				FieldType: *setElems(types.NewFieldType(mysql.TypeSet), []string{"a", "b", "c", "d"}),
			},
			"a,d",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{
				Name:  "date",
				Value: util.Must(types.ParseDate(types.DefaultStmtNoWarningContext, "2000-01-01")),
				Type:  mysql.TypeDate,
			},
			model.ColumnInfo{
				ID:        32,
				FieldType: *types.NewFieldType(mysql.TypeDate),
			},
			"2000-01-01",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "datetime",
				Value: util.Must(types.ParseDatetime(types.DefaultStmtNoWarningContext, "2015-12-20 23:58:58")),
				Type:  mysql.TypeDatetime,
			},
			model.ColumnInfo{
				ID:        33,
				FieldType: *types.NewFieldType(mysql.TypeDatetime),
			},
			"2015-12-20 23:58:58",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "timestamp",
				Value: util.Must(types.ParseTimestamp(types.DefaultStmtNoWarningContext, "1973-12-30 15:30:00")),
				Type:  mysql.TypeTimestamp,
			},
			model.ColumnInfo{
				ID:        34,
				FieldType: *types.NewFieldType(mysql.TypeTimestamp),
			},
			"1973-12-30 15:30:00",
			config.BinaryEncodingBase64,
		},
		{
			commonType.Column{
				Name:  "time",
				Value: types.NewDuration(23, 59, 59, 0, 0),
				Type:  mysql.TypeDuration,
			},
			model.ColumnInfo{
				ID:        35,
				FieldType: *types.NewFieldType(mysql.TypeDuration),
			},
			"23:59:59",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{Name: "year", Value: int64(1970), Type: mysql.TypeYear},
			model.ColumnInfo{
				ID:        36,
				FieldType: *types.NewFieldType(mysql.TypeYear),
			},
			int64(1970),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			commonType.Column{Name: "vectorfloat32", Value: util.Must(types.ParseVectorFloat32("[1,2,3,4,5]")), Type: mysql.TypeTiDBVectorFloat32},
			model.ColumnInfo{
				ID:        37,
				FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
			},
			"[1,2,3,4,5]",
			config.BinaryEncodingBase64,
		},
	},
}

func setBinChsClnFlag(ft *types.FieldType) *types.FieldType {
	types.SetBinChsClnFlag(ft)
	return ft
}

//nolint:unparam
func setFlag(ft *types.FieldType, flag uint) *types.FieldType {
	ft.SetFlag(flag)
	return ft
}

func setElems(ft *types.FieldType, elems []string) *types.FieldType {
	ft.SetElems(elems)
	return ft
}

func TestFormatWithQuotes(t *testing.T) {
	config := &common.Config{
		Quote: "\"",
	}

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "string does not contain quote mark",
			input:    "a,b,c",
			expected: `"a,b,c"`,
		},
		{
			name:     "string contains quote mark",
			input:    `"a,b,c`,
			expected: `"""a,b,c"`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
	}
	for _, tc := range testCases {
		csvMessage := newCSVMessage(config)
		strBuilder := new(strings.Builder)
		csvMessage.formatWithQuotes(tc.input, strBuilder)
		require.Equal(t, tc.expected, strBuilder.String(), tc.name)
	}
}

func TestFormatWithEscape(t *testing.T) {
	testCases := []struct {
		name     string
		config   *common.Config
		input    string
		expected string
	}{
		{
			name:     "string does not contain CR/LF/backslash/delimiter",
			config:   &common.Config{Delimiter: ","},
			input:    "abcdef",
			expected: "abcdef",
		},
		{
			name:     "string contains CRLF",
			config:   &common.Config{Delimiter: ","},
			input:    "abc\r\ndef",
			expected: "abc\\r\\ndef",
		},
		{
			name:     "string contains backslash",
			config:   &common.Config{Delimiter: ","},
			input:    `abc\def`,
			expected: `abc\\def`,
		},
		{
			name:     "string contains a single character delimiter",
			config:   &common.Config{Delimiter: ","},
			input:    "abc,def",
			expected: `abc\,def`,
		},
		{
			name:     "string contains multi-character delimiter",
			config:   &common.Config{Delimiter: "***"},
			input:    "abc***def",
			expected: `abc\*\*\*def`,
		},
		{
			name:     "string contains CR, LF, backslash and delimiter",
			config:   &common.Config{Delimiter: "?"},
			input:    `abc\def?ghi\r\n`,
			expected: `abc\\def\?ghi\\r\\n`,
		},
	}

	for _, tc := range testCases {
		csvMessage := newCSVMessage(tc.config)
		strBuilder := new(strings.Builder)
		csvMessage.formatWithEscapes(tc.input, strBuilder)
		require.Equal(t, tc.expected, strBuilder.String())
	}
}

func TestCSVMessageEncode(t *testing.T) {
	type fields struct {
		config     *common.Config
		opType     operation
		tableName  string
		schemaName string
		commitTs   uint64
		preColumns []any
		columns    []any
		HandleKey  kv.Handle
	}
	testCases := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			name: "csv encode with typical configurations",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationInsert,
				tableName:  "table1",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{123, "hello,world"},
			},
			want: []byte("\"I\",\"table1\",\"test\",435661838416609281,123,\"hello,world\"\n"),
		},
		{
			name: "csv encode values containing single-character delimter string, without quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       "!",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationUpdate,
				tableName:  "table2",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a!b!c", "def"},
			},
			want: []byte(`U!table2!test!435661838416609281!a\!b\!c!def` + "\n"),
		},
		// {
		// 	name: "csv encode values containing single-character delimter string, without quote mark, update with old value",
		// 	fields: fields{
		// 		config: &common.Config{
		// 			Delimiter:       "!",
		// 			Quote:           "",
		// 			Terminator:      "\n",
		// 			NullString:      "\\N",
		// 			IncludeCommitTs: true,
		// 			OutputOldValue:  true,
		// 			OutputHandleKey: true, // not supported
		// 		},
		// 		opType:     operationUpdate,
		// 		tableName:  "table2",
		// 		schemaName: "test",
		// 		commitTs:   435661838416609281,
		// 		preColumns: []any{"a!b!c", "abc"},
		// 		columns:    []any{"a!b!c", "def"},
		// 		HandleKey:  kv.IntHandle(1),
		// 	},
		// 	want: []byte(`D!table2!test!435661838416609281!true!1!a\!b\!c!abc` + "\n" +
		// 		`I!table2!test!435661838416609281!true!1!a\!b\!c!def` + "\n"),
		// },
		{
			name: "csv encode values containing single-character delimter string, without quote mark, update with old value",
			fields: fields{
				config: &common.Config{
					Delimiter:       "!",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
					OutputOldValue:  true,
				},
				opType:     operationInsert,
				tableName:  "table2",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a!b!c", "def"},
			},
			want: []byte(`I!table2!test!435661838416609281!false!a\!b\!c!def` + "\n"),
		},
		{
			name: "csv encode values containing single-character delimter string, with quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationUpdate,
				tableName:  "table3",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a,b,c", "def", "2022-08-31 17:07:00"},
			},
			want: []byte(`"U","table3","test",435661838416609281,"a,b,c","def","2022-08-31 17:07:00"` + "\n"),
		},
		{
			name: "csv encode values containing multi-character delimiter string, without quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       "[*]",
					Quote:           "",
					Terminator:      "\r\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     operationDelete,
				tableName:  "table4",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a[*]b[*]c", "def"},
			},
			want: []byte(`D[*]table4[*]test[*]a\[\*\]b\[\*\]c[*]def` + "\r\n"),
		},
		{
			name: "csv encode with values containing multi-character delimiter string, with quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       "[*]",
					Quote:           "'",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     operationInsert,
				tableName:  "table5",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a[*]b[*]c", "def", nil, 12345.678},
			},
			want: []byte(`'I'[*]'table5'[*]'test'[*]'a[*]b[*]c'[*]'def'[*]\N[*]12345.678` + "\n"),
		},
		{
			name: "csv encode with values containing backslash and LF, without quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationUpdate,
				tableName:  "table6",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a\\b\\c", "def\n"},
			},
			want: []byte(`U,table6,test,435661838416609281,a\\b\\c,def\n` + "\n"),
		},
		{
			name: "csv encode with values containing backslash and CR, with quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "'",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     operationInsert,
				tableName:  "table7",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"\\", "\\\r", "\\\\"},
			},
			want: []byte("'I','table7','test','\\','\\\r','\\\\'" + "\n"),
		},
		{
			name: "csv encode with values containing unicode characters",
			fields: fields{
				config: &common.Config{
					Delimiter:       "\t",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationDelete,
				tableName:  "table8",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a\tb", 123.456, "你好，世界"},
			},
			want: []byte("\"D\"\t\"table8\"\t\"test\"\t435661838416609281\t\"a\tb\"\t123.456\t\"你好，世界\"\n"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &csvMessage{
				config:     tc.fields.config,
				opType:     tc.fields.opType,
				tableName:  tc.fields.tableName,
				schemaName: tc.fields.schemaName,
				commitTs:   tc.fields.commitTs,
				columns:    tc.fields.columns,
				preColumns: tc.fields.preColumns,
				newRecord:  true,
				// HandleKey:  tc.fields.HandleKey,
			}

			require.Equal(t, tc.want, c.encode())
		})
	}
}

func TestConvertToCSVType(t *testing.T) {
	for _, group := range csvTestColumnsGroup {
		for _, c := range group {
			cfg := &common.Config{BinaryEncodingMethod: c.BinaryEncodingMethod}
			row := chunk.MutRowFromValues(c.col.Value).ToRow()
			val, _ := fromColValToCsvVal(cfg, &row, 0, &c.colInfo, &c.col.Flag)
			require.Equal(t, c.want, val, c.col.Name)
		}
	}
}

func TestRowChangeEventConversion(t *testing.T) {
	for idx, group := range csvTestColumnsGroup {
		e := &commonEvent.RowEvent{
			Event: commonEvent.RowChange{},
		}
		cols := make([]interface{}, 0, len(group))
		colInfos := make([]*model.ColumnInfo, 0, len(group))
		for _, c := range group {
			cols = append(cols, c.col.Value)
			colInfos = append(colInfos, &c.colInfo)
		}
		tidbTableInfo := &model.TableInfo{
			Name:    pmodel.NewCIStr(fmt.Sprintf("table%d", idx)),
			Columns: colInfos,
		}
		e.TableInfo = commonType.WrapTableInfo("test", tidbTableInfo)

		if idx%3 == 0 { // delete operation
			e.Event.PreRow = chunk.MutRowFromValues(cols...).ToRow()
		} else if idx%3 == 1 { // insert operation
			e.Event.Row = chunk.MutRowFromValues(cols...).ToRow()
		} else { // update operation
			e.Event.PreRow = chunk.MutRowFromValues(cols...).ToRow()
			e.Event.Row = chunk.MutRowFromValues(cols...).ToRow()
		}
		csvMsg, err := rowChangedEvent2CSVMsg(&common.Config{
			Delimiter:            "\t",
			Quote:                "\"",
			Terminator:           "\n",
			NullString:           "\\N",
			IncludeCommitTs:      true,
			BinaryEncodingMethod: group[0].BinaryEncodingMethod,
		}, e)
		require.NotNil(t, csvMsg)
		require.Nil(t, err)

		// row2, err := csvMsg2RowChangedEvent(&common.Config{
		// 	BinaryEncodingMethod: group[0].BinaryEncodingMethod,
		// }, csvMsg, row.TableInfo)
		// require.Nil(t, err)
		// require.NotNil(t, row2)
	}
}

func TestCSVMessageDecode(t *testing.T) {
	// datums := make([][]types.Datum, 0, 4)
	testCases := []struct {
		row              []types.Datum
		expectedCommitTs uint64
		expectedColsCnt  int
		expectedErr      string
	}{
		{
			row: []types.Datum{
				types.NewStringDatum("I"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
				types.NewStringDatum("433305438660591626"),
				types.NewStringDatum("101"),
				types.NewStringDatum("Smith"),
				types.NewStringDatum("Bob"),
				types.NewStringDatum("2014-06-04"),
				types.NewDatum(nil),
			},
			expectedCommitTs: 433305438660591626,
			expectedColsCnt:  5,
			expectedErr:      "",
		},
		{
			row: []types.Datum{
				types.NewStringDatum("U"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
				types.NewStringDatum("433305438660591627"),
				types.NewStringDatum("101"),
				types.NewStringDatum("Smith"),
				types.NewStringDatum("Bob"),
				types.NewStringDatum("2015-10-08"),
				types.NewStringDatum("Los Angeles"),
			},
			expectedCommitTs: 433305438660591627,
			expectedColsCnt:  5,
			expectedErr:      "",
		},
		{
			row: []types.Datum{
				types.NewStringDatum("D"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
			},
			expectedCommitTs: 0,
			expectedColsCnt:  0,
			expectedErr:      "the csv row should have at least four columns",
		},
		{
			row: []types.Datum{
				types.NewStringDatum("D"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
				types.NewStringDatum("hello world"),
			},
			expectedCommitTs: 0,
			expectedColsCnt:  0,
			expectedErr:      "the 4th column(hello world) of csv row should be a valid commit-ts",
		},
	}
	for _, tc := range testCases {
		csvMsg := newCSVMessage(&common.Config{
			Delimiter:       ",",
			Quote:           "\"",
			Terminator:      "\n",
			NullString:      "\\N",
			IncludeCommitTs: true,
		})
		err := csvMsg.decode(tc.row)
		if tc.expectedErr != "" {
			require.Contains(t, err.Error(), tc.expectedErr)
		} else {
			require.Nil(t, err)
			require.Equal(t, tc.expectedCommitTs, csvMsg.commitTs)
			require.Equal(t, tc.expectedColsCnt, len(csvMsg.columns))
		}
	}
}
