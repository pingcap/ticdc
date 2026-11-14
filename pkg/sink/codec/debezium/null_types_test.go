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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package debezium

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/avro"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestDebeziumReplaceNullWithDefaultTypes(t *testing.T) {
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(t.Context(), common.NewConfig(config.ProtocolAvro))
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	type columnCase struct {
		name         string
		sqlType      string
		defaultValue string
		value        string
		zero         string
	}
	var columns []columnCase
	for _, sqlType := range []string{"tinyint", "smallint", "mediumint", "int", "bigint"} {
		for _, unsigned := range []bool{false, true} {
			column := columnCase{name: sqlType, sqlType: sqlType, defaultValue: "7", value: "9", zero: "0"}
			if unsigned {
				column.name += "_unsigned"
				column.sqlType += " unsigned"
			}
			columns = append(columns, column)
		}
	}
	for _, numeric := range []struct {
		name    string
		sqlType string
	}{
		{"float", "float"},
		{"float_md", "float(10,4)"},
		{"double", "double"},
		{"decimal", "decimal(10,4)"},
		{"decimal_scale_zero", "decimal(10,0)"},
	} {
		for _, unsigned := range []bool{false, true} {
			column := columnCase{name: numeric.name, sqlType: numeric.sqlType, defaultValue: "1.25", value: "2.5", zero: "0"}
			if numeric.name == "decimal_scale_zero" {
				column.defaultValue, column.value = "7", "9"
			}
			if unsigned {
				column.name += "_unsigned"
				column.sqlType += " unsigned"
			}
			columns = append(columns, column)
		}
	}
	// These aliases share codec types with the integer, floating-point, and decimal cases.
	columns = append(columns,
		columnCase{"bool_alias", "bool", "true", "true", "false"},
		columnCase{"boolean_alias", "boolean", "true", "true", "false"},
		columnCase{"integer_alias", "integer", "7", "9", "0"},
		columnCase{"real_alias", "real", "1.25", "2.5", "0"},
		columnCase{"numeric_alias", "numeric(10,4)", "1.25", "2.5", "0"},
	)
	for _, width := range []int{1, 8, 9, 64} {
		columns = append(columns, columnCase{
			name:         fmt.Sprintf("bit_%d", width),
			sqlType:      fmt.Sprintf("bit(%d)", width),
			defaultValue: "b'1'",
			value:        "b'" + strings.Repeat("1", width) + "'",
			zero:         "b'0'",
		})
	}
	columns = append(columns,
		columnCase{"char_value", "char(16)", "'fallback'", "'value'", "''"},
		columnCase{"varchar_value", "varchar(16)", "'fallback'", "'value'", "''"},
		columnCase{"binary_value", "binary(4)", "x'6162'", "x'0001'", "x''"},
		columnCase{"varbinary_value", "varbinary(16)", "x'6162'", "x'0001'", "x''"},
	)
	// TiDB rejects non-NULL literal defaults for these types, but accepts these
	// deterministic expression defaults. Actual DEFAULT inserts evaluate them in TiDB.
	for _, sqlType := range []string{"tinytext", "text", "mediumtext", "longtext", "tinyblob", "blob", "mediumblob", "longblob"} {
		columns = append(columns, columnCase{sqlType, sqlType, "(json_quote('fallback'))", "'value'", "''"})
	}
	columns = append(columns,
		columnCase{"enum_value", "enum('','a','b')", "'a'", "'b'", "''"},
		columnCase{"set_value", "set('a','b')", "'a'", "'a,b'", "''"},
		columnCase{"date_value", "date", "'2024-01-02'", "'2024-02-03'", "'1970-01-01'"},
		columnCase{"year_value", "year", "2024", "2025", "0"},
	)
	for _, sqlType := range []string{"time", "datetime", "timestamp"} {
		for fsp := 0; fsp <= 6; fsp++ {
			fraction := ""
			if fsp > 0 {
				fraction = ".123456"[:fsp+1]
			}
			defaultValue, value, zero := "01:02:03", "-02:03:04", "00:00:00"
			if sqlType != "time" {
				defaultValue, value, zero = "2024-01-02 03:04:05", "2024-02-03 04:05:06", "2000-01-01 00:00:00"
			}
			columns = append(columns, columnCase{
				name:         fmt.Sprintf("%s_%d", sqlType, fsp),
				sqlType:      fmt.Sprintf("%s(%d)", sqlType, fsp),
				defaultValue: "'" + defaultValue + fraction + "'",
				value:        "'" + value + fraction + "'",
				zero:         "'" + zero + "'",
			})
		}
	}
	columns = append(columns,
		columnCase{"json_value", "json", "(json_object('d',1))", `'{"v":2}'`, "'null'"},
		columnCase{"vector_value", "vector", "(vec_from_text('[1,2,3]'))", "'[4,5,6]'", "'[0,0,0]'"},
		columnCase{"vector_fixed_value", "vector(3)", "(vec_from_text('[1,2,3]'))", "'[4,5,6]'", "'[0,0,0]'"},
	)

	definitions := []string{"id int primary key"}
	nulls, values, zeroes := []string{"1"}, []string{"3"}, []string{"4"}
	for _, column := range columns {
		definitions = append(definitions, fmt.Sprintf("`%s` %s null default %s", column.name, column.sqlType, column.defaultValue))
		nulls = append(nulls, "null")
		values = append(values, column.value)
		zeroes = append(zeroes, column.zero)
	}
	helper := NewSQLTestHelper(t, "null_types", "create table null_types ("+strings.Join(definitions, ",")+")")
	defer helper.Close()
	cases := []struct {
		name   string
		sql    string
		isNull bool
	}{
		{"SQL NULL", "insert into null_types values (" + strings.Join(nulls, ",") + ")", true},
		{"DEFAULT", "insert into null_types (id) values (2)", false},
		{"non-NULL", "insert into null_types values (" + strings.Join(values, ",") + ")", false},
		{"zero and empty", "insert into null_types values (" + strings.Join(zeroes, ",") + ")", false},
	}
	events := make([]*commonEvent.DMLEvent, len(cases))
	for i, tc := range cases {
		events[i] = helper.helper.DML2Event("test", "null_types", tc.sql)
		row, ok := events[i].GetNextRow()
		require.True(t, ok)
		for idx, column := range columns {
			require.Equal(t, tc.isNull, row.Row.IsNull(idx+1), "%s: %s", tc.name, column.name)
		}
		events[i].Rewind()
	}

	for _, format := range []struct {
		name          string
		protocol      config.Protocol
		disableSchema bool
	}{
		{"JSON with schema", config.ProtocolDebezium, false},
		{"JSON without schema", config.ProtocolDebezium, true},
		{"Avro", config.ProtocolDebeziumAvro, false},
	} {
		t.Run(format.name, func(t *testing.T) {
			cfg := common.NewConfig(format.protocol)
			cfg.TimeZone = time.UTC
			cfg.DebeziumDisableSchema = format.disableSchema
			if format.protocol == config.ProtocolDebeziumAvro {
				cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
			}
			for i, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					cfg.DebeziumReplaceNullWithDefault = false
					data, schema := decodeNullTypesMessage(t, format.protocol, encodeNullTypesEvent(t, cfg, helper, events[i]))
					if format.disableSchema {
						require.Nil(t, schema)
					} else {
						require.NotNil(t, schema)
					}
					after := data["after"].(map[string]any)
					for _, column := range columns {
						require.Contains(t, after, column.name)
						if tc.isNull {
							require.Nil(t, after[column.name], column.name)
						} else {
							require.NotNil(t, after[column.name], column.name)
						}
					}
					if tc.name == "zero and empty" {
						require.Equal(t, "null", after["json_value"])
						require.Equal(t, "", after["varchar_value"])
						require.Equal(t, "", after["enum_value"])
						require.Equal(t, "", after["set_value"])
						require.Equal(t, "[0,0,0]", after["vector_fixed_value"])
					}
					if !tc.isNull {
						// TiDB has already evaluated expression defaults for these rows.
						// Disabling NULL replacement must leave their encoding unchanged.
						cfg.DebeziumReplaceNullWithDefault = true
						legacy, legacySchema := decodeNullTypesMessage(t, format.protocol, encodeNullTypesEvent(t, cfg, helper, events[i]))
						require.Equal(t, legacy, data)
						require.Equal(t, legacySchema, schema)
					}
				})
			}
		})
	}
}

func encodeNullTypesEvent(t *testing.T, cfg *common.Config, helper *SQLTestHelper, event *commonEvent.DMLEvent) []byte {
	t.Helper()
	row, ok := event.GetNextRow()
	require.True(t, ok)
	defer event.Rewind()
	var encoder common.EventEncoder = NewBatchEncoder(cfg, "dbserver1")
	if cfg.Protocol == config.ProtocolDebeziumAvro {
		var err error
		encoder, err = newAvroBatchEncoderForTest(t.Context(), cfg, "dbserver1")
		require.NoError(t, err)
	}
	require.NoError(t, encoder.AppendRowChangedEvent(t.Context(), "dbserver1.test.null_types", &commonEvent.RowEvent{
		TableInfo:      helper.tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}))
	messages := encoder.Build()
	require.Len(t, messages, 1)
	return messages[0].Value
}

func decodeNullTypesMessage(t *testing.T, protocol config.Protocol, data []byte) (map[string]any, any) {
	t.Helper()
	if protocol == config.ProtocolDebeziumAvro {
		// Decode the actual Avro binary with goavro rather than TiCDC's decoder.
		message := decodeConfluentAvroForTest(t, data)
		require.Equal(t, "c", message["op"])
		require.Contains(t, message, "before")
		require.Nil(t, message["before"])
		afterUnion, ok := message["after"].(map[string]any)
		require.True(t, ok)
		require.Len(t, afterUnion, 1)
		var after map[string]any
		for _, value := range afterUnion {
			after, ok = value.(map[string]any)
			require.True(t, ok)
		}
		for name, value := range after {
			if union, ok := value.(map[string]any); ok {
				require.Len(t, union, 1, name)
				for _, value := range union {
					after[name] = value
				}
			}
		}
		return map[string]any{"before": message["before"], "after": after}, decodeConfluentAvroSchemaForTest(t, data)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var message map[string]any
	require.NoError(t, decoder.Decode(&message))
	payload, ok := message["payload"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "c", payload["op"])
	require.Contains(t, payload, "before")
	require.Nil(t, payload["before"])
	after, ok := payload["after"].(map[string]any)
	require.True(t, ok)
	return map[string]any{"before": payload["before"], "after": after}, message["schema"]
}
