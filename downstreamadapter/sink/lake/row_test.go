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

package lake

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func testSchema() tableSchema {
	return tableSchema{database: "lake", table: "orders", columns: []column{{name: "id"}, {name: "value"}}, primaryKey: []string{"id"}}
}

func testRow(t *testing.T, action rowAction, ts uint64, id int) normalizedRow {
	t.Helper()
	row, err := normalizeNativeRow(testSchema(), action, ts, map[string]any{"id": id, "value": "payload"})
	require.NoError(t, err)
	return row
}

func TestNativeRowNormalizesExactlyOnce(t *testing.T) {
	schema := tableSchema{
		database: "lake", table: "native_values",
		columns: []column{
			{name: "id"},
			{name: "enum_label"},
			{name: "set_label"},
			{name: "binary", encoding: binaryColumn},
			{name: "document", encoding: jsonColumn},
			{name: "sql_null", encoding: jsonColumn},
			{name: "json_null", encoding: jsonColumn},
			{name: "decimal"},
		},
		primaryKey: []string{"id"},
	}
	bytes := make([]byte, 256)
	for i := range bytes {
		bytes[i] = byte(i)
	}
	wantHex := hex.EncodeToString(bytes)
	document := `{"uint":18446744073709551615,"decimal":12345678901234567890.123456789}`
	row, err := normalizeNativeRow(schema, insertAction, 123, map[string]any{
		"id": uint64(math.MaxUint64), "enum_label": "2", "set_label": "last",
		"binary": bytes, "document": json.RawMessage(document), "sql_null": nil, "json_null": "null",
		"decimal": "12345678901234567890.123456789",
	})
	require.NoError(t, err)
	clear(bytes) // The payload must no longer reference source chunk bytes.
	var values map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(row.payload), &values))
	require.JSONEq(t, `"2"`, string(values["enum_label"]), "a numeric ENUM label is not an ordinal")
	require.JSONEq(t, `"last"`, string(values["set_label"]))
	require.JSONEq(t, `"`+wantHex+`"`, string(values["binary"]))
	require.Equal(t, "18446744073709551615", string(values["id"]))
	require.Equal(t, "18446744073709551615", row.id)
	var actualDocument string
	require.NoError(t, json.Unmarshal(values["document"], &actualDocument))
	require.Equal(t, document, actualDocument)
	require.Equal(t, "null", string(values["sql_null"]))
	require.Equal(t, `"null"`, string(values["json_null"]))
	require.JSONEq(t, `"12345678901234567890.123456789"`, string(values["decimal"]))
}

func TestNativeRowRetainsCompositeIdentity(t *testing.T) {
	schema := tableSchema{
		database: "lake", table: "events",
		columns: []column{{name: "tenant"}, {name: "id"}}, primaryKey: []string{"tenant", "id"},
	}
	row, err := normalizeNativeRow(schema, deleteAction, 100, map[string]any{"tenant": "a|b", "id": int64(42)})
	require.NoError(t, err)
	require.Equal(t, `[{"name":"id","value":"42"},{"name":"tenant","value":"a|b"}]`, row.id)
	schema.primaryKey = []string{"id", "tenant"}
	replayed, err := normalizeNativeRow(schema, deleteAction, 100, map[string]any{"tenant": "a|b", "id": "42"})
	require.NoError(t, err)
	require.Equal(t, row.id, replayed.id, "typed and Canal-style string keys retain the baseline identity")
}

func TestNativeRowRejectsInvalidInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action rowAction
		ts     uint64
		values map[string]any
	}{
		{"zero TSO", insertAction, 0, map[string]any{"id": 1, "value": "v"}},
		{"TSO overflow", insertAction, math.MaxUint64, map[string]any{"id": 1, "value": "v"}},
		{"unknown action", "replace", 1, map[string]any{"id": 1, "value": "v"}},
		{"missing after image", updateAction, 1, map[string]any{"id": 1}},
		{"null key", insertAction, 1, map[string]any{"id": nil, "value": "v"}},
		{"empty key", insertAction, 1, map[string]any{"id": "", "value": "v"}},
		{"extra column", insertAction, 1, map[string]any{"id": 1, "value": "v", "extra": 1}},
		{"non finite", insertAction, 1, map[string]any{"id": 1, "value": math.Inf(1)}},
		{"invalid number", insertAction, 1, map[string]any{"id": 1, "value": json.Number("not-a-number")}},
		{"invalid UTF8", insertAction, 1, map[string]any{"id": 1, "value": string([]byte{0xff})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := normalizeNativeRow(testSchema(), tc.action, tc.ts, tc.values)
			require.Error(t, err)
		})
	}
	_, err := normalizeNativeRow(testSchema(), deleteAction, 1, map[string]any{"id": 1})
	require.NoError(t, err, "a tombstone need not invent a full after image")
	for _, encoding := range []columnEncoding{binaryColumn, jsonColumn} {
		_, err := normalizeNativeValue(encoding, 123)
		require.Error(t, err)
	}
	_, err = normalizeNativeValue(jsonColumn, "not-json")
	require.Error(t, err)
}

func TestTableMetadataValidation(t *testing.T) {
	for _, change := range []func(*tableSchema){
		func(s *tableSchema) { s.database = "" },
		func(s *tableSchema) { s.table = "bad\x00name" },
		func(s *tableSchema) { s.primaryKey = nil },
		func(s *tableSchema) { s.primaryKey = []string{"missing"} },
		func(s *tableSchema) { s.primaryKey = []string{"id", "id"} },
		func(s *tableSchema) { s.columns = append(s.columns, column{name: "ID"}) },
	} {
		s := testSchema()
		change(&s)
		require.Error(t, s.validate())
	}
}
