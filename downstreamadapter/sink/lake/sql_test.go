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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRawInsertUsesParametersAndStoredPrecision(t *testing.T) {
	schema := testSchema()
	rows := []normalizedRow{
		testRow(t, deleteAction, 100, 1), testRow(t, insertAction, 100, 1), testRow(t, updateAction, 100, 1),
	}
	var clock addTimeClock
	now := time.Date(2026, 10, 24, 1, 2, 3, 456789001, time.UTC)
	statement, args := schema.insertRawSQL(rows, &clock, now)
	require.Equal(t, 18, len(args))
	require.Equal(t, 3, strings.Count(statement, "(?, ?, ?, ?, ?, ?)"))
	require.NotContains(t, statement, "payload")
	require.Equal(t, []any{"2026-10-24 01:02:03.456789", "2026-10-24 01:02:03.456790", "2026-10-24 01:02:03.456791"}, []any{args[3], args[9], args[15]})
	require.Equal(t, "delete", args[5])
	require.Equal(t, "insert", args[11])
	_, later := schema.insertRawSQL(rows[:1], &clock, now.Add(-time.Hour))
	require.Equal(t, "2026-10-24 01:02:03.456792", later[3], "clock rollback must not reverse source tie order")
}

func TestFullRawMergeSQLKeepsBaselineSemantics(t *testing.T) {
	schema := tableSchema{
		database: `lake"db`, table: `order"s`,
		columns:    []column{{name: "tenant"}, {name: "id"}, {name: `raw"bytes`, encoding: binaryColumn}, {name: "json", encoding: jsonColumn}},
		primaryKey: []string{"tenant", "id"},
	}
	require.NoError(t, schema.validate())
	statement := schema.mergeSQL(500)
	for _, want := range []string{
		`MERGE INTO "lake""db"."order""s" a`,
		`FROM "lake""db"."order""s_cdc_raw" s`,
		`s.event_pos > 500`, `h.event_pos > s.event_pos`, `h.add_time > s.add_time`,
		`PARTITION BY s.id ORDER BY s.event_pos DESC, s.add_time DESC`,
		`a."id" = b."id" AND a."tenant" = b."tenant"`,
		`unhex(get(s.raw_data, 'raw"bytes'))`, `is_null_value(get(s.raw_data, 'json'))`, `parse_json(get(s.raw_data, 'json')::VARCHAR)`,
		`WHEN MATCHED AND b."__ticdc_operation" = 'delete' THEN DELETE`,
		`WHEN NOT MATCHED AND b."__ticdc_operation" != 'delete' THEN INSERT *`,
	} {
		require.Contains(t, statement, want)
	}
	require.NotContains(t, statement, "_cdc_stream")
	require.NotContains(t, statement, "LIMIT")
	require.NotContains(t, statement, "TRY_")
	compact := schema.compactRawSQL()
	require.Contains(t, compact, `newer.id = stale.id`)
	require.Contains(t, compact, `newer.event_pos > stale.event_pos`)
	require.NotContains(t, compact, "NOW()", "latest tombstones are not TTL deleted")
	require.NotContains(t, compact, ">=", "equal physical replays must not delete every latest row")
	require.NotContains(t, schema.createRawSQL(), "STREAM")
}

func TestMergeOperationAliasDoesNotCollideWithSourceColumn(t *testing.T) {
	schema := testSchema()
	schema.columns = append(schema.columns, column{name: "action"}, column{name: "__ticdc_operation"})
	statement := schema.mergeSQL(0)
	require.Contains(t, statement, `get(s.raw_data, 'action') AS "action"`)
	require.Contains(t, statement, `s.action AS "__ticdc_operation_"`)
	require.Contains(t, statement, `b."__ticdc_operation_" = 'delete'`)
}

func TestMergeProjectionUsesLiteralJSONKeyNotKeyPath(t *testing.T) {
	schema := testSchema()
	schema.columns = append(schema.columns, column{name: `a"b`}, column{name: "a.b"}, column{name: "O'Reilly"}, column{name: `a\b`})
	sql := schema.mergeSQL(0)
	require.Contains(t, sql, `get(s.raw_data, 'a"b') AS "a""b"`)
	require.Contains(t, sql, `get(s.raw_data, 'a.b') AS "a.b"`)
	require.Contains(t, sql, `get(s.raw_data, 'O''Reilly') AS "O'Reilly"`)
	require.Contains(t, sql, `get(s.raw_data, 'a\\b') AS "a\b"`)
	require.NotContains(t, sql, "s.raw_data:")
}
