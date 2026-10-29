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
	"fmt"
	"strings"
	"time"
)

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (s tableSchema) targetName() string {
	return quoteIdentifier(s.database) + "." + quoteIdentifier(s.table)
}

func (s tableSchema) rawName() string {
	return quoteIdentifier(s.database) + "." + quoteIdentifier(s.table+"_cdc_raw")
}

func (s tableSchema) createRawSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
id VARCHAR(255), table_name VARCHAR(255), raw_data JSON,
add_time TIMESTAMP, event_pos BIGINT, action VARCHAR(20))`, s.rawName())
}

// addTimeClock assigns timestamps in the precision actually sent to Lake.
// Comparing nanoseconds before formatting microseconds would lose tie ordering.
type addTimeClock struct {
	last time.Time
}

func (c *addTimeClock) next(candidate time.Time) time.Time {
	candidate = candidate.UTC().Truncate(time.Microsecond)
	if !c.last.IsZero() && !candidate.After(c.last) {
		candidate = c.last.Add(time.Microsecond)
	}
	c.last = candidate
	return candidate
}

func (s tableSchema) insertRawSQL(rows []normalizedRow, clock *addTimeClock, now time.Time) (string, []any) {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*6)
	for i, row := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?)"
		args = append(args, row.id, s.table, row.payload,
			clock.next(now).Format("2006-01-02 15:04:05.000000"), int64(row.commitTs), string(row.action))
	}
	return "INSERT INTO " + s.rawName() +
		" (id, table_name, raw_data, add_time, event_pos, action) VALUES " + strings.Join(placeholders, ", "), args
}

func (s tableSchema) mergeSQL(epoch uint64) string {
	projections := make([]string, 0, len(s.columns))
	for _, col := range s.columns {
		// A source name is a literal JSON object key, not a JSONPath. Quoting
		// it as an SQL identifier still leaves embedded quotes unescaped in
		// Lake's get_by_keypath conversion. GET uses the exact string key.
		path := "get(s.raw_data, " + sqlString(col.name) + ")"
		alias := quoteIdentifier(col.name)
		switch col.encoding {
		case binaryColumn:
			projections = append(projections, fmt.Sprintf("unhex(%s) AS %s", path, alias))
		case jsonColumn:
			projections = append(projections, fmt.Sprintf(
				"CASE WHEN %[1]s IS NULL OR is_null_value(%[1]s) THEN NULL ELSE parse_json(%[1]s::VARCHAR) END AS %[2]s", path, alias))
		default:
			projections = append(projections, path+" AS "+alias)
		}
	}
	// Keep the operation discriminator separate from arbitrary source names.
	operation := "__ticdc_operation"
	for {
		collision := false
		for _, c := range s.columns {
			if strings.EqualFold(c.name, operation) {
				collision = true
				break
			}
		}
		if !collision {
			break
		}
		operation += "_"
	}
	op := quoteIdentifier(operation)
	joins := make([]string, 0, len(s.primaryKey))
	for _, key := range s.canonicalKeys() {
		quoted := quoteIdentifier(key)
		joins = append(joins, "a."+quoted+" = b."+quoted)
	}
	// The source is deliberately raw, not a stream. This is bend-cdc's strict
	// ordering fallback, including latest tombstones and the applied DDL fence.
	return fmt.Sprintf(`MERGE INTO %s a USING (
SELECT %s, s.action AS %s FROM %s s
WHERE s.event_pos > %d AND NOT EXISTS (
SELECT 1 FROM %s h WHERE h.id = s.id AND (
h.event_pos > s.event_pos OR (h.event_pos = s.event_pos AND h.add_time > s.add_time)))
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY s.event_pos DESC, s.add_time DESC) = 1
) b ON %s
WHEN MATCHED AND b.%s != 'delete' THEN UPDATE *
WHEN MATCHED AND b.%s = 'delete' THEN DELETE
WHEN NOT MATCHED AND b.%s != 'delete' THEN INSERT *`,
		s.targetName(), strings.Join(projections, ", "), op, s.rawName(), epoch, s.rawName(), strings.Join(joins, " AND "), op, op, op)
}

func (s tableSchema) compactRawSQL() string {
	return fmt.Sprintf(`DELETE FROM %s AS stale WHERE EXISTS (
SELECT 1 FROM %s AS newer WHERE newer.id = stale.id AND (
newer.event_pos > stale.event_pos OR (newer.event_pos = stale.event_pos AND newer.add_time > stale.add_time)))`, s.rawName(), s.rawName())
}
