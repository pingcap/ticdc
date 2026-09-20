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

package event

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestExtractTableSchemas(t *testing.T) {
	cases := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "unqualified table",
			query:    "SELECT * FROM `t`",
			expected: []string{""},
		},
		{
			name:     "mixed qualified tables",
			query:    "SELECT * FROM `db1`.`t1` JOIN `t2` ON `db1`.`t1`.`id` = `t2`.`id`",
			expected: []string{"db1", ""},
		},
		{
			name:     "subquery preserves visit order",
			query:    "SELECT * FROM `db1`.`t1` WHERE EXISTS (SELECT 1 FROM `db2`.`t2`)",
			expected: []string{"db1", "db2"},
		},
	}

	p := parser.New()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(tc.query, "", "")
			require.NoError(t, err)
			require.Equal(t, tc.expected, extractTableSchemas(stmt))
		})
	}

	require.Nil(t, extractTableSchemas(nil))
}

func TestCorrelatedColumns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  string
	}{
		{"outer table", "SELECT * FROM source_db.orders WHERE EXISTS (SELECT 1 FROM source_db.lines WHERE lines.id = orders.id)", "`source_db`.`lines`.`id`=`source_db`.`orders`.`id`"},
		{"multiple levels", "SELECT * FROM source_db.orders WHERE EXISTS (SELECT 1 FROM source_db.lines WHERE EXISTS (SELECT 1 WHERE orders.id = 1))", "`source_db`.`orders`.`id`=1"},
		{"alias shadows outer", "SELECT * FROM source_db.orders WHERE EXISTS (SELECT 1 FROM source_db.lines AS orders WHERE orders.id = 1)", "WHERE `orders`.`id`=1"},
		{"local table shadows outer", "SELECT * FROM source_db.orders WHERE EXISTS (SELECT 1 FROM other_db.orders WHERE orders.id = 1)", "WHERE `other_db`.`orders`.`id`=1"},
		{"unqualified local table", "SELECT * FROM source_db.orders WHERE EXISTS (SELECT 1 FROM orders WHERE orders.id = 1)", "WHERE `orders`.`id`=1"},
		{"CTE shadows outer", "SELECT * FROM source_db.orders WHERE EXISTS (WITH orders AS (SELECT 1 AS id) SELECT 1 FROM orders WHERE orders.id = 1)", "WHERE `orders`.`id`=1"},
		{"CTE definition skips consumer table", "SELECT t.id FROM source_db.t WHERE EXISTS (WITH c AS (SELECT t.id AS id) SELECT 1 FROM other_db.t JOIN c ON c.id = other_db.t.id)", "WITH `c` AS (SELECT `source_db`.`t`.`id` AS `id`)"},
		{"CTE on union retains outer table", "SELECT t.id FROM source_db.t WHERE EXISTS (WITH c AS (SELECT t.id AS id) SELECT id FROM c UNION SELECT id FROM c)", "WITH `c` AS (SELECT `source_db`.`t`.`id` AS `id`)"},
		{"ambiguous local table", "SELECT * FROM source_db.orders WHERE EXISTS (SELECT 1 FROM a.orders JOIN b.orders WHERE orders.id = 1)", "WHERE `orders`.`id`=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(tc.query, "", "")
			require.NoError(t, err)
			normalizeCreateViewSelect(stmt, "source_db")
			query, err := Restore(stmt)
			require.NoError(t, err)
			require.Contains(t, query, tc.want)
		})
	}
}
