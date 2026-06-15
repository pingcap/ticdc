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
