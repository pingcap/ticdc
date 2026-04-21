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

package common

import "testing"

func TestTableNameIsRouted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName TableName
		expected  bool
	}{
		{
			name:      "not routed",
			tableName: TableName{Schema: "test", Table: "t"},
			expected:  false,
		},
		{
			name:      "target schema only",
			tableName: TableName{Schema: "test", Table: "t", TargetSchema: "target"},
			expected:  true,
		},
		{
			name:      "target table only",
			tableName: TableName{Schema: "test", Table: "t", TargetTable: "target_t"},
			expected:  true,
		},
		{
			name:      "target schema and table",
			tableName: TableName{Schema: "test", Table: "t", TargetSchema: "target", TargetTable: "target_t"},
			expected:  true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.tableName.IsRouted(); got != tt.expected {
				t.Fatalf("IsRouted() = %v, expected %v", got, tt.expected)
			}
		})
	}
}
