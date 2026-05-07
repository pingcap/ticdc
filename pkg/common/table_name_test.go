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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestTableNameTargetAccessors(t *testing.T) {
	t.Parallel()

	t.Run("fallback to source names", func(t *testing.T) {
		tableName := TableName{
			Schema: "source_db",
			Table:  "source_table",
		}

		require.Equal(t, "source_db", tableName.GetTargetSchema())
		require.Equal(t, "source_table", tableName.GetTargetTable())
		require.Equal(t, "`source_db`.`source_table`", tableName.QuoteTargetString())
	})

	t.Run("use routed names when present", func(t *testing.T) {
		tableName := TableName{
			Schema:       "source_db",
			Table:        "source_table",
			TargetSchema: "target_db",
			TargetTable:  "target_table",
		}

		require.Equal(t, "target_db", tableName.GetTargetSchema())
		require.Equal(t, "target_table", tableName.GetTargetTable())
		require.Equal(t, "`target_db`.`target_table`", tableName.QuoteTargetString())
	})
}

func TestTableNameMsgpackRoundTripDropsRoutingOverlay(t *testing.T) {
	t.Parallel()

	original := TableName{
		Schema:       "source_db",
		Table:        "source_table",
		TableID:      42,
		IsPartition:  true,
		TargetSchema: "target_db",
		TargetTable:  "target_table",
	}

	data, err := original.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded TableName
	rest, err := decoded.UnmarshalMsg(data)
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, original.Schema, decoded.Schema)
	require.Equal(t, original.Table, decoded.Table)
	require.Equal(t, original.TableID, decoded.TableID)
	require.Equal(t, original.IsPartition, decoded.IsPartition)
	require.Empty(t, decoded.TargetSchema)
	require.Empty(t, decoded.TargetTable)
	require.Equal(t, original.Schema, decoded.GetTargetSchema())
	require.Equal(t, original.Table, decoded.GetTargetTable())
}
