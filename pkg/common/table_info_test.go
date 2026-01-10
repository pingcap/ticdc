// Copyright 2025 PingCAP, Inc.
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

func TestCloneWithRouting(t *testing.T) {
	t.Parallel()

	t.Run("nil TableInfo", func(t *testing.T) {
		var ti *TableInfo
		cloned := ti.CloneWithRouting("target_schema", "target_table")
		require.Nil(t, cloned)
	})

	t.Run("basic cloning with routing", func(t *testing.T) {
		original := &TableInfo{
			TableName: TableName{
				Schema:  "source_db",
				Table:   "source_table",
				TableID: 123,
			},
			Charset:          "utf8mb4",
			Collate:          "utf8mb4_bin",
			Comment:          "test table",
			HasPKOrNotNullUK: true,
			UpdateTS:         1000,
		}

		cloned := original.CloneWithRouting("target_db", "target_table")

		// Verify cloned has routing applied
		require.Equal(t, "source_db", cloned.TableName.Schema)
		require.Equal(t, "source_table", cloned.TableName.Table)
		require.Equal(t, "target_db", cloned.TableName.TargetSchema)
		require.Equal(t, "target_table", cloned.TableName.TargetTable)
		require.Equal(t, int64(123), cloned.TableName.TableID)

		// Verify other fields are copied
		require.Equal(t, "utf8mb4", cloned.Charset)
		require.Equal(t, "utf8mb4_bin", cloned.Collate)
		require.Equal(t, "test table", cloned.Comment)
		require.Equal(t, true, cloned.HasPKOrNotNullUK)
		require.Equal(t, uint64(1000), cloned.UpdateTS)

		// Verify original is NOT modified
		require.Equal(t, "", original.TableName.TargetSchema)
		require.Equal(t, "", original.TableName.TargetTable)
	})

	t.Run("cloning does not affect original preSQLs", func(t *testing.T) {
		original := &TableInfo{
			TableName: TableName{
				Schema:  "source_db",
				Table:   "source_table",
				TableID: 123,
			},
		}

		// Clone with different routing
		cloned1 := original.CloneWithRouting("db1", "table1")
		cloned2 := original.CloneWithRouting("db2", "table2")

		// Each clone should have its own routing
		require.Equal(t, "db1", cloned1.TableName.TargetSchema)
		require.Equal(t, "table1", cloned1.TableName.TargetTable)
		require.Equal(t, "db2", cloned2.TableName.TargetSchema)
		require.Equal(t, "table2", cloned2.TableName.TargetTable)

		// Original should be unchanged
		require.Equal(t, "", original.TableName.TargetSchema)
		require.Equal(t, "", original.TableName.TargetTable)

		// Clones should be independent
		require.NotSame(t, cloned1, cloned2)
	})
}
