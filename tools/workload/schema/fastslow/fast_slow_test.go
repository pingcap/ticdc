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

package fastslow

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"workload/schema"
)

func TestFastSlowGrouping(t *testing.T) {
	workload := NewFastSlowWorkload(2048, 4, 10).(*FastSlowWorkload)

	require.False(t, workload.isSlowTable(10))
	require.False(t, workload.isSlowTable(11))
	require.True(t, workload.isSlowTable(12))
	require.True(t, workload.isSlowTable(13))

	require.Equal(t, "fast_table_10", workload.tableName(10))
	require.Equal(t, "slow_table_12", workload.tableName(12))
}

func TestFastSlowInsertAndUpdateSQL(t *testing.T) {
	workload := NewFastSlowWorkload(2048, 4, 0).(*FastSlowWorkload)

	fastInsert := workload.BuildInsertSql(0, 2)
	require.True(t, strings.Contains(fastInsert, "INSERT INTO fast_table_0"))
	require.True(t, strings.Contains(fastInsert, "touch_count"))

	slowInsert := workload.BuildInsertSql(2, 1)
	require.True(t, strings.Contains(slowInsert, "INSERT INTO slow_table_2"))
	require.True(t, strings.Contains(slowInsert, "payload_shadow"))

	require.Empty(t, workload.BuildUpdateSql(schema.UpdateOption{TableIndex: 3, Batch: 1}))

	fastUpdate := workload.BuildUpdateSql(schema.UpdateOption{TableIndex: 0, Batch: 2})
	require.True(t, strings.Contains(fastUpdate, "UPDATE fast_table_0"))
	require.True(t, strings.Contains(fastUpdate, "WHERE id BETWEEN"))

	slowUpdate := workload.BuildUpdateSql(schema.UpdateOption{TableIndex: 2, Batch: 8})
	require.True(t, strings.Contains(slowUpdate, "UPDATE slow_table_2"))
	require.True(t, strings.Contains(slowUpdate, "payload_shadow"))
	require.True(t, strings.Contains(slowUpdate, "WHERE id = "))
}
