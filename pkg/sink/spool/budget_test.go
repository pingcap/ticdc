// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBudgetTracksMemoryAndDiskBytes(t *testing.T) {
	t.Parallel()

	budget := NewBudget(Limits{
		DiskQuotaBytes:     100,
		MemoryQuotaBytes:   20,
		HighWatermarkBytes: 80,
		LowWatermarkBytes:  60,
	})

	require.False(t, budget.ShouldSpill(10))
	require.False(t, budget.Acquire(10, false))
	require.Equal(t, int64(10), budget.MemoryBytes())
	require.Equal(t, int64(0), budget.DiskBytes())
	require.Equal(t, int64(10), budget.TotalBytes())

	require.True(t, budget.ShouldSpill(11))
	require.False(t, budget.Acquire(11, true))
	require.Equal(t, int64(10), budget.MemoryBytes())
	require.Equal(t, int64(11), budget.DiskBytes())
	require.Equal(t, int64(21), budget.TotalBytes())

	require.True(t, budget.Release(50, false))
	require.Equal(t, int64(0), budget.MemoryBytes())
	require.Equal(t, int64(11), budget.DiskBytes())

	require.True(t, budget.Release(50, true))
	require.Equal(t, int64(0), budget.TotalBytes())
}

func TestBudgetTracksWatermarkAndDiskQuota(t *testing.T) {
	t.Parallel()

	budget := NewBudget(Limits{
		DiskQuotaBytes:     100,
		MemoryQuotaBytes:   20,
		HighWatermarkBytes: 80,
		LowWatermarkBytes:  60,
	})

	require.True(t, budget.EntryExceedsDiskQuota(101))
	require.False(t, budget.SpillWouldExceedDiskQuota(81))
	require.True(t, budget.Acquire(81, true))
	require.True(t, budget.SpillWouldExceedDiskQuota(20))
	require.True(t, budget.Release(21, true))
}
