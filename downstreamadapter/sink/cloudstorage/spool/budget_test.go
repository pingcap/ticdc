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

	core := newBudget(&options{
		diskQuotaBytes:     100,
		memoryRatio:        0.2,
		highWatermarkRatio: 0.8,
		lowWatermarkRatio:  0.6,
	})

	require.False(t, core.shouldSpill(10))

	overHighWatermark := core.acquire(10, false)
	require.False(t, overHighWatermark)
	require.Equal(t, int64(10), core.memoryBytes)
	require.Equal(t, int64(0), core.diskBytes)
	require.Equal(t, int64(10), core.totalBytes())

	require.True(t, core.shouldSpill(11))

	overHighWatermark = core.acquire(11, true)
	require.False(t, overHighWatermark)
	require.Equal(t, int64(10), core.memoryBytes)
	require.Equal(t, int64(11), core.diskBytes)
	require.Equal(t, int64(21), core.totalBytes())

	atOrBelowLowWatermark := core.release(50, false)
	require.True(t, atOrBelowLowWatermark)
	require.Equal(t, int64(0), core.memoryBytes)
	require.Equal(t, int64(11), core.diskBytes)
	require.Equal(t, int64(11), core.totalBytes())

	atOrBelowLowWatermark = core.release(50, true)
	require.True(t, atOrBelowLowWatermark)
	require.Equal(t, int64(0), core.memoryBytes)
	require.Equal(t, int64(0), core.diskBytes)
	require.Equal(t, int64(0), core.totalBytes())
}

func TestBudgetTracksWatermarkState(t *testing.T) {
	t.Parallel()

	core := newBudget(&options{
		diskQuotaBytes:     100,
		memoryRatio:        0.2,
		highWatermarkRatio: 0.8,
		lowWatermarkRatio:  0.6,
	})

	require.LessOrEqual(t, core.totalBytes(), core.lowWatermarkBytes)
	require.LessOrEqual(t, core.totalBytes(), core.highWatermarkBytes)

	overHighWatermark := core.acquire(81, true)
	require.True(t, overHighWatermark)
	require.Greater(t, core.totalBytes(), core.lowWatermarkBytes)

	atOrBelowLowWatermark := core.release(21, true)
	require.True(t, atOrBelowLowWatermark)
	require.LessOrEqual(t, core.totalBytes(), core.highWatermarkBytes)

	core.reset()
	require.Equal(t, int64(0), core.memoryBytes)
	require.Equal(t, int64(0), core.diskBytes)
	require.Equal(t, int64(0), core.totalBytes())
	require.LessOrEqual(t, core.totalBytes(), core.lowWatermarkBytes)
}
