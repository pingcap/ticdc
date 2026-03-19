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

func TestBudgetCoreTracksMemoryAndDiskBytes(t *testing.T) {
	t.Parallel()

	core := newBudgetCore(&Options{
		QuotaBytes:         100,
		MemoryRatio:        0.2,
		HighWatermarkRatio: 0.8,
		LowWatermarkRatio:  0.6,
	})

	require.False(t, core.shouldSpill(10))

	snapshot := core.reserve(10, false)
	require.Equal(t, int64(10), snapshot.memoryBytes)
	require.Equal(t, int64(0), snapshot.diskBytes)

	require.True(t, core.shouldSpill(11))

	snapshot = core.reserve(11, true)
	require.Equal(t, int64(10), snapshot.memoryBytes)
	require.Equal(t, int64(11), snapshot.diskBytes)

	snapshot = core.release(50, false)
	require.Equal(t, int64(0), snapshot.memoryBytes)
	require.Equal(t, int64(11), snapshot.diskBytes)

	snapshot = core.release(50, true)
	require.Equal(t, int64(0), snapshot.memoryBytes)
	require.Equal(t, int64(0), snapshot.diskBytes)
}

func TestBudgetCoreTracksWatermarkState(t *testing.T) {
	t.Parallel()

	core := newBudgetCore(&Options{
		QuotaBytes:         100,
		MemoryRatio:        0.2,
		HighWatermarkRatio: 0.8,
		LowWatermarkRatio:  0.6,
	})

	require.False(t, core.overHighWatermark())
	require.True(t, core.atOrBelowLowWatermark())

	core.reserve(81, true)
	require.True(t, core.overHighWatermark())
	require.False(t, core.atOrBelowLowWatermark())

	core.release(21, true)
	require.False(t, core.overHighWatermark())
	require.True(t, core.atOrBelowLowWatermark())

	snapshot := core.reset()
	require.Equal(t, int64(0), snapshot.memoryBytes)
	require.Equal(t, int64(0), snapshot.diskBytes)
	require.True(t, core.atOrBelowLowWatermark())
}
