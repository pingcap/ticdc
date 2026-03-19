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
		quotaBytes:         100,
		memoryRatio:        0.2,
		highWatermarkRatio: 0.8,
		lowWatermarkRatio:  0.6,
	})

	require.False(t, core.shouldSpill(10))

	core.reserve(10, false)
	require.Equal(t, int64(10), core.memoryBytes)
	require.Equal(t, int64(0), core.diskBytes)
	require.Equal(t, int64(10), core.totalBytes())

	require.True(t, core.shouldSpill(11))

	core.reserve(11, true)
	require.Equal(t, int64(10), core.memoryBytes)
	require.Equal(t, int64(11), core.diskBytes)
	require.Equal(t, int64(21), core.totalBytes())

	core.release(50, false)
	require.Equal(t, int64(0), core.memoryBytes)
	require.Equal(t, int64(11), core.diskBytes)
	require.Equal(t, int64(11), core.totalBytes())

	core.release(50, true)
	require.Equal(t, int64(0), core.memoryBytes)
	require.Equal(t, int64(0), core.diskBytes)
	require.Equal(t, int64(0), core.totalBytes())
}

func TestBudgetTracksWatermarkState(t *testing.T) {
	t.Parallel()

	core := newBudget(&options{
		quotaBytes:         100,
		memoryRatio:        0.2,
		highWatermarkRatio: 0.8,
		lowWatermarkRatio:  0.6,
	})

	require.False(t, core.overHighWatermark())
	require.True(t, core.atOrBelowLowWatermark())

	core.reserve(81, true)
	require.True(t, core.overHighWatermark())
	require.False(t, core.atOrBelowLowWatermark())

	core.release(21, true)
	require.False(t, core.overHighWatermark())
	require.True(t, core.atOrBelowLowWatermark())

	core.reset()
	require.Equal(t, int64(0), core.memoryBytes)
	require.Equal(t, int64(0), core.diskBytes)
	require.Equal(t, int64(0), core.totalBytes())
	require.True(t, core.atOrBelowLowWatermark())
}
