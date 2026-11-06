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

package logpuller

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func withRedactMode(t *testing.T, mode string) func() {
	t.Helper()
	original := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(mode)
	return func() {
		errors.RedactLogEnabled.Store(original)
	}
}

func TestFormatUnlockedRanges(t *testing.T) {
	ranges := []regionlock.UnLockRangeStatistic{
		{Span: heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{0x74, 0x80}, EndKey: []byte{0x74, 0x81}}, ResolvedTs: 100},
	}

	restore := withRedactMode(t, "OFF")
	require.Equal(t, []string{"tableID: 1, startKey: 7480, endKey: 7481, resolvedTs: 100"}, formatUnlockedRanges(ranges))
	restore()

	restore = withRedactMode(t, "ON")
	require.Equal(t, []string{"tableID: 1, startKey: ?, endKey: ?, resolvedTs: 100"}, formatUnlockedRanges(ranges))
	restore()

	defer withRedactMode(t, "MARKER")()
	require.Equal(t, []string{"tableID: 1, startKey: ‹7480›, endKey: ‹7481›, resolvedTs: 100"},
		formatUnlockedRanges(ranges))
	require.Empty(t, formatUnlockedRanges(nil))
}
