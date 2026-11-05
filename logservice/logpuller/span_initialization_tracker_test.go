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
	"github.com/stretchr/testify/require"
)

func TestSpanInitializationTracker(t *testing.T) {
	t.Parallel()

	totalSpan := heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("z")}
	tracker := &spanInitializationTracker{}

	require.False(t, tracker.add(totalSpan,
		heartbeatpb.TableSpan{StartKey: []byte("m"), EndKey: []byte("x")}))
	require.False(t, tracker.add(totalSpan,
		heartbeatpb.TableSpan{StartKey: []byte("x"), EndKey: []byte("z")}))
	require.False(t, tracker.add(totalSpan,
		heartbeatpb.TableSpan{StartKey: []byte("m"), EndKey: []byte("x")}))
	require.True(t, tracker.add(totalSpan,
		heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("m")}))
	require.False(t, tracker.add(totalSpan, totalSpan))
}

func TestSpanInitializationTrackerMergesOverlappingRanges(t *testing.T) {
	t.Parallel()

	totalSpan := heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("z")}
	tracker := &spanInitializationTracker{}

	require.False(t, tracker.add(totalSpan,
		heartbeatpb.TableSpan{StartKey: []byte("h"), EndKey: []byte("t")}))
	require.False(t, tracker.add(totalSpan,
		heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("m")}))
	require.True(t, tracker.add(totalSpan,
		heartbeatpb.TableSpan{StartKey: []byte("s"), EndKey: []byte("z")}))
}
