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

package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasicDispatcherStartTsConcurrentAccess(t *testing.T) {
	d := &BasicDispatcher{}
	const finalStartTs uint64 = 1000
	done := make(chan struct{})

	// Merge tasks update startTs while the event collector reads it.
	go func() {
		defer close(done)
		for ts := uint64(1); ts <= finalStartTs; ts++ {
			d.SetStartTs(ts)
		}
	}()

	for range finalStartTs {
		require.LessOrEqual(t, d.GetStartTs(), finalStartTs)
	}
	<-done

	require.Equal(t, finalStartTs, d.GetStartTs())
	require.Equal(t, finalStartTs, d.GetResolvedTs())
}
