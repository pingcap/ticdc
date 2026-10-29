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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lake

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompletionRequiresSealAndAllFragments(t *testing.T) {
	var calls atomic.Int64
	c := newEventCompletion(func() { calls.Add(1) })
	first, ok := c.register()
	require.True(t, ok)
	first.applied()
	first.applied()
	require.Zero(t, calls.Load(), "registered==applied while OPEN is not complete")
	second, ok := c.register()
	require.True(t, ok)
	c.seal()
	require.Zero(t, calls.Load())
	second.applied()
	c.seal()
	second.applied()
	require.EqualValues(t, 1, calls.Load())
	_, ok = c.register()
	require.False(t, ok)
}

func TestCompletionFailureAndEmptyEvent(t *testing.T) {
	var calls atomic.Int64
	c := newEventCompletion(func() { calls.Add(1) })
	f, ok := c.register()
	require.True(t, ok)
	c.fail() // Resource cleanup must not manufacture an application result.
	c.seal()
	f.applied() // Even a late worker result cannot revive a failed event.
	require.Zero(t, calls.Load())
	_, ok = c.register()
	require.False(t, ok)
	empty := newEventCompletion(func() { calls.Add(1) })
	empty.seal()
	empty.seal()
	require.EqualValues(t, 1, calls.Load())
}

func TestCompletionConcurrentSealAndReentrantCallback(t *testing.T) {
	for range 50 {
		var calls atomic.Int64
		var last *fragmentCompletion
		c := newEventCompletion(func() {
			calls.Add(1)
			last.applied() // Source callbacks may re-enter after completion.
		})
		fragments := make([]*fragmentCompletion, 16)
		for i := range fragments {
			f, ok := c.register()
			require.True(t, ok)
			fragments[i] = f
		}
		last = fragments[len(fragments)-1]
		var wg sync.WaitGroup
		for _, f := range fragments {
			wg.Go(func() {
				f.applied()
				f.applied()
			})
		}
		wg.Go(c.seal)
		wg.Wait()
		require.EqualValues(t, 1, calls.Load())
	}
}
