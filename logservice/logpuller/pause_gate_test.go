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

package logpuller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPauseGateBlocksAndResumes(t *testing.T) {
	gate := newPauseGate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate.pause()

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	released := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ok := gate.wait(ctx)
			require.True(t, ok)
			released <- struct{}{}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, len(released))

	gate.resume()

	require.Eventually(t, func() bool {
		return len(released) == n
	}, time.Second, 5*time.Millisecond)

	wg.Wait()
}

func TestPauseGateNoMissedWakeup(t *testing.T) {
	gate := newPauseGate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Stress resume racing with wait.
	for i := 0; i < 2000; i++ {
		gate.pause()
		done := make(chan struct{})
		go func() {
			_ = gate.wait(ctx)
			close(done)
		}()
		gate.resume()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("wait stuck at iter %d", i)
		}
	}
}
