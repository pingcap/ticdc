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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPullerMemoryQuotaThresholds(t *testing.T) {
	quota := newPullerMemoryQuota()

	pause, resume, _ := quota.ShouldPauseArea(false, 49, 100)
	require.False(t, pause)
	require.False(t, resume)
	_, scanPaused := quota.regionScanResumeNotify()
	require.False(t, scanPaused)

	pause, resume, _ = quota.ShouldPauseArea(false, 50, 100)
	require.False(t, pause)
	require.False(t, resume)
	_, scanPaused = quota.regionScanResumeNotify()
	require.True(t, scanPaused)

	pause, resume, _ = quota.ShouldPauseArea(false, 100, 100)
	require.True(t, pause)
	require.False(t, resume)

	pause, resume, _ = quota.ShouldPauseArea(true, 99, 100)
	require.False(t, pause)
	require.True(t, resume)
	_, scanPaused = quota.regionScanResumeNotify()
	require.True(t, scanPaused)

	quota.ShouldPauseArea(false, 40, 100)
	_, scanPaused = quota.regionScanResumeNotify()
	require.True(t, scanPaused)

	quota.ShouldPauseArea(false, 39, 100)
	_, scanPaused = quota.regionScanResumeNotify()
	require.False(t, scanPaused)
	require.NoError(t, quota.waitRegionScanAllowed(context.Background()))
}

func TestPullerMemoryQuotaWaitCancellation(t *testing.T) {
	quota := newPullerMemoryQuota()
	quota.ShouldPauseArea(false, 50, 100)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- quota.waitRegionScanAllowed(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("wait returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestProducerGateStopsWaitingAfterUnsubscribe(t *testing.T) {
	quota := newPullerMemoryQuota()
	quota.ShouldPauseArea(false, 50, 100)
	client := &subscriptionClient{memoryQuota: quota}
	span := &subscribedSpan{stoppedCh: make(chan struct{})}

	type result struct {
		stopped bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		stopped, err := client.waitRegionScanAllowed(context.Background(), span)
		done <- result{stopped: stopped, err: err}
	}()

	span.stopped.Store(true)
	close(span.stoppedCh)
	res := <-done
	require.NoError(t, res.err)
	require.True(t, res.stopped)
}
