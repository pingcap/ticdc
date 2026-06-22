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

func TestPullerMemoryQuotaAdmissionAndScanThresholds(t *testing.T) {
	quota := newPullerMemoryQuota(100)
	ctx := context.Background()

	first, err := quota.acquire(ctx, 1, 49, nil)
	require.NoError(t, err)
	_, scanPaused := quota.regionScanResumeNotify()
	require.False(t, scanPaused)

	second, err := quota.acquire(ctx, 1, 1, nil)
	require.NoError(t, err)
	_, scanPaused = quota.regionScanResumeNotify()
	require.True(t, scanPaused)

	third, err := quota.acquire(ctx, 1, 50, nil)
	require.NoError(t, err)
	used, capacity := quota.usage()
	require.Equal(t, uint64(100), used)
	require.Equal(t, uint64(100), capacity)

	blocked := make(chan *pullerMemoryReservation, 1)
	go func() {
		reservation, acquireErr := quota.acquire(ctx, 2, 1, nil)
		require.NoError(t, acquireErr)
		blocked <- reservation
	}()
	select {
	case <-blocked:
		t.Fatal("event admitted when quota was full")
	case <-time.After(20 * time.Millisecond):
	}

	third.release()
	fourth := <-blocked
	used, _ = quota.usage()
	require.Equal(t, uint64(51), used)
	_, scanPaused = quota.regionScanResumeNotify()
	require.True(t, scanPaused)

	first.release()
	_, scanPaused = quota.regionScanResumeNotify()
	require.False(t, scanPaused)
	require.NoError(t, quota.waitRegionScanAllowed(ctx))

	second.release()
	fourth.release()
	used, _ = quota.usage()
	require.Zero(t, used)
}

func TestPullerMemoryQuotaReleaseSubscription(t *testing.T) {
	quota := newPullerMemoryQuota(100)
	reservation, err := quota.acquire(context.Background(), 1, 60, nil)
	require.NoError(t, err)

	quota.releaseSubscription(1)
	used, _ := quota.usage()
	require.Zero(t, used)

	// A stale event can still be handled after subscription cleanup. Its release
	// must not affect a later generation of accounting.
	newReservation, err := quota.acquire(context.Background(), 1, 20, nil)
	require.NoError(t, err)
	reservation.release()
	used, _ = quota.usage()
	require.Equal(t, uint64(20), used)
	newReservation.release()
}

func TestPullerMemoryQuotaOversizedEventRunsAlone(t *testing.T) {
	quota := newPullerMemoryQuota(100)
	reservation, err := quota.acquire(context.Background(), 1, 101, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, acquireErr := quota.acquire(ctx, 2, 1, nil)
		done <- acquireErr
	}()
	select {
	case err := <-done:
		t.Fatalf("second event admitted with oversized event: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	reservation.release()
}

func TestProducerGateStopsWaitingAfterUnsubscribe(t *testing.T) {
	quota := newPullerMemoryQuota(100)
	reservation, err := quota.acquire(context.Background(), 1, 50, nil)
	require.NoError(t, err)
	defer reservation.release()
	client := &subscriptionClient{memoryQuota: quota}
	span := &subscribedSpan{stoppedCh: make(chan struct{})}

	type result struct {
		stopped bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		stopped, waitErr := client.waitRegionScanAllowed(context.Background(), span)
		done <- result{stopped: stopped, err: waitErr}
	}()

	span.stopped.Store(true)
	close(span.stoppedCh)
	res := <-done
	require.NoError(t, res.err)
	require.True(t, res.stopped)
}
