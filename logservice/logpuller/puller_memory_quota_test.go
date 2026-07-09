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

func TestPullerMemoryQuotaEventWaitsForRelease(t *testing.T) {
	quota := newPullerMemoryQuota(minPullerEventMemoryBytes)
	chargedBytes, ok := quota.acquireEvent(context.Background(), minPullerEventMemoryBytes)
	require.True(t, ok)

	done := make(chan struct{})
	go func() {
		_, ok := quota.acquireEvent(context.Background(), minPullerEventMemoryBytes)
		require.True(t, ok)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("event acquire should wait when quota is full")
	case <-time.After(100 * time.Millisecond):
	}

	quota.release(chargedBytes)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("event acquire should resume after release")
	}
}

func TestPullerMemoryQuotaCloseWakesEventWaiter(t *testing.T) {
	quota := newPullerMemoryQuota(minPullerEventMemoryBytes)
	_, ok := quota.acquireEvent(context.Background(), minPullerEventMemoryBytes)
	require.True(t, ok)

	done := make(chan bool)
	go func() {
		_, ok := quota.acquireEvent(context.Background(), minPullerEventMemoryBytes)
		done <- ok
	}()

	select {
	case <-done:
		t.Fatal("event acquire should wait before quota is closed")
	case <-time.After(100 * time.Millisecond):
	}

	quota.close()
	select {
	case ok := <-done:
		require.False(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("event acquire should resume after quota close")
	}
}

func TestPullerMemoryQuotaAllowsSingleOversizedEvent(t *testing.T) {
	quota := newPullerMemoryQuota(minPullerEventMemoryBytes)
	chargedBytes, ok := quota.acquireEvent(context.Background(), minPullerEventMemoryBytes*2)
	require.True(t, ok)
	require.Equal(t, uint64(minPullerEventMemoryBytes*2), chargedBytes)
	require.Equal(t, uint64(minPullerEventMemoryBytes*2), quota.snapshot().UsedBytes)
}

func TestPullerMemoryQuotaRegionRequestWaitsAtHighWatermark(t *testing.T) {
	quota := newPullerMemoryQuota(1000)
	chargedBytes, ok := quota.acquireEvent(context.Background(), 800)
	require.True(t, ok)

	done := make(chan struct{})
	go func() {
		require.True(t, quota.waitRegionRequest(context.Background()))
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("region request should wait when usage reaches high watermark")
	case <-time.After(100 * time.Millisecond):
	}

	quota.release(chargedBytes)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("region request should resume after release")
	}
}
