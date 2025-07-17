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

package eventservice

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestByteRateLimit_Basic(t *testing.T) {
	// Create a rate limiter: 1000 bytes/sec, burst 2000 bytes
	limiter := newByteRateLimit(1000, 2000)

	// Should allow initial burst
	require.True(t, limiter.AllowN(2000))
	require.Equal(t, int64(0), limiter.Available())

	// Should not allow more than burst capacity
	require.False(t, limiter.AllowN(1))

	// Wait for some bytes to be replenished
	time.Sleep(100 * time.Millisecond)

	// Should have approximately 100 bytes available (1000 bytes/sec * 0.1 sec)
	// Allow some tolerance due to timing
	available := limiter.Available()
	require.True(t, available >= 90 && available <= 110, "expected ~100 bytes, got %d", available)

	// Should allow the available amount
	require.True(t, limiter.AllowN(90))
}

func TestByteRateLimit_RateLimiting(t *testing.T) {
	// Create a rate limiter: 100 bytes/sec, burst 100 bytes
	limiter := newByteRateLimit(100, 100)

	// Consume the burst capacity
	require.True(t, limiter.AllowN(100))
	require.False(t, limiter.AllowN(1))

	// Wait for 0.5 seconds, should have ~50 bytes
	time.Sleep(500 * time.Millisecond)

	// Should be able to consume around 50 bytes
	require.True(t, limiter.AllowN(40))
	require.False(t, limiter.AllowN(20)) // Should not have 20 more bytes
}

func TestByteRateLimit_BurstCapacity(t *testing.T) {
	// Create a rate limiter: 1000 bytes/sec, burst 500 bytes
	limiter := newByteRateLimit(1000, 500)

	// Should start with burst capacity
	require.Equal(t, int64(500), limiter.Available())

	// Wait for more than enough time to exceed burst capacity
	time.Sleep(1 * time.Second)

	// Should be capped at burst capacity
	require.Equal(t, int64(500), limiter.Available())
}

func TestByteRateLimit_Concurrent(t *testing.T) {
	// Create a rate limiter: 10000 bytes/sec, burst 10000 bytes
	limiter := newByteRateLimit(10000, 10000)

	const numGoroutines = 10
	const bytesPerGoroutine = 500

	var wg sync.WaitGroup
	successCount := int64(0)
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.AllowN(bytesPerGoroutine) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Should have allowed some goroutines (at least 10000 / 500 = 20, but we only have 10)
	// But due to timing and initial burst, all should succeed
	require.Equal(t, int64(numGoroutines), successCount)
}

func TestByteRateLimit_ZeroRequest(t *testing.T) {
	limiter := newByteRateLimit(1000, 1000)

	// Zero bytes should always be allowed
	require.True(t, limiter.AllowN(0))
	require.True(t, limiter.AllowN(0))

	// Available bytes should not change
	require.Equal(t, int64(1000), limiter.Available())
}

func TestByteRateLimit_GettersAndSetters(t *testing.T) {
	limiter := newByteRateLimit(1500, 3000)

	require.Equal(t, int64(1500), limiter.Rate())
	require.Equal(t, int64(3000), limiter.Burst())
}

func TestByteRateLimit_LargeRequest(t *testing.T) {
	limiter := newByteRateLimit(1000, 1000)

	// Request more than burst capacity should fail
	require.False(t, limiter.AllowN(1001))

	// Burst capacity should still be available
	require.Equal(t, int64(1000), limiter.Available())

	// Should still be able to use the full burst capacity
	require.True(t, limiter.AllowN(1000))
}

// Benchmark our atomic-based implementation
func BenchmarkByteRateLimit_AllowN(b *testing.B) {
	limiter := newByteRateLimit(1000000, 1000000) // 1MB/sec, 1MB burst
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.AllowN(1000) // 1KB request
		}
	})
}

// Benchmark golang.org/x/time/rate for comparison
func BenchmarkStandardRateLimit_AllowN(b *testing.B) {
	limiter := rate.NewLimiter(1000000, 1000000) // 1MB/sec, 1MB burst
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.AllowN(time.Now(), 1000) // 1KB request
		}
	})
}

// Benchmark concurrent access with high contention
func BenchmarkByteRateLimit_HighContention(b *testing.B) {
	limiter := newByteRateLimit(1000000, 1000000) // 1MB/sec, 1MB burst
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.AllowN(100) // Small requests for high contention
		}
	})
}

func BenchmarkStandardRateLimit_HighContention(b *testing.B) {
	limiter := rate.NewLimiter(1000000, 1000000) // 1MB/sec, 1MB burst
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.AllowN(time.Now(), 100) // Small requests for high contention
		}
	})
}
