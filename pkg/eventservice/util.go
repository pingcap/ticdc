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
	"sync/atomic"
	"time"
)

// byteRateLimit implements a rate limiter for bytes using atomic operations instead of locks.
// It allows a certain number of bytes per second and supports burst capacity.
type byteRateLimit struct {
	// availableBytes represents the current available bytes (atomic)
	availableBytes int64
	// bytesPerSecond is the rate of bytes allowed per second
	bytesPerSecond int64
	// maxBurstBytes is the maximum burst capacity
	maxBurstBytes int64
	// lastUpdateNano is the timestamp of last update in nanoseconds (atomic)
	lastUpdateNano int64
}

// newByteRateLimit creates a new byte rate limiter.
// bytesPerSecond: the number of bytes allowed per second
// maxBurstBytes: the maximum burst capacity (bucket size)
func newByteRateLimit(bytesPerSecond, maxBurstBytes int64) *byteRateLimit {
	now := time.Now().UnixNano()
	return &byteRateLimit{
		availableBytes: maxBurstBytes,
		bytesPerSecond: bytesPerSecond,
		maxBurstBytes:  maxBurstBytes,
		lastUpdateNano: now,
	}
}

// AllowN checks if n bytes are available. If yes, it consumes n bytes and returns true.
// If not enough bytes are available, it returns false without consuming any bytes.
func (b *byteRateLimit) AllowN(n int64) bool {
	now := time.Now().UnixNano()

	for {
		lastUpdate := atomic.LoadInt64(&b.lastUpdateNano)
		available := atomic.LoadInt64(&b.availableBytes)

		// Calculate how many bytes should be added based on elapsed time
		elapsed := now - lastUpdate
		if elapsed > 0 {
			// Add bytes based on the rate (bytes per second)
			bytesToAdd := (elapsed * b.bytesPerSecond) / int64(time.Second)
			newAvailable := available + bytesToAdd

			// Cap at maximum burst capacity
			if newAvailable > b.maxBurstBytes {
				newAvailable = b.maxBurstBytes
			}

			// Try to update both timestamp and available bytes atomically
			if atomic.CompareAndSwapInt64(&b.lastUpdateNano, lastUpdate, now) {
				atomic.StoreInt64(&b.availableBytes, newAvailable)
				available = newAvailable
			} else {
				// Another goroutine updated the timestamp, retry
				continue
			}
		}

		// Check if we have enough bytes
		if available < n {
			return false
		}

		// Try to consume n bytes
		if atomic.CompareAndSwapInt64(&b.availableBytes, available, available-n) {
			return true
		}

		// Another goroutine modified availableBytes, retry
	}
}

// Available returns the current number of available bytes.
// This is a snapshot and may change immediately after the call.
func (b *byteRateLimit) Available() int64 {
	now := time.Now().UnixNano()
	lastUpdate := atomic.LoadInt64(&b.lastUpdateNano)
	available := atomic.LoadInt64(&b.availableBytes)

	// Calculate potential available bytes (without updating the actual state)
	elapsed := now - lastUpdate
	if elapsed > 0 {
		bytesToAdd := (elapsed * b.bytesPerSecond) / int64(time.Second)
		available += bytesToAdd
		if available > b.maxBurstBytes {
			available = b.maxBurstBytes
		}
	}

	return available
}

// Rate returns the configured bytes per second rate.
func (b *byteRateLimit) Rate() int64 {
	return b.bytesPerSecond
}

// Burst returns the configured maximum burst capacity.
func (b *byteRateLimit) Burst() int64 {
	return b.maxBurstBytes
}
