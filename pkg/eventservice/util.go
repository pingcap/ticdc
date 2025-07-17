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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import (
	"context"
	"sync/atomic"
	"time"
)

// byteRateLimit implements a rate limiter for bytes using atomic operations instead of locks.
// It uses a separate goroutine to refill tokens periodically.
type byteRateLimit struct {
	// availableBytes represents the current available bytes (atomic)
	availableBytes int64
	// bytesPerSecond is the rate of bytes allowed per second
	bytesPerSecond int64
	// maxBurstBytes is the maximum burst capacity
	maxBurstBytes int64
	// ctx and cancel for controlling the refill goroutine
	ctx    context.Context
	cancel context.CancelFunc
}

// newByteRateLimit creates a new byte rate limiter.
// bytesPerSecond: the number of bytes allowed per second
// maxBurstBytes: the maximum burst capacity (bucket size)
func newByteRateLimit(bytesPerSecond, maxBurstBytes int64) *byteRateLimit {
	ctx, cancel := context.WithCancel(context.Background())

	b := &byteRateLimit{
		availableBytes: maxBurstBytes,
		bytesPerSecond: bytesPerSecond,
		maxBurstBytes:  maxBurstBytes,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Start the token refill goroutine
	go b.refillLoop()

	return b
}

// refillLoop runs in a separate goroutine and refills tokens every second
func (b *byteRateLimit) refillLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			// Refill to maximum burst capacity every second
			atomic.StoreInt64(&b.availableBytes, b.maxBurstBytes)
		}
	}
}

// AllowN checks if n bytes are available. If yes, it consumes n bytes and returns true.
// If not enough bytes are available, it returns false without consuming any bytes.
func (b *byteRateLimit) AllowN(n int64) bool {
	available := atomic.LoadInt64(&b.availableBytes)
	// Check if we have enough bytes
	if available < n {
		return false
	}
	atomic.StoreInt64(&b.availableBytes, available-n)
	return true
}

// Available returns the current number of available bytes.
// This is a snapshot and may change immediately after the call.
func (b *byteRateLimit) Available() int64 {
	return atomic.LoadInt64(&b.availableBytes)
}

// Rate returns the configured bytes per second rate.
func (b *byteRateLimit) Rate() int64 {
	return b.bytesPerSecond
}

// Burst returns the configured maximum burst capacity.
func (b *byteRateLimit) Burst() int64 {
	return b.maxBurstBytes
}

// Close stops the refill goroutine and cleans up resources.
func (b *byteRateLimit) Close() {
	if b.cancel != nil {
		b.cancel()
	}
}
