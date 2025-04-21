// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewMemoryLimiter(t *testing.T) {
	t.Parallel()

	config := &MemoryLimitConfig{
		CurrentMemoryLimit:      100,
		MinMemoryLimit:          50,
		MaxMemoryLimit:          200,
		MemoryLimitIncreaseRate: 1.5,
		IncreaseInterval:        time.Millisecond * 10,
	}

	limiter := NewMemoryLimiter("test", config)
	require.NotNil(t, limiter)
	require.Equal(t, 100, limiter.GetCurrentMemoryLimit())
}

func TestMemoryLimiterDecrease(t *testing.T) {
	t.Parallel()

	// Test normal decrease
	config := &MemoryLimitConfig{
		CurrentMemoryLimit:      100,
		MinMemoryLimit:          50,
		MaxMemoryLimit:          200,
		MemoryLimitIncreaseRate: 1.5,
		IncreaseInterval:        time.Hour, // Set to a long time to avoid auto-increase during test
	}

	limiter := NewMemoryLimiter("test", config)
	require.Equal(t, 100, limiter.GetCurrentMemoryLimit())

	limiter.Decrease()
	require.Equal(t, 50, limiter.GetCurrentMemoryLimit())

	// Test decrease at minimum limit
	limiter.Decrease()
	require.Equal(t, 50, limiter.GetCurrentMemoryLimit(), "Should not decrease below MinMemoryLimit")

	// Test decrease with odd numbers
	config = &MemoryLimitConfig{
		CurrentMemoryLimit:      101,
		MinMemoryLimit:          50,
		MaxMemoryLimit:          200,
		MemoryLimitIncreaseRate: 1.5,
		IncreaseInterval:        time.Hour,
	}

	limiter = NewMemoryLimiter("test", config)
	require.Equal(t, 101, limiter.GetCurrentMemoryLimit())

	limiter.Decrease()
	require.Equal(t, 50, limiter.GetCurrentMemoryLimit(), "Should decrease to minimum if calculated value is below minimum")
}

func TestMemoryLimiterIncreaseMemoryLimit(t *testing.T) {
	t.Parallel()

	// Test normal increase
	config := &MemoryLimitConfig{
		CurrentMemoryLimit:      100,
		MinMemoryLimit:          50,
		MaxMemoryLimit:          200,
		MemoryLimitIncreaseRate: 1.5,
		IncreaseInterval:        time.Millisecond * 10,
	}

	limiter := NewMemoryLimiter("test", config)
	require.Equal(t, 100, limiter.GetCurrentMemoryLimit())

	// Manually call the private method using reflection
	limiter.increaseMemoryLimit()
	require.Equal(t, 150, limiter.GetCurrentMemoryLimit())

	// Test increase at maximum limit
	limiter.increaseMemoryLimit()
	require.Equal(t, 200, limiter.GetCurrentMemoryLimit(), "Should not increase above MaxMemoryLimit")

	// Test increase with odd rates
	config = &MemoryLimitConfig{
		CurrentMemoryLimit:      100,
		MinMemoryLimit:          50,
		MaxMemoryLimit:          300,
		MemoryLimitIncreaseRate: 2.5,
		IncreaseInterval:        time.Hour,
	}

	limiter = NewMemoryLimiter("test", config)
	require.Equal(t, 100, limiter.GetCurrentMemoryLimit())

	limiter.increaseMemoryLimit()
	require.Equal(t, 250, limiter.GetCurrentMemoryLimit())

	limiter.increaseMemoryLimit()
	require.Equal(t, 300, limiter.GetCurrentMemoryLimit(), "Should increase to maximum if calculated value is above maximum")
}

func TestMemoryLimiterRunAutoIncrease(t *testing.T) {
	t.Parallel()

	config := &MemoryLimitConfig{
		CurrentMemoryLimit:      100,
		MinMemoryLimit:          50,
		MaxMemoryLimit:          200,
		MemoryLimitIncreaseRate: 1.5,
		IncreaseInterval:        time.Millisecond * 10,
	}

	limiter := NewMemoryLimiter("test", config)
	require.Equal(t, 100, limiter.GetCurrentMemoryLimit())

	// Wait for auto-increase to happen
	time.Sleep(time.Millisecond * 30)

	// Since run() is running in a goroutine, we should observe increases
	currentLimit := limiter.GetCurrentMemoryLimit()
	require.Greater(t, currentLimit, 100, "Auto-increase should have happened")
	require.LessOrEqual(t, currentLimit, 200, "Should not exceed maximum limit")
}

func TestMemoryLimiterWaitN(t *testing.T) {
	t.Parallel()

	config := &MemoryLimitConfig{
		CurrentMemoryLimit:      100,
		MinMemoryLimit:          50,
		MaxMemoryLimit:          200,
		MemoryLimitIncreaseRate: 1.5,
		IncreaseInterval:        time.Hour,
	}

	limiter := NewMemoryLimiter("test", config)

	// Small wait that should return quickly
	start := time.Now()
	limiter.WaitN(1)
	elapsed := time.Since(start)
	require.Less(t, elapsed, time.Millisecond*100, "Small WaitN should return quickly")

	// Larger wait that should take some time
	start = time.Now()
	limiter.WaitN(100)
	elapsed = time.Since(start)

	// After consuming the current limit, next requests should be throttled
	start = time.Now()
	limiter.WaitN(10)
	elapsed = time.Since(start)
	require.GreaterOrEqual(t, elapsed, time.Millisecond*50, "Should be throttled after consuming limit")
}
