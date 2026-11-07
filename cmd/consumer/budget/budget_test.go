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

package budget

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testBudget(limit int64) *Budget {
	return &Budget{limit: limit, changed: make(chan struct{})}
}

func TestAcquireWaitsForRelease(t *testing.T) {
	b := testBudget(10)
	b.ForceAcquire(6)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	acquired := make(chan error, 1)
	go func() { acquired <- b.Acquire(ctx, 8) }()

	require.Eventually(t, func() bool {
		return b.requested.Load() != 0
	}, 5*time.Second, time.Millisecond)
	select {
	case err := <-acquired:
		t.Fatalf("acquire completed without capacity: %v", err)
	default:
	}

	b.Release(6)
	require.NoError(t, <-acquired)
	require.Equal(t, int64(8), b.Used())
}

func TestAcquireHonorsCancellation(t *testing.T) {
	b := testBudget(10)
	b.ForceAcquire(6)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, b.Acquire(ctx, 8), context.DeadlineExceeded)
}

func TestReleaseReturnsCapacity(t *testing.T) {
	b := testBudget(10)
	b.ForceAcquire(4)
	require.Equal(t, int64(4), b.Used())
	b.Release(4)
	require.Zero(t, b.Used())
}
