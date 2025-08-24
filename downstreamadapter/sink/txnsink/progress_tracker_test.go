// Copyright 2024 PingCAP, Inc.
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

package txnsink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProgressTracker_Basic(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Initial state
	require.Equal(t, uint64(0), tracker.GetEffectiveTs())
	require.Equal(t, uint64(0), tracker.GetProgress())
}

func TestProgressTracker_AddPendingTxn(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Add pending transaction
	tracker.AddPendingTxn(100, 1)
	require.Equal(t, uint64(99), tracker.GetEffectiveTs()) // min(pending) - 1 = 100 - 1 = 99

	// Add another pending transaction
	tracker.AddPendingTxn(200, 2)
	require.Equal(t, uint64(99), tracker.GetEffectiveTs()) // Still 99, as 100 is still pending

	progress := tracker.GetProgress()
	require.Equal(t, uint64(99), progress)
}

func TestProgressTracker_RemoveCompletedTxn(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Add pending transactions
	tracker.AddPendingTxn(100, 1)
	tracker.AddPendingTxn(200, 2)

	// Remove completed transaction (200, 2)
	tracker.RemoveCompletedTxn(200, 2)
	require.Equal(t, uint64(99), tracker.GetEffectiveTs()) // Still 99, as 100 is still pending

	// Remove completed transaction (100, 1)
	tracker.RemoveCompletedTxn(100, 1)
	require.Equal(t, uint64(0), tracker.GetEffectiveTs()) // No pending, use checkpointTs (0)

	progress := tracker.GetProgress()
	require.Equal(t, uint64(0), progress)
}

func TestProgressTracker_UpdateCheckpointTs(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Update checkpoint TS
	tracker.UpdateCheckpointTs(100)
	require.Equal(t, uint64(100), tracker.GetEffectiveTs()) // No pending, use checkpointTs

	// Add pending transaction
	tracker.AddPendingTxn(50, 1)
	require.Equal(t, uint64(49), tracker.GetEffectiveTs()) // min(pending) - 1 = 50 - 1 = 49

	// Update checkpoint TS (should not affect effective TS when there are pending transactions)
	tracker.UpdateCheckpointTs(200)
	require.Equal(t, uint64(49), tracker.GetEffectiveTs()) // Still 49, as 50 is still pending

	// Remove pending transaction
	tracker.RemoveCompletedTxn(50, 1)
	require.Equal(t, uint64(200), tracker.GetEffectiveTs()) // No pending, use checkpointTs (200)
}

func TestProgressTracker_EffectiveTs(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Initially should be 0
	require.Equal(t, uint64(0), tracker.GetEffectiveTs())

	// Only checkpoint TS set
	tracker.UpdateCheckpointTs(100)
	require.Equal(t, uint64(100), tracker.GetEffectiveTs()) // Use checkpointTs

	// Add pending transactions
	tracker.AddPendingTxn(50, 1)
	tracker.AddPendingTxn(75, 2)
	require.Equal(t, uint64(49), tracker.GetEffectiveTs()) // min(50, 75) - 1 = 49

	// Remove first pending transaction
	tracker.RemoveCompletedTxn(50, 1)
	require.Equal(t, uint64(74), tracker.GetEffectiveTs()) // min(75) - 1 = 74

	// Remove second pending transaction
	tracker.RemoveCompletedTxn(75, 2)
	require.Equal(t, uint64(100), tracker.GetEffectiveTs()) // No pending, use checkpointTs (100)
}

func TestProgressTracker_SameCommitTsDifferentStartTs(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Add transactions with same commitTs but different startTs
	tracker.AddPendingTxn(100, 1)
	tracker.AddPendingTxn(100, 2)
	tracker.AddPendingTxn(100, 3)

	require.Equal(t, uint64(99), tracker.GetEffectiveTs()) // min(100) - 1 = 99

	// Remove one transaction
	tracker.RemoveCompletedTxn(100, 2)
	require.Equal(t, uint64(99), tracker.GetEffectiveTs()) // Still 99, as 100 is still pending

	// Remove another transaction
	tracker.RemoveCompletedTxn(100, 1)
	require.Equal(t, uint64(99), tracker.GetEffectiveTs()) // Still 99, as 100 is still pending

	// Remove the last transaction
	tracker.RemoveCompletedTxn(100, 3)
	require.Equal(t, uint64(0), tracker.GetEffectiveTs()) // No pending, use checkpointTs (0)
}

func TestProgressTracker_Reset(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Set some values
	tracker.AddPendingTxn(100, 1)
	tracker.AddPendingTxn(200, 2)
	tracker.UpdateCheckpointTs(300)

	// Reset
	tracker.Reset()

	// Check all values are reset
	require.Equal(t, uint64(0), tracker.GetEffectiveTs())
	require.Equal(t, uint64(0), tracker.GetProgress())
}

func TestProgressTracker_Concurrent(t *testing.T) {
	t.Parallel()

	tracker := NewProgressTracker()

	// Test concurrent updates
	done := make(chan bool, 3)

	// Add transactions
	go func() {
		for i := uint64(1); i <= 10; i++ {
			tracker.AddPendingTxn(i, i)
		}
		done <- true
	}()

	// Update checkpoint TS
	go func() {
		for i := uint64(1); i <= 10; i++ {
			tracker.UpdateCheckpointTs(i)
		}
		done <- true
	}()

	// Read progress concurrently
	go func() {
		for i := uint64(1); i <= 10; i++ {
			tracker.GetEffectiveTs()
			tracker.GetProgress()
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	<-done
	<-done
	<-done

	// Final state should be consistent
	effectiveTs := tracker.GetEffectiveTs()
	progress := tracker.GetProgress()
	require.Equal(t, effectiveTs, progress)
}

func TestProgressTracker_Monitor(t *testing.T) {
	t.Parallel()

	// Create tracker with monitor
	tracker := NewProgressTrackerWithMonitor("test-changefeed")
	defer tracker.Close()

	// Add some transactions and update checkpoint
	tracker.AddPendingTxn(100, 1)
	tracker.AddPendingTxn(200, 2)
	tracker.UpdateCheckpointTs(150)

	// Wait for at least one monitor cycle
	time.Sleep(1500 * time.Millisecond)

	// Remove a transaction and wait for another cycle
	tracker.RemoveCompletedTxn(100, 1)
	time.Sleep(1500 * time.Millisecond)

	// Verify the tracker is still working
	require.Equal(t, uint64(199), tracker.GetEffectiveTs()) // min(200) - 1 = 199
}
