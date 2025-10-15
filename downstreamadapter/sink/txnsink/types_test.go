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
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestTxnStore_AddEvent(t *testing.T) {
	t.Parallel()

	store := NewTxnStore()

	// Create test event
	event := &commonEvent.DMLEvent{
		CommitTs: 100,
		StartTs:  50,
		Length:   1,
	}

	// Add event to store
	store.AddEvent(event)

	// Verify event is stored correctly
	store.mu.Lock()
	defer store.mu.Unlock()

	require.Contains(t, store.store, uint64(100))
	require.Contains(t, store.store[100], uint64(50))
	require.Len(t, store.store[100][50], 1)
	require.Equal(t, event, store.store[100][50][0])
}

func TestTxnStore_AddMultipleEvents(t *testing.T) {
	t.Parallel()

	store := NewTxnStore()

	// Create multiple events with same commitTs but different startTs
	event1 := &commonEvent.DMLEvent{CommitTs: 100, StartTs: 50}
	event2 := &commonEvent.DMLEvent{CommitTs: 100, StartTs: 60}
	event3 := &commonEvent.DMLEvent{CommitTs: 100, StartTs: 50} // Same as event1

	store.AddEvent(event1)
	store.AddEvent(event2)
	store.AddEvent(event3)

	store.mu.Lock()
	defer store.mu.Unlock()

	// Should have one commitTs entry
	require.Len(t, store.store, 1)
	require.Contains(t, store.store, uint64(100))

	// Should have two startTs entries under commitTs 100
	require.Len(t, store.store[100], 2)
	require.Contains(t, store.store[100], uint64(50))
	require.Contains(t, store.store[100], uint64(60))

	// Should have two events under startTs 50
	require.Len(t, store.store[100][50], 2)
	require.Equal(t, event1, store.store[100][50][0])
	require.Equal(t, event3, store.store[100][50][1])

	// Should have one event under startTs 60
	require.Len(t, store.store[100][60], 1)
	require.Equal(t, event2, store.store[100][60][0])
}

func TestTxnStore_GetEventsByCheckpointTs(t *testing.T) {
	t.Parallel()

	store := NewTxnStore()

	// Add events with different commitTs
	events := []*commonEvent.DMLEvent{
		{CommitTs: 50, StartTs: 10},
		{CommitTs: 100, StartTs: 20},
		{CommitTs: 150, StartTs: 30},
		{CommitTs: 200, StartTs: 40},
	}

	for _, event := range events {
		store.AddEvent(event)
	}

	// Test getting events with checkpoint 100
	groups := store.GetEventsByCheckpointTs(100)

	// Should return 2 groups (commitTs 50 and 100)
	require.Len(t, groups, 2)

	// Verify the groups
	commitTsSet := make(map[uint64]bool)
	for _, group := range groups {
		commitTsSet[group.CommitTs] = true
		require.True(t, group.CommitTs <= 100)
		require.Len(t, group.Events, 1)
	}

	require.True(t, commitTsSet[50])
	require.True(t, commitTsSet[100])
}

func TestTxnStore_RemoveEventsByCheckpointTs(t *testing.T) {
	t.Parallel()

	store := NewTxnStore()

	// Add events with different commitTs
	events := []*commonEvent.DMLEvent{
		{CommitTs: 50, StartTs: 10},
		{CommitTs: 100, StartTs: 20},
		{CommitTs: 150, StartTs: 30},
		{CommitTs: 200, StartTs: 40},
	}

	for _, event := range events {
		store.AddEvent(event)
	}

	// Remove events with checkpoint 100
	store.RemoveEventsByCheckpointTs(100)

	store.mu.Lock()
	defer store.mu.Unlock()

	// Should only have commitTs 150 and 200 remaining
	require.Len(t, store.store, 2)
	require.Contains(t, store.store, uint64(150))
	require.Contains(t, store.store, uint64(200))
	require.NotContains(t, store.store, uint64(50))
	require.NotContains(t, store.store, uint64(100))
}

func TestTxnStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	store := NewTxnStore()
	var wg sync.WaitGroup

	// Concurrent writes
	numWriters := 10
	eventsPerWriter := 100

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < eventsPerWriter; j++ {
				event := &commonEvent.DMLEvent{
					CommitTs: uint64(writerID*eventsPerWriter + j),
					StartTs:  uint64(j),
				}
				store.AddEvent(event)
			}
		}(i)
	}

	// Concurrent reads
	numReaders := 5
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = store.GetEventsByCheckpointTs(uint64(j * 10))
			}
		}()
	}

	wg.Wait()

	// Verify final state
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Len(t, store.store, numWriters*eventsPerWriter)
}

func TestTxnGroup_GetTxnKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		commitTs uint64
		startTs  uint64
		expected string
	}{
		{
			name:     "normal case",
			commitTs: 123,
			startTs:  456,
			expected: "123_456",
		},
		{
			name:     "zero values",
			commitTs: 0,
			startTs:  0,
			expected: "0_0",
		},
		{
			name:     "large values",
			commitTs: 18446744073709551615, // max uint64
			startTs:  18446744073709551614,
			expected: "18446744073709551615_18446744073709551614",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			group := &TxnGroup{
				CommitTs: tt.commitTs,
				StartTs:  tt.startTs,
			}

			result := group.GetTxnKey()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTxnGroup_ExtractKeys(t *testing.T) {
	t.Parallel()

	// Create test events with row keys
	events := []*commonEvent.DMLEvent{
		{
			RowKeys: [][]byte{
				[]byte("key1"),
				[]byte("key2"),
			},
		},
		{
			RowKeys: [][]byte{
				[]byte("key2"), // Duplicate key
				[]byte("key3"),
			},
		},
	}

	group := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   events,
	}

	keys := group.ExtractKeys()

	// Should have 3 unique keys
	require.Len(t, keys, 3)
	require.Contains(t, keys, "key1")
	require.Contains(t, keys, "key2")
	require.Contains(t, keys, "key3")
}

func TestTxnGroup_PostFlushFuncs(t *testing.T) {
	t.Parallel()

	group := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
	}

	var executed []int

	// Add post-flush functions
	group.AddPostFlushFunc(func() {
		executed = append(executed, 1)
	})
	group.AddPostFlushFunc(func() {
		executed = append(executed, 2)
	})
	group.AddPostFlushFunc(func() {
		executed = append(executed, 3)
	})

	// Execute post-flush functions
	group.PostFlush()

	// Verify all functions were executed in order
	require.Equal(t, []int{1, 2, 3}, executed)
}

func TestTxnSinkConfig_Defaults(t *testing.T) {
	t.Parallel()

	config := &TxnSinkConfig{}

	// Test default values are reasonable
	require.Equal(t, 0, config.MaxConcurrentTxns)
	require.Equal(t, 0, config.BatchSize)
	require.Equal(t, 0, config.FlushInterval)
}

func TestConflictDetector_Creation(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test("test", "test")
	detector := NewConflictDetector(changefeedID, 16)

	require.NotNil(t, detector)
	require.NotNil(t, detector.resolvedTxnCaches)
	require.NotNil(t, detector.slots)
	require.NotNil(t, detector.notifiedNodes)
	require.NotNil(t, detector.metricConflictDetectDuration)
	require.Equal(t, changefeedID, detector.changefeedID)
}

func TestConflictDetector_GetOutChByCacheID(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test("test", "test")
	detector := NewConflictDetector(changefeedID, 16)

	// Test valid cache ID
	ch := detector.GetOutChByCacheID(0)
	require.NotNil(t, ch)

	// Test another valid cache ID
	ch2 := detector.GetOutChByCacheID(1)
	require.NotNil(t, ch2)

	// Channels should be different
	require.NotEqual(t, ch, ch2)

	// Test invalid cache ID (should still return a channel but may be nil depending on implementation)
	ch3 := detector.GetOutChByCacheID(999)
	// The behavior here depends on the underlying causality implementation
	// We just verify it doesn't panic
	_ = ch3
}

// Helper function to create a test DML event
func createTestDMLEvent(commitTs, startTs uint64, tableID int64) *commonEvent.DMLEvent {
	return &commonEvent.DMLEvent{
		CommitTs: commitTs,
		StartTs:  startTs,
		Length:   1,
		RowKeys:  [][]byte{[]byte("test_key")},
	}
}

// Benchmark tests
func BenchmarkTxnStore_AddEvent(b *testing.B) {
	store := NewTxnStore()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event := &commonEvent.DMLEvent{
			CommitTs: uint64(i % 1000), // Reuse some commitTs values
			StartTs:  uint64(i),
		}
		store.AddEvent(event)
	}
}

func BenchmarkTxnStore_GetEventsByCheckpointTs(b *testing.B) {
	store := NewTxnStore()

	// Pre-populate store
	for i := 0; i < 10000; i++ {
		event := &commonEvent.DMLEvent{
			CommitTs: uint64(i),
			StartTs:  uint64(i),
		}
		store.AddEvent(event)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.GetEventsByCheckpointTs(uint64(i % 5000))
	}
}

func BenchmarkTxnGroup_ExtractKeys(b *testing.B) {
	// Create a transaction group with many events
	events := make([]*commonEvent.DMLEvent, 100)
	for i := 0; i < 100; i++ {
		events[i] = &commonEvent.DMLEvent{
			RowKeys: [][]byte{
				[]byte("key_" + string(rune(i%10))), // Some duplicate keys
				[]byte("unique_key_" + string(rune(i))),
			},
		}
	}

	group := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   events,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = group.ExtractKeys()
	}
}
