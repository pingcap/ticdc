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

package eventstore

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mockDB struct {
	mu           sync.Mutex
	deleteCalls  [][]byte
	compactCalls [][]byte
}

func (m *mockDB) DeleteRange(start, end []byte, _ *pebble.WriteOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls = append(m.deleteCalls, start, end)
	return nil
}

func (m *mockDB) Compact(start, end []byte, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactCalls = append(m.compactCalls, start, end)
	return nil
}

func (m *mockDB) getDeleteCalls() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteCalls
}

func (m *mockDB) getCompactCalls() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.compactCalls
}

func TestGCManager(t *testing.T) {
	mdb := &mockDB{}
	deleteFn := func(
		db *pebble.DB, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
	) error {
		return mdb.DeleteRange(
			encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, startTxnCommitTs),
			encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, endTxnCommitTs),
			nil)
	}
	compactFn := func(
		db *pebble.DB, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
	) error {
		return mdb.Compact(
			encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, startTxnCommitTs),
			encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, endTxnCommitTs),
			false)
	}
	gcm := newGCManager([]*pebble.DB{nil}, deleteFn, compactFn)

	// --- Test delete logic ---
	gcm.addGCItem(0, 1, 10, 100, 200)
	gcm.addGCItem(0, 1, 20, 300, 400) // Add a second table

	{
		ranges := gcm.fetchGCItems(time.Now(), 0, 0)
		require.Len(t, ranges, 2)
		gcm.doGCJob(ranges)
		gcm.updateCompactRanges(ranges)
	}
	// check
	compactKey1 := compactItemKey{dbIndex: 0, uniqueKeyID: 1, tableID: 10}
	compactKey2 := compactItemKey{dbIndex: 0, uniqueKeyID: 1, tableID: 20}
	{
		deleteCalls := mdb.getDeleteCalls()
		require.Len(t, deleteCalls, 4)
		// The order of delete ranges is not guaranteed because it iterates over a map.
		if bytes.Equal(deleteCalls[0], encodeTxnCommitTsBoundaryKey(1, 10, 100)) {
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 10, 200), deleteCalls[1])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 300), deleteCalls[2])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 400), deleteCalls[3])
		} else {
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 300), deleteCalls[0])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 400), deleteCalls[1])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 10, 100), deleteCalls[2])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 10, 200), deleteCalls[3])
		}

		// Check internal state for compaction
		gcm.mu.Lock()
		state1, ok := gcm.compactRanges[compactKey1]
		require.True(t, ok)
		require.Equal(t, uint64(200), state1.endTxnCommitTs)
		require.False(t, state1.compacted)
		state2, ok := gcm.compactRanges[compactKey2]
		require.True(t, ok)
		require.Equal(t, uint64(400), state2.endTxnCommitTs)
		require.False(t, state2.compacted)
		gcm.mu.Unlock()
	}

	gcm.doCompaction()
	{
		compactCalls := mdb.getCompactCalls()
		require.Len(t, compactCalls, 4)
		// The order of compaction is not guaranteed because it iterates over a map.
		if bytes.Equal(compactCalls[0], encodeTxnCommitTsBoundaryKey(1, 10, 0)) {
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 10, 200), compactCalls[1])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 0), compactCalls[2])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 400), compactCalls[3])
		} else {
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 0), compactCalls[0])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 20, 400), compactCalls[1])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 10, 0), compactCalls[2])
			require.Equal(t, encodeTxnCommitTsBoundaryKey(1, 10, 200), compactCalls[3])
		}
		// Verify internal state is now compacted
		gcm.mu.Lock()
		state1, ok := gcm.compactRanges[compactKey1]
		require.True(t, ok)
		require.True(t, state1.compacted)
		state2, ok := gcm.compactRanges[compactKey2]
		require.True(t, ok)
		require.True(t, state2.compacted)
		gcm.mu.Unlock()
	}

	gcm.doCompaction()
	require.Len(t, mdb.getCompactCalls(), 4, "should not compact again")

	// --- Test re-compaction after new delete ---
	gcm.addGCItem(0, 1, 10, 200, 300) // Add new item for the first table

	{
		ranges := gcm.fetchGCItems(time.Now(), 0, 0)
		require.Len(t, ranges, 1)
		gcm.doGCJob(ranges)
		gcm.updateCompactRanges(ranges)
		gcm.doCompaction()

		compactCalls := mdb.getCompactCalls()
		require.Len(t, compactCalls, 6)
	}
}

func TestGCManagerDelaysSmallDeleteRanges(t *testing.T) {
	gcm := newGCManager(nil, nil, nil)
	now := time.Unix(100, 0)

	startTxnCommitTs := oracle.ComposeTS(1_000, 0)
	midTxnCommitTs := oracle.ComposeTS(2_000, 0)
	endTxnCommitTs := oracle.ComposeTS(3_000, 0)

	gcm.addGCItem(0, 1, 10, startTxnCommitTs, midTxnCommitTs)
	gcm.addGCItem(0, 1, 10, midTxnCommitTs, endTxnCommitTs)

	compactKey := compactItemKey{dbIndex: 0, uniqueKeyID: 1, tableID: 10}
	gcm.mu.Lock()
	pending, ok := gcm.deleteRanges[compactKey]
	require.True(t, ok)
	pending.firstEnqueueTime = now
	gcm.mu.Unlock()

	ranges := gcm.fetchGCItems(now.Add(time.Minute), 5*time.Minute, 30*time.Minute)
	require.Empty(t, ranges)

	gcm.mu.Lock()
	pending, ok = gcm.deleteRanges[compactKey]
	require.True(t, ok)
	require.Equal(t, startTxnCommitTs, pending.item.startTxnCommitTs)
	require.Equal(t, endTxnCommitTs, pending.item.endTxnCommitTs)
	gcm.mu.Unlock()

	ranges = gcm.fetchGCItems(now.Add(31*time.Minute), 5*time.Minute, 30*time.Minute)
	require.Len(t, ranges, 1)
	require.Equal(t, gcRangeItem{
		dbIndex:          0,
		uniqueKeyID:      1,
		tableID:          10,
		startTxnCommitTs: startTxnCommitTs,
		endTxnCommitTs:   endTxnCommitTs,
	}, ranges[0])
	require.Equal(t, 0, gcm.pendingDeleteRangeCount())
}

func TestGCManagerFlushesLargeDeleteRangeImmediately(t *testing.T) {
	gcm := newGCManager(nil, nil, nil)
	now := time.Unix(100, 0)

	startTxnCommitTs := oracle.ComposeTS(1_000, 0)
	endTxnCommitTs := oracle.ComposeTS(1_000+6*60*1000, 0)

	gcm.addGCItem(0, 1, 10, startTxnCommitTs, endTxnCommitTs)

	compactKey := compactItemKey{dbIndex: 0, uniqueKeyID: 1, tableID: 10}
	gcm.mu.Lock()
	pending, ok := gcm.deleteRanges[compactKey]
	require.True(t, ok)
	pending.firstEnqueueTime = now
	gcm.mu.Unlock()

	ranges := gcm.fetchGCItems(now.Add(time.Minute), 5*time.Minute, 30*time.Minute)
	require.Len(t, ranges, 1)
	require.Equal(t, gcRangeItem{
		dbIndex:          0,
		uniqueKeyID:      1,
		tableID:          10,
		startTxnCommitTs: startTxnCommitTs,
		endTxnCommitTs:   endTxnCommitTs,
	}, ranges[0])
	require.Equal(t, 0, gcm.pendingDeleteRangeCount())
}

func TestGCManagerWidensDisjointDeleteRanges(t *testing.T) {
	gcm := newGCManager(nil, nil, nil)
	now := time.Unix(100, 0)

	firstStart := oracle.ComposeTS(1_000, 0)
	firstEnd := oracle.ComposeTS(2_000, 0)
	secondStart := oracle.ComposeTS(3_000, 0)
	secondEnd := oracle.ComposeTS(4_000, 0)

	gcm.addGCItem(0, 1, 10, firstStart, firstEnd)
	gcm.addGCItem(0, 1, 10, secondStart, secondEnd)

	compactKey := compactItemKey{dbIndex: 0, uniqueKeyID: 1, tableID: 10}
	gcm.mu.Lock()
	pending, ok := gcm.deleteRanges[compactKey]
	require.True(t, ok)
	pending.firstEnqueueTime = now
	require.Equal(t, firstStart, pending.item.startTxnCommitTs)
	require.Equal(t, secondEnd, pending.item.endTxnCommitTs)
	gcm.mu.Unlock()

	ranges := gcm.fetchGCItems(now.Add(31*time.Minute), 5*time.Minute, 30*time.Minute)
	require.Len(t, ranges, 1)
	require.Equal(t, gcRangeItem{
		dbIndex:          0,
		uniqueKeyID:      1,
		tableID:          10,
		startTxnCommitTs: firstStart,
		endTxnCommitTs:   secondEnd,
	}, ranges[0])
	require.Equal(t, 0, gcm.pendingDeleteRangeCount())
}
