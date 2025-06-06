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

package regionlock

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func mustSuccess(t *testing.T, res LockRangeResult, expectedResolvedTs uint64) {
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	require.Equal(t, expectedResolvedTs, res.LockedRangeState.ResolvedTs.Load())
}

func mustStale(t *testing.T, res LockRangeResult, expectedRetryRanges ...heartbeatpb.TableSpan) {
	require.Equal(t, LockRangeStatusStale, res.Status)
	require.Equal(t, expectedRetryRanges, res.RetryRanges)
}

func mustWaitFn(t *testing.T, res LockRangeResult) func() LockRangeResult {
	require.Equal(t, LockRangeStatusWait, res.Status)
	return res.WaitFn
}

func mustLockRangeSuccess(
	ctx context.Context,
	t *testing.T,
	l *RangeLock,
	startKey, endKey string,
	regionID, version, expectedResolvedTs uint64,
) {
	res := l.LockRange(ctx, []byte(startKey), []byte(endKey), regionID, version)
	mustSuccess(t, res, expectedResolvedTs)
}

// nolint:unparam
// NOTICE: For now, regionID always is 1.
func mustLockRangeStale(
	ctx context.Context,
	t *testing.T,
	l *RangeLock,
	startKey, endKey string,
	regionID, version uint64,
	expectRetrySpans ...string,
) {
	res := l.LockRange(ctx, []byte(startKey), []byte(endKey), regionID, version)
	spans := make([]heartbeatpb.TableSpan, 0)
	for i := 0; i < len(expectRetrySpans); i += 2 {
		spans = append(spans, heartbeatpb.TableSpan{
			StartKey: []byte(expectRetrySpans[i]), EndKey: []byte(expectRetrySpans[i+1]),
		})
	}
	mustStale(t, res, spans...)
}

// nolint:unparam
// NOTICE: For now, regionID always is 1.
func mustLockRangeWait(
	ctx context.Context,
	t *testing.T,
	l *RangeLock,
	startKey, endKey string,
	regionID, version uint64,
) func() LockRangeResult {
	res := l.LockRange(ctx, []byte(startKey), []byte(endKey), regionID, version)
	return mustWaitFn(t, res)
}

func unlockRange(l *RangeLock, startKey, endKey string, regionID, version uint64, resolvedTs uint64) {
	l.UnlockRange([]byte(startKey), []byte(endKey), regionID, version, resolvedTs)
}

func TestRegionRangeLock(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()
	l := NewRangeLock(1, []byte("a"), []byte("h"), math.MaxUint64)
	mustLockRangeSuccess(ctx, t, l, "a", "e", 1, 1, math.MaxUint64)
	unlockRange(l, "a", "e", 1, 1, 100)

	mustLockRangeSuccess(ctx, t, l, "a", "e", 1, 2, 100)
	mustLockRangeStale(ctx, t, l, "a", "e", 1, 2)
	wait := mustLockRangeWait(ctx, t, l, "a", "h", 1, 3)

	unlockRange(l, "a", "e", 1, 2, 110)
	res := wait()
	mustSuccess(t, res, 110)
	unlockRange(l, "a", "h", 1, 3, 120)
}

func TestRegionRangeLockStale(t *testing.T) {
	t.Parallel()

	l := NewRangeLock(1, []byte("a"), []byte("z"), math.MaxUint64)
	ctx := context.TODO()
	mustLockRangeSuccess(ctx, t, l, "c", "g", 1, 10, math.MaxUint64)
	mustLockRangeSuccess(ctx, t, l, "j", "n", 2, 8, math.MaxUint64)

	mustLockRangeStale(ctx, t, l, "c", "g", 1, 10)
	mustLockRangeStale(ctx, t, l, "c", "i", 1, 9, "g", "i")
	mustLockRangeStale(ctx, t, l, "a", "z", 1, 9, "a", "c", "g", "j", "n", "z")
	mustLockRangeStale(ctx, t, l, "a", "e", 1, 9, "a", "c")
	mustLockRangeStale(ctx, t, l, "e", "h", 1, 9, "g", "h")
	mustLockRangeStale(ctx, t, l, "e", "k", 1, 9, "g", "j")
	mustLockRangeSuccess(ctx, t, l, "g", "j", 3, 1, math.MaxUint64)
	unlockRange(l, "g", "j", 3, 1, 2)
	unlockRange(l, "c", "g", 1, 10, 5)
	unlockRange(l, "j", "n", 2, 8, 8)
	mustLockRangeSuccess(ctx, t, l, "a", "z", 1, 11, 2)
	unlockRange(l, "a", "z", 1, 11, 2)
}

func TestRegionRangeLockLockingRegionID(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()
	l := NewRangeLock(1, []byte("a"), []byte("z"), math.MaxUint64)
	mustLockRangeSuccess(ctx, t, l, "c", "d", 1, 10, math.MaxUint64)

	mustLockRangeStale(ctx, t, l, "e", "f", 1, 5, "e", "f")
	mustLockRangeStale(ctx, t, l, "e", "f", 1, 10, "e", "f")
	wait := mustLockRangeWait(ctx, t, l, "e", "f", 1, 11)
	unlockRange(l, "c", "d", 1, 10, 10)
	mustSuccess(t, wait(), math.MaxUint64)
	// Now ["e", "f") is locked by region 1 at version 11 and ts 11.

	mustLockRangeSuccess(ctx, t, l, "g", "h", 2, 10, math.MaxUint64)
	wait = mustLockRangeWait(ctx, t, l, "g", "h", 1, 12)
	ch := make(chan LockRangeResult, 1)
	go func() {
		ch <- wait()
	}()
	unlockRange(l, "g", "h", 2, 10, 20)
	// Locking should still be blocked because the regionID 1 is still locked.
	select {
	case <-ch:
		require.FailNow(t, "locking finished unexpectedly")
	case <-time.After(time.Millisecond * 50):
	}

	unlockRange(l, "e", "f", 1, 11, 11)
	res := <-ch
	// CheckpointTS calculation should still be based on range and do not consider the regionID. So
	// the result's resolvedTs should be 20 from of range ["g", "h"), instead of 11 from min(11, 20).
	mustSuccess(t, res, 20)
	unlockRange(l, "g", "h", 1, 12, 30)
}

func TestRegionRangeLockCanBeCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	l := NewRangeLock(1, []byte("a"), []byte("z"), math.MaxUint64)
	mustLockRangeSuccess(ctx, t, l, "g", "h", 1, 10, math.MaxUint64)
	wait := mustLockRangeWait(ctx, t, l, "g", "h", 1, 12)
	cancel()
	lockResult := wait()
	require.Equal(t, LockRangeStatusCancel, lockResult.Status)
}

func TestRangeTsMapSetUnset(t *testing.T) {
	t.Parallel()

	m := newRangeTsMap([]byte("a"), []byte("z"), 100)
	require.Equal(t, 1, m.m.Len())
	require.Equal(t, uint64(100), m.getMinTsInRange([]byte("a"), []byte("z")))

	// Double set, should panic.
	require.Panics(t, func() { m.clone().set([]byte("a"), []byte("m"), 101) })

	// Unset and then get other ranges.
	m.unset([]byte("m"), []byte("x"))
	require.Equal(t, 3, m.m.Len())
	require.Equal(t, uint64(100), m.getMinTsInRange([]byte("a"), []byte("m")))
	require.Equal(t, uint64(100), m.getMinTsInRange([]byte("x"), []byte("z")))

	// Unset and then get, should panic.
	require.Panics(t, func() { m.clone().getMinTsInRange([]byte("m"), []byte("x")) })
	require.Panics(t, func() { m.clone().getMinTsInRange([]byte("n"), []byte("w")) })
	require.Panics(t, func() { m.clone().getMinTsInRange([]byte("a"), []byte("z")) })

	// Unset all ranges.
	m.unset([]byte("a"), []byte("m"))
	m.unset([]byte("x"), []byte("z"))
	require.Equal(t, 1, m.m.Len())
	require.Panics(t, func() { m.clone().getMinTsInRange([]byte("a"), []byte("z")) })
}

func TestRegionRangeLockCollect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	l := NewRangeLock(1, []byte("a"), []byte("z"), 100)
	attrs := l.IterAll(nil)
	require.Equal(t, 1, len(attrs.UnLockedRanges))

	l.LockRange(ctx, []byte("a"), []byte("m"), 1, 1).LockedRangeState.ResolvedTs.Add(1)
	attrs = l.IterAll(nil)
	require.Equal(t, 1, len(attrs.UnLockedRanges))
	require.Equal(t, uint64(101), attrs.SlowestRegion.ResolvedTs)
	require.Equal(t, uint64(101), attrs.FastestRegion.ResolvedTs)

	l.LockRange(ctx, []byte("m"), []byte("z"), 2, 1).LockedRangeState.ResolvedTs.Add(2)
	attrs = l.IterAll(nil)
	require.Equal(t, 0, len(attrs.UnLockedRanges))
	require.Equal(t, uint64(101), attrs.SlowestRegion.ResolvedTs)
	require.Equal(t, uint64(102), attrs.FastestRegion.ResolvedTs)

	l.UnlockRange([]byte("a"), []byte("m"), 1, 1)
	attrs = l.IterAll(nil)
	require.Equal(t, 1, len(attrs.UnLockedRanges))
	require.Equal(t, uint64(102), attrs.SlowestRegion.ResolvedTs)
	require.Equal(t, uint64(102), attrs.FastestRegion.ResolvedTs)

	l.UnlockRange([]byte("m"), []byte("z"), 2, 1)
	attrs = l.IterAll(nil)
	require.Equal(t, 1, len(attrs.UnLockedRanges))
}

func TestCalculateMinResolvedTs(t *testing.T) {
	l := NewRangeLock(1, []byte("a"), []byte("z"), 100)

	res := l.LockRange(context.Background(), []byte("m"), []byte("x"), 1, 1)
	res.LockedRangeState.ResolvedTs.Store(101)
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	require.Equal(t, uint64(100), l.ResolvedTs())

	res = l.LockRange(context.Background(), []byte("a"), []byte("m"), 2, 1)
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.ResolvedTs.Store(102)
	res = l.LockRange(context.Background(), []byte("x"), []byte("z"), 3, 1)
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	res.LockedRangeState.ResolvedTs.Store(103)
	require.Equal(t, uint64(101), l.ResolvedTs())
}

func Benchmark100KRegions(b *testing.B) {
	ctx := context.Background()
	startKey, endKey := common.GetTableRange(1)
	l := NewRangeLock(1, startKey, endKey, 100)

	for i := 1; i <= 100*1000; i++ {
		var rangeStart, rangeEnd []byte

		rangeStart = make([]byte, len(startKey)+8)
		copy(rangeStart, startKey)
		binary.BigEndian.PutUint64(rangeStart[len(startKey):], uint64(i))

		if i < 100*1000 {
			rangeEnd = make([]byte, len(startKey)+8)
			copy(rangeEnd, startKey)
			binary.BigEndian.PutUint64(rangeEnd[len(startKey):], uint64(i+1))
		} else {
			rangeEnd = make([]byte, len(endKey))
			copy(rangeEnd, endKey)
		}
		lockRes := l.LockRange(ctx, rangeStart, rangeEnd, uint64(i), 1)
		if lockRes.Status != LockRangeStatusSuccess {
			panic(fmt.Sprintf("bad lock range, i: %d\n", i))
		}
		lockRes.LockedRangeState.ResolvedTs.Store(uint64(100 + i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.ResolvedTs()
	}
}

func TestRangeLockGetHeapMinTs(t *testing.T) {
	t.Parallel()

	updateLockedRangeResolvedTs := func(l *RangeLock, state *LockedRangeState, resolvedTs uint64) {
		state.ResolvedTs.Store(resolvedTs)
		l.UpdateLockedRangeStateHeap(state)
	}

	// Test case 1: empty heap
	l := NewRangeLock(1, []byte("a"), []byte("z"), 100)
	require.Equal(t, uint64(100), l.GetHeapMinTs())

	// Test case 2: Lock a range and then unlock it
	l = NewRangeLock(1, []byte("a"), []byte("z"), 50)
	res := l.LockRange(context.Background(), []byte("a"), []byte("m"), 1, 1)
	updateLockedRangeResolvedTs(l, res.LockedRangeState, 101)
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	require.Equal(t, uint64(50), l.GetHeapMinTs())
	require.Equal(t, l.ResolvedTs(), l.GetHeapMinTs())
	res = l.LockRange(context.Background(), []byte("m"), []byte("z"), 2, 1)
	updateLockedRangeResolvedTs(l, res.LockedRangeState, 60)
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	require.Equal(t, uint64(60), l.GetHeapMinTs())
	require.Equal(t, l.ResolvedTs(), l.GetHeapMinTs())
	l.UnlockRange([]byte("a"), []byte("m"), 1, 1, 100)
	require.Equal(t, uint64(60), l.GetHeapMinTs())
	require.Equal(t, l.ResolvedTs(), l.GetHeapMinTs())
	l.UnlockRange([]byte("m"), []byte("z"), 2, 1, 100)
	require.Equal(t, uint64(100), l.GetHeapMinTs())
	require.Equal(t, l.ResolvedTs(), l.GetHeapMinTs())

	l = NewRangeLock(1, []byte("a"), []byte("z"), 100)
	res = l.LockRange(context.Background(), []byte("a"), []byte("m"), 1, 1)
	updateLockedRangeResolvedTs(l, res.LockedRangeState, 101)
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	res = l.LockRange(context.Background(), []byte("m"), []byte("z"), 2, 1)
	updateLockedRangeResolvedTs(l, res.LockedRangeState, 102)
	require.Equal(t, LockRangeStatusSuccess, res.Status)
	require.Equal(t, uint64(101), l.GetHeapMinTs())
	require.Equal(t, l.ResolvedTs(), l.GetHeapMinTs())

	// Update the resolvedTs of the first range
	updateLockedRangeResolvedTs(l, res.LockedRangeState, 50)
	require.Equal(t, uint64(50), l.GetHeapMinTs())
	require.Equal(t, l.ResolvedTs(), l.GetHeapMinTs())
}
