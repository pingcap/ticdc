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

package causality

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testBarrier struct {
	mu        sync.Mutex
	done      chan struct{}
	err       error
	remaining int
	doneFuncs []func()
}

func newTestBarrier(workerCount int) *testBarrier {
	return &testBarrier{done: make(chan struct{}), remaining: workerCount}
}

func (b *testBarrier) Ack(int) {
	b.mu.Lock()
	if b.remaining == 0 {
		b.mu.Unlock()
		return
	}
	b.remaining--
	if b.remaining > 0 {
		b.mu.Unlock()
		return
	}
	doneFuncs := b.doneFuncs
	b.doneFuncs = nil
	close(b.done)
	b.mu.Unlock()
	for _, f := range doneFuncs {
		f()
	}
}

func (b *testBarrier) Fail(err error) {
	b.mu.Lock()
	if b.remaining == 0 {
		b.mu.Unlock()
		return
	}
	b.err = err
	b.remaining = 0
	doneFuncs := b.doneFuncs
	b.doneFuncs = nil
	close(b.done)
	b.mu.Unlock()
	for _, f := range doneFuncs {
		f()
	}
}

func (b *testBarrier) OnDone(f func()) {
	b.mu.Lock()
	if b.remaining == 0 {
		b.mu.Unlock()
		f()
		return
	}
	b.doneFuncs = append(b.doneFuncs, f)
	b.mu.Unlock()
}

func TestBroadcastBarrierEnqueuesOneTokenPerWriter(t *testing.T) {
	detector := New(4, TxnCacheOption{Count: 2, Size: 1, BlockStrategy: BlockStrategyWaitEmpty}, testChangefeedID())
	barrier := newTestBarrier(2)

	require.NoError(t, detector.BroadcastBarrier(barrier))

	for i := 0; i < 2; i++ {
		items, ok := detector.GetOutChByCacheID(i).GetMultipleNoGroup(make([]WriterItem, 0, 1))
		require.True(t, ok)
		require.Len(t, items, 1)
		require.True(t, barrier == items[0].Barrier)
		items[0].Barrier.Ack(i)
	}

	require.Eventually(t, func() bool {
		select {
		case <-barrier.done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestBroadcastBarrierReturnsErrorWhenDetectorClosed(t *testing.T) {
	detector := New(4, TxnCacheOption{Count: 1, Size: 1, BlockStrategy: BlockStrategyWaitEmpty}, testChangefeedID())
	detector.CloseNotifiedNodes()

	err := detector.BroadcastBarrier(newTestBarrier(1))
	require.Error(t, err)
}

func TestRemovalOnlyFenceDoesNotResolveDependersOnAssignment(t *testing.T) {
	assigned := false
	fence := &Node{id: genNextNodeID(), assignedTo: unassigned, resolveByRemovalOnly: true}
	fence.RandCacheID = func() cacheID { return 0 }
	fence.TrySendToTxnCache = func(cacheID) bool { return true }
	fence.OnNotified = func(callback func()) { callback() }

	depender := &Node{id: genNextNodeID(), assignedTo: unassigned}
	depender.RandCacheID = func() cacheID { return 0 }
	depender.TrySendToTxnCache = func(cacheID) bool {
		assigned = true
		return true
	}
	depender.OnNotified = func(callback func()) { callback() }

	depender.dependOn(map[int64]*Node{fence.nodeID(): fence})
	fence.maybeResolve()
	require.False(t, assigned)

	fence.remove()
	require.True(t, assigned)
}

func TestTxnCacheForceAddBypassesBlockedCache(t *testing.T) {
	cache := newTxnCache(TxnCacheOption{Count: 1, Size: 1, BlockStrategy: BlockStrategyWaitEmpty})
	require.True(t, cache.add(NewDMLItem(nil)))
	require.True(t, cache.add(NewDMLItem(nil)))
	require.False(t, cache.add(NewDMLItem(nil)))

	barrier := newTestBarrier(1)
	require.True(t, cache.forceAdd(NewBarrierItem(barrier)))

	items, ok := cache.out().GetMultipleNoGroup(make([]WriterItem, 0, 3))
	require.True(t, ok)
	require.Len(t, items, 3)
	require.True(t, barrier == items[2].Barrier)
}

func TestTxnCacheForceAddFailsWhenClosed(t *testing.T) {
	cache := newTxnCache(TxnCacheOption{Count: 1, Size: 1, BlockStrategy: BlockStrategyWaitEmpty})
	cache.out().Close()

	barrier := newTestBarrier(1)
	require.False(t, cache.forceAdd(NewBarrierItem(barrier)))
	barrier.Fail(errors.New("closed"))
	require.Error(t, barrier.err)
}

func BenchmarkTxnCacheAddDMLItem(b *testing.B) {
	cache := newTxnCache(TxnCacheOption{Count: 1, Size: 4096, BlockStrategy: BlockStrategyWaitAvailable})
	buffer := make([]WriterItem, 0, 1024)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if !cache.add(NewDMLItem(nil)) {
			b.Fatal("cache unexpectedly rejected DML item")
		}
		if (i+1)%cap(buffer) == 0 {
			var ok bool
			buffer, ok = cache.out().GetMultipleNoGroup(buffer)
			if !ok {
				b.Fatal("cache closed")
			}
			buffer = buffer[:0]
		}
	}
}
