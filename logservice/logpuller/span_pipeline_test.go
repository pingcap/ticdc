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

package logpuller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestSpanPipelineOutOfOrderPersistCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newSpanPipelineManager(ctx, 1, 1024, 1024*1024*1024)

	var (
		mu        sync.Mutex
		callbacks []func()
	)
	consumeCalled := make(chan struct{}, 16)
	consumeKVEvents := func(_ []common.RawKVEntry, finishCallback func()) bool {
		mu.Lock()
		callbacks = append(callbacks, finishCallback)
		mu.Unlock()
		consumeCalled <- struct{}{}
		return true
	}
	resolvedCh := make(chan uint64, 16)
	advanceResolvedTs := func(ts uint64) { resolvedCh <- ts }

	span := &subscribedSpan{
		subID:             1,
		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
	}
	p.Register(span)

	p.EnqueueData(ctx, span, []common.RawKVEntry{{Key: []byte("a")}})
	p.EnqueueData(ctx, span, []common.RawKVEntry{{Key: []byte("b")}})
	p.EnqueueData(ctx, span, []common.RawKVEntry{{Key: []byte("c")}})

	require.Eventually(t, func() bool { return len(consumeCalled) == 3 }, time.Second, 10*time.Millisecond)

	p.EnqueueResolved(span, 100)

	mu.Lock()
	require.Len(t, callbacks, 3)
	cb1, cb2, cb3 := callbacks[0], callbacks[1], callbacks[2]
	mu.Unlock()

	// Persist callbacks arrive out-of-order: 2, 3, 1.
	cb2()
	cb3()
	select {
	case <-resolvedCh:
		require.FailNow(t, "resolved ts must not advance before persisted prefix is complete")
	case <-time.After(200 * time.Millisecond):
	}

	cb1()
	select {
	case ts := <-resolvedCh:
		require.Equal(t, uint64(100), ts)
	case <-time.After(time.Second):
		require.FailNow(t, "resolved ts should advance after all prior data persisted")
	}
}

func TestSpanPipelineResolvedMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newSpanPipelineManager(ctx, 1, 1024, 1024*1024*1024)

	var (
		mu        sync.Mutex
		callbacks []func()
	)
	consumeKVEvents := func(_ []common.RawKVEntry, finishCallback func()) bool {
		mu.Lock()
		callbacks = append(callbacks, finishCallback)
		mu.Unlock()
		return true
	}
	resolvedCh := make(chan uint64, 16)
	advanceResolvedTs := func(ts uint64) { resolvedCh <- ts }

	span := &subscribedSpan{
		subID:             2,
		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
	}
	p.Register(span)

	p.EnqueueData(ctx, span, []common.RawKVEntry{{Key: []byte("a")}})
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(callbacks) == 1
	}, time.Second, 10*time.Millisecond)

	// Same waitSeq, keep max ts.
	p.EnqueueResolved(span, 10)
	p.EnqueueResolved(span, 12)
	p.EnqueueResolved(span, 11)

	mu.Lock()
	cb := callbacks[0]
	mu.Unlock()
	cb()

	select {
	case ts := <-resolvedCh:
		require.Equal(t, uint64(12), ts)
	case <-time.After(time.Second):
		require.FailNow(t, "resolved ts should advance")
	}
	select {
	case <-resolvedCh:
		require.FailNow(t, "redundant resolved ts should be merged")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSpanPipelineMultipleSubIDsIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newSpanPipelineManager(ctx, 2, 1024, 1024*1024*1024)

	type sub struct {
		span       *subscribedSpan
		callbackCh chan func()
		resolvedCh chan uint64
	}
	newSub := func(subID SubscriptionID) *sub {
		callbackCh := make(chan func(), 16)
		resolvedCh := make(chan uint64, 16)
		s := &sub{
			callbackCh: callbackCh,
			resolvedCh: resolvedCh,
		}
		s.span = &subscribedSpan{
			subID: subID,
			consumeKVEvents: func(_ []common.RawKVEntry, finishCallback func()) bool {
				callbackCh <- finishCallback
				return true
			},
			advanceResolvedTs: func(ts uint64) {
				resolvedCh <- ts
			},
		}
		p.Register(s.span)
		return s
	}

	s1 := newSub(1)
	s2 := newSub(2)

	p.EnqueueData(ctx, s1.span, []common.RawKVEntry{{Key: []byte("a")}})
	p.EnqueueResolved(s1.span, 100)

	p.EnqueueData(ctx, s2.span, []common.RawKVEntry{{Key: []byte("b")}})
	p.EnqueueResolved(s2.span, 200)

	// Only persist sub2.
	cb2 := <-s2.callbackCh
	cb2()

	select {
	case ts := <-s2.resolvedCh:
		require.Equal(t, uint64(200), ts)
	case <-time.After(time.Second):
		require.FailNow(t, "sub2 resolved ts should advance independently")
	}

	select {
	case <-s1.resolvedCh:
		require.FailNow(t, "sub1 resolved ts must not advance without persistence")
	case <-time.After(200 * time.Millisecond):
	}
}
