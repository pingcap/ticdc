package logpuller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestSpanPipeline_ResolvedWaitsForPersistedPrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := newSpanPipelineManager(ctx, 1, 1024, 1<<20)
	defer mgr.Close()

	var (
		mu        sync.Mutex
		callbacks []func()
		advanced  []uint64
	)

	span := &subscribedSpan{subID: SubscriptionID(1)}
	span.consumeKVEvents = func(_ []common.RawKVEntry, finishCallback func()) bool {
		mu.Lock()
		callbacks = append(callbacks, finishCallback)
		mu.Unlock()
		return true
	}
	span.advanceResolvedTs = func(ts uint64) {
		mu.Lock()
		advanced = append(advanced, ts)
		mu.Unlock()
	}

	mgr.Register(span)

	require.True(t, mgr.EnqueueData(ctx, span, span.emitSeq.Add(1), []common.RawKVEntry{{Key: []byte("a"), CRTs: 1}}))
	require.True(t, mgr.EnqueueData(ctx, span, span.emitSeq.Add(1), []common.RawKVEntry{{Key: []byte("b"), CRTs: 2}}))
	require.True(t, mgr.EnqueueResolved(span, span.emitSeq.Add(1), 100))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(callbacks) == 2
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	cb1 := callbacks[0]
	cb2 := callbacks[1]
	mu.Unlock()

	cb2()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	require.Len(t, advanced, 0)
	mu.Unlock()

	cb1()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(advanced) == 1 && advanced[0] == 100
	}, time.Second, 10*time.Millisecond)
}

func TestSpanPipeline_ResolvedMergeSameWaitSeq(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := newSpanPipelineManager(ctx, 1, 1024, 1<<20)
	defer mgr.Close()

	var (
		mu        sync.Mutex
		callbacks []func()
		advanced  []uint64
	)

	span := &subscribedSpan{subID: SubscriptionID(1)}
	span.consumeKVEvents = func(_ []common.RawKVEntry, finishCallback func()) bool {
		mu.Lock()
		callbacks = append(callbacks, finishCallback)
		mu.Unlock()
		return true
	}
	span.advanceResolvedTs = func(ts uint64) {
		mu.Lock()
		advanced = append(advanced, ts)
		mu.Unlock()
	}

	mgr.Register(span)

	require.True(t, mgr.EnqueueData(ctx, span, span.emitSeq.Add(1), []common.RawKVEntry{{Key: []byte("a"), CRTs: 1}}))
	require.True(t, mgr.EnqueueResolved(span, span.emitSeq.Add(1), 10))
	require.True(t, mgr.EnqueueResolved(span, span.emitSeq.Add(1), 12))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(callbacks) == 1
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	cb := callbacks[0]
	mu.Unlock()
	cb()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(advanced) == 1 && advanced[0] == 12
	}, time.Second, 10*time.Millisecond)
}

func TestSpanPipeline_MultipleSubscriptionsIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := newSpanPipelineManager(ctx, 1, 1024, 1<<20)
	defer mgr.Close()

	var (
		mu            sync.Mutex
		cbBySub       = map[SubscriptionID][]func(){}
		advancedBySub = map[SubscriptionID][]uint64{}
		span1         = &subscribedSpan{subID: SubscriptionID(1)}
		span2         = &subscribedSpan{subID: SubscriptionID(2)}
	)

	makeConsume := func(subID SubscriptionID) func([]common.RawKVEntry, func()) bool {
		return func(_ []common.RawKVEntry, finishCallback func()) bool {
			mu.Lock()
			cbBySub[subID] = append(cbBySub[subID], finishCallback)
			mu.Unlock()
			return true
		}
	}
	makeAdvance := func(subID SubscriptionID) func(uint64) {
		return func(ts uint64) {
			mu.Lock()
			advancedBySub[subID] = append(advancedBySub[subID], ts)
			mu.Unlock()
		}
	}

	span1.consumeKVEvents = makeConsume(span1.subID)
	span1.advanceResolvedTs = makeAdvance(span1.subID)
	span2.consumeKVEvents = makeConsume(span2.subID)
	span2.advanceResolvedTs = makeAdvance(span2.subID)

	mgr.Register(span1)
	mgr.Register(span2)

	require.True(t, mgr.EnqueueData(ctx, span1, span1.emitSeq.Add(1), []common.RawKVEntry{{Key: []byte("a"), CRTs: 1}}))
	require.True(t, mgr.EnqueueResolved(span1, span1.emitSeq.Add(1), 100))

	require.True(t, mgr.EnqueueData(ctx, span2, span2.emitSeq.Add(1), []common.RawKVEntry{{Key: []byte("b"), CRTs: 1}}))
	require.True(t, mgr.EnqueueResolved(span2, span2.emitSeq.Add(1), 200))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(cbBySub[span1.subID]) == 1 && len(cbBySub[span2.subID]) == 1
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	cb2 := cbBySub[span2.subID][0]
	mu.Unlock()
	cb2()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(advancedBySub[span2.subID]) == 1 && advancedBySub[span2.subID][0] == 200
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	require.Len(t, advancedBySub[span1.subID], 0)
	mu.Unlock()
}
