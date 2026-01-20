package main

import (
	"container/heap"
	"context"
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
)

// benchSubscriptionClient feeds synthetic events into the eventStore.
type benchSubscriptionClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	scenario benchScenario
	stats    *benchStats

	idGen atomic.Uint64

	mu   sync.Mutex
	subs map[logpuller.SubscriptionID]*benchSubscription
}

func newBenchSubscriptionClient(ctx context.Context, scenario benchScenario, stats *benchStats) *benchSubscriptionClient {
	cctx, cancel := context.WithCancel(ctx)
	return &benchSubscriptionClient{
		ctx:      cctx,
		cancel:   cancel,
		scenario: scenario,
		stats:    stats,
		subs:     make(map[logpuller.SubscriptionID]*benchSubscription),
	}
}

func (c *benchSubscriptionClient) Name() string {
	return "eventstore-bench-subclient"
}

func (c *benchSubscriptionClient) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (c *benchSubscriptionClient) Close(ctx context.Context) error {
	c.cancel()
	c.mu.Lock()
	for id, sub := range c.subs {
		sub.stop()
		delete(c.subs, id)
	}
	c.mu.Unlock()
	return nil
}

func (c *benchSubscriptionClient) AllocSubscriptionID() logpuller.SubscriptionID {
	return logpuller.SubscriptionID(c.idGen.Add(1))
}

func (c *benchSubscriptionClient) Subscribe(
	subID logpuller.SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consume func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	bdrMode bool,
) {
	sub := newBenchSubscription(c.ctx, c.scenario, c.stats, subID, span, startTs, consume, advanceResolvedTs, advanceInterval)
	c.mu.Lock()
	c.subs[subID] = sub
	c.mu.Unlock()
	sub.start()
}

func (c *benchSubscriptionClient) Unsubscribe(subID logpuller.SubscriptionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sub, ok := c.subs[subID]; ok {
		sub.stop()
		delete(c.subs, subID)
	}
}

type benchSubscription struct {
	ctx      context.Context
	scenario benchScenario
	stats    *benchStats

	subID   logpuller.SubscriptionID
	span    heartbeatpb.TableSpan
	startTs uint64

	consume func([]common.RawKVEntry, func()) bool
	advance func(uint64)

	advanceInterval time.Duration

	rowID      atomic.Uint64
	commitTs   atomic.Uint64
	resolvedTs atomic.Uint64

	pendingMu   sync.Mutex
	pendingHeap pendingRangeHeap

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

type pendingRange struct {
	start uint64
	end   uint64
	acked atomic.Bool
}

type pendingRangeHeap []*pendingRange

func (h pendingRangeHeap) Len() int { return len(h) }

func (h pendingRangeHeap) Less(i, j int) bool {
	return h[i].start < h[j].start
}

func (h pendingRangeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *pendingRangeHeap) Push(x any) {
	*h = append(*h, x.(*pendingRange))
}

func (h *pendingRangeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func newBenchSubscription(
	ctx context.Context,
	scenario benchScenario,
	stats *benchStats,
	subID logpuller.SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consume func([]common.RawKVEntry, func()) bool,
	advance func(uint64),
	advanceInterval int64,
) *benchSubscription {
	return &benchSubscription{
		ctx:             ctx,
		scenario:        scenario,
		stats:           stats,
		subID:           subID,
		span:            span,
		startTs:         startTs,
		consume:         consume,
		advance:         advance,
		advanceInterval: scenario.advanceTicker(advanceInterval),
		stopCh:          make(chan struct{}),
	}
}

func (s *benchSubscription) start() {
	s.commitTs.Store(s.startTs)
	s.resolvedTs.Store(s.startTs)
	writerCount := s.scenario.writers()
	if writerCount <= 0 {
		writerCount = 1
	}
	for i := 0; i < writerCount; i++ {
		s.wg.Add(1)
		go s.writerLoop(i)
	}
	s.wg.Add(1)
	go s.resolvedLoop()
	// Seed the initial resolved ts to keep the subscription active.
	s.advance(s.startTs)
}

func (s *benchSubscription) stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

func (s *benchSubscription) writerLoop(workerID int) {
	defer s.wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID) + int64(s.span.TableID)*7919))
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.stopCh:
			return
		default:
		}
		batchSize := s.scenario.batchSize()
		startCommitTs, _, pending := s.allocPendingRange(batchSize)

		batch := make([]common.RawKVEntry, batchSize)
		var totalBytes int64
		for i := 0; i < batchSize; i++ {
			commitTs := startCommitTs + uint64(i)
			rowID := s.rowID.Add(1)
			kv := common.RawKVEntry{
				OpType:   common.OpTypePut,
				CRTs:     commitTs,
				StartTs:  commitTs - 1,
				RegionID: uint64(s.span.TableID%1024 + 1),
				Key:      encodeRowKey(s.span.TableID, rowID),
				Value:    randomBytes(rng, s.scenario.payloadSize()),
			}
			if s.shouldAddOldValue(rowID) {
				kv.OldValue = randomBytes(rng, s.scenario.oldValueSize())
			}
			totalBytes += kv.GetSize()
			batch[i] = kv
		}
		ack := s.stats.recordBatch(len(batch), totalBytes, time.Now())
		ackWithMark := func() {
			pending.acked.Store(true)
			ack()
		}
		if !s.consume(batch, ackWithMark) {
			return
		}
	}
}

func (s *benchSubscription) allocPendingRange(batchSize int) (start uint64, end uint64, pending *pendingRange) {
	if batchSize <= 0 {
		batchSize = 1
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	end = s.commitTs.Add(uint64(batchSize))
	start = end - uint64(batchSize) + 1
	pending = &pendingRange{start: start, end: end}
	heap.Push(&s.pendingHeap, pending)
	return start, end, pending
}

func (s *benchSubscription) resolvedLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.advanceInterval)
	defer ticker.Stop()
	lag := s.scenario.resolvedLag()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			floor := s.safeResolvedFloor()
			if floor <= lag {
				continue
			}
			target := floor - lag
			current := s.resolvedTs.Load()
			if target > current {
				if s.resolvedTs.CompareAndSwap(current, target) {
					s.advance(target)
				}
			}
		}
	}
}

func (s *benchSubscription) safeResolvedFloor() uint64 {
	s.pendingMu.Lock()
	for s.pendingHeap.Len() > 0 {
		head := s.pendingHeap[0]
		if !head.acked.Load() {
			break
		}
		heap.Pop(&s.pendingHeap)
	}
	var floor uint64
	if s.pendingHeap.Len() > 0 {
		floor = s.pendingHeap[0].start - 1
	} else {
		floor = s.commitTs.Load()
	}
	s.pendingMu.Unlock()
	if floor < s.startTs {
		return s.startTs
	}
	return floor
}

func (s *benchSubscription) shouldAddOldValue(rowID uint64) bool {
	if s.scenario.oldValueSize() == 0 {
		return false
	}
	updateEvery := s.scenario.updateEvery()
	if updateEvery <= 0 {
		return true
	}
	return int(rowID%uint64(updateEvery)) == 0
}

func encodeRowKey(tableID int64, rowID uint64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(tableID))
	binary.BigEndian.PutUint64(buf[8:], rowID)
	return buf
}

func randomBytes(rng *rand.Rand, size int) []byte {
	if size <= 0 {
		return nil
	}
	buf := make([]byte, size)
	_, _ = rng.Read(buf)
	return buf
}
