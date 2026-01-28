package dispatcher

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const defaultMaxWakePerRound = 64

type changefeedInflightBudget struct {
	_ noCopy

	enabled bool

	low  atomic.Int64
	high atomic.Int64

	inflight atomic.Int64

	mu           sync.Mutex
	blockedSet   map[common.DispatcherID]blockedEntry
	blockedQueue []common.DispatcherID
	queueHead    int

	maxWakePerRound int

	changefeedID common.ChangeFeedID
	sinkType     common.SinkType

	globalBlockedDuration prometheus.Observer
	globalBlockedCount    prometheus.Gauge
	globalBytes           prometheus.Gauge
}

type blockedEntry struct {
	wake      func()
	blockedAt int64
}

func newChangefeedInflightBudget(
	sinkType common.SinkType,
	changefeedID common.ChangeFeedID,
	highBytes int64,
	lowBytes int64,
) *changefeedInflightBudget {
	if highBytes <= 0 || lowBytes <= 0 || lowBytes >= highBytes {
		log.Warn("invalid inflight budget watermarks, budget disabled",
			zap.Stringer("changefeedID", changefeedID),
			zap.String("sinkType", sinkType.String()),
			zap.Int64("highBytes", highBytes),
			zap.Int64("lowBytes", lowBytes),
		)
		return &changefeedInflightBudget{}
	}

	b := &changefeedInflightBudget{
		enabled:         true,
		blockedSet:      make(map[common.DispatcherID]blockedEntry),
		maxWakePerRound: defaultMaxWakePerRound,
		changefeedID:    changefeedID,
		sinkType:        sinkType,

		globalBlockedDuration: metrics.InflightBudgetGlobalBlockedDurationHist.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), sinkType.String()),
		globalBlockedCount: metrics.InflightBudgetGlobalBlockedDispatcherCountGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), sinkType.String()),
		globalBytes: metrics.InflightBudgetGlobalUnflushedBytesGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), sinkType.String()),
	}
	b.high.Store(highBytes)
	b.low.Store(lowBytes)
	return b
}

func (b *changefeedInflightBudget) OnEnqueue(bytes int64) {
	if !b.enabled || bytes <= 0 {
		return
	}
	b.inflight.Add(bytes)
	b.globalBytes.Add(float64(bytes))
}

func (b *changefeedInflightBudget) TryBlock(dispatcherID common.DispatcherID, wake func()) bool {
	if !b.enabled {
		return false
	}
	if b.inflight.Load() < b.high.Load() {
		return false
	}
	b.registerBlocked(dispatcherID, wake)
	return true
}

func (b *changefeedInflightBudget) registerBlocked(dispatcherID common.DispatcherID, wake func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if entry, ok := b.blockedSet[dispatcherID]; ok {
		// update wake callback in case it changes (should be stable for a path, but keep it safe).
		entry.wake = wake
		b.blockedSet[dispatcherID] = entry
		return
	}
	b.blockedSet[dispatcherID] = blockedEntry{
		wake:      wake,
		blockedAt: time.Now().UnixNano(),
	}
	b.blockedQueue = append(b.blockedQueue, dispatcherID)
	b.globalBlockedCount.Inc()
}

func (b *changefeedInflightBudget) OnFlush(bytes int64) {
	if !b.enabled || bytes <= 0 {
		return
	}
	inFlightBytes := b.inflight.Add(-bytes)
	b.globalBytes.Sub(float64(bytes))
	if inFlightBytes < 0 {
		log.Warn("global inflight bytes underflow",
			zap.Stringer("changefeedID", b.changefeedID),
			zap.String("sinkType", b.sinkType.String()),
			zap.Int64("inFlightBytes", inFlightBytes),
		)
		b.inflight.Store(0)
		inFlightBytes = 0
	}

	if inFlightBytes > b.low.Load() {
		return
	}
	b.wakeBlockedDispatchers()
}

func (b *changefeedInflightBudget) wakeBlockedDispatchers() {
	now := time.Now()
	toWake := make([]func(), 0, b.maxWakePerRound)
	blockedAts := make([]int64, 0, b.maxWakePerRound)

	b.mu.Lock()
	for len(toWake) < b.maxWakePerRound && b.queueHead < len(b.blockedQueue) {
		if b.inflight.Load() >= b.high.Load() {
			break
		}

		dispatcherID := b.blockedQueue[b.queueHead]
		b.queueHead++

		entry, ok := b.blockedSet[dispatcherID]
		if !ok {
			continue
		}
		delete(b.blockedSet, dispatcherID)
		b.globalBlockedCount.Dec()
		toWake = append(toWake, entry.wake)
		blockedAts = append(blockedAts, entry.blockedAt)
	}

	if b.queueHead >= len(b.blockedQueue) {
		b.blockedQueue = b.blockedQueue[:0]
		b.queueHead = 0
	} else if b.queueHead > 1024 && b.queueHead*2 > len(b.blockedQueue) {
		copy(b.blockedQueue, b.blockedQueue[b.queueHead:])
		b.blockedQueue = b.blockedQueue[:len(b.blockedQueue)-b.queueHead]
		b.queueHead = 0
	}
	b.mu.Unlock()

	for i, wake := range toWake {
		if blockedAt := blockedAts[i]; blockedAt > 0 {
			b.globalBlockedDuration.Observe(now.Sub(time.Unix(0, blockedAt)).Seconds())
		}
		wake()
	}
}

func (b *changefeedInflightBudget) CleanupDispatcher(dispatcherID common.DispatcherID) {
	if !b.enabled {
		return
	}
	var blockedAt int64
	b.mu.Lock()
	if entry, ok := b.blockedSet[dispatcherID]; ok {
		blockedAt = entry.blockedAt
		delete(b.blockedSet, dispatcherID)
		b.globalBlockedCount.Dec()
	}
	b.mu.Unlock()

	if blockedAt > 0 {
		b.globalBlockedDuration.Observe(time.Since(time.Unix(0, blockedAt)).Seconds())
	}
}
