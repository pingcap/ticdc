package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	inflightBytesMultiplier int64 = 2
	// inflightDefaultPerLowBytes is a conservative default derived from:
	// - cloudstorage: file-size default 64MiB
	// - redo: max-log-size default 64MiB
	// This is the Phase 1 (per-dispatcher only) watermark until we wire exact values.
	inflightDefaultPerLowBytes  int64 = 64 << 20
	inflightDefaultPerHighBytes       = inflightDefaultPerLowBytes * 2
)

// inflightBudget is a per-dispatcher inflight-bytes budget.
//
// It controls whether the dynstream path should return await=true (block) when the in-flight bytes
// exceed the high watermark, and wakes the path when bytes fall back to the low watermark.
//
// This struct is concurrency-safe: it is accessed by the dynstream handler goroutine (enqueue),
// and by sink flush callbacks (dequeue).
type inflightBudget struct {
	_ noCopy

	enabled  bool
	sinkType common.SinkType

	sharedInfo   *SharedInfo
	dispatcherID common.DispatcherID

	low        int64
	high       int64
	multiplier int64

	inflight      atomic.Int64
	blocked       atomic.Bool
	blockedAt     atomic.Int64
	globalBlocked atomic.Bool

	// dmlProgress tracks in-flight DML events only.
	// It's used for commitTs-based DDL deferral when in-flight budget is enabled.
	// it's not relevant to the dispatcher checkpointTs calculation.
	dmlProgress *TableProgress

	inflightBudgetBlockDuration prometheus.Observer
	inflightBudgetBlockedCount  prometheus.Gauge
	inflightBudgetBytes         prometheus.Gauge
}

// noCopy is used to help `go vet` detect accidental copies of structs containing atomics.
// See https://github.com/golang/go/issues/8005
type noCopy struct{}

func (*noCopy) Lock() {}

func newInflightBudget(
	dispatcherID common.DispatcherID,
	sharedInfo *SharedInfo,
	sinkType common.SinkType,
) inflightBudget {
	switch sinkType {
	case common.CloudStorageSinkType, common.RedoSinkType:
	default:
		// just return zero value to reduce the memory allocation
		return inflightBudget{}
	}
	var (
		namespace = sharedInfo.changefeedID.Keyspace()
		name      = sharedInfo.changefeedID.Name()
		t         = sinkType.String()
	)
	return inflightBudget{
		enabled:      true,
		sinkType:     sinkType,
		sharedInfo:   sharedInfo,
		low:          inflightDefaultPerLowBytes,
		high:         inflightDefaultPerHighBytes,
		dispatcherID: dispatcherID,
		multiplier:   inflightBytesMultiplier,
		dmlProgress:  NewTableProgress(),

		inflightBudgetBlockDuration: metrics.InflightBudgetBlockedDurationHist.
			WithLabelValues(namespace, name, t),
		inflightBudgetBlockedCount: metrics.InflightBudgetBlockedDispatcherCountGauge.
			WithLabelValues(namespace, name, t),
		inflightBudgetBytes: metrics.InflightBudgetUnflushedBytesGauage.
			WithLabelValues(namespace, name, t),
	}
}

func (b *inflightBudget) isEnabled() bool {
	return b.enabled
}

func (b *inflightBudget) isBlocked() bool {
	return b.blocked.Load() || b.globalBlocked.Load()
}

func (b *inflightBudget) global() *globalInflightBudget {
	if b.sharedInfo == nil {
		return nil
	}
	return b.sharedInfo.getChangefeedInflightBudget(b.sinkType)
}

func (b *inflightBudget) trackDML(dml *commonEvent.DMLEvent, wakeCallback func()) {
	if !b.enabled {
		return
	}
	b.dmlProgress.Add(dml)
	nBytes := dml.GetSize() * b.multiplier
	b.inflight.Add(nBytes)
	b.inflightBudgetBytes.Add(float64(nBytes))
	if global := b.global(); global != nil {
		global.acquire(nBytes)
	}
	dml.AddPostFlushFunc(func() {
		inFlightBytes := b.inflight.Add(-nBytes)
		b.inflightBudgetBytes.Sub(float64(nBytes))
		if global := b.global(); global != nil {
			global.release(nBytes)
		}
		if inFlightBytes < 0 {
			log.Warn("inflight bytes underflow",
				zap.Stringer("changefeedID", b.sharedInfo.changefeedID),
				zap.Stringer("dispatcher", b.dispatcherID),
				zap.Int64("inFlightBytes", inFlightBytes),
			)
			b.inflight.Store(0)
			inFlightBytes = 0
		}

		if inFlightBytes <= b.low && b.blocked.CompareAndSwap(true, false) {
			if blockedAt := b.blockedAt.Swap(0); blockedAt > 0 {
				b.inflightBudgetBlockDuration.Observe(time.Since(time.Unix(0, blockedAt)).Seconds())
			}
			b.inflightBudgetBlockedCount.Dec()
			log.Debug("dispatcher unblocked by inflight budget",
				zap.Stringer("changefeedID", b.sharedInfo.changefeedID),
				zap.Stringer("dispatcher", b.dispatcherID),
				zap.Int64("inFlightBytes", inFlightBytes),
			)
			if b.globalBlocked.Load() {
				return
			}
			if global := b.global(); global != nil && global.TryBlock(b.dispatcherID, b.makeGlobalWake(wakeCallback)) {
				b.globalBlocked.Store(true)
				return
			}
			wakeCallback()
		}
	})
}

func (b *inflightBudget) makeGlobalWake(wakeCallback func()) func() {
	return func() {
		if !b.globalBlocked.CompareAndSwap(true, false) {
			return
		}
		if b.blocked.Load() {
			return
		}
		wakeCallback()
	}
}

// return true if there is inflight DMLs whose commit-ts smaller than the passed in commitTs.
func (b *inflightBudget) hasInflightBeforeCommitTs(commitTs uint64) bool {
	if !b.enabled {
		return false
	}
	checkpointTs, isEmpty := b.dmlProgress.GetCheckpointTs()
	if isEmpty {
		return false
	}
	// When dmlProgress is not empty, its checkpoint is (earliestUnflushedCommitTs - 1).
	earliestUnflushedCommitTs := checkpointTs + 1
	return earliestUnflushedCommitTs < commitTs
}

func (b *inflightBudget) tryBlock(wakeCallback func()) bool {
	if !b.enabled {
		return false
	}

	var blocked bool
	if b.inflight.Load() >= b.high {
		blocked = true
		if b.blocked.CompareAndSwap(false, true) {
			b.blockedAt.Store(time.Now().UnixNano())
			b.inflightBudgetBlockedCount.Inc()
			log.Debug("dispatcher blocked by inflight bytes",
				zap.Stringer("changefeedID", b.sharedInfo.changefeedID),
				zap.Stringer("dispatcher", b.dispatcherID),
				zap.Int64("inFlightBytes", b.inflight.Load()),
			)
		}
	}
	if blocked {
		return true
	}

	if global := b.global(); global != nil && global.TryBlock(b.dispatcherID, b.makeGlobalWake(wakeCallback)) {
		blocked = true
		b.globalBlocked.Store(true)
	}
	return blocked
}

func (b *inflightBudget) cleanup() {
	if !b.enabled {
		return
	}

	b.inflightBudgetBytes.Sub(float64(b.inflight.Load()))

	if global := b.global(); global != nil {
		global.cleanupDispatcher(b.dispatcherID)
	}
	b.globalBlocked.Store(false)

	if !b.blocked.CompareAndSwap(true, false) {
		return
	}
	if blockedAt := b.blockedAt.Swap(0); blockedAt > 0 {
		b.inflightBudgetBlockDuration.Observe(time.Since(time.Unix(0, blockedAt)).Seconds())
	}
	b.inflightBudgetBlockedCount.Dec()
}
