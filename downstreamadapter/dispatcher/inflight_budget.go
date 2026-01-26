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

	enabled bool

	low        int64
	high       int64
	multiplier int64

	inflight  atomic.Int64
	blocked   atomic.Bool
	blockedAt atomic.Int64

	// dmlProgress tracks in-flight DML events only.
	// It's used for commitTs-based DDL deferral when in-flight budget is enabled.
	dmlProgress *TableProgress

	changefeedID common.ChangeFeedID
	dispatcherID common.DispatcherID

	inflightBudgetBlockDuration prometheus.Observer
	inflightBudgetBlockedCount  prometheus.Gauge
	inflightBudgetBytes         prometheus.Gauge
}

// noCopy is used to help `go vet` detect accidental copies of structs containing atomics.
// See https://github.com/golang/go/issues/8005
type noCopy struct{}

func (*noCopy) Lock() {}

func newInflightBudget(
	sinkType common.SinkType,
	changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
) inflightBudget {
	enabled := sinkType == common.CloudStorageSinkType || sinkType == common.RedoSinkType
	// just return zero value to reduce the memory allocation
	if !enabled {
		return inflightBudget{}
	}
	return inflightBudget{
		enabled:      enabled,
		low:          inflightDefaultPerLowBytes,
		high:         inflightDefaultPerHighBytes,
		changefeedID: changefeedID,
		dispatcherID: dispatcherID,
		multiplier:   inflightBytesMultiplier,
		dmlProgress:  NewTableProgress(),

		inflightBudgetBlockDuration: metrics.InflightBudgetBlockedDurationHist.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), sinkType.String()),
		inflightBudgetBlockedCount: metrics.InflightBudgetBlockedDispatcherCountGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), sinkType.String()),
		inflightBudgetBytes: metrics.InflightBudgetUnflushedBytesGauage.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), sinkType.String()),
	}
}

func (b *inflightBudget) isEnabled() bool {
	return b.enabled
}

func (b *inflightBudget) isBlocked() bool {
	return b.blocked.Load()
}

func (b *inflightBudget) trackDML(dml *commonEvent.DMLEvent, wakeCallback func()) {
	if !b.enabled {
		return
	}
	b.dmlProgress.Add(dml)
	effectiveBytes := dml.GetSize() * b.multiplier
	b.inflight.Add(effectiveBytes)
	b.inflightBudgetBytes.Add(float64(effectiveBytes))
	dml.AddPostFlushFunc(func() {
		inFlightBytes := b.inflight.Add(-effectiveBytes)
		b.inflightBudgetBytes.Sub(float64(effectiveBytes))
		if inFlightBytes < 0 {
			log.Warn("inflight bytes underflow",
				zap.Stringer("changefeedID", b.changefeedID),
				zap.Stringer("dispatcher", b.dispatcherID),
				zap.Int64("inFlightBytes", inFlightBytes),
			)
			b.inflight.Store(0)
			inFlightBytes = 0
		}
		if b.blocked.Load() && inFlightBytes <= b.low {
			if b.blocked.CompareAndSwap(true, false) {
				if blockedAt := b.blockedAt.Swap(0); blockedAt > 0 {
					b.inflightBudgetBlockDuration.Observe(time.Since(time.Unix(0, blockedAt)).Seconds())
				}
				b.inflightBudgetBlockedCount.Dec()
				log.Debug("dispatcher unblocked by inflight budget",
					zap.Stringer("changefeedID", b.changefeedID),
					zap.Stringer("dispatcher", b.dispatcherID),
					zap.Int64("inFlightBytes", inFlightBytes),
				)
				wakeCallback()
			}
		}
	})
}

// return true if there is inflight DMLs whose commit-ts smaller than the passed in commitTs.
func (b *inflightBudget) hasInflightBeforeCommitTs(commitTs uint64) bool {
	if !b.enabled || b.dmlProgress == nil {
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

func (b *inflightBudget) tryBlock() bool {
	if !b.enabled {
		return false
	}
	if b.inflight.Load() < b.high {
		return false
	}
	if b.blocked.CompareAndSwap(false, true) {
		b.blockedAt.Store(time.Now().UnixNano())
		b.inflightBudgetBlockedCount.Inc()
		log.Debug("dispatcher blocked by inflight bytes",
			zap.Stringer("changefeedID", b.changefeedID),
			zap.Stringer("dispatcher", b.dispatcherID),
			zap.Int64("inFlightBytes", b.inflight.Load()),
		)
	}
	return true
}

func (b *inflightBudget) cleanup() {
	if !b.enabled {
		return
	}

	b.inflightBudgetBytes.Sub(float64(b.inflight.Load()))
	if !b.blocked.CompareAndSwap(true, false) {
		return
	}
	if blockedAt := b.blockedAt.Swap(0); blockedAt > 0 {
		b.inflightBudgetBlockDuration.Observe(time.Since(time.Unix(0, blockedAt)).Seconds())
	}
	b.inflightBudgetBlockedCount.Dec()
}
