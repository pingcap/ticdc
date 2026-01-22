package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
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
	enabled bool

	low        int64
	high       int64
	multiplier int64

	inflight  atomic.Int64
	blocked   atomic.Bool
	blockedAt atomic.Int64

	changefeedID common.ChangeFeedID
	dispatcherID common.DispatcherID
}

func newInflightBudget(
	sinkType common.SinkType,
	changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
) inflightBudget {
	enabled := sinkType == common.CloudStorageSinkType || sinkType == common.RedoSinkType
	return inflightBudget{
		enabled:      enabled,
		low:          inflightDefaultPerLowBytes,
		high:         inflightDefaultPerHighBytes,
		changefeedID: changefeedID,
		dispatcherID: dispatcherID,
		multiplier:   inflightBytesMultiplier,
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
	effectiveBytes := dml.GetSize() * b.multiplier
	b.inflight.Add(effectiveBytes)
	dml.AddPostFlushFunc(func() {
		inFlightBytes := b.inflight.Add(-effectiveBytes)
		if b.blocked.Load() && inFlightBytes <= b.low {
			if b.blocked.CompareAndSwap(true, false) {
				if blockedAt := b.blockedAt.Swap(0); blockedAt > 0 {
					metrics.AsyncSinkInflightBudgetBlockedDuration.
						WithLabelValues(b.changefeedID.Keyspace(), b.changefeedID.Name()).
						Observe(time.Since(time.Unix(0, blockedAt)).Seconds())
					metrics.AsyncSinkInflightBudgetBlockedDispatcherCount.
						WithLabelValues(b.changefeedID.Keyspace(), b.changefeedID.Name()).
						Dec()
					log.Info("dispatcher unblocked by inflight budget",
						zap.Stringer("changefeedID", b.changefeedID),
						zap.Stringer("dispatcher", b.dispatcherID),
						zap.Int64("inFlightBytes", inFlightBytes),
					)
				}
				wakeCallback()
			}
		}
	})
}

// tryBlock returns true if the caller should block the dynstream path due to exceeding high watermark.
// It also handles metrics/logging when entering blocked state.
func (b *inflightBudget) tryBlock() bool {
	if !b.enabled {
		return false
	}
	if b.inflight.Load() < b.high {
		return false
	}
	if b.blocked.CompareAndSwap(false, true) {
		b.blockedAt.Store(time.Now().UnixNano())
		metrics.AsyncSinkInflightBudgetBlockedDispatcherCount.
			WithLabelValues(b.changefeedID.Keyspace(), b.changefeedID.Name()).
			Inc()
		log.Info("dispatcher blocked by inflight bytes",
			zap.Stringer("changefeedID", b.changefeedID),
			zap.Stringer("dispatcher", b.dispatcherID),
			zap.Int64("inFlightBytes", b.inflight.Load()),
		)
	}
	return true
}
