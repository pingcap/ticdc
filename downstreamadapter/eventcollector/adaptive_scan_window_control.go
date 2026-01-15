package eventcollector

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

func (c *EventCollector) updateScanMaxTsForChangefeed(cfStat *changefeedStat, memoryUsageRatio float64) uint64 {
	var (
		scanLimitBaseTs uint64 = ^uint64(0)

		hasSyncPoint         bool
		minSyncPointInterval time.Duration
	)

	cfStat.dispatcherIDs.Range(func(k, _ any) bool {
		dispatcherID := k.(common.DispatcherID)
		v, ok := c.dispatcherMap.Load(dispatcherID)
		if !ok {
			return true
		}
		stat := v.(*dispatcherStat)

		checkpointTs := stat.target.GetCheckpointTs()
		if checkpointTs > 0 && checkpointTs < scanLimitBaseTs {
			scanLimitBaseTs = checkpointTs
		}

		if stat.target.EnableSyncPoint() {
			interval := stat.target.GetSyncPointInterval()
			if interval > 0 && (!hasSyncPoint || interval < minSyncPointInterval) {
				hasSyncPoint = true
				minSyncPointInterval = interval
			}
		}
		return true
	})

	if scanLimitBaseTs == ^uint64(0) {
		scanLimitBaseTs = 0
	}

	maxInterval := adaptiveScanWindowMax
	if hasSyncPoint && minSyncPointInterval > 0 {
		maxInterval = min(maxInterval, minSyncPointInterval)
	}

	interval, needReset := cfStat.scanWindow.observe(memoryUsageRatio, maxInterval)
	if needReset {
		log.Warn("changefeed memory usage exceeds reset threshold for too long, reset dispatchers",
			zap.Stringer("changefeedID", cfStat.changefeedID),
			zap.Float64("memoryUsageRatio", memoryUsageRatio),
			zap.Duration("scanInterval", interval),
			zap.Duration("maxInterval", maxInterval),
		)
		c.resetChangefeedDispatchers(cfStat)
	}

	return calcScanMaxTs(scanLimitBaseTs, interval)
}

func (c *EventCollector) resetChangefeedDispatchers(cfStat *changefeedStat) {
	cfStat.dispatcherIDs.Range(func(k, _ any) bool {
		dispatcherID := k.(common.DispatcherID)
		v, ok := c.dispatcherMap.Load(dispatcherID)
		if !ok {
			return true
		}
		stat := v.(*dispatcherStat)

		// Best-effort: drop buffered events first to free memory quickly.
		if common.IsRedoMode(stat.target.GetMode()) {
			c.redoDs.Release(dispatcherID)
		} else {
			c.ds.Release(dispatcherID)
		}

		eventServiceID := stat.connState.getEventServiceID()
		if eventServiceID != "" {
			stat.reset(eventServiceID)
		}
		return true
	})
}
