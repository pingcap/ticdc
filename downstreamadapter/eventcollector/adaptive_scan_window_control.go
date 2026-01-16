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

		syncPointSeen     bool
		syncPointEnabled  bool
		syncPointInterval time.Duration
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

		enableSyncPoint := stat.target.EnableSyncPoint()
		interval := stat.target.GetSyncPointInterval()
		if !syncPointSeen {
			syncPointSeen = true
			syncPointEnabled = enableSyncPoint
			syncPointInterval = interval
		} else {
			if enableSyncPoint != syncPointEnabled {
				log.Panic("syncpoint enabled mismatch among dispatchers",
					zap.Stringer("changefeedID", cfStat.changefeedID),
					zap.Stringer("dispatcherID", dispatcherID),
					zap.Bool("enableSyncPoint", enableSyncPoint))
			}
			if interval != syncPointInterval {
				log.Panic("syncpoint interval mismatch among dispatchers",
					zap.Stringer("changefeedID", cfStat.changefeedID),
					zap.Stringer("dispatcherID", dispatcherID),
					zap.Duration("syncPointInterval", syncPointInterval),
					zap.Duration("interval", interval))
			}
		}
		return true
	})

	if scanLimitBaseTs == ^uint64(0) {
		scanLimitBaseTs = 0
	}

	maxInterval := adaptiveScanWindowMax
	if syncPointEnabled && syncPointInterval > 0 {
		maxInterval = min(maxInterval, syncPointInterval)
	}

	scanInterval := cfStat.scanWindow.observe(memoryUsageRatio, maxInterval)
	return calcScanMaxTs(scanLimitBaseTs, scanInterval)
}
