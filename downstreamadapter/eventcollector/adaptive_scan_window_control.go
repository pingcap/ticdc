package eventcollector

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
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

	scanInterval := cfStat.scanWindow.observe(memoryUsageRatio, maxInterval)
	return calcScanMaxTs(scanLimitBaseTs, scanInterval)
}
