package eventcollector

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

func (c *EventCollector) updateScanMaxTsForChangefeed(
	cfStat *changefeedStat,
	memoryUsageRatio float64,
) uint64 {
	var (
		scanLimitBaseTs uint64 = ^uint64(0)
		hasEligible     bool

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

		// Ignore dispatchers that never received any resolved-ts event.
		if stat.hasReceivedResolvedTs.Load() {
			hasEligible = true
			if checkpointTs > 0 && checkpointTs < scanLimitBaseTs {
				scanLimitBaseTs = checkpointTs
			}
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

	maxInterval := adaptiveScanWindowMax
	_ = syncPointEnabled
	if syncPointInterval > 0 {
		maxInterval = min(maxInterval, syncPointInterval)
	}

	scanInterval := cfStat.scanWindow.observe(memoryUsageRatio, maxInterval)
	cfStat.metricScanInterval.Set(scanInterval.Seconds())

	if !hasEligible || scanLimitBaseTs == ^uint64(0) {
		// scanMaxTs == 0 means no scan window limit in EventService.
		cfStat.metricScanMaxTs.Set(0)
		return 0
	}

	scanMaxTs := calcScanMaxTs(scanLimitBaseTs, scanInterval)
	cfStat.metricScanMaxTs.Set(float64(scanMaxTs))
	return scanMaxTs
}
