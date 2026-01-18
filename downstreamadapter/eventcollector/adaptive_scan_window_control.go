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
		scanLimitBaseTs          uint64 = ^uint64(0)
		scanLimitBaseTsCandidate uint64 = ^uint64(0)
		hasEligible              bool

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
		if checkpointTs > 0 && checkpointTs < scanLimitBaseTsCandidate {
			scanLimitBaseTsCandidate = checkpointTs
		}

		// Ignore dispatchers that never received any handshake event.
		// These dispatchers haven't started a new event stream yet (handshake not received for current epoch).
		if stat.hasReceivedHandshakeEventOnce.Load() {
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

	if hasEligible && scanLimitBaseTs != ^uint64(0) {
		cfStat.scanLimitMu.Lock()
		if !cfStat.lastScanLimitBaseTsValid || cfStat.lastScanLimitBaseTs != scanLimitBaseTs {
			cfStat.lastScanLimitBaseTs = scanLimitBaseTs
			cfStat.lastScanLimitBaseTsValid = true
		}
		cfStat.scanLimitMu.Unlock()
	} else {
		// No eligible dispatcher in this tick, reuse last base if available.
		// If base hasn't been initialized yet, fall back to the min checkpointTs among all dispatchers.
		cfStat.scanLimitMu.Lock()
		if cfStat.lastScanLimitBaseTsValid {
			scanLimitBaseTs = cfStat.lastScanLimitBaseTs
		} else if scanLimitBaseTsCandidate != ^uint64(0) {
			scanLimitBaseTs = scanLimitBaseTsCandidate
			cfStat.lastScanLimitBaseTs = scanLimitBaseTs
			cfStat.lastScanLimitBaseTsValid = true
		} else {
			scanLimitBaseTs = 0
		}
		cfStat.scanLimitMu.Unlock()
	}

	maxInterval := adaptiveScanWindowMax
	_ = syncPointEnabled
	if syncPointInterval > 0 {
		maxInterval = min(maxInterval, syncPointInterval)
	}

	scanInterval := cfStat.scanWindow.observe(memoryUsageRatio, maxInterval)
	scanMaxTs := calcScanMaxTs(scanLimitBaseTs, scanInterval)
	cfStat.metricScanInterval.Set(scanInterval.Seconds())
	cfStat.metricScanMaxTs.Set(float64(scanMaxTs))
	return scanMaxTs
}
