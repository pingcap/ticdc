// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	defaultScanInterval          = 5 * time.Second
	minScanInterval              = 1 * time.Second
	maxScanInterval              = 30 * time.Minute
	scanIntervalAdjustCooldown   = 30 * time.Second
	memoryUsageWindowDuration    = 30 * time.Second
	memoryUsageHighThreshold     = 0.7
	memoryUsageCriticalThreshold = 0.9
	memoryUsageLowThreshold      = 0.2
	memoryUsageVeryLowThreshold  = 0.1
)

type memoryUsageSample struct {
	ts    time.Time
	ratio float64
}

type memoryUsageWindow struct {
	window  time.Duration
	mu      sync.Mutex
	samples []memoryUsageSample
}

func newMemoryUsageWindow(window time.Duration) *memoryUsageWindow {
	return &memoryUsageWindow{
		window: window,
	}
}

func (w *memoryUsageWindow) addSample(now time.Time, ratio float64) {
	if ratio < 0 {
		ratio = 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	w.samples = append(w.samples, memoryUsageSample{ts: now, ratio: ratio})
	w.pruneLocked(now)
}

func (w *memoryUsageWindow) average(now time.Time) float64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.pruneLocked(now)
	if len(w.samples) == 0 {
		return 0
	}
	var sum float64
	for _, sample := range w.samples {
		sum += sample.ratio
	}
	return sum / float64(len(w.samples))
}

func (w *memoryUsageWindow) pruneLocked(now time.Time) {
	cutoff := now.Add(-w.window)
	idx := 0
	for idx < len(w.samples) && w.samples[idx].ts.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		w.samples = w.samples[idx:]
	}
}

func (w *memoryUsageWindow) span(now time.Time) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneLocked(now)
	if len(w.samples) == 0 {
		return 0
	}
	return now.Sub(w.samples[0].ts)
}

func (w *memoryUsageWindow) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.samples)
}

func (c *changefeedStatus) updateMemoryUsage(now time.Time, used uint64, max uint64) {
	if max == 0 || c.usageWindow == nil {
		return
	}

	ratio := float64(used) / float64(max)
	c.usageWindow.addSample(now, ratio)
	avg := c.usageWindow.average(now)
	c.adjustScanInterval(now, avg)
}

func (c *changefeedStatus) adjustScanInterval(now time.Time, avg float64) {
	if time.Since(c.lastAdjustTime.Load()) < scanIntervalAdjustCooldown {
		return
	}

	current := time.Duration(c.scanInterval.Load())
	if current <= 0 {
		current = defaultScanInterval
	}
	maxInterval := c.maxScanInterval()
	if maxInterval < minScanInterval {
		maxInterval = minScanInterval
	}

	newInterval := current
	switch {
	case avg > memoryUsageCriticalThreshold:
		newInterval = minScanInterval
	case avg > memoryUsageHighThreshold:
		newInterval = maxDuration(current/2, minScanInterval)
	case avg < memoryUsageVeryLowThreshold:
		// When memory pressure is very low, allow the scan interval to grow beyond
		// the sync point interval cap.
		maxInterval = maxScanInterval
		newInterval = minDuration(current*2, maxInterval)
	case avg < memoryUsageLowThreshold:
		newInterval = minDuration(current*2, maxInterval)
	}

	if newInterval != current {
		c.scanInterval.Store(int64(newInterval))
		c.lastAdjustTime.Store(now)
		log.Info("scan interval adjusted",
			zap.Stringer("changefeedID", c.changefeedID),
			zap.Duration("oldInterval", current),
			zap.Duration("newInterval", newInterval),
			zap.Duration("maxInterval", maxInterval),
			zap.Float64("avgUsage", avg),
			zap.Bool("syncPointEnabled", c.syncPointEnabled.Load()),
		)
	}
}

func (c *changefeedStatus) maxScanInterval() time.Duration {
	if !c.syncPointEnabled.Load() {
		return maxScanInterval
	}
	interval := time.Duration(c.syncPointInterval.Load())
	if interval <= 0 {
		return maxScanInterval
	}
	if interval < maxScanInterval {
		return interval
	}
	return maxScanInterval
}

func (c *changefeedStatus) refreshMinSentResolvedTs() {
	minSentResolvedTs := ^uint64(0)
	c.dispatchers.Range(func(_ any, value any) bool {
		dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
		if dispatcher == nil || dispatcher.isRemoved.Load() || dispatcher.seq.Load() == 0 {
			return true
		}
		sentResolvedTs := dispatcher.sentResolvedTs.Load()
		if sentResolvedTs < minSentResolvedTs {
			minSentResolvedTs = sentResolvedTs
		}
		return true
	})

	if minSentResolvedTs == ^uint64(0) {
		c.storeMinSentTs(0)
		return
	}
	c.storeMinSentTs(minSentResolvedTs)
}

func (c *changefeedStatus) getScanMaxTs() uint64 {
	baseTs := c.minSentTs.Load()
	if baseTs == 0 {
		return 0
	}
	interval := time.Duration(c.scanInterval.Load())
	if interval <= 0 {
		interval = defaultScanInterval
	}

	return oracle.GoTimeToTS(oracle.GetTimeFromTS(baseTs).Add(interval))
}

func (c *changefeedStatus) storeMinSentTs(value uint64) {
	prev := c.minSentTs.Load()
	if prev == value {
		return
	}
	c.minSentTs.Store(value)
	log.Info("scan window base updated",
		zap.Stringer("changefeedID", c.changefeedID),
		zap.Uint64("oldBaseTs", prev),
		zap.Uint64("newBaseTs", value),
	)
}

func (c *changefeedStatus) updateSyncPointConfig(info DispatcherInfo) {
	if !info.SyncPointEnabled() {
		return
	}
	c.syncPointEnabled.Store(true)
	interval := info.GetSyncPointInterval()
	if interval <= 0 {
		return
	}
	for {
		current := time.Duration(c.syncPointInterval.Load())
		if current != 0 && interval >= current {
			return
		}
		if c.syncPointInterval.CompareAndSwap(int64(current), int64(interval)) {
			return
		}
	}
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
