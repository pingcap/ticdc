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

package dynstream

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// MemoryControlForPuller is the algorithm of the memory control.
	// It sill send pause and resume [area, path] feedback.
	MemoryControlForPuller = 0
	// MemoryControlForEventCollector is the algorithm of the memory control.
	// It will only send pause and resume [path] feedback.
	// For now, we only use it in event collector.
	MemoryControlForEventCollector = 1

	defaultReleaseMemoryRatio     = 0.4
	defaultDeadlockDuration       = 5 * time.Second
	defaultReleaseMemoryThreshold = 256 // 256 bytes
	defaultBurstMemoryFactor      = 2   // burst memory factor.

	// Congestion-control knobs for smoothing AvailableMemory after forced releases.
	defaultCongestionDecreaseFactor   = 0.5             // multiplicative decrease factor (<=1).
	defaultCongestionMinWindowRatio   = 0.1             // never shrink window below 10% capacity.
	defaultCongestionAdditiveRatio    = 0.05            // additive increase per interval.
	defaultCongestionMinStepBytes     = 64 * 1024       // 64KB, ensure growth even when capacity is small.
	defaultCongestionFreezeDuration   = 4 * time.Second // pause growth after a release.
	defaultCongestionIncreaseInterval = 1 * time.Second

	defaultCongestionMaxWindowRatio = 0.5 // never grow window above 50% capacity.

	// Drain-smoothing knobs.
	defaultDrainEWMASmoothing        = 0.25
	defaultDrainSampleInterval       = 100 * time.Millisecond
	defaultDrainWindowSeconds        = 2.0
	defaultDrainMinBudgetBytes int64 = 64 * 1024

	// Multi-level watermark ratios.
	defaultSoftWatermarkRatio        = 0.65
	defaultHardWatermarkRatio        = 0.85
	defaultHardWatermarkFreezeWindow = 6 * time.Second
)

// areaMemStat is used to store the memory statistics of an area.
// It is a global level struct, not stream level.
type areaMemStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area    A
	pathMap sync.Map // key: path, value: pathInfo

	// Reverse reference to the memControl this area belongs to.
	memControl *memControl[A, P, T, D, H]

	settings     atomic.Pointer[AreaSettings]
	feedbackChan chan<- Feedback[A, P, D]

	pathCount            atomic.Int64
	totalPendingSize     atomic.Int64
	paused               atomic.Bool
	lastSendFeedbackTime atomic.Value
	algorithm            MemoryControlAlgorithm

	lastAppendEventTime   atomic.Value
	lastSizeDecreaseTime  atomic.Value
	lastReleaseMemoryTime atomic.Value

	// Congestion-control state for smoothed AvailableMemory reporting.
	congestionWindow        atomic.Int64
	lastCongestionEventTime atomic.Value
	lastWindowUpdateTime    atomic.Value
	congestionFreezeUntil   atomic.Value

	// Drain estimator state.
	drainSampleBytes    atomic.Int64
	lastDrainUpdateTime atomic.Value
	drainRateEWMA       atomic.Uint64
}

func newAreaMemStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	area A,
	memoryControl *memControl[A, P, T, D, H],
	settings AreaSettings,
	feedbackChan chan<- Feedback[A, P, D],
) *areaMemStat[A, P, T, D, H] {
	settings.fix()
	res := &areaMemStat[A, P, T, D, H]{
		area:                 area,
		memControl:           memoryControl,
		feedbackChan:         feedbackChan,
		lastSendFeedbackTime: atomic.Value{},
		lastSizeDecreaseTime: atomic.Value{},
		algorithm:            NewMemoryControlAlgorithm(settings.algorithm),
	}

	res.lastAppendEventTime.Store(time.Now())
	res.lastSendFeedbackTime.Store(time.Now())
	res.lastSizeDecreaseTime.Store(time.Now())
	res.lastReleaseMemoryTime.Store(time.Now())
	res.lastCongestionEventTime.Store(time.Time{})
	now := time.Now()
	res.lastWindowUpdateTime.Store(now)
	res.congestionFreezeUntil.Store(time.Time{})
	res.lastDrainUpdateTime.Store(time.Time{})
	res.drainRateEWMA.Store(math.Float64bits(0))
	res.congestionWindow.Store(int64(settings.maxPendingSize))
	res.settings.Store(&settings)
	return res
}

// var testCounter atomic.Int64

// appendEvent try to append an event to the path's pending queue.
// It returns true if the event is appended successfully.
// This method is called by streams' handleLoop concurrently, but it is thread safe.
// We use atomic operations to update the totalPendingSize and the path's pendingSize.
func (as *areaMemStat[A, P, T, D, H]) appendEvent(
	path *pathInfo[A, P, T, D, H],
	event eventWrap[A, P, T, D, H],
	handler H,
) bool {
	defer as.updateAreaPauseState(path)
	as.lastAppendEventTime.Store(time.Now())

	// Check if we should merge periodic signals.
	if event.eventType.Property == PeriodicSignal {
		back, ok := path.pendingQueue.BackRef()
		if ok && back.eventType.Property == PeriodicSignal {
			// If the last event is a periodic signal, we only need to keep the latest one.
			// And we don't need to add a new signal.
			*back = event
			return true
		}
	}

	if as.checkDeadlock() {
		as.releaseMemory()
	}

	if as.memoryUsageRatio() >= defaultBurstMemoryFactor && as.settings.Load().algorithm ==
		MemoryControlForEventCollector {
		as.releaseMemory()
		if event.eventType.Droppable {
			dropEvent := handler.OnDrop(event.event)
			if dropEvent != nil {
				event.eventType = handler.GetType(dropEvent.(T))
				event.event = dropEvent.(T)
				path.pendingQueue.PushBack(event)
				return true
			}
		}
	}

	failpoint.Inject("InjectDropEvent", func() {
		if as.settings.Load().algorithm == MemoryControlForEventCollector {
			dropEvent := handler.OnDrop(event.event)
			if dropEvent != nil {
				event.eventType = handler.GetType(dropEvent.(T))
				event.event = dropEvent.(T)
				path.pendingQueue.PushBack(event)
				failpoint.Return(true)
			}
		}
	})

	// Add the event to the pending queue.
	path.pendingQueue.PushBack(event)
	// Update the pending size.
	path.updatePendingSize(int64(event.eventSize))
	as.totalPendingSize.Add(int64(event.eventSize))
	return true
}

func (as *areaMemStat[A, P, T, D, H]) checkDeadlock() bool {
	failpoint.Inject("InjectDeadlock", func() { failpoint.Return(true) })

	if as.settings.Load().algorithm !=
		MemoryControlForEventCollector {
		return false
	}

	hasEventComeButNotOut := time.Since(as.lastAppendEventTime.Load().(time.Time)) < defaultDeadlockDuration && time.Since(as.lastSizeDecreaseTime.Load().(time.Time)) > defaultDeadlockDuration

	memoryHighWaterMark := as.memoryUsageRatio() > (1 - defaultReleaseMemoryRatio)

	return hasEventComeButNotOut && memoryHighWaterMark
}

func (as *areaMemStat[A, P, T, D, H]) releaseMemory() {
	if time.Since(as.lastReleaseMemoryTime.Load().(time.Time)) < 100*time.Millisecond {
		return
	}
	as.lastReleaseMemoryTime.Store(time.Now())

	paths := make([]*pathInfo[A, P, T, D, H], 0, as.pathCount.Load())
	as.pathMap.Range(func(k, v interface{}) bool {
		paths = append(paths, v.(*pathInfo[A, P, T, D, H]))
		return true
	})

	// sort by the last handle event ts in descending order
	sort.Slice(paths, func(i, j int) bool {
		return paths[i].lastHandleEventTs.Load() > paths[j].lastHandleEventTs.Load()
	})

	sizeToRelease := int64(float64(as.totalPendingSize.Load()) * defaultReleaseMemoryRatio)
	releasedSize := int64(0)
	releasedPaths := make([]*pathInfo[A, P, T, D, H], 0)

	log.Info("release memory", zap.Any("area", as.area), zap.Int64("sizeToRelease", sizeToRelease), zap.Int64("totalPendingSize", as.totalPendingSize.Load()), zap.Float64("releaseMemoryRatio", defaultReleaseMemoryRatio))

	for _, path := range paths {
		// Only release path that is blocking and has pending size larger than the threshold.
		if releasedSize >= sizeToRelease ||
			path.pendingSize.Load() < int64(defaultReleaseMemoryThreshold) ||
			!path.blocking.Load() {
			continue
		}

		releasedSize += int64(path.pendingSize.Load())
		releasedPaths = append(releasedPaths, path)
	}

	for _, path := range releasedPaths {
		log.Debug("release path", zap.Any("area", as.area), zap.Any("path", path.path), zap.Any("dest", path.dest), zap.Int64("releasedSize", path.pendingSize.Load()))
		as.feedbackChan <- Feedback[A, P, D]{
			Area:         as.area,
			Path:         path.path,
			Dest:         path.dest,
			FeedbackType: ReleasePath,
		}
	}
	as.lastSizeDecreaseTime.Store(time.Now())
	as.recordCongestionEvent(releasedSize)
}

func (as *areaMemStat[A, P, T, D, H]) memoryUsageRatio() float64 {
	return float64(as.totalPendingSize.Load()) / float64(as.settings.Load().maxPendingSize)
}

func (as *areaMemStat[A, P, T, D, H]) updateAreaPauseState(path *pathInfo[A, P, T, D, H]) {
	pause, resume, memoryUsageRatio := as.algorithm.ShouldPauseArea(
		as.paused.Load(),
		as.totalPendingSize.Load(),
		as.settings.Load().maxPendingSize,
	)

	sendFeedback := func(pause bool) {
		now := time.Now()
		lastTime := as.lastSendFeedbackTime.Load().(time.Time)

		if !as.lastSendFeedbackTime.CompareAndSwap(lastTime, now) {
			return // Another goroutine already updated the time
		}

		feedbackType := PauseArea
		if !pause {
			feedbackType = ResumeArea
		}

		as.feedbackChan <- Feedback[A, P, D]{
			Area:         as.area,
			Path:         path.path,
			Dest:         path.dest,
			FeedbackType: feedbackType,
		}
		as.paused.Store(pause)

		log.Info("send area feedback",
			zap.Any("area", as.area),
			zap.Stringer("feedbackType", feedbackType),
			zap.Float64("memoryUsageRatio", memoryUsageRatio),
			zap.Time("lastTime", lastTime),
			zap.Time("now", now),
			zap.Duration("sinceLastTime", time.Since(lastTime)),
			zap.String("component", as.settings.Load().component),
		)
	}

	if as.settings.Load().algorithm != MemoryControlForEventCollector {
		failpoint.Inject("PauseArea", func() {
			log.Warn("inject PauseArea")
			sendFeedback(true)
		})
	}

	switch {
	case pause:
		sendFeedback(true)
	case resume:
		sendFeedback(false)
	}

	if as.settings.Load().algorithm == MemoryControlForEventCollector && as.paused.Load() {
		log.Panic("area is paused, but the algorithm is v2, this should not happen", zap.String("component", as.settings.Load().component))
	}
}

func (as *areaMemStat[A, P, T, D, H]) decPendingSize(path *pathInfo[A, P, T, D, H], size int64) {
	as.totalPendingSize.Add(int64(-size))
	if as.totalPendingSize.Load() < 0 {
		log.Debug("Total pending size is less than 0, reset it to 0", zap.Int64("totalPendingSize", as.totalPendingSize.Load()), zap.String("component", as.settings.Load().component))
		as.totalPendingSize.Store(0)
	}
	as.updateDrainRate(size)

	as.updateAreaPauseState(path)
}

func (as *areaMemStat[A, P, T, D, H]) getAvailableMemory() int64 {
	rawAvailable := max(0, int64(as.settings.Load().maxPendingSize)-as.totalPendingSize.Load())
	if rawAvailable == 0 {
		return 0
	}
	return as.applyDrainBudget(as.applyCongestionWindow(rawAvailable))
}

// applyCongestionWindow enforces the AIMD-style window so that AvailableMemory
// does not rebound immediately after a release.
func (as *areaMemStat[A, P, T, D, H]) applyCongestionWindow(rawAvailable int64) int64 {
	settings := as.settings.Load()
	maxPending := int64(settings.maxPendingSize)
	maxWindow := int64(float64(maxPending) * defaultCongestionMaxWindowRatio)

	as.maybeGrowCongestionWindow(maxWindow)

	window := as.congestionWindow.Load()
	if window <= 0 {
		return rawAvailable
	}
	if window < rawAvailable {
		return window
	}
	return rawAvailable
}

// maybeGrowCongestionWindow implements the additive-increase phase.
func (as *areaMemStat[A, P, T, D, H]) maybeGrowCongestionWindow(maxWindow int64) {
	now := time.Now()
	lastUpdate := as.lastWindowUpdateTime.Load().(time.Time)
	if now.Sub(lastUpdate) < defaultCongestionIncreaseInterval {
		return
	}

	if as.isCongestionGrowthFrozen(now) {
		return
	}

	usageRatio := as.memoryUsageRatio()
	if usageRatio >= defaultHardWatermarkRatio {
		as.freezeCongestionGrowth(now)
		return
	}
	if usageRatio >= defaultSoftWatermarkRatio {
		as.lastWindowUpdateTime.Store(now)
		return
	}

	// If the last congestion event is too recent, we don't grow the congestion window.
	if lastCongested := as.lastCongestionEventTime.Load().(time.Time); !lastCongested.IsZero() && now.Sub(lastCongested) < defaultCongestionFreezeDuration {
		as.lastWindowUpdateTime.Store(now)
		return
	}

	additiveStep := int64(float64(maxWindow) * defaultCongestionAdditiveRatio)
	if additiveStep < defaultCongestionMinStepBytes {
		additiveStep = defaultCongestionMinStepBytes
	}

	for {
		current := as.congestionWindow.Load()
		if current >= maxWindow {
			as.lastWindowUpdateTime.Store(now)
			return
		}

		next := current + additiveStep
		if next > maxWindow {
			next = maxWindow
		}

		if as.congestionWindow.CompareAndSwap(current, next) {
			as.lastWindowUpdateTime.Store(now)
			return
		}
	}
}

// recordCongestionEvent applies multiplicative decrease once a forced release happens.
func (as *areaMemStat[A, P, T, D, H]) recordCongestionEvent(releasedSize int64) {
	if releasedSize <= 0 {
		return
	}

	usageRatio := as.memoryUsageRatio()
	now := time.Now()
	maxPending := int64(as.settings.Load().maxPendingSize)
	minWindow := int64(float64(maxPending) * defaultCongestionMinWindowRatio)
	if minWindow < defaultCongestionMinStepBytes {
		minWindow = defaultCongestionMinStepBytes
	}

	for {
		current := as.congestionWindow.Load()
		next := int64(float64(current) * defaultCongestionDecreaseFactor)
		if next < minWindow {
			next = minWindow
		}
		if next <= 0 {
			next = minWindow
		}
		if as.congestionWindow.CompareAndSwap(current, next) {
			break
		}
	}

	if usageRatio >= defaultSoftWatermarkRatio {
		as.forceWindowHalve(minWindow)
	}
	if usageRatio >= defaultHardWatermarkRatio {
		as.freezeCongestionGrowth(now)
	}

	as.lastCongestionEventTime.Store(now)
	as.lastWindowUpdateTime.Store(now)
}

func (as *areaMemStat[A, P, T, D, H]) forceWindowHalve(minWindow int64) {
	for {
		current := as.congestionWindow.Load()
		next := current / 2
		if next < minWindow {
			next = minWindow
		}
		if as.congestionWindow.CompareAndSwap(current, next) {
			return
		}
	}
}

func (as *areaMemStat[A, P, T, D, H]) freezeCongestionGrowth(now time.Time) {
	as.congestionFreezeUntil.Store(now.Add(defaultHardWatermarkFreezeWindow))
}

func (as *areaMemStat[A, P, T, D, H]) isCongestionGrowthFrozen(now time.Time) bool {
	freezeUntil := as.congestionFreezeUntil.Load().(time.Time)
	return !freezeUntil.IsZero() && now.Before(freezeUntil)
}

func (as *areaMemStat[A, P, T, D, H]) applyDrainBudget(rawAvailable int64) int64 {
	rate := math.Float64frombits(as.drainRateEWMA.Load())
	if rate <= 0 {
		return rawAvailable
	}

	budget := int64(rate * defaultDrainWindowSeconds)
	if budget < defaultDrainMinBudgetBytes {
		budget = defaultDrainMinBudgetBytes
	}
	if budget <= 0 {
		return rawAvailable
	}
	if budget < rawAvailable {
		return budget
	}
	return rawAvailable
}

func (as *areaMemStat[A, P, T, D, H]) updateDrainRate(consumed int64) {
	if consumed <= 0 {
		return
	}

	now := time.Now()
	last := as.lastDrainUpdateTime.Load().(time.Time)
	as.drainSampleBytes.Add(consumed)
	if last.IsZero() {
		as.lastDrainUpdateTime.Store(now)
		return
	}

	if now.Sub(last) < defaultDrainSampleInterval {
		return
	}

	bytes := as.drainSampleBytes.Swap(0)
	if bytes <= 0 {
		as.lastDrainUpdateTime.Store(now)
		return
	}

	elapsed := now.Sub(last).Seconds()
	if elapsed <= 0 {
		as.lastDrainUpdateTime.Store(now)
		return
	}

	instantRate := float64(bytes) / elapsed
	prev := math.Float64frombits(as.drainRateEWMA.Load())
	var ewma float64
	if prev <= 0 {
		ewma = instantRate
	} else {
		ewma = prev*(1-defaultDrainEWMASmoothing) + instantRate*defaultDrainEWMASmoothing
	}

	as.drainRateEWMA.Store(math.Float64bits(ewma))
	as.lastDrainUpdateTime.Store(now)
}

// A memControl is used to control the memory usage of the dynamic stream.
// It is a global level struct, not stream level.
type memControl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	// Since this struct is global level, different streams may access it concurrently.
	mutex sync.Mutex

	areaStatMap map[A]*areaMemStat[A, P, T, D, H]
}

func newMemControl[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]]() *memControl[A, P, T, D, H] {
	return &memControl[A, P, T, D, H]{
		areaStatMap: make(map[A]*areaMemStat[A, P, T, D, H]),
	}
}

func (m *memControl[A, P, T, D, H]) setAreaSettings(area A, settings AreaSettings) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// Update the settings
	if as, ok := m.areaStatMap[area]; ok {
		settings.fix()
		as.settings.Store(&settings)
	}
}

func (m *memControl[A, P, T, D, H]) addPathToArea(path *pathInfo[A, P, T, D, H], settings AreaSettings, feedbackChan chan<- Feedback[A, P, D]) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	area, ok := m.areaStatMap[path.area]
	if !ok {
		area = newAreaMemStat(path.area, m, settings, feedbackChan)
		m.areaStatMap[path.area] = area
	}

	path.areaMemStat = area
	area.pathMap.Store(path.path, path)
	area.pathCount.Add(1)
	// Update the settings
	area.settings.Store(&settings)
}

// This method is called after the path is removed.
func (m *memControl[A, P, T, D, H]) removePathFromArea(path *pathInfo[A, P, T, D, H]) {
	area := path.areaMemStat
	area.decPendingSize(path, int64(path.pendingSize.Load()))

	m.mutex.Lock()
	defer m.mutex.Unlock()

	area.pathCount.Add(-1)
	if area.pathCount.Load() == 0 {
		delete(m.areaStatMap, area.area)
	}
}

// FIXME/TODO: We use global metric here, which is not good for multiple streams.
func (m *memControl[A, P, T, D, H]) getMetrics() MemoryMetric[A, P] {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	metrics := MemoryMetric[A, P]{}
	for _, area := range m.areaStatMap {
		areaMetric := AreaMemoryMetric[A, P]{
			AreaValue:           area.area,
			PathAvailableMemory: make(map[P]int64),
			UsedMemoryValue:     area.totalPendingSize.Load(),
			AvailableMemory:     area.getAvailableMemory(),
			MaxMemoryValue:      int64(area.settings.Load().maxPendingSize),
			PathMaxMemoryValue:  int64(area.settings.Load().pathMaxPendingSize),
		}
		area.pathMap.Range(func(k, v interface{}) bool {
			usedMemory := v.(*pathInfo[A, P, T, D, H]).pendingSize.Load()
			availableMemory := max(0, areaMetric.PathMaxMemoryValue-usedMemory)
			areaMetric.PathAvailableMemory[k.(P)] = availableMemory
			return true
		})
		metrics.AreaMemoryMetrics = append(metrics.AreaMemoryMetrics, areaMetric)
	}
	return metrics
}

type MemoryMetric[A Area, P Path] struct {
	AreaMemoryMetrics []AreaMemoryMetric[A, P]
}

type AreaMemoryMetric[A Area, P Path] struct {
	AreaValue           A
	PathAvailableMemory map[P]int64
	AvailableMemory     int64
	UsedMemoryValue     int64

	MaxMemoryValue     int64
	PathMaxMemoryValue int64
}

func (a *AreaMemoryMetric[A, P]) MemoryUsageRatio() float64 {
	return float64(a.AvailableMemory) / float64(a.MaxMemoryValue)
}

func (a *AreaMemoryMetric[A, P]) MemoryUsage() int64 {
	return a.UsedMemoryValue
}

func (a *AreaMemoryMetric[A, P]) MaxMemory() int64 {
	return a.MaxMemoryValue
}

func (a *AreaMemoryMetric[A, P]) Area() A {
	return a.AreaValue
}

func (a *AreaMemoryMetric[A, P]) PathMetrics() map[P]int64 {
	return a.PathAvailableMemory
}
