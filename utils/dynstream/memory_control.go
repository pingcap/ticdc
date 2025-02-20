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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// areaMemStat is used to store the memory statistics of an area.
// It is a global level struct, not stream level.
type areaMemStat[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A
	// Reverse reference to the memControl this area belongs to.
	memControl *memControl[A, P, T, D, H]

	settings     atomic.Pointer[AreaSettings]
	feedbackChan chan<- Feedback[A, P, D]

	pathCount            atomic.Int64
	totalPendingSize     atomic.Int64
	paused               atomic.Bool
	lastSendFeedbackTime atomic.Value
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
	}
	res.lastSendFeedbackTime.Store(time.Unix(0, 0))
	res.settings.Store(&settings)
	return res
}

// appendEvent try to append an event to the path's pending queue.
// It returns true if the event is appended successfully.
// This method is called by streams' handleLoop concurrently, but it is thread safe.
// We use atomic operations to update the totalPendingSize and the path's pendingSize.
func (as *areaMemStat[A, P, T, D, H]) appendEvent(
	path *pathInfo[A, P, T, D, H],
	event eventWrap[A, P, T, D, H],
	_ H,
) bool {
	defer as.updatePathPauseState(path)
	defer as.updateAreaPauseState(path)

	// Check if we should merge periodic signals.
	if event.eventType.Property == PeriodicSignal {
		back, ok := path.pendingQueue.BackRef()
		if ok && back.eventType.Property == PeriodicSignal {
			// If the last event is a periodic signal, we only need to keep the latest one.
			// And we don't need to add a new signal.
			*back = event
			return false
		}
	}

	// Add the event to the pending queue.
	path.pendingQueue.PushBack(event)
	// Update the pending size.
	path.updatePendingSize(int64(event.eventSize))
	as.totalPendingSize.Add(int64(event.eventSize))
	return true
}

// updatePathPauseState determines the pause state of a path and sends feedback to handler if the state is changed.
// It needs to be called after a event is appended.
// Note: Our gaol is to fast pause, and lazy resume.
func (as *areaMemStat[A, P, T, D, H]) updatePathPauseState(path *pathInfo[A, P, T, D, H]) {
	algorithm := as.settings.Load().algorithm
	var pause, resume bool
	var memoryUsageRatio float64
	switch algorithm {
	case "v2":
		pause, resume, memoryUsageRatio = shouldPausePathV2(
			path.paused.Load(),
			path.pendingSize.Load(),
			as.totalPendingSize.Load(),
			as.settings.Load().maxPendingSize,
			as.pathCount.Load(),
		)
	default:
		pause, resume, memoryUsageRatio = shouldPausePath(
			path.paused.Load(),
			path.pendingSize.Load(),
			as.settings.Load().maxPendingSize,
		)
	}

	sendFeedback := func(pause bool) {
		now := time.Now()
		lastTime := path.lastSendFeedbackTime.Load().(time.Time)

		// Fast pause, lazy resume.
		if !pause && time.Since(lastTime) < as.settings.Load().feedbackInterval {
			return
		}

		if !path.lastSendFeedbackTime.CompareAndSwap(lastTime, now) {
			return // Another goroutine already updated the time
		}

		feedbackType := PausePath
		if !pause {
			feedbackType = ResumePath
		}

		as.feedbackChan <- Feedback[A, P, D]{
			Area:         path.area,
			Path:         path.path,
			Dest:         path.dest,
			FeedbackType: feedbackType,
		}
		path.paused.Store(pause)

		log.Info("send path feedback", zap.Any("area", as.area),
			zap.Any("path", path.path), zap.Stringer("feedbackType", feedbackType),
			zap.Float64("memoryUsageRatio", memoryUsageRatio))
	}

	failpoint.Inject("PausePath", func() {
		log.Warn("inject PausePath")
		sendFeedback(true)
	})

	switch {
	case pause:
		sendFeedback(true)
	case resume:
		sendFeedback(false)
	}
}

func (as *areaMemStat[A, P, T, D, H]) updateAreaPauseState(path *pathInfo[A, P, T, D, H]) {
	algorithm := as.settings.Load().algorithm
	var pause, resume bool
	var memoryUsageRatio float64
	switch algorithm {
	case "v2":
		pause, resume, memoryUsageRatio = shouldPauseAreaV2(
			as.paused.Load(),
			as.totalPendingSize.Load(),
			as.settings.Load().maxPendingSize,
		)
	default:
		pause, resume, memoryUsageRatio = shouldPauseArea(
			as.paused.Load(),
			as.totalPendingSize.Load(),
			as.settings.Load().maxPendingSize,
		)
	}

	sendFeedback := func(pause bool) {
		now := time.Now()
		lastTime := as.lastSendFeedbackTime.Load().(time.Time)

		// Fast pause, lazy resume.
		if !pause && time.Since(lastTime) < as.settings.Load().feedbackInterval {
			return
		}

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
		)
	}

	if algorithm != "v2" {
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

	if algorithm == "v2" && as.paused.Load() {
		log.Panic("area is paused, but the algorithm is v2, this should not happen")
	}
}

func (as *areaMemStat[A, P, T, D, H]) decPendingSize(path *pathInfo[A, P, T, D, H], size int64) {
	as.totalPendingSize.Add(int64(-size))
	if as.totalPendingSize.Load() < 0 {
		log.Warn("Total pending size is less than 0, reset it to 0", zap.Int64("totalPendingSize", as.totalPendingSize.Load()))
		as.totalPendingSize.Store(0)
	}
	as.updatePathPauseState(path)
	as.updateAreaPauseState(path)
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
func (m *memControl[A, P, T, D, H]) getMetrics() MemoryMetric[A] {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	metrics := MemoryMetric[A]{}
	for _, area := range m.areaStatMap {
		areaMetric := AreaMemoryMetric[A]{
			area:       area.area,
			usedMemory: area.totalPendingSize.Load(),
			maxMemory:  int64(area.settings.Load().maxPendingSize),
		}
		metrics.AreaMemoryMetrics = append(metrics.AreaMemoryMetrics, areaMetric)
	}
	return metrics
}

type MemoryMetric[A Area] struct {
	AreaMemoryMetrics []AreaMemoryMetric[A]
}

type AreaMemoryMetric[A Area] struct {
	area       A
	usedMemory int64
	maxMemory  int64
}

func (a *AreaMemoryMetric[A]) MemoryUsageRatio() float64 {
	return float64(a.usedMemory) / float64(a.maxMemory)
}

func (a *AreaMemoryMetric[A]) MemoryUsage() int64 {
	return a.usedMemory
}

func (a *AreaMemoryMetric[A]) MaxMemory() int64 {
	return a.maxMemory
}

func (a *AreaMemoryMetric[A]) Area() A {
	return a.area
}

// shouldPausePath determines if a path should be paused based on memory usage.
// If the memory usage is greater than the 20% of max pending size, the path should be paused.
func shouldPausePath(
	paused bool,
	pendingSize int64,
	maxPendingSize uint64,
) (pause bool, resume bool, memoryUsageRatio float64) {
	memoryUsageRatio = float64(pendingSize) / float64(maxPendingSize)

	switch {
	case paused:
		// If the path is paused, we only need to resume it when the memory usage is less than 10%.
		if memoryUsageRatio < 0.1 {
			resume = true
		}
	default:
		// If the path is not paused, we need to pause it when the memory usage is greater than 30% of max pending size.
		if memoryUsageRatio >= 0.2 {
			pause = true
		}
	}

	return pause, resume, memoryUsageRatio
}

// shouldPauseArea determines if the area should be paused based on memory usage.
// If the memory usage is greater than the 80% of max pending size, the area should be paused.
func shouldPauseArea(
	paused bool,
	pendingSize int64,
	maxPendingSize uint64,
) (pause bool, resume bool, memoryUsageRatio float64) {
	memoryUsageRatio = float64(pendingSize) / float64(maxPendingSize)

	switch {
	case paused:
		// If the area is already paused, we need to resume it when the memory usage is less than 50%.
		if memoryUsageRatio < 0.5 {
			resume = true
		}
	default:
		// If the area is not paused, we need to pause it when the memory usage is greater than 80% of max pending size.
		if memoryUsageRatio >= 0.8 {
			pause = true
		}
	}

	return
}

func shouldPausePathV2(
	paused bool,
	pathPendingSize int64,
	areaPendingSize int64,
	maxPendingSize uint64,
	pathCount int64,
) (pause bool, resume bool, memoryUsageRatio float64) {
	if pathCount == 0 {
		log.Warn("pathCount is 0, this should not happen, adjust it to 1")
		pathCount = 1
	}

	pathMemoryUsageRatio := float64(pathPendingSize) / float64(maxPendingSize)
	areaMemoryUsageRatio := float64(areaPendingSize) / float64(maxPendingSize)

	pauseLimit, resumeLimit := calculateThresholds(pathCount, areaMemoryUsageRatio)

	if paused {
		resume = pathMemoryUsageRatio < resumeLimit
	} else {
		pause = pathMemoryUsageRatio > pauseLimit
	}

	return pause, resume, pathMemoryUsageRatio
}

// shouldPauseAreaV2 determines never pause a area.
func shouldPauseAreaV2(
	paused bool,
	pendingSize int64,
	maxPendingSize uint64,
) (pause bool, resume bool, memoryUsageRatio float64) {
	memoryUsageRatio = float64(pendingSize) / float64(maxPendingSize)
	return false, false, memoryUsageRatio
}

// calculateThresholds calculates dynamic pause and resume memory thresholds for path-level flow control.
// It takes into account both the number of paths and current area memory usage to determine appropriate limits.
// The function implements an adaptive threshold strategy:
// - For 1-2 paths, it uses fixed thresholds
// - For >2 paths, it:
//  1. Applies a penalty factor that increases with path count
//  2. Uses a tiered threshold system based on area memory usage
//  3. Ensures minimum thresholds based on path count
//
// Parameters:
//   - pathCount: number of active paths in the area
//   - areaMemoryUsageRatio: current memory usage ratio of the area (used/max)
//
// Returns:
//   - pauseLimit: memory ratio threshold to trigger path pausing
//   - resumeLimit: memory ratio threshold to trigger path resuming
func calculateThresholds(pathCount int64, areaMemoryUsageRatio float64) (pauseLimit, resumeLimit float64) {
	if pathCount == 0 {
		pathCount = 1
	}

	// Special case for path count 1 and 2
	switch pathCount {
	case 1:
		return 0.8, 0.4
	case 2:
		return 0.5, 0.25
	}

	// calculate penalty factor based on path count
	penaltyFactor := 1.0 - 0.5/float64(pathCount)

	type threshold struct {
		areaUsage   float64
		pauseLimit  float64
		resumeLimit float64
	}

	baseThresholds := []threshold{
		//
		{0.5, 0.2, 0.1},   // area usage <= 50%
		{0.8, 0.1, 0.05},  // area usage <= 80%
		{1.0, 0.05, 0.01}, // area usage <= 100%
		// Default maxPendingSize is 1024MB, so the default lowest pause limit is 10 MB.
		{1.2, 0.01, 0.005}, // area usage > 100%
	}

	// find applicable threshold
	var t threshold
	for _, th := range baseThresholds {
		if areaMemoryUsageRatio <= th.areaUsage {
			t = th
			log.Info("find applicable threshold",
				zap.Any("pathCount", pathCount),
				zap.Any("areaMemoryUsageRatio", areaMemoryUsageRatio),
				zap.Float64("pauseLimit", t.pauseLimit),
				zap.Float64("resumeLimit", t.resumeLimit),
				zap.Float64("penaltyFactor", penaltyFactor),
			)
			break
		}
	}

	return t.pauseLimit / penaltyFactor, t.resumeLimit / penaltyFactor
}
