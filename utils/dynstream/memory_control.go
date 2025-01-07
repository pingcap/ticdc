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

	pathCount            int
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
	handler H,
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
	path.pendingSize.Add(uint32(event.eventSize))
	as.totalPendingSize.Add(int64(event.eventSize))
	return true
}

// updatePathPauseState determines the pause state of a path and sends feedback to handler if the state is changed.
// It needs to be called after a event is appended.
// Note: Our gaol is to fast pause, and lazy resume.
func (as *areaMemStat[A, P, T, D, H]) updatePathPauseState(path *pathInfo[A, P, T, D, H]) {
	shouldPause := as.shouldPausePath(path)

	sendFeedback := func(pause bool) {
		as.feedbackChan <- Feedback[A, P, D]{
			Area:         path.area,
			Path:         path.path,
			Dest:         path.dest,
			FeedbackType: 0,
			PausePath:    pause,
		}
		path.lastSendFeedbackTime.Store(time.Now())
	}

	// If the path is not paused previously but should be paused, we need to pause it.
	// And send pause feedback.
	if path.paused.Load() != shouldPause &&
		time.Since(path.lastSendFeedbackTime.Load().(time.Time)) >= as.settings.Load().FeedbackInterval {
		path.paused.Store(shouldPause)
		sendFeedback(shouldPause)
	}
}

func (as *areaMemStat[A, P, T, D, H]) updateAreaPauseState(path *pathInfo[A, P, T, D, H]) {
	shouldPause := as.shouldPauseArea()

	sendFeedback := func(pause bool) {
		as.feedbackChan <- Feedback[A, P, D]{
			Area:         as.area,
			Path:         path.path,
			Dest:         path.dest,
			PauseArea:    pause,
			FeedbackType: 1,
		}
		log.Debug("fizz: send area feedback",
			zap.Any("area", as.area),
			zap.Any("path", path.path),
			zap.Bool("pause", pause),
			zap.Int64("totalPendingSize", as.totalPendingSize.Load()),
			zap.Int64("maxPendingSize", int64(as.settings.Load().MaxPendingSize)),
		)
		as.lastSendFeedbackTime.Store(time.Now())
	}

	prevPaused := as.paused.Load()
	if prevPaused != shouldPause &&
		time.Since(as.lastSendFeedbackTime.Load().(time.Time)) >= as.settings.Load().FeedbackInterval {
		as.paused.Store(shouldPause)
		sendFeedback(shouldPause)
		return
	}
}

// shouldPausePath determines if a path should be paused based on memory usage.
// If the memory usage is greater than the 20% of max pending size, the path should be paused.
func (as *areaMemStat[A, P, T, D, H]) shouldPausePath(path *pathInfo[A, P, T, D, H]) bool {
	memoryUsageRatio := float64(path.pendingSize.Load()) / float64(as.settings.Load().MaxPendingSize)

	// If the path is paused, we only need to resume it when the memory usage is less than 10%.
	if path.paused.Load() {
		return memoryUsageRatio >= 0.1
	}

	// If the path is not paused, we need to pause it when the memory usage is greater than 20% of max pending size.
	return memoryUsageRatio >= 0.2
}

// shouldPauseArea determines if the area should be paused based on memory usage.
// If the memory usage is greater than the 80% of max pending size, the area should be paused.
func (as *areaMemStat[A, P, T, D, H]) shouldPauseArea() bool {
	memoryUsageRatio := float64(as.totalPendingSize.Load()) / float64(as.settings.Load().MaxPendingSize)

	log.Debug("fizz: should pause area",
		zap.Any("area", as.area),
		zap.Float64("memoryUsageRatio", memoryUsageRatio),
		zap.Int("maxPendingSize", as.settings.Load().MaxPendingSize),
		zap.Int64("totalPendingSize", as.totalPendingSize.Load()),
		zap.Bool("paused", as.paused.Load()))

	// If the area is paused, we only need to resume it when the memory usage is less than 50%.
	if as.paused.Load() {
		return memoryUsageRatio >= 0.5
	}

	// If the area is not paused, we need to pause it when the memory usage is greater than 80% of max pending size.
	return memoryUsageRatio >= 0.8
}

func (as *areaMemStat[A, P, T, D, H]) decPendingSize(size int64) {
	as.totalPendingSize.Add(int64(-size))
	if as.totalPendingSize.Load() < 0 {
		log.Debug("fizz: total pending size is less than 0, reset it to 0", zap.Int64("totalPendingSize", as.totalPendingSize.Load()))
		as.totalPendingSize.Store(0)
	}
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
	area.pathCount++
	// Update the settings
	area.settings.Store(&settings)
}

// This method is called after the path is removed.
func (m *memControl[A, P, T, D, H]) removePathFromArea(path *pathInfo[A, P, T, D, H]) {
	area := path.areaMemStat
	area.decPendingSize(int64(path.pendingSize.Load()))

	m.mutex.Lock()
	defer m.mutex.Unlock()

	area.pathCount--
	if area.pathCount == 0 {
		delete(m.areaStatMap, area.area)
	}
}

// FIXME/TODO: We use global metric here, which is not good for multiple streams.
func (m *memControl[A, P, T, D, H]) getMetrics() (usedMemory int64, maxMemory int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	usedMemory = int64(0)
	maxMemory = int64(0)
	for _, area := range m.areaStatMap {
		usedMemory += area.totalPendingSize.Load()
		maxMemory += int64(area.settings.Load().MaxPendingSize)
	}
	return usedMemory, maxMemory
}
