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

const (
	// MemoryControlForPuller is the algorithm of the memory control.
	// It sill send pause and resume [area, path] feedback.
	MemoryControlForPuller = 0
	// MemoryControlForEventCollector is the algorithm of the memory control.
	// It will only send pause and resume [path] feedback.
	// For now, we only use it in event collector.
	MemoryControlForEventCollector = 1
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
	algorithm            MemoryControlAlgorithm
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
		algorithm:            NewMemoryControlAlgorithm(settings.algorithm),
	}
	res.lastSendFeedbackTime.Store(time.Unix(0, 0))
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
	defer as.updatePathPauseState(path)
	defer as.updateAreaPauseState(path)

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

	if as.memoryUsageRatio() >= 1 && as.settings.Load().algorithm ==
		MemoryControlForEventCollector && event.eventType.Droppable {
		dropEvent := handler.OnDrop(event.event)
		if dropEvent != nil {
			event.eventType = handler.GetType(dropEvent.(T))
			event.event = dropEvent.(T)
			path.pendingQueue.PushBack(event)
			return true
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

func (as *areaMemStat[A, P, T, D, H]) memoryUsageRatio() float64 {
	return float64(as.totalPendingSize.Load()) / float64(as.settings.Load().maxPendingSize)
}

// updatePathPauseState determines the pause state of a path and sends feedback to handler if the state is changed.
// It needs to be called after a event is appended.
// Note: Our gaol is to fast pause, and lazy resume.
func (as *areaMemStat[A, P, T, D, H]) updatePathPauseState(path *pathInfo[A, P, T, D, H]) {
	pause, resume, memoryUsageRatio := as.algorithm.ShouldPausePath(
		path.paused.Load(),
		path.pendingSize.Load(),
		as.totalPendingSize.Load(),
		as.settings.Load().maxPendingSize,
		as.pathCount.Load(),
	)

	sendFeedback := func(pause bool) {
		now := time.Now()
		lastTime := path.lastSendFeedbackTime.Load().(time.Time)

		// fast pause and lazy resume path
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
			zap.Float64("pathMemoryUsageRatio", memoryUsageRatio), zap.String("component", as.settings.Load().component))
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
		log.Warn("Total pending size is less than 0, reset it to 0", zap.Int64("totalPendingSize", as.totalPendingSize.Load()), zap.String("component", as.settings.Load().component))
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
