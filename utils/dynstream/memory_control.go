package dynstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/go-log/log"
	"go.uber.org/zap"
)

// memoryPauseRule defines a mapping rule between memory usage ratio and path pause ratio
type memoryPauseRule struct {
	// alarmThreshold represents the memory usage ratio (used/max) that triggers the control
	// e.g., 0.95 means when memory usage reaches 95% of max allowed memory
	alarmThreshold float64
	// pausePathRatio represents the proportion of paths that should be paused
	// e.g., 1.0 means pause all paths, 0.5 means pause top 50% paths with largest pending size
	pausePathRatio float64
}

// rules defines the mapping rules for memory control:
// - When memory usage >= 95%, pause all paths (100%)
// - When memory usage >= 90%, pause top 80% paths
// - When memory usage >= 85%, pause top 50% paths
// - When memory usage >= 80%, pause top 20% paths
// - When memory usage < 80%, no paths will be paused
var rules = []memoryPauseRule{
	{0.95, 1.0}, // Critical level: pause all paths
	{0.90, 0.8}, // Severe level: pause most paths
	{0.85, 0.5}, // Warning level: pause half paths
	{0.80, 0.2}, // Caution level: pause few paths
}

// findPausePathRatio finds the pause path ratio based on the memory usage ratio.
func findPausePathRatio(memoryUsageRatio float64) float64 {
	for _, rule := range rules {
		if memoryUsageRatio >= rule.alarmThreshold {
			return rule.pausePathRatio
		}
	}
	return 0 // No paths need to be paused
}

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

// This method is called by streams' handleLoop concurrently.
// Although the method is called concurrently, we don't need a mutex here. Because we only change totalPendingSize,
// which is an atomic variable. Although the settings could be updated concurrently, we don't really care about the accuracy.
func (as *areaMemStat[A, P, T, D, H]) appendEvent(
	path *pathInfo[A, P, T, D, H],
	event eventWrap[A, P, T, D, H],
	handler H,
) bool {
	defer as.updatePathPauseState(path, event)
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

var lastLogTime atomic.Value

func init() {
	lastLogTime.Store(time.Unix(0, 0))
}

// updatePathPauseState determines the pause state of a path and sends feedback to handler if the state is changed.
// It needs to be called after a event is appended.
// Note: Our gaol is to fast pause, and lazy resume.
func (as *areaMemStat[A, P, T, D, H]) updatePathPauseState(path *pathInfo[A, P, T, D, H], event eventWrap[A, P, T, D, H]) {
	shouldPause := as.shouldPausePath(path)

	if time.Since(lastLogTime.Load().(time.Time)) >= 2*time.Second {
		log.Info("updatePathPauseState",
			zap.Any("path", path.path),
			zap.Any("shouldPause", shouldPause),
			zap.Any("pendingSize", path.pendingSize.Load()),
			zap.Any("totalPendingSize", as.totalPendingSize.Load()),
			zap.Any("lastSendFeedbackTime", path.lastSendFeedbackTime.Load().(time.Time)),
			zap.Any("feedbackInterval", as.settings.Load().FeedbackInterval),
		)
		lastLogTime.Store(time.Now())
	}

	sendFeedback := func(pause bool) {
		as.feedbackChan <- Feedback[A, P, D]{
			Area:         path.area,
			Path:         path.path,
			Dest:         path.dest,
			FeedbackType: 0,
			Pause:        pause,
		}
		path.lastSendFeedbackTime.Store(time.Now())
	}

	// If the path is not paused previously but should be paused, we need to pause it.
	// And send pause feedback.
	if path.paused != shouldPause &&
		time.Since(path.lastSendFeedbackTime.Load().(time.Time)) >= as.settings.Load().FeedbackInterval {
		path.paused = shouldPause
		sendFeedback(shouldPause)
		return
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
	return memoryUsageRatio >= 0.2
}

// shouldPauseArea determines if the area should be paused based on memory usage.
// If the memory usage is greater than the 80% of max pending size, the area should be paused.
func (as *areaMemStat[A, P, T, D, H]) shouldPauseArea() bool {
	memoryUsageRatio := float64(as.totalPendingSize.Load()) / float64(as.settings.Load().MaxPendingSize)
	pauseRatio := findPausePathRatio(memoryUsageRatio)
	return pauseRatio > 0
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
	area.totalPendingSize.Add(int64(-path.pendingSize.Load()))

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

func (p *pathInfo[A, P, T, D, H]) SetHeapIndex(index int) {
	p.sizeHeapIndex = index
}

func (p *pathInfo[A, P, T, D, H]) GetHeapIndex() int {
	return p.sizeHeapIndex
}

func (p *pathInfo[A, P, T, D, H]) LessThan(other *pathInfo[A, P, T, D, H]) bool {
	// pathSizeHeap should be in descending order. That say the node with the largest pending size is the top.
	return p.pendingSize.Load() > other.pendingSize.Load()
}
