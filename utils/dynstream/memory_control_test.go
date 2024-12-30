package dynstream

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/utils/deque"
	"github.com/stretchr/testify/require"
)

// Helper function to create test components
func setupTestComponents() (*memControl[int, string, *mockEvent, any, *mockHandler], *pathInfo[int, string, *mockEvent, any, *mockHandler]) {
	mc := newMemControl[int, string, *mockEvent, any, *mockHandler]()

	area := 1

	path := &pathInfo[int, string, *mockEvent, any, *mockHandler]{
		area:         area,
		path:         "test-path",
		dest:         "test-dest",
		pendingQueue: deque.NewDeque[eventWrap[int, string, *mockEvent, any, *mockHandler]](32),
	}

	return mc, path
}

func TestMemControlAddRemovePath(t *testing.T) {
	mc, path := setupTestComponents()
	settings := AreaSettings{
		MaxPendingSize:   1000,
		FeedbackInterval: time.Second,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)

	// Test adding path
	mc.addPathToArea(path, settings, feedbackChan)
	require.NotNil(t, path.areaMemStat)
	require.Equal(t, 1, path.areaMemStat.pathCount)
	require.Equal(t, 1, path.areaMemStat.pathSizeHeap.len())

	// Test removing path
	mc.removePathFromArea(path)
	require.Equal(t, 0, path.areaMemStat.pathCount)
	require.Equal(t, 0, path.areaMemStat.pathSizeHeap.len())
	require.Empty(t, mc.areaStatMap)
}

func TestAreaMemStatAppendEvent(t *testing.T) {
	mc, path1 := setupTestComponents()
	settings := AreaSettings{
		MaxPendingSize:   15,
		FeedbackInterval: time.Millisecond * 10,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)
	mc.addPathToArea(path1, settings, feedbackChan)

	handler := &mockHandler{}
	option := NewOption()
	option.EnableMemoryControl = true

	// 1. Append normal event, it should be accepted
	normalEvent1 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 1, path: "test-path"},
		timestamp: 1,
		eventSize: 10,
		queueTime: time.Now(),
	}
	ok := path1.areaMemStat.appendEvent(path1, normalEvent1, handler)
	require.True(t, ok)
	require.Equal(t, int64(10), path1.areaMemStat.totalPendingSize.Load())

	// Append 2 periodic signals, and the second one will replace the first one
	periodicEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 2, path: "test-path"},
		eventSize: 5,
		timestamp: 2,
		queueTime: time.Now(),
		eventType: EventType{Property: PeriodicSignal},
	}
	ok = path1.areaMemStat.appendEvent(path1, periodicEvent, handler)
	require.True(t, ok)
	require.Equal(t, int64(15), path1.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 2, path1.pendingQueue.Length())
	back, _ := path1.pendingQueue.BackRef()
	require.Equal(t, periodicEvent.timestamp, back.timestamp)
	periodicEvent2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 3, path: "test-path"},
		timestamp: 3,
		eventSize: 5,
		queueTime: time.Now(),
		eventType: EventType{Property: PeriodicSignal},
	}
	ok = path1.areaMemStat.appendEvent(path1, periodicEvent2, handler)
	require.False(t, ok)
	// Size should remain the same as the signal was replaced
	require.Equal(t, int64(15), path1.areaMemStat.totalPendingSize.Load())
	// The pending queue should only have 2 events
	require.Equal(t, 2, path1.pendingQueue.Length())
	// The last event timestamp should be the latest
	back, _ = path1.pendingQueue.BackRef()
	require.Equal(t, periodicEvent2.timestamp, back.timestamp)

	// 3. Add a normal event, and it should be dropped, because the total pending size is exceed the max pending size
	normalEvent2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 4, path: "test-path"},
		eventSize: 20,
		queueTime: time.Now(),
		timestamp: 4,
	}
	ok = path1.areaMemStat.appendEvent(path1, normalEvent2, handler)
	require.False(t, ok)
	require.Equal(t, int64(15), path1.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 2, path1.pendingQueue.Length())
	back, _ = path1.pendingQueue.BackRef()
	// The last event should be the periodic event
	require.Equal(t, periodicEvent2.timestamp, back.timestamp)
	events := handler.drainDroppedEvents()
	require.Equal(t, 1, len(events))
	require.Equal(t, normalEvent2.event, events[0])

	// 4. Change the settings, enlarge the max pending size
	newSettings := AreaSettings{
		MaxPendingSize:   100,
		FeedbackInterval: time.Millisecond * 10,
	}
	mc.setAreaSettings(path1.area, newSettings)
	require.Equal(t, 100, path1.areaMemStat.settings.Load().MaxPendingSize)
	require.Equal(t, newSettings, *path1.areaMemStat.settings.Load())
	addr1 := fmt.Sprintf("%p", path1.areaMemStat.settings.Load())
	addr2 := fmt.Sprintf("%p", &newSettings)
	require.NotEqual(t, addr1, addr2)
	// 5. Add a normal event, and it should be accepted,
	//  because the total pending size is less than the max pending size
	normalEvent3 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 5, path: "test-path"},
		eventSize: 20,
		queueTime: time.Now(),
		timestamp: 5,
	}
	ok = path1.areaMemStat.appendEvent(path1, normalEvent3, handler)
	require.True(t, ok)
	require.Equal(t, int64(35), path1.areaMemStat.totalPendingSize.Load())
	require.Equal(t, 3, path1.pendingQueue.Length())
	back, _ = path1.pendingQueue.BackRef()
	require.Equal(t, normalEvent3.timestamp, back.timestamp)

	// 6. Add a new path, and append a large event to it, it will consume the remaining memory
	path2 := &pathInfo[int, string, *mockEvent, any, *mockHandler]{
		area:         1,
		path:         "test-path-2",
		pendingQueue: deque.NewDeque[eventWrap[int, string, *mockEvent, any, *mockHandler]](32),
	}
	mc.addPathToArea(path2, newSettings, feedbackChan)
	largeEvent := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 6, path: "test-path-2"},
		timestamp: 6,
		eventSize: int(newSettings.MaxPendingSize - int(path1.areaMemStat.totalPendingSize.Load())),
		queueTime: time.Now(),
	}
	ok = path2.areaMemStat.appendEvent(path2, largeEvent, handler)
	require.True(t, ok)
	require.Equal(t, newSettings.MaxPendingSize, int(path2.areaMemStat.totalPendingSize.Load()))
	require.Equal(t, 2, path2.areaMemStat.pathCount)
	// There are 4 events in the eventQueue, [normalEvent1, periodicEvent2, normalEvent3, largeEvent]
	// The new path should be paused, because the pending size is reach the max pending size
	require.True(t, path2.paused)

	time.Sleep(2 * newSettings.FeedbackInterval)
	// 7. Add a normal event to path1, and the large event of path2 should be dropped
	// Because we will find the path with the largest pending size, and it's path2
	// So the large event of path2 will be dropped
	normalEvent4 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event: &mockEvent{id: 7, path: "test-path"},
		// Make it the smallest timestamp,
		//so it will cause the large event of path2 to be dropped
		timestamp: 0,
		eventSize: 10,
		queueTime: time.Now(),
	}
	// Force update the heap to make sure the path2 is moved to the front of the heap
	path1.areaMemStat.pathSizeHeap.tryUpdate(true)
	ok = path1.areaMemStat.appendEvent(path1, normalEvent4, handler)
	require.True(t, ok)
	require.Equal(t, 45, int(path1.areaMemStat.totalPendingSize.Load()))
	require.Equal(t, 0, path2.pendingQueue.Length())
	droppedEvents := handler.drainDroppedEvents()
	require.Equal(t, 1, len(droppedEvents))
	require.Equal(t, largeEvent.event, droppedEvents[0])

	// 8. Add a signal event to path2, and it should be accepted, and its state should be resumed
	periodicEvent3 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		event:     &mockEvent{id: 8, path: "test-path-2"},
		timestamp: 7,
		eventSize: 5,
		queueTime: time.Now(),
	}
	ok = path2.areaMemStat.appendEvent(path2, periodicEvent3, handler)
	require.True(t, ok)
	require.Equal(t, 1, path2.pendingQueue.Length())
	require.False(t, path2.paused)
}

func TestShouldPausePath(t *testing.T) {
	mc, path := setupTestComponents()

	maxPendingSize := 0
	for i := 0; i < 100; i++ {
		newPath := &pathInfo[int, string, *mockEvent, any, *mockHandler]{
			area: path.area,
			path: fmt.Sprintf("test-path-%d", i),
		}
		newPath.pendingSize.Store(uint32(i))
		mc.addPathToArea(newPath, AreaSettings{
			MaxPendingSize:   maxPendingSize,
			FeedbackInterval: time.Second,
		}, nil)
		maxPendingSize += i
	}

	feedbackChan := make(chan Feedback[int, string, any], 10)

	mc.addPathToArea(path, AreaSettings{
		MaxPendingSize:   maxPendingSize,
		FeedbackInterval: time.Second,
	}, feedbackChan)

	tests := []struct {
		name          string
		pathHeapIndex int
		pendingSize   int64
		expectedPause bool
	}{
		{"No pause needed", 50, int64(maxPendingSize / 2), false},
		{"Need pause", 20, int64(maxPendingSize * 4 / 5), true},
		{"Critical level", 80, int64(maxPendingSize), true},
		{"Not in heap", 0, int64(maxPendingSize / 2), false},
		{"Not in heap but critical", 0, int64(maxPendingSize), true},
	}

	for _, tt := range tests {
		path.sizeHeapIndex = tt.pathHeapIndex
		path.areaMemStat.totalPendingSize.Store(tt.pendingSize)
		result := path.areaMemStat.shouldPausePath(path)
		require.Equal(t, tt.expectedPause, result, tt.name)
	}
}

func TestSetAreaSettings(t *testing.T) {
	mc, path := setupTestComponents()
	// Case 1: Set the initial settings.
	initialSettings := AreaSettings{
		MaxPendingSize:   1000,
		FeedbackInterval: time.Second,
	}
	feedbackChan := make(chan Feedback[int, string, any], 10)
	mc.addPathToArea(path, initialSettings, feedbackChan)
	require.Equal(t, initialSettings, *path.areaMemStat.settings.Load())

	// Case 2: Set the new settings.
	newSettings := AreaSettings{
		MaxPendingSize:   2000,
		FeedbackInterval: 2 * time.Second,
	}
	mc.setAreaSettings(path.area, newSettings)
	require.Equal(t, newSettings, *path.areaMemStat.settings.Load())

	// Case 3: Set a invalid settings.
	invalidSettings := AreaSettings{
		MaxPendingSize:   0,
		FeedbackInterval: 0,
	}
	mc.setAreaSettings(path.area, invalidSettings)
	require.NotEqual(t, invalidSettings, *path.areaMemStat.settings.Load())
	require.Equal(t, DefaultFeedbackInterval, path.areaMemStat.settings.Load().FeedbackInterval)
	require.Equal(t, DefaultMaxPendingSize, path.areaMemStat.settings.Load().MaxPendingSize)
}

func TestGetMetrics(t *testing.T) {
	mc, path := setupTestComponents()
	usedMemory, maxMemory := mc.getMetrics()
	require.Equal(t, int64(0), usedMemory)
	require.Equal(t, int64(0), maxMemory)

	mc.addPathToArea(path, AreaSettings{
		MaxPendingSize:   100,
		FeedbackInterval: time.Second,
	}, nil)
	usedMemory, maxMemory = mc.getMetrics()
	require.Equal(t, int64(0), usedMemory)
	require.Equal(t, int64(100), maxMemory)

	path.areaMemStat.totalPendingSize.Store(100)
	usedMemory, maxMemory = mc.getMetrics()
	require.Equal(t, int64(100), usedMemory)
	require.Equal(t, int64(100), maxMemory)
}
