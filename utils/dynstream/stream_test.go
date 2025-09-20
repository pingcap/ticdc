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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockWork interface {
	Do()
}

type mockEvent struct {
	id    int
	path  string
	value int
	sleep time.Duration

	work mockWork

	start *sync.WaitGroup
	done  *sync.WaitGroup
}

func newMockEvent(id int, path string, sleep time.Duration, work mockWork, start *sync.WaitGroup, done *sync.WaitGroup) *mockEvent {
	e := &mockEvent{id: id, path: path, sleep: sleep, work: work, start: start, done: done}
	if e.start != nil {
		e.start.Add(1)
	}
	if e.done != nil {
		e.done.Add(1)
	}
	return e
}

type mockHandler struct {
	mu            sync.Mutex
	droppedEvents []*mockEvent
}

func (h *mockHandler) Path(event *mockEvent) string {
	return event.path
}

func (h *mockHandler) Handle(dest any, events ...*mockEvent) (await bool) {
	event := events[0]
	if event.start != nil {
		event.start.Done()
	}

	if event.sleep > 0 {
		time.Sleep(event.sleep)
	}

	if event.work != nil {
		event.work.Do()
	}

	if event.done != nil {
		event.done.Done()
	}
	return false
}

func (h *mockHandler) GetSize(event *mockEvent) int            { return 0 }
func (h *mockHandler) GetArea(path string, dest any) int       { return 0 }
func (h *mockHandler) GetTimestamp(event *mockEvent) Timestamp { return 0 }
func (h *mockHandler) GetType(event *mockEvent) EventType      { return DefaultEventType }
func (h *mockHandler) IsPaused(event *mockEvent) bool          { return false }
func (h *mockHandler) OnDrop(event *mockEvent) interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.droppedEvents = append(h.droppedEvents, event)
	return nil
}

func (h *mockHandler) drainDroppedEvents() []*mockEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	events := h.droppedEvents
	h.droppedEvents = nil
	return events
}

type Inc struct {
	num    int64
	inc    *atomic.Int64
	notify *sync.WaitGroup
}

func (i *Inc) Do() {
	i.inc.Add(i.num)
	if i.notify != nil {
		i.notify.Done()
	}
}

func newInc(num int64, inc *atomic.Int64, notify ...*sync.WaitGroup) *Inc {
	i := &Inc{num: num, inc: inc}
	if len(notify) > 0 {
		i.notify = notify[0]
		i.notify.Add(1)
	}
	return i
}

func TestStreamBasic(t *testing.T) {
	handler := mockHandler{}
	stream := newStream(1, &handler, Option{UseBuffer: false})
	require.Equal(t, 0, stream.getPendingSize())

	stream.start()
	defer stream.close()
	pi := newPathInfo[int, string, *mockEvent, any, *mockHandler](1, "test/path", nil)
	stream.addPath(pi)
	// Test basic event handling
	inc := &atomic.Int64{}
	// notify is used to wait for the event to be processed
	notify := &sync.WaitGroup{}

	newEvent := func(num int) eventWrap[int, string, *mockEvent, any, *mockHandler] {
		return eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo: pi,
			event:    newMockEvent(num, pi.path, 0, newInc(int64(num), inc, notify), nil, nil),
		}
	}

	// Send event to stream
	stream.addEvent(newEvent(1))

	notify.Wait()
	// Verify event was processed
	require.Equal(t, int64(1), inc.Load())
	require.Equal(t, 0, stream.getPendingSize())

	// Test multiple events
	stream.addEvent(newEvent(2))
	stream.addEvent(newEvent(3))

	notify.Wait()
	// Verify all events were processed
	require.Equal(t, int64(6), inc.Load()) // 1 + 2 + 3 = 6
	require.Equal(t, 0, stream.getPendingSize())
}

func TestStreamBasicWithBuffer(t *testing.T) {
	handler := mockHandler{}
	stream := newStream(1, &handler, Option{UseBuffer: true})
	require.Equal(t, 0, stream.getPendingSize())

	stream.start()
	defer stream.close()
	pi := newPathInfo[int, string, *mockEvent, any, *mockHandler](1, "test/path", nil)
	stream.addPath(pi)
	// Test basic event handling
	inc := &atomic.Int64{}
	// notify is used to wait for the event to be processed
	notify := &sync.WaitGroup{}

	newEvent := func(num int) eventWrap[int, string, *mockEvent, any, *mockHandler] {
		return eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo: pi,
			event:    newMockEvent(num, pi.path, 0, newInc(int64(num), inc, notify), nil, nil),
		}
	}

	// Send event to stream
	stream.addEvent(newEvent(1))

	notify.Wait()
	// Verify event was processed
	require.Equal(t, int64(1), inc.Load())
	require.Equal(t, 0, stream.getPendingSize())

	// Test multiple events
	stream.addEvent(newEvent(2))
	stream.addEvent(newEvent(3))

	notify.Wait()
	// Verify all events were processed
	require.Equal(t, int64(6), inc.Load()) // 1 + 2 + 3 = 6
	require.Equal(t, 0, stream.getPendingSize())
}

func TestPathInfo(t *testing.T) {
	// case 1: new path info
	pi := newPathInfo[int, string, *mockEvent, any, *mockHandler](1, "test/path", nil)
	require.Equal(t, 1, pi.area)
	require.Equal(t, "test/path", pi.path)
	require.Equal(t, int64(0), pi.pendingSize.Load())
	require.Equal(t, false, pi.paused.Load())
	require.Equal(t, time.Unix(0, 0), pi.lastSendFeedbackTime.Load())
}
