package dynstream

import (
	"sync"
	"sync/atomic"
	"time"
)

type mockWork interface {
	Do()
}

type mockEvent struct {
	id    int
	path  string
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
func (h *mockHandler) OnDrop(event *mockEvent) {
	h.droppedEvents = append(h.droppedEvents, event)
}
func (h *mockHandler) drainDroppedEvents() []*mockEvent {
	events := h.droppedEvents
	h.droppedEvents = nil
	return events
}

type Inc struct {
	num int64
	inc *atomic.Int64
}

func (i *Inc) Do() {
	i.inc.Add(i.num)
}
