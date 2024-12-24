package dynstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestStreamBasic(t *testing.T) {
	handler := &mockHandler{}
	option := NewOption()
	option.ReportInterval = 8 * time.Millisecond
	statWait := sync.WaitGroup{}
	statWait.Add(1)

	p1 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p1", "d1")
	p2 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p2", "d2")
	s1 := newStream(1 /*id*/, handler, option)
	s2 := newStream(2 /*id*/, handler, option)

	s1.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p1})
	s2.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p2})

	incr := &atomic.Int64{}

	eventDone := &sync.WaitGroup{}
	event1 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(1, "p1", 10*time.Millisecond /*sleep*/, &Inc{num: 1, inc: incr}, nil, eventDone), pathInfo: p1}
	event2 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(2, "p2", 10*time.Millisecond /*sleep*/, &Inc{num: 2, inc: incr}, nil, eventDone), pathInfo: p2}
	event3 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(3, "p1", 10*time.Millisecond /*sleep*/, &Inc{num: 3, inc: incr}, nil, eventDone), pathInfo: p1}
	event4 := eventWrap[int, string, *mockEvent, any, *mockHandler]{event: newMockEvent(4, "p2", 10*time.Millisecond /*sleep*/, &Inc{num: 4, inc: incr}, nil, eventDone), pathInfo: p2}

	s1.in() <- event1
	s1.in() <- event3

	s2.in() <- event2
	s2.in() <- event4

	eventDone.Wait()

	assert.Equal(t, int64(10), incr.Load())

	statWait.Wait()
	s1.close()
	s2.close()

}

func TestStreamManyEvents(t *testing.T) {
	handler := &mockHandler{}

	p1 := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "p1", "d1")
	option := NewOption()
	option.ReportInterval = 1 * time.Hour
	s1 := newStream(1 /*id*/, handler, option)
	s1.start([]*pathInfo[int, string, *mockEvent, any, *mockHandler]{p1})

	incr := &atomic.Int64{}
	wg := &sync.WaitGroup{}
	total := 100000
	for i := 0; i < total; i++ {
		s1.in() <- eventWrap[int, string, *mockEvent, any, *mockHandler]{
			event: newMockEvent(i, "p1", 0 /*sleep*/, &Inc{num: 1, inc: incr}, nil, wg), pathInfo: p1}
	}
	wg.Wait()
	s1.close()

	assert.Equal(t, int64(total), incr.Load())
}
