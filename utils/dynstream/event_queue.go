package dynstream

import (
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/deque"
)

type eventSignal[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathInfo   *pathInfo[A, P, T, D, H]
	eventCount int
}

type eventQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	option  Option
	handler H
	// Used to reduce the block allocation in the paths' pending queue.
	eventBlockAlloc *deque.BlockAllocator[eventWrap[A, P, T, D, H]]

	// Signal queue is used to decide which path's events should be popped.
	signalQueue        *deque.Deque[eventSignal[A, P, T, D, H]]
	totalPendingLength *atomic.Int64 // The total signal count in the queue.
}

func newEventQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](option Option, handler H) eventQueue[A, P, T, D, H] {
	eq := eventQueue[A, P, T, D, H]{
		option:             option,
		handler:            handler,
		eventBlockAlloc:    deque.NewBlockAllocator[eventWrap[A, P, T, D, H]](32, 1024),
		signalQueue:        deque.NewDeque(1024, deque.NewBlockAllocator[eventSignal[A, P, T, D, H]](1024, 32)),
		totalPendingLength: &atomic.Int64{},
	}

	return eq
}

func (q *eventQueue[A, P, T, D, H]) initPath(path *pathInfo[A, P, T, D, H]) {
	path.pendingQueue.SetBlockAllocator(q.eventBlockAlloc)
	if len := path.pendingQueue.Length(); len > 0 {
		q.signalQueue.PushBack(eventSignal[A, P, T, D, H]{pathInfo: path, eventCount: len})
		q.totalPendingLength.Add(int64(len))
	}
}

func (q *eventQueue[A, P, T, D, H]) appendEvent(event eventWrap[A, P, T, D, H]) {
	path := event.pathInfo

	addSignal := func() {
		back, ok := q.signalQueue.BackRef()
		if ok && back.pathInfo == path {
			back.eventCount++
		} else {
			q.signalQueue.PushBack(eventSignal[A, P, T, D, H]{pathInfo: path, eventCount: 1})
		}
		q.totalPendingLength.Add(1)
	}

	if path.appendEvent(event, q.handler) {
		addSignal()
	}
}

func (q *eventQueue[A, P, T, D, H]) blockPath(path *pathInfo[A, P, T, D, H]) {
	path.blocking = true
}

func (q *eventQueue[A, P, T, D, H]) wakePath(path *pathInfo[A, P, T, D, H]) {
	path.blocking = false
	count := path.pendingQueue.Length()
	if count > 0 {
		q.signalQueue.PushFront(eventSignal[A, P, T, D, H]{pathInfo: path, eventCount: count})
		q.totalPendingLength.Add(int64(count))
	}
}

func (q *eventQueue[A, P, T, D, H]) popEvents(buf []T) ([]T, *pathInfo[A, P, T, D, H]) {
	// Append the event to the buffer
	appendToBuf := func(event *eventWrap[A, P, T, D, H], path *pathInfo[A, P, T, D, H]) {
		buf = append(buf, event.event)
		path.popEvent()
	}

	for {
		// We are going to update the signal directly, so we need the reference.
		signal, ok := q.signalQueue.FrontRef()
		if !ok {
			return buf, nil
		}

		path := signal.pathInfo
		pendingQueue := path.pendingQueue

		if signal.eventCount == 0 {
			log.Panic("signal event count is zero")
		}
		if path.blocking || path.removed {
			// The path is blocking or removed, we should ignore the signal completely.
			// Since when it is waked, a signal event will be added to the queue.
			q.totalPendingLength.Add(-int64(signal.eventCount))
			q.signalQueue.PopFront()
			continue
		}

		batchSize := min(signal.eventCount, q.option.BatchCount)

		firstEvent, ok := pendingQueue.FrontRef()
		if !ok {
			// The signal could contain more events than the pendingQueue,
			// which is possible when the path is removed or recovered from blocked.
			// We should ignore the signal completely.
			q.signalQueue.PopFront()
			q.totalPendingLength.Add(-int64(signal.eventCount))
			continue
		}
		firstGroup := firstEvent.eventType.DataGroup
		firstProperty := firstEvent.eventType.Property

		appendToBuf(firstEvent, path)

		// Try to batch events with the same data group.
		count := 1
		for ; count < batchSize; count++ {
			// Get the reference of the front event of the path.
			// We don't use PopFront here because we need to keep the event in the path.
			// Otherwise, the event may lost when the loop is break below.
			front, ok := pendingQueue.FrontRef()
			// Only batch events with the same data group and when the event is batchable.
			if !ok ||
				(firstGroup != front.eventType.DataGroup) ||
				firstProperty == NonBatchable ||
				front.eventType.Property == NonBatchable {
				break
			}
			appendToBuf(front, path)
		}

		signal.eventCount -= count
		if signal.eventCount == 0 {
			q.signalQueue.PopFront()
		}
		q.totalPendingLength.Add(-int64(count))

		return buf, path
	}
}
