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
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/deque"
)

type eventSignal[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathInfo   *pathInfo[A, P, T, D, H]
	eventCount int
}

type eventQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	handler H
	// Used to reduce the block allocation in the paths pending queue.
	eventBlockAlloc *deque.BlockAllocator[eventWrap[A, P, T, D, H]]

	batchConfigRegistry *areaBatchConfigRegistry[A]

	// Signal queue is used to decide which paths events should be popped.
	signalQueue        *deque.Deque[eventSignal[A, P, T, D, H]]
	totalPendingLength *atomic.Int64 // The total signal count in the queue.
}

func newEventQueue[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	handler H,
	batchConfigRegistry *areaBatchConfigRegistry[A],
) eventQueue[A, P, T, D, H] {
	return eventQueue[A, P, T, D, H]{
		handler:             handler,
		eventBlockAlloc:     deque.NewBlockAllocator[eventWrap[A, P, T, D, H]](32, 1024),
		batchConfigRegistry: batchConfigRegistry,
		signalQueue:         deque.NewDeque(1024, deque.NewBlockAllocator[eventSignal[A, P, T, D, H]](1024, 32)),
		totalPendingLength:  &atomic.Int64{},
	}
}

func (q *eventQueue[A, P, T, D, H]) initPath(path *pathInfo[A, P, T, D, H]) {
	path.pendingQueue.SetBlockAllocator(q.eventBlockAlloc)
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

	accepted, appended := path.appendEvent(event, q.handler)
	if !accepted {
		q.handler.OnDrop(event.event)
		return
	}
	if !appended {
		return
	}
	if event.eventType.Droppable && event.eventType.Property != PeriodicSignal {
		path.reclaimed.Store(false)
	}
	addSignal()
}

func (q *eventQueue[A, P, T, D, H]) releasePath(
	path *pathInfo[A, P, T, D, H],
	notifyDrop bool,
) {
	if !notifyDrop && path.areaMemStat != nil &&
		path.areaMemStat.settings.Load().algorithm == MemoryControlForEventCollector {
		q.reclaimBlockedPath(path)
		return
	}
	for {
		event, ok := path.pendingQueue.PopFront()
		if !ok {
			break
		}
		if notifyDrop {
			q.handler.OnDrop(event.event)
		}
	}

	if path.areaMemStat != nil {
		path.areaMemStat.decPendingSize(path, int64(path.pendingSize.Load()))
		path.areaMemStat.lastSizeDecreaseTime.Store(time.Now())
	}

	path.pendingSize.Store(0)
}

// reclaimBlockedPath runs only on the queue owner. Keep recovery notifications
// and watermarks so a later Wake can recover even without new upstream events.
func (q *eventQueue[A, P, T, D, H]) reclaimBlockedPath(path *pathInfo[A, P, T, D, H]) {
	if path.removed.Load() || !path.blocking.Load() {
		return
	}
	releasedBytes := int64(0)
	count := path.pendingQueue.Length()
	for i := 0; i < count; i++ {
		e, _ := path.pendingQueue.PopFront()
		if e.eventType.Droppable && e.eventType.Property != PeriodicSignal {
			if replacement := q.handler.OnDrop(e.event); replacement != nil {
				recoveryEvent := replacement.(T)
				// Preserve the wrapper overhead included in the original size.
				recoverySize := e.eventSize - q.handler.GetSize(e.event) + q.handler.GetSize(recoveryEvent)
				if recoverySize < e.eventSize {
					releasedBytes += int64(e.eventSize - recoverySize)
					e.event = recoveryEvent
					e.eventSize = recoverySize
					e.eventType = q.handler.GetType(recoveryEvent)
				}
			}
		}
		path.pendingQueue.PushBack(e)
	}
	path.reclaimed.Store(true)
	for releasedBytes > 0 {
		pending := path.pendingSize.Load()
		// RemovePath concurrently transfers the remaining accounting out of
		// this path. Claim only bytes still owned here, avoiding a double
		// decrement against other paths in the same area.
		if path.removed.Load() || pending < releasedBytes {
			return
		}
		if path.pendingSize.CompareAndSwap(pending, pending-releasedBytes) {
			path.areaMemStat.decPendingSize(path, releasedBytes)
			path.areaMemStat.lastSizeDecreaseTime.Store(time.Now())
			return
		}
	}
}

func (q *eventQueue[A, P, T, D, H]) blockPath(path *pathInfo[A, P, T, D, H]) {
	path.blocking.Store(true)
}

func (q *eventQueue[A, P, T, D, H]) wakePath(path *pathInfo[A, P, T, D, H]) {
	path.blocking.Store(false)
	count := path.pendingQueue.Length()
	if count > 0 {
		q.signalQueue.PushFront(eventSignal[A, P, T, D, H]{pathInfo: path, eventCount: count})
		q.totalPendingLength.Add(int64(count))
	}
}

func (q *eventQueue[A, P, T, D, H]) popEvents(b *batcher[T]) ([]T, *pathInfo[A, P, T, D, H], int, time.Duration) {
	for {
		// We are going to update the signal directly, so we need the reference.
		signal, ok := q.signalQueue.FrontRef()
		if !ok {
			return nil, nil, 0, 0
		}

		if signal.eventCount == 0 {
			log.Panic("signal event count is zero")
		}

		path := signal.pathInfo
		pendingQueue := path.pendingQueue
		if path.removed.Load() {
			// A removed path can still receive stale in-flight signals/events.
			// Clear the path queue and reconcile memory before dropping the signal.
			q.releasePath(path, true)
			q.totalPendingLength.Add(-int64(signal.eventCount))
			q.signalQueue.PopFront()
			continue
		}
		if path.blocking.Load() {
			// The path is blocking, we should ignore the signal completely.
			// Since when it is waked, a signal event will be added to the queue.
			q.totalPendingLength.Add(-int64(signal.eventCount))
			q.signalQueue.PopFront()
			continue
		}

		firstEvent, ok := pendingQueue.FrontRef()
		if !ok {
			// The signal could contain more events than the pendingQueue,
			// which is possible when the path is removed or recovered from blocked.
			// We should ignore the signal completely.
			q.totalPendingLength.Add(-int64(signal.eventCount))
			q.signalQueue.PopFront()
			continue
		}
		firstGroup := firstEvent.eventType.DataGroup
		firstProperty := firstEvent.eventType.Property

		batchConfig := q.batchConfigRegistry.getBatchConfig(path.area)
		b.setLimit(batchConfig)

		var count int
		b.addEvent(firstEvent.event, firstEvent.eventSize)
		path.popEvent()
		count++

		// Try to batch events with the same data group.
		// We check isFull before append, so batch-by-bytes can exceed the limit by at most one event.
		for count < signal.eventCount && !b.isFull() {
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
			b.addEvent(front.event, front.eventSize)
			path.popEvent()
			count++
		}

		signal.eventCount -= count
		if signal.eventCount == 0 {
			q.signalQueue.PopFront()
		}
		q.totalPendingLength.Add(-int64(count))
		events, nBytes, duration := b.flush()
		return events, path, nBytes, duration
	}
}
