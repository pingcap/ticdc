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
	"github.com/pingcap/ticdc/utils/deque"
	"go.uber.org/zap"
)

const BlockLenInPendingQueue = 32

// A stream has two goroutines: receiver and handleLoop.
// The receiver receives the events and buffers them.
// The handleLoop handles the events.
// While if UseBuffer is false, the receiver is not needed, and the handleLoop directly receives the events.
type stream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	id int

	handler Handler[A, P, T, D]

	// These fields are used when UseBuffer is true.
	// They are used to buffer the events between the receiver and the handleLoop.
	bufferCount atomic.Int64
	inChan      chan eventWrap[A, P, T, D, H] // The buffer channel to receive the events.
	outChan     chan eventWrap[A, P, T, D, H] // The buffer channel to send the events.

	// The channel used by the handleLoop to receive the events.
	eventChan chan eventWrap[A, P, T, D, H]

	// The queue to store the pending events of this stream.
	eventQueue eventQueue[A, P, T, D, H]

	option Option

	isClosed atomic.Bool

	wg sync.WaitGroup

	startTime time.Time
}

func newStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	id int,
	handler H,
	option Option,
) *stream[A, P, T, D, H] {
	s := &stream[A, P, T, D, H]{
		id:         id,
		handler:    handler,
		eventQueue: newEventQueue(option, handler),
		option:     option,
		startTime:  time.Now(),
	}

	if option.UseBuffer {
		s.inChan = make(chan eventWrap[A, P, T, D, H], 64)
		s.outChan = make(chan eventWrap[A, P, T, D, H], 64)

		s.eventChan = s.outChan
	} else {
		s.eventChan = make(chan eventWrap[A, P, T, D, H], 64)
	}
	return s
}

func (s *stream[A, P, T, D, H]) addPath(path *pathInfo[A, P, T, D, H]) {
	s.in() <- eventWrap[A, P, T, D, H]{pathInfo: path, newPath: true}
}

func (s *stream[A, P, T, D, H]) getPendingSize() int {
	if s.option.UseBuffer {
		return len(s.inChan) + int(s.bufferCount.Load()) + len(s.outChan) + int(s.eventQueue.totalPendingLength.Load())
	} else {
		return len(s.eventChan) + int(s.eventQueue.totalPendingLength.Load())
	}
}

func (s *stream[A, P, T, D, H]) in() chan eventWrap[A, P, T, D, H] {
	if s.option.UseBuffer {
		return s.inChan
	} else {
		return s.eventChan
	}
}

// Start the stream.
func (s *stream[A, P, T, D, H]) start() {
	if s.isClosed.Load() {
		panic("The stream has been closed.")
	}

	if s.option.UseBuffer {
		s.wg.Add(1)
		go s.receiver()
	}

	s.wg.Add(1)
	go s.handleLoop()
}

// Close the stream and wait for all goroutines to exit.
// wait is by default true, which means to wait for the goroutines to exit.
func (s *stream[A, P, T, D, H]) close(wait ...bool) {
	if s.isClosed.CompareAndSwap(false, true) {
		if s.option.UseBuffer {
			close(s.inChan)
		} else {
			close(s.eventChan)
		}
	}
	if len(wait) == 0 || wait[0] {
		s.wg.Wait()
	}
}

func (s *stream[A, P, T, D, H]) receiver() {
	buffer := deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue)
	defer func() {
		// Move all remaining events in the buffer to the outChan.
		for {
			event, ok := buffer.FrontRef()
			if !ok {
				break
			} else {
				s.outChan <- *event
				buffer.PopFront()
				s.bufferCount.Add(-1)
			}
		}
		close(s.outChan)
		s.wg.Done()
	}()

	for {
		event, ok := buffer.FrontRef()
		if !ok {
			e, ok := <-s.inChan
			if !ok {
				return
			}
			buffer.PushBack(e)
			s.bufferCount.Add(1)
		} else {
			select {
			case e, ok := <-s.inChan:
				if !ok {
					return
				}
				buffer.PushBack(e)
				s.bufferCount.Add(1)
			case s.outChan <- *event:
				buffer.PopFront()
				s.bufferCount.Add(-1)
			}
		}
	}
}

// handleLoop is the main loop of the stream.
// It handles the events.
func (s *stream[A, P, T, D, H]) handleLoop() {
	handleEvent := func(e eventWrap[A, P, T, D, H]) {
		switch {
		case e.wake:
			s.eventQueue.wakePath(e.pathInfo)
		case e.newPath:
			s.eventQueue.initPath(e.pathInfo)
		case e.pathInfo.removed.Load():
			// The path is removed, so we don't need to handle its events.
			return
		default:
			s.eventQueue.appendEvent(e)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			log.Panic("handleLoop panic",
				zap.Any("recover", r),
				zap.Stack("stack"))
		}

		// Move remaining events in the eventChan to pendingQueue.
		for e := range s.eventChan {
			handleEvent(e)
		}

		s.wg.Done()
	}()

	// Variables below will be used in the Loop below.
	// Declared here to avoid repeated allocation.
	var (
		eventQueueEmpty = false
		eventBuf        = make([]T, 0, s.option.BatchCount)
		zeroT           T
		cleanUpEventBuf = func() {
			for i := range eventBuf {
				eventBuf[i] = zeroT
			}
			eventBuf = eventBuf[:0]
		}
		path *pathInfo[A, P, T, D, H]
	)

	// For testing. Don't handle events until this wait group is done.
	if s.option.handleWait != nil {
		s.option.handleWait.Wait()
	}

	// 1. Drain the eventChan to pendingQueue.
	// 2. Pop events from the eventQueue and handle them.
Loop:
	for {
		if eventQueueEmpty {
			e, ok := <-s.eventChan
			if !ok {
				// The stream is closed.
				return
			}
			handleEvent(e)
			eventQueueEmpty = false
		} else {
			select {
			case e, ok := <-s.eventChan:
				if !ok {
					return
				}
				handleEvent(e)
				eventQueueEmpty = false
			default:
				eventBuf, path = s.eventQueue.popEvents(eventBuf)
				if len(eventBuf) == 0 {
					eventQueueEmpty = true
					continue Loop
				}
				path.blocking = s.handler.Handle(path.dest, eventBuf...)
				if path.blocking {
					s.eventQueue.blockPath(path)
				}
				cleanUpEventBuf()
			}
		}
	}
}

// ====== internal types ======

// pathInfo contains the status of a path.
// Note that although this struct is used by multiple goroutines, it doesn't need synchronization because
// different fields are either immutable or accessed by different goroutines.
// We use one struct to store them together to avoid mapping by path in different places in many times,
// and to avoid the overhead of creating a new struct.

type pathInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A
	path P
	dest D

	// The current stream this path belongs to.
	stream *stream[A, P, T, D, H]
	// This field is used to mark the path as removed, so that the handle goroutine can ignore it.
	// Note that we should not need to use a atomic.Bool here, because this field is set by the RemovePaths method,
	// and we use sync.WaitGroup to wait for finish. So if RemovePaths is called in the handle goroutine, it should be
	// guaranteed to see the memory change of this field.
	removed atomic.Bool
	// The path is blocked by the handler.
	blocking bool

	// The pending events of the path.
	pendingQueue *deque.Deque[eventWrap[A, P, T, D, H]]

	// Fields used by the memory control.
	areaMemStat *areaMemStat[A, P, T, D, H]

	pendingSize          atomic.Int64 // The total size(bytes) of pending events in the pendingQueue of the path.
	paused               atomic.Bool  // The path is paused to send events.
	lastSendFeedbackTime atomic.Value
}

func newPathInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](area A, path P, dest D) *pathInfo[A, P, T, D, H] {
	pi := &pathInfo[A, P, T, D, H]{
		area:                 area,
		path:                 path,
		dest:                 dest,
		pendingQueue:         deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue),
		lastSendFeedbackTime: atomic.Value{},
	}
	pi.lastSendFeedbackTime.Store(time.Unix(0, 0))
	return pi
}

func (pi *pathInfo[A, P, T, D, H]) setStream(stream *stream[A, P, T, D, H]) {
	pi.stream = stream
}

// appendEvent appends an event to the pending queue.
// It returns true if the event is appended successfully.
func (pi *pathInfo[A, P, T, D, H]) appendEvent(event eventWrap[A, P, T, D, H], handler H) bool {
	if pi.areaMemStat != nil {
		return pi.areaMemStat.appendEvent(pi, event, handler)
	}

	if event.eventType.Property != PeriodicSignal {
		pi.pendingQueue.PushBack(event)
		pi.updatePendingSize(int64(event.eventSize))
		return true
	}

	back, ok := pi.pendingQueue.BackRef()
	if ok && back.eventType.Property == PeriodicSignal {
		// If the last event is a periodic signal, we only need to keep the latest one.
		// And we don't need to add a new signal.
		*back = event
		return false
	} else {
		pi.pendingQueue.PushBack(event)
		return true
	}
}

func (pi *pathInfo[A, P, T, D, H]) popEvent() (eventWrap[A, P, T, D, H], bool) {
	e, ok := pi.pendingQueue.PopFront()
	if !ok {
		return eventWrap[A, P, T, D, H]{}, false
	}
	pi.updatePendingSize(int64(-e.eventSize))

	if pi.areaMemStat != nil {
		pi.areaMemStat.decPendingSize(pi, int64(e.eventSize))
	}
	return e, true
}

func (pi *pathInfo[A, P, T, D, H]) updatePendingSize(delta int64) {
	oldSize := pi.pendingSize.Load()
	// Check for integer overflow/underflow
	newSize := oldSize + delta
	if delta > 0 && newSize < oldSize {
		log.Error("Integer overflow detected in updatePendingSize",
			zap.Int64("oldSize", oldSize),
			zap.Int64("delta", delta))
		return
	}
	if delta < 0 && newSize > oldSize {
		log.Error("Integer underflow detected in updatePendingSize",
			zap.Int64("oldSize", oldSize),
			zap.Int64("delta", delta))
		return
	}
	pi.pendingSize.Store(newSize)
}

// eventWrap contains the event and the path info.
// It can be a event or a wake signal.
type eventWrap[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	event   T
	wake    bool
	newPath bool

	pathInfo *pathInfo[A, P, T, D, H]

	paused    bool
	eventSize int
	eventType EventType

	timestamp Timestamp
	queueTime time.Time
}
