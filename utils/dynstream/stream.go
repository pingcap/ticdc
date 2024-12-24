package dynstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/utils/deque"
)

var nextReportRound = atomic.Int64{}

const BlockLenInPendingQueue = 32

// ====== internal types ======

// An area info contains the path nodes of the area in a stream.
// Note that the instance is stream level, not global level.
type streamAreaInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	area A

	// The path bound to the area info instance.
	// Since timestampHeap and queueTimeHeap only store the paths who has pending events,
	// the pathCount could be larger than the length of the heaps.
	pathCount int
}

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
	removed bool
	// The path is blocked by the handler.
	blocking bool

	// The pending events of the path.
	pendingQueue *deque.Deque[eventWrap[A, P, T, D, H]]

	// Fields used by the memory control.
	areaMemStat *areaMemStat[A, P, T, D, H]

	// Those values below are used to compare in the heap.
	frontTimestamp Timestamp // The timestamp of the front event.
	frontQueueTime time.Time // The queue time of the front event.

	pendingSize          int  // The total size(bytes) of pending events in the pendingQueue of the path.
	paused               bool // The path is paused to send events.
	lastSwitchPausedTime time.Time
	lastSendFeedbackTime time.Time
}

func newPathInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](area A, path P, dest D) *pathInfo[A, P, T, D, H] {
	pi := &pathInfo[A, P, T, D, H]{
		area:         area,
		path:         path,
		dest:         dest,
		pendingQueue: deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue),
	}
	return pi
}

func (pi *pathInfo[A, P, T, D, H]) setStream(stream *stream[A, P, T, D, H]) {
	pi.stream = stream
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

type doneInfo[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	pathInfo   *pathInfo[A, P, T, D, H]
	handleTime time.Duration
}

// A stream uses two goroutines
// 1. handleLoop: to handle the events.
// 2. reportStatLoop: to report the statistics.
type stream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	id int

	handler Handler[A, P, T, D]

	// These fields are used when UseBuffer is true.
	// They are used to buffer the events between the reciever and the handleLoop.
	bufferCount atomic.Int64
	inChan      chan eventWrap[A, P, T, D, H] // The buffer channel to receive the events.
	outChan     chan eventWrap[A, P, T, D, H] // The buffer channel to send the events.

	// The channel used by the handleLoop to receive the events.
	eventChan chan eventWrap[A, P, T, D, H]

	eventQueue eventQueue[A, P, T, D, H] // The queue to store the pending events.

	option Option

	isClosed atomic.Bool

	handleWg sync.WaitGroup
	reportWg sync.WaitGroup

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
func (s *stream[A, P, T, D, H]) start(acceptedPaths []*pathInfo[A, P, T, D, H], formerStreams ...*stream[A, P, T, D, H]) {
	if s.isClosed.Load() {
		panic("The stream has been closed.")
	}
	if s.option.UseBuffer {
		go s.reciever()
	}

	s.handleWg.Add(1)
	go s.handleLoop(acceptedPaths, formerStreams)
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
		s.handleWg.Wait()
	}
}

func (s *stream[A, P, T, D, H]) reciever() {
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
func (s *stream[A, P, T, D, H]) handleLoop(
	acceptedPaths []*pathInfo[A, P, T, D, H],
	formerStreams []*stream[A, P, T, D, H],
) {
	pushToPendingQueue := func(e eventWrap[A, P, T, D, H]) {
		switch {
		case e.wake:
			s.eventQueue.wakePath(e.pathInfo)
		case e.newPath:
			s.eventQueue.initPath(e.pathInfo)
		case e.pathInfo.removed:
			s.eventQueue.removePath(e.pathInfo)
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
			pushToPendingQueue(e)
		}

		s.reportWg.Wait()
		s.handleWg.Done()
	}()

	// Close and wait for the former streams.
	for _, stream := range formerStreams {
		stream.close()
		// We don't need to explicitly remove the paths from the pendingQueue.
		// Because the stream is closed already, and the data structure is not used anymore.
	}

	// We initialize the pathMap here to avoid blocking the main goroutine.
	// As there could be many paths, and the initialization could be time-consuming.
	for _, p := range acceptedPaths {
		s.eventQueue.initPath(p)
	}

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
			pushToPendingQueue(e)
			eventQueueEmpty = false
		} else {
			select {
			case e, ok := <-s.eventChan:
				if !ok {
					return
				}
				pushToPendingQueue(e)
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
