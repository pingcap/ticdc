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
	"fmt"
	"runtime"
	"sync"
	"time"
)

// Path is a unique identifier of a destination.
type Path comparable

// Area manages a group of paths, usually a GID.
type Area comparable

// The timestamp an event carries. E.g. the commit TS of a DML.
// Normally, events with smaller timestamps are processed first among the same Area, but it is not guaranteed.
// In a path, events come earlier should have smaller timestamps. DynamicStream will not check the
// order of the timestamps, it is the handler's responsibility to handle the events in the correct order.
type Timestamp uint64

// An event belongs to a path.
type Event any

// A destination is the place where the event is sent to.
type Dest any

type EventType struct {
	// The group of the event. It is used to group the events for the handler to process.
	// Events with different groups will not be processed in a group by the handler.
	DataGroup int
	Property  Property
	// If the event is droppable, it means the event can be dropped by the memory control.
	Droppable bool
}

func (t EventType) String() string {
	return fmt.Sprintf("EventType{DataGroup: %d, Property: %s}", t.DataGroup, t.Property.String())
}

var DefaultEventType = EventType{DataGroup: 0, Property: BatchableData}

// The property of the event, it is used to how dynamic stream handles the event.
type Property int

const (
	// BatchableData - Events that can be processed in batches
	// These events carry data and can be batched together for better performance
	BatchableData Property = iota
	// PeriodicSignal - Periodic signal events
	// 1. Contains no actual data, only indicates occurrence of an event
	// 2. System drops early duplicate signals to reduce load
	// 3. Must continue sending even when path is paused (for memory control)
	// 4. Should be small and consistent in size
	// Example: resolvedTs
	PeriodicSignal
	// NonBatchable - Events that must be processed individually
	// These events require sequential, one-by-one processing
	NonBatchable
)

func (p Property) String() string {
	switch p {
	case BatchableData:
		return "BatchableData"
	case PeriodicSignal:
		return "PeriodicSignal"
	case NonBatchable:
		return "NonBatchable"
	default:
		return fmt.Sprintf("Unknown Property: %d", p)
	}
}

// The handler interface. The handler processes the event.
type Handler[A Area, P Path, T Event, D Dest] interface {
	// Path of the event. This method is called once for each event.
	Path(event T) P

	// Handle processes the event.
	// The dest is included in the argument to avoid the requirement of another mapping to get the destination.
	// If the events are processed successfully, it should return false.
	// If the events are processed asynchronously, it should return true. The later events of the path are blocked
	// until a wake signal is sent to DynamicStream's Wake channel.
	// The len(events) is guaranteed to be greater than 0.
	Handle(dest D, events ...T) (await bool)

	// The methods below are optional.

	// GetSize get the size of the event. This method is called once for each event.
	// You should return all the memory usage of the event, including the size of the event itself and the size of the data it carries.
	// Return 0 by default implementation, if the size is not used.
	//
	// Used by the memory control.
	GetSize(event T) int

	// IsPaused Returns the pause status from the upstream status.
	// DynamicStream sends feedbacks if the pause status of upstream is not equals to the local status.
	//
	// Used by the memory control, to decide whether we should send feedbacks to the upstream.
	IsPaused(event T) bool

	// GetArea Get the area of the path. This method is called once for each path.
	// Return zero by default implementation. I.e. all paths are in the default area.
	//
	// Used in deciding the handle priority of the events from different areas.
	GetArea(path P, dest D) A

	// GetTimestamp Get the timestamp of the event. This method is called once for each event.
	// Events are processed in the order of the timestamps.
	// Return zero by default implementation. In this case, the events are processed
	// in the order of the arrival.
	//
	// Used in deciding the handle priority of the events from different paths in the same area.
	GetTimestamp(event T) Timestamp

	// GetType Get the type of the event. This method is called once for each event.
	// Return zero by default implementation. I.e. all events are in the same type.
	//
	// Only the events with the same type are processed in a group.
	GetType(event T) EventType

	// OnDrop is called when an event is dropped. Could be caused by the memory control or cannot find the path.
	// Do nothing by default implementation.
	OnDrop(event T) interface{}
}

type PathAndDest[P Path, D Dest] struct {
	Path P
	Dest D
}

/*
DynamicStream is a stream that can process events with from different paths concurrently.
  - Events from the same path are processed sequentially.
  - Events from different paths are processed concurrently.

We assume that the handler is CPU-bound and should not be blocked by any waiting. Otherwise, events from other paths will be blocked.
*/
type DynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] interface {
	// Start starts the dynamic stream.
	// It should be called before any other methods.
	Start()

	// Close closes the dynamic stream.
	// No more events can be sent to or processed by the stream after it is closed.
	Close()

	// Push an event to path.
	Push(path P, event T)

	// Wake marks the path as ready to process the next event.
	// It is used when the handler returns true in the Handle method.
	Wake(path P)

	// Feedback returns the channel to receive the feedbacks for the listener.
	// Current the feedbacks are used for the memory control.
	// Return nil if Option.EnableMemoryControl is false.
	Feedback() <-chan Feedback[A, P, D]

	// AddPath add the path to the dynamic stream to receive the events.
	// An event of a path not already added will be dropped.
	// Return ErrorTypeDuplicate if the path already exists.
	AddPath(path P, dest D, area ...AreaSettings) error

	// RemovePath removes the path from the dynamic stream.
	// After this call return, future events with the path will be dropped, including events which are already in the stream.
	// If the path doesn't exist, it will return ErrorTypeNotExist.
	RemovePath(path P) error

	// SetAreaSettings sets the settings of the area. An area uses the default settings if it is not set.
	// This method can be called at any time. But to avoid the memory leak, setting on a area without existing paths is a no-op.
	SetAreaSettings(area A, settings AreaSettings)

	GetMetrics() Metrics[A]
}

const (
	DefaultInputBufferSize   = 1024
	DefaultSchedulerInterval = 16 * time.Second
	DefaultReportInterval    = 10 * time.Second
	DefaultMaxPendingSize    = uint64(1024 * 1024 * 1024) // 1 GB
	DefaultFeedbackInterval  = 1000 * time.Millisecond
)

type Option struct {
	InputChanSize int // The buffer size of the input channel. By default 0, means 1024.

	SchedulerInterval time.Duration // The interval of the scheduler. The scheduler is used to balance the paths between streams.
	ReportInterval    time.Duration // The interval of reporting the status of stream, the status is used by the scheduler.

	StreamCount int // The count of streams. I.e. the count of goroutines to handle events. By default 0, means runtime.NumCPU().
	BatchCount  int // The batch count of handling events. <= 1 means no batch. By default 1.
	BatchBytes  int // The max bytes of the batch. <= 1 means no limit. By default 0.

	EnableMemoryControl bool // Enable the memory control. By default false.

	UseBuffer bool // Use buffers inside the dynamic stream. By default false.

	handleWait *sync.WaitGroup // For testing. Don't handle events until this wait group is done.
}

func NewOption() Option {
	return Option{
		SchedulerInterval: DefaultSchedulerInterval,
		ReportInterval:    DefaultReportInterval,
		StreamCount:       0,
		BatchCount:        1,
		UseBuffer:         false,
	}
}

func (o *Option) fix() {
	if o.InputChanSize <= 0 {
		o.InputChanSize = DefaultInputBufferSize
	}
	if o.StreamCount == 0 {
		o.StreamCount = runtime.NumCPU()
	}
	if o.BatchCount <= 0 {
		o.BatchCount = 1
	}
}

type AreaSettings struct {
	component        string
	maxPendingSize   uint64        // The max memory usage of the pending events of the area. Must be larger than 0. By default 1GB.
	feedbackInterval time.Duration // The interval of the feedback. By default 1000ms.
	// Remove it when we determine the v2 is working well.
	algorithm int // The algorithm of the memory control.
}

func (s *AreaSettings) fix() {
	if s.maxPendingSize <= 0 {
		s.maxPendingSize = DefaultMaxPendingSize
	}

	if s.feedbackInterval == 0 {
		s.feedbackInterval = DefaultFeedbackInterval
	}
}

func NewAreaSettingsWithMaxPendingSize(size uint64, memoryControlAlgorithm int, component string) AreaSettings {
	return AreaSettings{
		component:        component,
		feedbackInterval: DefaultFeedbackInterval,
		maxPendingSize:   size,
		algorithm:        memoryControlAlgorithm,
	}
}

type FeedbackType int

const (
	PausePath FeedbackType = iota
	ResumePath
	PauseArea
	ResumeArea
)

func (f FeedbackType) String() string {
	switch f {
	case PausePath:
		return "PausePath"
	case ResumePath:
		return "ResumePath"
	case PauseArea:
		return "PauseArea"
	case ResumeArea:
		return "ResumeArea"
	default:
		return fmt.Sprintf("Unknown FeedbackType: %d", f)
	}
}

type Feedback[A Area, P Path, D Dest] struct {
	Area A
	Path P
	Dest D

	FeedbackType FeedbackType
}

func (f *Feedback[A, P, D]) String() string {
	return fmt.Sprintf("DynamicStream Feedback{Area: %v, Path: %v, FeedbackType: %s}", f.Area, f.Path, f.FeedbackType.String())
}

func NewParallelDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](handler H, option ...Option) DynamicStream[A, P, T, D, H] {
	opt := NewOption()
	if len(option) > 0 {
		opt = option[0]
	}
	return newParallelDynamicStream(handler, opt)
}

type Metrics[A Area] struct {
	EventChanSize   int
	PendingQueueLen int
	AddPath         int
	RemovePath      int

	MemoryControl MemoryMetric[A]
}
