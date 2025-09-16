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
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/log"
	. "github.com/pingcap/ticdc/pkg/errors"
)

// Use a hasher to select target stream for the path.
// It implements the DynamicStream interface.
type parallelDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]] struct {
	handler    H
	pathHasher PathHasher[P]
	streams    []*stream[A, P, T, D, H]
	pathMap    struct {
		sync.RWMutex
		m map[P]*pathInfo[A, P, T, D, H]
	}

	eventExtraSize int
	memControl     *memControl[A, P, T, D, H] // TODO: implement memory control

	feedbackChan chan Feedback[A, P, D]

	_statAddPathCount    atomic.Int64
	_statRemovePathCount atomic.Int64
}

func newParallelDynamicStream[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](hasher PathHasher[P], handler H, option Option) *parallelDynamicStream[A, P, T, D, H] {
	option.fix()
	var (
		eventExtraSize int
		zero           T
	)
	if reflect.TypeOf(zero).Kind() == reflect.Pointer {
		eventExtraSize = int(unsafe.Sizeof(eventWrap[A, P, T, D, H]{}))
	} else {
		a := unsafe.Sizeof(eventWrap[A, P, T, D, H]{})
		b := unsafe.Sizeof(zero)
		eventExtraSize = int(a - b)
	}

	s := &parallelDynamicStream[A, P, T, D, H]{
		handler:        handler,
		pathHasher:     hasher,
		eventExtraSize: eventExtraSize,
	}

	s.pathMap.m = make(map[P]*pathInfo[A, P, T, D, H])

	if option.EnableMemoryControl {
		log.Info("Dynamic stream enable memory control")
		s.feedbackChan = make(chan Feedback[A, P, D], 1024)
		s.memControl = newMemControl[A, P, T, D, H]()
	}
	for i := range option.StreamCount {
		s.streams = append(s.streams, newStream(i, handler, option))
	}
	return s
}

func (s *parallelDynamicStream[A, P, T, D, H]) Start() {
	for _, ds := range s.streams {
		ds.start()
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) Close() {
	// clean pathMap, to avoid sending into a closed channel after close()
	s.pathMap.Lock()
	defer s.pathMap.Unlock() // release lock after close all ds, to avoid data race with add path
	clear(s.pathMap.m)
	for _, ds := range s.streams {
		ds.close()
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) Push(path P, e T) {
	s.pathMap.RLock()
	pi, ok := s.pathMap.m[path]
	if !ok {
		s.handler.OnDrop(e)
		s.pathMap.RUnlock()
		return
	}
	s.pathMap.RUnlock()

	ew := eventWrap[A, P, T, D, H]{
		event:     e,
		pathInfo:  pi,
		paused:    s.handler.IsPaused(e),
		eventType: s.handler.GetType(e),
		eventSize: s.eventExtraSize + s.handler.GetSize(e),
		timestamp: s.handler.GetTimestamp(e),
		queueTime: time.Now(),
	}
	pi.stream.in() <- ew
}

func (s *parallelDynamicStream[A, P, T, D, H]) Wake(path P) {
	s.pathMap.RLock()
	pi, ok := s.pathMap.m[path]
	if !ok {
		s.pathMap.RUnlock()
		return
	}
	s.pathMap.RUnlock()
	pi.stream.in() <- eventWrap[A, P, T, D, H]{wake: true, pathInfo: pi}
}

func (s *parallelDynamicStream[A, P, T, D, H]) Feedback() <-chan Feedback[A, P, D] {
	return s.feedbackChan
}

func (s *parallelDynamicStream[A, P, T, D, H]) AddPath(path P, dest D, as ...AreaSettings) error {
	s.pathMap.Lock()

	_, ok := s.pathMap.m[path]
	if ok {
		s.pathMap.Unlock()
		return NewAppError(ErrorTypeDuplicate, fmt.Sprintf("path %v already exists", path))
	}

	area := s.handler.GetArea(path, dest)
	pi := newPathInfo[A, P, T, D, H](area, path, dest)
	pi.setStream(s.streams[s.hash(path)])
	s.pathMap.m[path] = pi
	s.pathMap.Unlock()

	s.setMemControl(pi, as...)

	pi.stream.addPath(pi)

	s._statAddPathCount.Add(1)
	return nil
}

func (s *parallelDynamicStream[A, P, T, D, H]) RemovePath(path P) error {
	s.pathMap.Lock()

	pi, ok := s.pathMap.m[path]
	if !ok {
		s.pathMap.Unlock()
		return NewAppErrorS(ErrorTypeNotExist)
	}

	pi.removed.Store(true)

	if s.memControl != nil {
		s.memControl.removePathFromArea(pi)
	}
	delete(s.pathMap.m, path)
	s.pathMap.Unlock()

	pi.stream.in() <- eventWrap[A, P, T, D, H]{pathInfo: pi}

	s._statRemovePathCount.Add(1)
	return nil
}

func (s *parallelDynamicStream[A, P, T, D, H]) SetAreaSettings(area A, settings AreaSettings) {
	if s.memControl != nil {
		s.memControl.setAreaSettings(area, settings)
	}
}

func (s *parallelDynamicStream[A, P, T, D, H]) GetMetrics() Metrics[A] {
	metrics := Metrics[A]{}
	for _, ds := range s.streams {
		metrics.PendingQueueLen += ds.getPendingSize()
	}
	metrics.AddPath = int(s._statAddPathCount.Load())
	metrics.RemovePath = int(s._statRemovePathCount.Load())

	if s.memControl != nil {
		metrics.MemoryControl = s.memControl.getMetrics()
	}

	return metrics
}

func (s *parallelDynamicStream[A, P, T, D, H]) hash(path P) int {
	hash := s.pathHasher(path)
	return int(hash % uint64(len(s.streams)))
}

func (s *parallelDynamicStream[A, P, T, D, H]) setMemControl(
	pi *pathInfo[A, P, T, D, H],
	as ...AreaSettings,
) {
	if s.memControl != nil {
		setting := AreaSettings{}
		if len(as) > 0 {
			setting = as[0]
		}
		s.memControl.addPathToArea(pi, setting, s.feedbackChan)
	}
}
