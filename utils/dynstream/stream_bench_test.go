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
)

type inc struct {
	times int
	n     *atomic.Int64
	done  *sync.WaitGroup

	path string
}

type D struct{}
type incHandler struct{}

func (h *incHandler) Path(event *inc) string {
	return event.path
}
func (h *incHandler) Handle(dest D, events ...*inc) (await bool) {
	event := events[0]
	for i := 0; i < event.times; i++ {
		event.n.Add(1)
	}
	event.done.Done()
	return false
}

func (h *incHandler) GetSize(event *inc) int            { return 0 }
func (h *incHandler) GetArea(path string, dest D) int   { return 0 }
func (h *incHandler) GetTimestamp(event *inc) Timestamp { return 0 }
func (h *incHandler) GetType(event *inc) EventType      { return DefaultEventType }
func (h *incHandler) IsPaused(event *inc) bool          { return false }
func (h *incHandler) OnDrop(event *inc)                 {}

func runStream(eventCount int, times int) {
	handler := &incHandler{}

	pi := newPathInfo[int, string, *inc, D, *incHandler](0, "p1", D{})
	stream := newStream[int, string, *inc, D](1 /*id*/, handler, NewOption())
	stream.start()

	total := &atomic.Int64{}
	done := &sync.WaitGroup{}

	done.Add(eventCount)
	for i := 0; i < eventCount; i++ {
		stream.in() <- eventWrap[int, string, *inc, D, *incHandler]{event: &inc{times: times, n: total, done: done}, pathInfo: pi}
	}

	done.Wait()
	stream.close()

}

func runLoop(eventCount int, times int) {
	total := &atomic.Int64{}
	done := &sync.WaitGroup{}

	done.Add(eventCount)

	signal := make(chan struct{}, 100)

	go func() {
		for range signal {
			for j := 0; j < times; j++ {
				total.Add(1)
			}
			done.Done()
		}
	}()

	for i := 0; i < eventCount; i++ {
		signal <- struct{}{}
	}
	done.Wait()
}

func BenchmarkSStream1000x1(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(1000, 1)
	}
}

func BenchmarkSStream1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(1000, 100)
	}
}

func BenchmarkSStream100000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runStream(100000, 100)
	}
}

func BenchmarkSLoop1000x1(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(1000, 1)
	}
}

func BenchmarkSLoop1000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(1000, 100)
	}
}

func BenchmarkSLoop100000x100(b *testing.B) {
	for k := 0; k < b.N; k++ {
		runLoop(100000, 100)
	}
}
