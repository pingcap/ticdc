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
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/utils/deque"
)

type inc struct {
	times int
	n     *atomic.Int64
	done  *sync.WaitGroup

	path string
}

type (
	D          struct{}
	incHandler struct{}
)

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
func (h *incHandler) OnDrop(event *inc) interface{}     { return nil }

func runStream(eventCount int, times int) {
	handler := &incHandler{}

	pi := newPathInfo[int, string, *inc, D, *incHandler](0, "p1", D{})
	stream := newStream[int, string, *inc, D](1 /*id*/, handler, NewOption())
	stream.start()

	total := &atomic.Int64{}
	done := &sync.WaitGroup{}

	done.Add(eventCount)
	for i := 0; i < eventCount; i++ {
		stream.addEvent(eventWrap[int, string, *inc, D, *incHandler]{event: &inc{times: times, n: total, done: done}, pathInfo: pi})
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

// addEventDirectSend simulates direct channel send without select
func addEventDirectSend[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	ch chan eventWrap[A, P, T, D, H],
	event eventWrap[A, P, T, D, H],
) bool {
	ch <- event
	return true
}

// go test -bench=BenchmarkChannel -benchmem ./utils/dynstream/
// goos: darwin
// goarch: arm64
// pkg: github.com/pingcap/ticdc/utils/dynstream
// cpu: Apple M4 Max
// BenchmarkChannelDirectSend-14                                   38532294                30.59 ns/op            0 B/op          0 allocs/op
// BenchmarkChannelDirectSendHighContention-14                      7781707               162.4 ns/op             0 B/op          0 allocs/op
// BenchmarkChannelOptimizedWithContext-14                         39117944                35.40 ns/op            0 B/op          0 allocs/op
// BenchmarkChannelOptimizedWithContextHighContention-14            5897019               205.1 ns/op             0 B/op          0 allocs/op
// PASS
// ok      github.com/pingcap/ticdc/utils/dynstream        6.454s
func BenchmarkChannelDirectSend(b *testing.B) {
	ch := make(chan eventWrap[int, string, *inc, D, *incHandler], 1000)
	event := eventWrap[int, string, *inc, D, *incHandler]{
		event: &inc{times: 1, n: &atomic.Int64{}, done: &sync.WaitGroup{}},
	}

	// Consumer goroutine
	go func() {
		for range ch {
			// consume events
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addEventDirectSend(ch, event)
	}
}

func BenchmarkChannelDirectSendHighContention(b *testing.B) {
	ch := make(chan eventWrap[int, string, *inc, D, *incHandler], 10) // Small buffer to create contention
	event := eventWrap[int, string, *inc, D, *incHandler]{
		event: &inc{times: 1, n: &atomic.Int64{}, done: &sync.WaitGroup{}},
	}

	// Slow consumer to create backpressure
	go func() {
		for range ch {
			// Simulate slow processing
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			addEventDirectSend(ch, event)
		}
	})
}

// addEventOptimized implements the fast path optimization with atomic check first
func addEventWithContextAndCloseCheck[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	closed *atomic.Bool,
	ctx context.Context,
	ch chan eventWrap[A, P, T, D, H],
	event eventWrap[A, P, T, D, H],
) bool {
	if closed.Load() {
		return false
	}

	// Fast path: try direct send without blocking
	select {
	case ch <- event:
		return true
	default:
		// Slow path: check for closure while waiting
		select {
		case <-ctx.Done():
			return false
		case ch <- event:
			return true
		}
	}
}

func BenchmarkChannelOptimizedWithContext(b *testing.B) {
	ctx := context.Background()
	ch := make(chan eventWrap[int, string, *inc, D, *incHandler], 1000)
	closed := &atomic.Bool{}
	event := eventWrap[int, string, *inc, D, *incHandler]{
		event: &inc{times: 1, n: &atomic.Int64{}, done: &sync.WaitGroup{}},
	}

	// Consumer goroutine
	go func() {
		for range ch {
			// consume events
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addEventWithContextAndCloseCheck(closed, ctx, ch, event)
	}
}

// High contention benchmarks for optimized versions
func BenchmarkChannelOptimizedWithContextHighContention(b *testing.B) {
	ctx := context.Background()
	ch := make(chan eventWrap[int, string, *inc, D, *incHandler], 10) // Small buffer to create contention
	closed := &atomic.Bool{}
	event := eventWrap[int, string, *inc, D, *incHandler]{
		event: &inc{times: 1, n: &atomic.Int64{}, done: &sync.WaitGroup{}},
	}

	// Slow consumer to create backpressure
	go func() {
		for range ch {
			// Simulate slow processing
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			addEventWithContextAndCloseCheck(closed, ctx, ch, event)
		}
	})
}

// receiverOnlyEventCh simulates receiver that only listens to event channel (no ctx.Done())
func receiverOnlyEventCh[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	inChan <-chan eventWrap[A, P, T, D, H],
	outChan chan<- eventWrap[A, P, T, D, H],
	closed *atomic.Bool,
	bufferCount *atomic.Int64,
	wg *sync.WaitGroup,
) {
	buffer := deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue)
	defer func() {
		// Move all remaining events out of the buffer.
		for {
			_, ok := buffer.FrontRef()
			if !ok {
				break
			} else {
				buffer.PopFront()
				bufferCount.Add(-1)
			}
		}
		close(outChan)
		wg.Done()
	}()

	for {
		event, ok := buffer.FrontRef()
		if closed.Load() {
			return
		}
		if !ok {
			// Only listen to inChan, no ctx.Done()
			e, ok := <-inChan
			if !ok {
				return
			}
			buffer.PushBack(e)
			bufferCount.Add(1)
		} else {
			if closed.Load() {
				return
			}

			select {
			case e, ok := <-inChan:
				if !ok {
					return
				}
				buffer.PushBack(e)
				bufferCount.Add(1)
			case outChan <- *event:
				buffer.PopFront()
				bufferCount.Add(-1)
			}
		}
	}
}

// receiverWithContextCheck simulates receiver that listens to both event channel and ctx.Done()
func receiverWithContextCheck[A Area, P Path, T Event, D Dest, H Handler[A, P, T, D]](
	ctx context.Context,
	inChan <-chan eventWrap[A, P, T, D, H],
	outChan chan<- eventWrap[A, P, T, D, H],
	closed *atomic.Bool,
	bufferCount *atomic.Int64,
	wg *sync.WaitGroup,
) {
	buffer := deque.NewDeque[eventWrap[A, P, T, D, H]](BlockLenInPendingQueue)
	defer func() {
		// Move all remaining events out of the buffer.
		for {
			_, ok := buffer.FrontRef()
			if !ok {
				break
			} else {
				buffer.PopFront()
				bufferCount.Add(-1)
			}
		}
		close(outChan)
		wg.Done()
	}()

	for {
		event, ok := buffer.FrontRef()
		if closed.Load() {
			return
		}
		if !ok {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-inChan:
				if !ok {
					return
				}
				buffer.PushBack(e)
				bufferCount.Add(1)
			}
		} else {
			if closed.Load() {
				return
			}

			select {
			case e, ok := <-inChan:
				if !ok {
					return
				}
				buffer.PushBack(e)
				bufferCount.Add(1)
			case outChan <- *event:
				buffer.PopFront()
				bufferCount.Add(-1)
			case <-ctx.Done():
				return
			}
		}
	}
}

// runReceiverBenchmark runs a benchmark for receiver performance
func runReceiverBenchmark(eventCount int, withContext bool) {
	inChan := make(chan eventWrap[int, string, *inc, D, *incHandler], 1000)
	outChan := make(chan eventWrap[int, string, *inc, D, *incHandler], 1000)
	closed := &atomic.Bool{}
	bufferCount := &atomic.Int64{}
	wg := &sync.WaitGroup{}

	ctx := context.Background()

	// Start receiver
	wg.Add(1)
	if withContext {
		go receiverWithContextCheck(ctx, inChan, outChan, closed, bufferCount, wg)
	} else {
		go receiverOnlyEventCh(inChan, outChan, closed, bufferCount, wg)
	}

	// Start consumer
	consumeDone := &sync.WaitGroup{}
	consumeDone.Add(1)
	go func() {
		defer consumeDone.Done()
		count := 0
		for range outChan {
			count++
			if count >= eventCount {
				break
			}
		}
	}()

	// Send events
	pi := newPathInfo[int, string, *inc, D, *incHandler](0, "p1", D{})
	for i := 0; i < eventCount; i++ {
		event := eventWrap[int, string, *inc, D, *incHandler]{
			event:    &inc{times: 1, n: &atomic.Int64{}, done: &sync.WaitGroup{}},
			pathInfo: pi,
		}
		inChan <- event
	}

	// Wait for consumption to complete
	consumeDone.Wait()

	// Clean up
	closed.Store(true)
	close(inChan)
	wg.Wait()
}

// go test -bench="BenchmarkReceiver.*1000" -benchmem ./utils/dynstream/
// goos: darwin
// goarch: arm64
// pkg: github.com/pingcap/ticdc/utils/dynstream
// cpu: Apple M4 Max
// BenchmarkReceiverOnlyEventCh1000-14                 4467            260194 ns/op          373209 B/op       3079 allocs/op
// BenchmarkReceiverWithContext1000-14                 4453            257699 ns/op          373228 B/op       3079 allocs/op
// BenchmarkReceiverOnlyEventCh10000-14                 469           2573837 ns/op         1933906 B/op      30641 allocs/op
// BenchmarkReceiverWithContext10000-14                 452           2621993 ns/op         1933926 B/op      30641 allocs/op
// BenchmarkReceiverOnlyEventCh100000-14                 48          25852419 ns/op        17547378 B/op     306267 allocs/op
// BenchmarkReceiverWithContext100000-14                 46          26000457 ns/op        17547428 B/op     306267 allocs/op
// PASS
// ok      github.com/pingcap/ticdc/utils/dynstream        9.749s
func BenchmarkReceiverOnlyEventCh1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runReceiverBenchmark(1000, false)
	}
}

func BenchmarkReceiverWithContext1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runReceiverBenchmark(1000, true)
	}
}

func BenchmarkReceiverOnlyEventCh10000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runReceiverBenchmark(10000, false)
	}
}

func BenchmarkReceiverWithContext10000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runReceiverBenchmark(10000, true)
	}
}

func BenchmarkReceiverOnlyEventCh100000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runReceiverBenchmark(100000, false)
	}
}

func BenchmarkReceiverWithContext100000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runReceiverBenchmark(100000, true)
	}
}
