// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dynstream

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Use the existing handler's routing and instrumentation with sized data and
// recovery events, like EventCollector's DML -> DropEvent replacement.
type recoveryHandler struct {
	mockHandler
	dropHook func()
}

func (h *recoveryHandler) GetSize(e *mockEvent) int { return e.value }
func (h *recoveryHandler) GetType(e *mockEvent) EventType {
	if e.id < 0 {
		return EventType{Property: NonBatchable}
	}
	if e.id == 0 {
		return EventType{Property: PeriodicSignal, Droppable: true}
	}
	return EventType{Property: BatchableData, Droppable: true}
}

func (h *recoveryHandler) OnDrop(e *mockEvent) any {
	if h.dropHook != nil {
		h.dropHook()
	}
	return &mockEvent{id: -e.id, path: e.path, value: 32}
}

func TestReleaseRetainsRecoveryAndIdleWatermark(t *testing.T) {
	h := &recoveryHandler{}
	q := newEventQueue[int, string, *mockEvent, any](h, newTestBatchConfigRegistry())
	p := newPathInfo[int, string, *mockEvent, any, *recoveryHandler](0, "test", "a", nil)
	mc := newMemControl[int, string, *mockEvent, any, *recoveryHandler]()
	mc.addPathToArea(p, AreaSettings{maxPendingSize: 1024, algorithm: MemoryControlForEventCollector}, make(chan Feedback[int, string, any], 1))
	q.initPath(p)
	q.blockPath(p)
	for _, e := range []*mockEvent{{id: 1, value: 900}, {id: -2, value: 32}, {id: 0, value: 32}} {
		q.appendEvent(eventWrap[int, string, *mockEvent, any, *recoveryHandler]{pathInfo: p, event: e, eventSize: e.value, eventType: h.GetType(e)})
	}
	q.releasePath(p, false)
	require.True(t, p.blocking.Load(), "reclamation must not release the active barrier")
	require.Equal(t, 3, p.pendingQueue.Length(), "recovery, control and final watermark must survive")
	require.Equal(t, int64(96), p.pendingSize.Load())
	require.Equal(t, int64(96), p.areaMemStat.totalPendingSize.Load())
	require.True(t, p.reclaimed.Load())
	// Repeated release must not discard recovery notifications.
	q.releasePath(p, false)
	require.Equal(t, int64(96), p.pendingSize.Load())
	q.wakePath(p)
	b := newDefaultBatcher[*mockEvent]()
	var ids []int
	for {
		events, _, _, _ := q.popEvents(b)
		if len(events) == 0 {
			break
		}
		for _, e := range events {
			ids = append(ids, e.id)
		}
	}
	require.Equal(t, []int{-1, -2, 0}, ids)
	require.Zero(t, p.areaMemStat.totalPendingSize.Load())
	require.Zero(t, q.totalPendingLength.Load())
}

func TestPeriodicReclamationWithoutInput(t *testing.T) {
	for _, algorithm := range []int{MemoryControlForPuller, MemoryControlForEventCollector} {
		mc := newMemControl[int, string, *mockEvent, any, *mockHandler]()
		p := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "a", nil)
		feedback := make(chan Feedback[int, string, any], 1)
		mc.addPathToArea(p, AreaSettings{maxPendingSize: 1024, algorithm: algorithm}, feedback)
		area := p.areaMemStat
		p.pendingSize.Store(900)
		area.totalPendingSize.Store(900)
		p.blocking.Store(true)
		old := time.Now().Add(-time.Minute)
		area.lastAppendEventTime.Store(old)
		area.lastSizeDecreaseTime.Store(old)
		area.lastReleaseMemoryTime.Store(old)
		require.False(t, area.checkDeadlock(), "the append-driven check cannot help a silent producer")

		var wg sync.WaitGroup
		for range 16 {
			wg.Go(mc.checkMemoryPressure)
		}
		wg.Wait()
		if algorithm == MemoryControlForPuller {
			require.Empty(t, feedback)
			continue
		}
		require.Len(t, feedback, 1)
		require.Equal(t, old, area.lastSizeDecreaseTime.Load(), "a request is not actual memory progress")
		// A saturated feedback channel does not block the publisher. It retries
		// once capacity returns, without pretending that reclamation succeeded.
		area.lastReleaseMemoryTime.Store(old)
		mc.checkMemoryPressure()
		require.Len(t, feedback, 1)
		<-feedback
		area.lastReleaseMemoryTime.Store(old)
		mc.checkMemoryPressure()
		require.Len(t, feedback, 1)
	}
}

func TestLateReleaseDoesNotDiscardRunningPath(t *testing.T) {
	h := &recoveryHandler{}
	q := newEventQueue[int, string, *mockEvent, any](h, newTestBatchConfigRegistry())
	p := newPathInfo[int, string, *mockEvent, any, *recoveryHandler](0, "test", "a", nil)
	mc := newMemControl[int, string, *mockEvent, any, *recoveryHandler]()
	mc.addPathToArea(p, AreaSettings{maxPendingSize: 1024, algorithm: MemoryControlForEventCollector}, make(chan Feedback[int, string, any], 1))
	q.initPath(p)
	e := &mockEvent{id: 1, value: 900}
	q.appendEvent(eventWrap[int, string, *mockEvent, any, *recoveryHandler]{pathInfo: p, event: e, eventSize: e.value, eventType: h.GetType(e)})
	q.releasePath(p, false)
	got, _, _, _ := q.popEvents(newDefaultBatcher[*mockEvent]())
	require.Equal(t, []*mockEvent{e}, got)
	require.Zero(t, p.areaMemStat.totalPendingSize.Load())
}

func TestReclamationSkipsRetainedMarkersUntilNewData(t *testing.T) {
	h := &recoveryHandler{}
	q := newEventQueue[int, string, *mockEvent, any](h, newTestBatchConfigRegistry())
	p := newPathInfo[int, string, *mockEvent, any, *recoveryHandler](0, "test", "a", nil)
	mc := newMemControl[int, string, *mockEvent, any, *recoveryHandler]()
	feedback := make(chan Feedback[int, string, any], 1)
	mc.addPathToArea(p, AreaSettings{maxPendingSize: 1024, algorithm: MemoryControlForEventCollector}, feedback)
	q.initPath(p)
	q.blockPath(p)
	marker := &mockEvent{id: -1, value: 700}
	q.appendEvent(eventWrap[int, string, *mockEvent, any, *recoveryHandler]{pathInfo: p, event: marker, eventSize: 700, eventType: h.GetType(marker)})
	q.releasePath(p, false)
	p.areaMemStat.lastSizeDecreaseTime.Store(time.Now().Add(-time.Minute))
	mc.checkMemoryPressure()
	require.Empty(t, feedback, "no repeated recovery request for retained controls")
	e := &mockEvent{id: 1, value: 280}
	q.appendEvent(eventWrap[int, string, *mockEvent, any, *recoveryHandler]{pathInfo: p, event: e, eventSize: 280, eventType: h.GetType(e)})
	p.areaMemStat.lastReleaseMemoryTime.Store(time.Now().Add(-time.Minute))
	mc.checkMemoryPressure()
	require.Len(t, feedback, 1, "new data makes the path reclaimable again")
}

func TestReleaseFeedbackKeepsOriginalPathIdentity(t *testing.T) {
	h := &recoveryHandler{}
	ds := newParallelDynamicStream("reclamation-identity", h, Option{StreamCount: 1, EnableMemoryControl: true})
	ds.Start()
	defer ds.Close()
	settings := NewAreaSettingsWithMaxPendingSize(1024, MemoryControlForEventCollector, "test")
	require.NoError(t, ds.AddPath("a", "old", settings))
	old, ok := ds.pathMap.m["a"]
	require.True(t, ok)
	old.blocking.Store(true)
	old.pendingSize.Store(900)
	old.areaMemStat.totalPendingSize.Store(900)
	old.areaMemStat.lastSizeDecreaseTime.Store(time.Now().Add(-time.Minute))
	old.areaMemStat.lastReleaseMemoryTime.Store(time.Now().Add(-time.Minute))
	ds.CheckMemoryPressure()
	feedback := <-ds.Feedback()
	require.NotNil(t, feedback.Release)
	require.NoError(t, ds.RemovePath("a"))
	require.NoError(t, ds.AddPath("a", "new", settings))
	current, ok := ds.pathMap.m["a"]
	require.True(t, ok)
	require.NotSame(t, old, current)
	feedback.Release()
	require.False(t, current.reclaimed.Load(), "stale feedback cannot target the replacement path")
	ds.Close()
	done := make(chan struct{})
	go func() { defer close(done); feedback.Release(); ds.CheckMemoryPressure() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late reclamation blocked after close")
	}
}

func TestRemoveDuringReclamationPreservesOtherPathAccounting(t *testing.T) {
	h := &recoveryHandler{}
	q := newEventQueue[int, string, *mockEvent, any](h, newTestBatchConfigRegistry())
	mc := newMemControl[int, string, *mockEvent, any, *recoveryHandler]()
	a := newPathInfo[int, string, *mockEvent, any, *recoveryHandler](0, "test", "a", nil)
	b := newPathInfo[int, string, *mockEvent, any, *recoveryHandler](0, "test", "b", nil)
	for _, p := range []*pathInfo[int, string, *mockEvent, any, *recoveryHandler]{a, b} {
		mc.addPathToArea(p, AreaSettings{maxPendingSize: 4096, algorithm: MemoryControlForEventCollector}, make(chan Feedback[int, string, any], 1))
		q.initPath(p)
		q.blockPath(p)
		e := &mockEvent{id: 1, value: 900}
		q.appendEvent(eventWrap[int, string, *mockEvent, any, *recoveryHandler]{pathInfo: p, event: e, eventSize: 900, eventType: h.GetType(e)})
	}
	h.dropHook = func() {
		done := make(chan struct{})
		go func() { a.removed.Store(true); mc.removePathFromArea(a); close(done) }()
		<-done
	}
	q.releasePath(a, false)
	require.Zero(t, a.pendingSize.Load())
	require.Zero(t, a.pendingQueue.Length())
	require.Equal(t, int64(900), b.pendingSize.Load())
	require.Equal(t, int64(900), b.areaMemStat.totalPendingSize.Load())
}

func TestPressureDropChargesRecoveryMarkerSize(t *testing.T) {
	h := &recoveryHandler{}
	q := newEventQueue[int, string, *mockEvent, any](h, newTestBatchConfigRegistry())
	mc := newMemControl[int, string, *mockEvent, any, *recoveryHandler]()
	p := newPathInfo[int, string, *mockEvent, any, *recoveryHandler](0, "test", "a", nil)
	mc.addPathToArea(p, AreaSettings{maxPendingSize: 1024, algorithm: MemoryControlForEventCollector}, make(chan Feedback[int, string, any], 1))
	q.initPath(p)
	q.blockPath(p)
	for i := 1; i <= 3; i++ {
		e := &mockEvent{id: i, value: 900}
		q.appendEvent(eventWrap[int, string, *mockEvent, any, *recoveryHandler]{pathInfo: p, event: e, eventSize: 900, eventType: h.GetType(e)})
	}
	require.Equal(t, int64(1832), p.pendingSize.Load(), "pressure drop must charge the retained marker, not the discarded payload")
	q.releasePath(p, false)
	require.Equal(t, int64(96), p.areaMemStat.totalPendingSize.Load())
}

func TestReleaseRequestDoesNotWaitForOwnerQueue(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		h := &recoveryHandler{}
		s := newStream(0, "test", h, Option{InputChanSize: 1, UseBuffer: buffered}, newTestBatchConfigRegistry())
		p := newPathInfo[int, string, *mockEvent, any, *recoveryHandler](0, "test", "a", nil)
		mc := newMemControl[int, string, *mockEvent, any, *recoveryHandler]()
		feedback := make(chan Feedback[int, string, any], 1)
		mc.addPathToArea(p, AreaSettings{maxPendingSize: 1024, algorithm: MemoryControlForEventCollector}, feedback)
		p.stream = s
		p.blocking.Store(true)
		p.pendingSize.Store(900)
		p.areaMemStat.totalPendingSize.Store(900)
		p.areaMemStat.lastReleaseMemoryTime.Store(time.Now().Add(-time.Minute))
		p.areaMemStat.releaseMemory()
		request := <-feedback
		capacity := cap(s.eventChan)
		if buffered {
			capacity = cap(s.inChan)
		}
		for i := 0; i < capacity; i++ {
			require.True(t, request.Release())
		}
		require.False(t, request.Release(), "full owner queue must not block feedback or shutdown")
		if buffered {
			<-s.inChan
		} else {
			<-s.eventChan
		}
		require.True(t, request.Release(), "a later retry can enter the owner queue")
		s.close()
		require.False(t, request.Release())
	}
}
