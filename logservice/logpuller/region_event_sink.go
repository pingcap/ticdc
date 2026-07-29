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
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

// regionEventSink delivers region events to dynstream and owns push-side flow control.
type regionEventSink struct {
	// mu/cond coordinate the paused push path with pause/resume and shutdown signals.
	mu   sync.Mutex
	cond *sync.Cond
	// paused tracks whether region event pushing is temporarily held back by feedback.
	paused atomic.Bool
	// stopped marks the sink as shutting down so blocked pushers can exit instead of waiting for resume.
	stopped atomic.Bool

	// ds owns the dynstream used to deliver region events and receive flow-control feedback.
	ds dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]
}

func newRegionEventSink(failureHandler *regionFailureHandler) *regionEventSink {
	sink := &regionEventSink{}
	sink.cond = sync.NewCond(&sink.mu)

	option := dynstream.NewOption()
	// Note: it is max batch size of the kv sent from tikv(not committed rows)
	option.BatchCount = 1024
	// TODO: Set `UseBuffer` to true until we refactor the `regionEventHandler.Handle` method so that it doesn't call any method of the dynamic stream. Currently, if `UseBuffer` is set to false, there will be a deadlock:
	// 	ds.handleLoop fetch events from `ch` -> regionEventHandler.Handle -> ds.RemovePath -> send event to `ch`
	option.UseBuffer = true
	option.EnableMemoryControl = true
	ds := dynstream.NewParallelDynamicStream(
		"log-puller",
		&regionEventHandler{eventSink: sink, failureHandler: failureHandler},
		option,
	)
	ds.Start()
	sink.ds = ds
	return sink
}

func (s *regionEventSink) AddPath(rt *subscribedSpan) {
	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(1*1024*1024*1024, dynstream.MemoryControlForPuller, "logPuller") // 1GB
	if err := s.ds.AddPath(rt.subID, rt, areaSetting); err != nil {
		log.Warn("subscription client add path failed",
			zap.Uint64("subscriptionID", uint64(rt.subID)),
			zap.Error(err))
	}
}

func (s *regionEventSink) RemovePath(subID SubscriptionID) error {
	return s.ds.RemovePath(subID)
}

func (s *regionEventSink) Wake(subID SubscriptionID) {
	s.ds.Wake(subID)
}

func (s *regionEventSink) Push(subID SubscriptionID, event regionEvent) {
	if s.stopped.Load() {
		return
	}
	// fast path
	if !s.paused.Load() {
		s.ds.Push(subID, event)
		return
	}

	// slow path: wait until paused is false
	s.mu.Lock()
	for s.paused.Load() && !s.stopped.Load() {
		s.cond.Wait()
	}
	stopped := s.stopped.Load()
	s.mu.Unlock()

	if stopped {
		return
	}
	s.ds.Push(subID, event)
}

func (s *regionEventSink) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			s.stop()
			return nil
		case feedback := <-s.ds.Feedback():
			switch feedback.FeedbackType {
			case dynstream.PauseArea:
				s.pause()
				log.Info("subscription client pause push region event")
			case dynstream.ResumeArea:
				s.resume()
				log.Info("subscription client resume push region event")
			case dynstream.ReleasePath, dynstream.ResumePath:
				// Ignore it, because it is no need to pause and resume a path in puller.
			}
		}
	}
}

func (s *regionEventSink) UpdateMetrics() {
	dsMetrics := s.ds.GetMetrics()
	metricSubscriptionClientDSChannelSize.Set(float64(dsMetrics.EventChanSize))
	metricSubscriptionClientDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))

	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) == 0 {
		return
	}
	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) != 1 {
		log.Warn("subscription client should have exactly one area")
		return
	}

	areaMetric := dsMetrics.MemoryControl.AreaMemoryMetrics[0]
	metrics.DynamicStreamMemoryUsage.WithLabelValues(
		"log-puller",
		"max",
		"default",
		"default",
	).Set(float64(areaMetric.MaxMemory()))
	metrics.DynamicStreamMemoryUsage.WithLabelValues(
		"log-puller",
		"used",
		"default",
		"default",
	).Set(float64(areaMetric.MemoryUsage()))
}

func (s *regionEventSink) Close() {
	s.stop()
	s.ds.Close()
}

func (s *regionEventSink) pause() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped.Load() || s.paused.Load() {
		return
	}
	s.paused.Store(true)
}

func (s *regionEventSink) resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.paused.Load() {
		return
	}
	s.paused.Store(false)
	s.cond.Broadcast()
}

func (s *regionEventSink) stop() {
	if !s.stopped.CompareAndSwap(false, true) {
		return
	}
	s.mu.Lock()
	s.paused.Store(false)
	s.cond.Broadcast()
	s.mu.Unlock()
}
