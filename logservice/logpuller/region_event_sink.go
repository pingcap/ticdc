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
	ctx context.Context
	ds  dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]

	memoryQuota *memoryQuotaController
	// the following three fields are used to manage feedback from ds and notify other goroutines
	mu     sync.Mutex
	cond   *sync.Cond
	paused atomic.Bool
}

func newRegionEventSink(
	ctx context.Context,
	failureHandler *regionFailureHandler,
	memoryQuota *memoryQuotaController,
) *regionEventSink {
	sink := &regionEventSink{ctx: ctx, memoryQuota: memoryQuota}

	option := dynstream.NewOption()
	// Note: it is max batch size of the kv sent from tikv(not committed rows)
	option.BatchCount = 1024
	// TODO: Set `UseBuffer` to true until we refactor the `regionEventHandler.Handle` method so that it doesn't call any method of the dynamic stream. Currently, if `UseBuffer` is set to false, there will be a deadlock:
	// 	ds.handleLoop fetch events from `ch` -> regionEventHandler.Handle -> ds.RemovePath -> send event to `ch`
	option.UseBuffer = true
	option.EnableMemoryControl = false
	ds := dynstream.NewParallelDynamicStream(
		"log-puller",
		&regionEventHandler{eventSink: sink, failureHandler: failureHandler},
		option,
	)
	ds.Start()
	sink.ds = ds
	sink.cond = sync.NewCond(&sink.mu)
	return sink
}

func (s *regionEventSink) AddPath(rt *subscribedSpan) {
	if err := s.ds.AddPath(rt.subID, rt); err != nil {
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
	if event.needMemoryQuota() && s.memoryQuota != nil {
		span := event.mustFirstState().region.subscribedSpan
		event.memoryQuota = s.memoryQuota.trackEvent(s.ctx, span, uint64(event.getSize()))
		if event.memoryQuota == nil {
			return
		}
	}
	// fast path
	if !s.paused.Load() {
		s.ds.Push(subID, event)
		return
	}

	// slow path: wait until paused is false
	s.mu.Lock()
	for s.paused.Load() {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			event.releaseMemoryQuota()
			return
		default:
			s.cond.Wait()
		}
	}
	s.mu.Unlock()
	s.ds.Push(subID, event)
}

func (s *regionEventSink) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case feedback := <-s.ds.Feedback():
			switch feedback.FeedbackType {
			case dynstream.PauseArea:
				s.mu.Lock()
				s.paused.Store(true)
				s.mu.Unlock()
				log.Info("subscription client pause push region event")
			case dynstream.ResumeArea:
				s.mu.Lock()
				s.paused.Store(false)
				s.cond.Broadcast()
				s.mu.Unlock()
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

	if s.memoryQuota == nil {
		return
	}

	used, capacity, _ := s.memoryQuota.snapshot()
	scanUsed, warmingScanUsed, warmingScanBudget, scanEstimate, hardLimit :=
		s.memoryQuota.scanSnapshot()
	metrics.LogPullerMemoryQuota.WithLabelValues("max").Set(float64(capacity))
	metrics.LogPullerMemoryQuota.WithLabelValues("used").Set(float64(used))
	metrics.LogPullerMemoryQuota.WithLabelValues("scan_used").Set(float64(scanUsed))
	metrics.LogPullerMemoryQuota.WithLabelValues("warming_scan_used").Set(float64(warmingScanUsed))
	metrics.LogPullerMemoryQuota.WithLabelValues("warming_scan_budget").Set(float64(warmingScanBudget))
	metrics.LogPullerMemoryQuota.WithLabelValues("scan_estimate").Set(float64(scanEstimate))
	metrics.LogPullerMemoryQuota.WithLabelValues("hard_limit").Set(float64(hardLimit))
	metrics.DynamicStreamMemoryUsage.WithLabelValues(
		"log-puller",
		"max",
		"default",
		"default",
	).Set(float64(capacity))
	metrics.DynamicStreamMemoryUsage.WithLabelValues(
		"log-puller",
		"used",
		"default",
		"default",
	).Set(float64(used))
}

func (s *regionEventSink) Close() {
	if s.memoryQuota != nil {
		s.memoryQuota.wakeAll()
	}
	s.mu.Lock()
	s.paused.Store(false)
	s.cond.Broadcast()
	s.mu.Unlock()
	s.ds.Close()
}
