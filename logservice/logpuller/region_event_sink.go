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
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

type regionEventSink struct {
	ctx context.Context
	ds  dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]

	memoryQuota *memoryQuotaController

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
	sink.cond = sync.NewCond(&sink.mu)

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
	if event.needMemoryQuota() {
		span := event.mustFirstState().region.subscribedSpan
		event.memoryQuota = s.memoryQuota.trackEvent(s.ctx, span, uint64(event.getSize()))
		if event.memoryQuota == nil && s.ctx.Err() != nil {
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
			return
		default:
			s.cond.Wait()
		}
	}
	s.mu.Unlock()
	s.ds.Push(subID, event)
}

func (s *regionEventSink) Run(ctx context.Context) error {
	return s.handleFeedback(ctx)
}

func (s *regionEventSink) handleFeedback(ctx context.Context) error {
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

func (s *regionEventSink) Metrics() dynstream.Metrics[int, SubscriptionID] {
	return s.ds.GetMetrics()
}

func (s *regionEventSink) Close() {
	s.memoryQuota.WakeAll()
	s.mu.Lock()
	s.paused.Store(false)
	s.cond.Broadcast()
	s.mu.Unlock()
	s.ds.Close()
}
