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

type regionEventSink struct {
	ctx context.Context
	ds  dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]
	// the following three fields are used to manage feedback from ds and notify other goroutines
	mu     sync.Mutex
	cond   *sync.Cond
	paused atomic.Bool
}

func newRegionEventSink(ctx context.Context, subClient *subscriptionClient) *regionEventSink {
	sink := &regionEventSink{ctx: ctx}

	option := dynstream.NewOption()
	// Note: it is max batch size of the kv sent from tikv(not committed rows)
	option.BatchCount = 1024
	// TODO: Set `UseBuffer` to true until we refactor the `regionEventHandler.Handle` method so that it doesn't call any method of the dynamic stream. Currently, if `UseBuffer` is set to false, there will be a deadlock:
	// 	ds.handleLoop fetch events from `ch` -> regionEventHandler.Handle -> ds.RemovePath -> send event to `ch`
	option.UseBuffer = true
	option.EnableMemoryControl = true
	ds := dynstream.NewParallelDynamicStream(
		"log-puller",
		&regionEventHandler{subClient: subClient},
		option,
	)
	ds.Start()
	sink.ds = ds
	sink.cond = sync.NewCond(&sink.mu)
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
	// fast path
	if !s.paused.Load() {
		s.ds.Push(subID, event)
		return
	}

	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// slow path: wait until paused is false
	s.mu.Lock()
	for s.paused.Load() {
		select {
		case <-ctx.Done():
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
	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 1 {
		log.Panic("subscription client should have only one area")
	}
	if len(dsMetrics.MemoryControl.AreaMemoryMetrics) == 0 {
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
	s.mu.Lock()
	s.paused.Store(false)
	s.cond.Broadcast()
	s.mu.Unlock()
	s.ds.Close()
}
