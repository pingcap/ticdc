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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

// regionEventSink delivers region events to dynstream and accounts their memory.
type regionEventSink struct {
	ctx context.Context
	ds  dynstream.DynamicStream[int, SubscriptionID, regionEvent, *subscribedSpan, *regionEventHandler]

	memoryQuota *memoryQuotaController
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
	if event.needsMemoryAccounting() {
		span := event.mustFirstState().region.subscribedSpan
		event.memoryBytes = uint64(event.getSize())
		if !s.memoryQuota.AcquireEvent(s.ctx, span, event.memoryBytes) {
			return
		}
	}
	s.ds.Push(subID, event)
}

func (s *regionEventSink) UpdateMetrics() {
	dsMetrics := s.ds.GetMetrics()
	metricSubscriptionClientDSChannelSize.Set(float64(dsMetrics.EventChanSize))
	metricSubscriptionClientDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
}

func (s *regionEventSink) Close() {
	s.memoryQuota.WakeAll()
	s.ds.Close()
}
