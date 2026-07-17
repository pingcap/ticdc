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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type mockRegionEventSinkStream struct {
	feedbackCh chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan]
	pushCount  atomic.Int32
	pushCh     chan struct{}
	eventCh    chan regionEvent
	metrics    dynstream.Metrics[int, SubscriptionID]
}

func newMockRegionEventSinkStream() *mockRegionEventSinkStream {
	return &mockRegionEventSinkStream{
		feedbackCh: make(chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan], 2),
		pushCh:     make(chan struct{}, 1),
		eventCh:    make(chan regionEvent, 1),
	}
}

func (s *mockRegionEventSinkStream) Start() {}

func (s *mockRegionEventSinkStream) Close() {}

func (s *mockRegionEventSinkStream) Push(_ SubscriptionID, event regionEvent) {
	s.pushCount.Add(1)
	s.pushCh <- struct{}{}
	s.eventCh <- event
}

func (s *mockRegionEventSinkStream) Wake(_ SubscriptionID) {}

func (s *mockRegionEventSinkStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return s.feedbackCh
}

func (s *mockRegionEventSinkStream) AddPath(_ SubscriptionID, _ *subscribedSpan, _ ...dynstream.AreaSettings) error {
	return nil
}

func (s *mockRegionEventSinkStream) RemovePath(_ SubscriptionID) error {
	return nil
}

func (s *mockRegionEventSinkStream) Release(_ SubscriptionID) {}

func (s *mockRegionEventSinkStream) SetAreaSettings(_ int, _ dynstream.AreaSettings) {}

func (s *mockRegionEventSinkStream) GetMetrics() dynstream.Metrics[int, SubscriptionID] {
	return s.metrics
}

func TestRegionEventSinkRunPausesAndResumesPush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds := newMockRegionEventSinkStream()
	sink := &regionEventSink{
		ctx:         ctx,
		ds:          ds,
		memoryQuota: newMemoryQuotaController(0, 0),
	}
	sink.cond = sync.NewCond(&sink.mu)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- sink.Run(ctx)
	}()

	ds.feedbackCh <- dynstream.Feedback[int, SubscriptionID, *subscribedSpan]{
		FeedbackType: dynstream.PauseArea,
	}
	require.Eventually(t, sink.paused.Load, time.Second, 10*time.Millisecond)

	pushDone := make(chan struct{})
	go func() {
		sink.Push(SubscriptionID(1), regionEvent{resolvedTs: 100})
		close(pushDone)
	}()

	select {
	case <-pushDone:
		t.Fatal("Push should block while the sink is paused")
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, int32(0), ds.pushCount.Load())

	ds.feedbackCh <- dynstream.Feedback[int, SubscriptionID, *subscribedSpan]{
		FeedbackType: dynstream.ResumeArea,
	}
	require.Eventually(t, func() bool { return !sink.paused.Load() }, time.Second, 10*time.Millisecond)

	select {
	case <-ds.pushCh:
	case <-time.After(time.Second):
		t.Fatal("Push should resume after ResumeArea feedback")
	}
	select {
	case <-pushDone:
	case <-time.After(time.Second):
		t.Fatal("Push should return after ResumeArea feedback")
	}

	cancel()
	select {
	case err := <-runErrCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run should exit after context cancellation")
	}
}

func TestRegionEventSinkUpdateMetrics(t *testing.T) {
	t.Run("quota updates memory gauges", func(t *testing.T) {
		ds := newMockRegionEventSinkStream()
		ds.metrics = dynstream.Metrics[int, SubscriptionID]{
			EventChanSize:   33,
			PendingQueueLen: 44,
		}
		quota := newMemoryQuotaController(66, 8)
		span := newTestQuotaSpan(1)
		quota.addSubscription(span)
		lease := quota.trackEvent(context.Background(), span, 55)
		require.NotNil(t, lease)
		t.Cleanup(lease.Release)

		sink := &regionEventSink{
			ctx:         context.Background(),
			ds:          ds,
			memoryQuota: quota,
		}
		sink.cond = sync.NewCond(&sink.mu)
		sink.UpdateMetrics()

		require.Equal(t, float64(33), testutil.ToFloat64(metricSubscriptionClientDSChannelSize))
		require.Equal(t, float64(44), testutil.ToFloat64(metricSubscriptionClientDSPendingQueueLen))
		require.Equal(t, float64(66), testutil.ToFloat64(metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"max",
			"default",
			"default",
		)))
		require.Equal(t, float64(55), testutil.ToFloat64(metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"used",
			"default",
			"default",
		)))
		require.Equal(t, float64(66), testutil.ToFloat64(
			metrics.LogPullerMemoryQuota.WithLabelValues("max")))
		require.Equal(t, float64(55), testutil.ToFloat64(
			metrics.LogPullerMemoryQuota.WithLabelValues("used")))
	})
}

func TestRegionEventSinkTracksEntriesUntilDrop(t *testing.T) {
	quota := newMemoryQuotaController(1024, 8)
	span := newTestQuotaSpan(1)
	quota.addSubscription(span)
	state := &regionFeedState{
		region: regionInfo{subscribedSpan: span},
		worker: &regionRequestWorker{},
	}
	ds := newMockRegionEventSinkStream()
	sink := &regionEventSink{
		ctx:         context.Background(),
		ds:          ds,
		memoryQuota: quota,
	}
	sink.cond = sync.NewCond(&sink.mu)

	sink.Push(span.subID, regionEvent{
		states: []*regionFeedState{state},
		entries: &cdcpb.Event_Entries_{Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{Key: []byte("key"), Value: []byte("value")}},
		}},
	})
	pushed := <-ds.eventCh
	require.NotNil(t, pushed.memoryQuota)
	used, _, _ := quota.snapshot()
	require.NotZero(t, used)

	(&regionEventHandler{}).OnDrop(pushed)
	used, _, _ = quota.snapshot()
	require.Zero(t, used)
}
