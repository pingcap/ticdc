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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type mockRegionEventSinkStream struct {
	feedbackCh chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan]
	pushCount  atomic.Int32
	pushCh     chan struct{}
	metrics    dynstream.Metrics[int, SubscriptionID]
}

func newMockRegionEventSinkStream() *mockRegionEventSinkStream {
	return &mockRegionEventSinkStream{
		feedbackCh: make(chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan], 2),
		pushCh:     make(chan struct{}, 1),
	}
}

func (s *mockRegionEventSinkStream) Start() {}

func (s *mockRegionEventSinkStream) Close() {}

func (s *mockRegionEventSinkStream) Push(_ SubscriptionID, _ regionEvent) {
	s.pushCount.Add(1)
	s.pushCh <- struct{}{}
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
		ds:     ds,
		stopCh: make(chan struct{}),
	}

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

<<<<<<< HEAD
func TestRegionEventSinkUpdateMetrics(t *testing.T) {
	t.Run("empty area metrics returns after queue gauges", func(t *testing.T) {
		ds := newMockRegionEventSinkStream()
		ds.metrics = dynstream.Metrics[int, SubscriptionID]{
			EventChanSize:   11,
			PendingQueueLen: 22,
		}

		metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"max",
			"default",
			"default",
		).Set(123)
		metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"used",
			"default",
			"default",
		).Set(456)

		sink := &regionEventSink{
			ds:     ds,
			stopCh: make(chan struct{}),
		}
		sink.UpdateMetrics()

		require.Equal(t, float64(11), testutil.ToFloat64(metricSubscriptionClientDSChannelSize))
		require.Equal(t, float64(22), testutil.ToFloat64(metricSubscriptionClientDSPendingQueueLen))
		require.Equal(t, float64(123), testutil.ToFloat64(metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"max",
			"default",
			"default",
		)))
		require.Equal(t, float64(456), testutil.ToFloat64(metrics.DynamicStreamMemoryUsage.WithLabelValues(
			"log-puller",
			"used",
			"default",
			"default",
		)))
	})

	t.Run("single area metrics updates memory gauges", func(t *testing.T) {
		ds := newMockRegionEventSinkStream()
		ds.metrics = dynstream.Metrics[int, SubscriptionID]{
			EventChanSize:   33,
			PendingQueueLen: 44,
			MemoryControl: dynstream.MemoryMetric[int, SubscriptionID]{
				AreaMemoryMetrics: []dynstream.AreaMemoryMetric[int, SubscriptionID]{
					{
						UsedMemoryValue:    55,
						MaxMemoryValue:     66,
						PathMaxMemoryValue: 66,
					},
				},
			},
		}

		sink := &regionEventSink{
			ds:     ds,
			stopCh: make(chan struct{}),
		}
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
	})
}

func TestRegionEventSinkRunCancelUnblocksPush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ds := newMockRegionEventSinkStream()
	sink := &regionEventSink{
		ds:     ds,
		stopCh: make(chan struct{}),
	}

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

	cancel()

	select {
	case <-pushDone:
	case <-time.After(time.Second):
		t.Fatal("Push should be unblocked by Run context cancellation")
	}
	require.Equal(t, int32(0), ds.pushCount.Load())

	select {
	case err := <-runErrCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run should exit after context cancellation")
	}
}
