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
	"testing"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type mockRegionEventSinkStream struct {
	eventCh chan regionEvent
	metrics dynstream.Metrics[int, SubscriptionID]
}

func newMockRegionEventSinkStream() *mockRegionEventSinkStream {
	return &mockRegionEventSinkStream{
		eventCh: make(chan regionEvent, 1),
	}
}

func (s *mockRegionEventSinkStream) Start() {}

func (s *mockRegionEventSinkStream) Close() {}

func (s *mockRegionEventSinkStream) Push(_ SubscriptionID, event regionEvent) {
	s.eventCh <- event
}

func (s *mockRegionEventSinkStream) Wake(_ SubscriptionID) {}

func (s *mockRegionEventSinkStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return nil
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

func TestRegionEventSinkUpdateMetrics(t *testing.T) {
	t.Run("quota updates memory gauges", func(t *testing.T) {
		ds := newMockRegionEventSinkStream()
		ds.metrics = dynstream.Metrics[int, SubscriptionID]{
			EventChanSize:   33,
			PendingQueueLen: 44,
		}
		quota := newMemoryQuotaController(66, 8)
		span := newTestQuotaSpan(1)
		require.True(t, quota.acquireEvent(context.Background(), span, 55))
		t.Cleanup(func() { quota.releaseEvent(55) })

		sink := &regionEventSink{
			ctx:         context.Background(),
			ds:          ds,
			memoryQuota: quota,
		}
		sink.UpdateMetrics()

		require.Equal(t, float64(33), testutil.ToFloat64(metricSubscriptionClientDSChannelSize))
		require.Equal(t, float64(44), testutil.ToFloat64(metricSubscriptionClientDSPendingQueueLen))
		require.Equal(t, float64(66), testutil.ToFloat64(
			metrics.LogPullerMemoryQuota.WithLabelValues("max")))
		require.Equal(t, float64(55), testutil.ToFloat64(
			metrics.LogPullerMemoryQuota.WithLabelValues("used")))
	})
}

func TestRegionEventSinkTracksEntriesUntilDrop(t *testing.T) {
	quota := newMemoryQuotaController(1024, 8)
	span := newTestQuotaSpan(1)
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

	sink.Push(span.subID, regionEvent{
		states: []*regionFeedState{state},
		entries: &cdcpb.Event_Entries_{Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{Key: []byte("key"), Value: []byte("value")}},
		}},
	})
	pushed := <-ds.eventCh
	require.NotZero(t, pushed.memoryBytes)
	used, _, _ := quota.snapshot()
	require.NotZero(t, used)

	(&regionEventHandler{eventSink: sink}).OnDrop(pushed)
	used, _, _ = quota.snapshot()
	require.Zero(t, used)
}
