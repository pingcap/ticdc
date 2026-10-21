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
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
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
	ds := newMockRegionEventSinkStream()
	ds.metrics = dynstream.Metrics[int, SubscriptionID]{
		EventChanSize:   33,
		PendingQueueLen: 44,
	}
	sink := &regionEventSink{ds: ds}

	sink.UpdateMetrics()

	require.Equal(t, float64(33), testutil.ToFloat64(metricSubscriptionClientDSChannelSize))
	require.Equal(t, float64(44), testutil.ToFloat64(metricSubscriptionClientDSPendingQueueLen))
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
	quotaState := getMemoryQuotaTestState(quota)
	require.NotZero(t, quotaState.used)

	(&regionEventHandler{eventSink: sink}).OnDrop(pushed)
	quotaState = getMemoryQuotaTestState(quota)
	require.Zero(t, quotaState.used)
}

func TestRegionEventSinkRemovePathReleasesQueuedEventMemory(t *testing.T) {
	quota := newMemoryQuotaController(1024*1024, 8)
	sink := newRegionEventSink(context.Background(), nil, quota)
	defer sink.Close()

	span := newTestQuotaSpan(1)
	span.resolvedTs.Store(100)
	callbackCh := make(chan func(), 1)
	span.consumeKVEvents = func(_ []common.RawKVEntry, callback func()) bool {
		callbackCh <- callback
		return true
	}
	span.advanceResolvedTs = func(uint64) {}
	sink.AddPath(span)

	worker := &regionRequestWorker{}
	region := newTestQuotaRegion(span)
	region.rpcCtx = &tikv.RPCContext{}
	state := newRegionFeedState(
		region,
		uint64(span.subID),
		worker,
		nil,
	)
	newEvent := func(commitTs uint64) regionEvent {
		return regionEvent{
			states: []*regionFeedState{state},
			entries: &cdcpb.Event_Entries_{Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					Type:     cdcpb.Event_COMMITTED,
					OpType:   cdcpb.Event_Row_PUT,
					CommitTs: commitTs,
				}},
			}},
		}
	}

	sink.Push(span.subID, newEvent(101))
	callback := <-callbackCh
	firstEventUsed := getMemoryQuotaTestState(quota).used
	require.NotZero(t, firstEventUsed)

	// The first event blocks the path until callback is invoked, so this event
	// remains queued when the path is removed.
	sink.Push(span.subID, newEvent(102))
	require.Greater(t, getMemoryQuotaTestState(quota).used, firstEventUsed)
	require.NoError(t, sink.RemovePath(span.subID))

	callback()
	require.Eventually(t, func() bool {
		return getMemoryQuotaTestState(quota).used == 0
	}, time.Second, 10*time.Millisecond)
}
