// Copyright 2024 PingCAP, Inc.
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
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRegionStatesOperation(t *testing.T) {
	worker := &regionRequestWorker{}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	require.Nil(t, worker.getRegionState(1, 2))
	require.Nil(t, worker.takeRegionState(1, 2))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))
}

func TestDispatchRegionChangeEventsHandlesResolvedTs(t *testing.T) {
	client := &subscriptionClient{}
	client.metrics.batchResolvedSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test_batch_resolved_size",
		Help: "test",
	})
	mockResolved := &mockResolvedStream{}
	client.resolvedStream = mockResolved

	worker := &regionRequestWorker{
		client:       client,
		requestCache: &requestCache{},
		requestedRegions: struct {
			sync.RWMutex
			subscriptions map[SubscriptionID]regionFeedStates
		}{},
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	subID := SubscriptionID(1)
	state := newRegionFeedState(regionInfo{
		verID:            tikv.NewRegionVerID(1, 1, 1),
		subscribedSpan:   &subscribedSpan{subID: subID},
		lockedRangeState: &regionlock.LockedRangeState{},
	}, uint64(subID), worker)
	worker.addRegionState(subID, 1, state)

	event := &cdcpb.Event{
		RegionId:  1,
		RequestId: uint64(subID),
		Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 10},
	}
	worker.dispatchRegionChangeEvents([]*cdcpb.Event{event})
	require.Equal(t, 1, mockResolved.pushCount())
	last := mockResolved.lastBatch()
	require.Equal(t, uint64(10), last.resolvedTs)
	require.Equal(t, 1, len(last.states))
}

func TestDispatchResolvedTsEventBatchesRegions(t *testing.T) {
	client := &subscriptionClient{}
	client.metrics.batchResolvedSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test_batch_resolved_size_total",
		Help: "test",
	})
	mockResolved := &mockResolvedStream{}
	client.resolvedStream = mockResolved

	worker := &regionRequestWorker{
		client:       client,
		requestCache: &requestCache{},
		requestedRegions: struct {
			sync.RWMutex
			subscriptions map[SubscriptionID]regionFeedStates
		}{},
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	subID := SubscriptionID(2)
	subSpan := &subscribedSpan{subID: subID}
	for _, regionID := range []uint64{1, 2} {
		state := newRegionFeedState(regionInfo{
			verID:            tikv.NewRegionVerID(regionID, regionID, regionID),
			subscribedSpan:   subSpan,
			lockedRangeState: &regionlock.LockedRangeState{},
		}, uint64(subID), worker)
		worker.addRegionState(subID, regionID, state)
	}

	event := &cdcpb.ResolvedTs{
		Regions:   []uint64{1, 2},
		Ts:        20,
		RequestId: uint64(subID),
	}

	worker.dispatchResolvedTsEvent(event)
	require.Equal(t, 1, mockResolved.pushCount())
	last := mockResolved.lastBatch()
	require.Equal(t, uint64(20), last.resolvedTs)
	require.Equal(t, 2, len(last.states))
}

type mockResolvedStream struct {
	mu        sync.Mutex
	pushTotal int
	lastEvent batchResolvedTsEvent
}

func (m *mockResolvedStream) Start() {}
func (m *mockResolvedStream) Close() {}
func (m *mockResolvedStream) Push(path SubscriptionID, event batchResolvedTsEvent) {
	m.mu.Lock()
	m.pushTotal++
	m.lastEvent = event
	m.mu.Unlock()
}
func (m *mockResolvedStream) Wake(path SubscriptionID) {}
func (m *mockResolvedStream) Feedback() <-chan dynstream.Feedback[int, SubscriptionID, *subscribedSpan] {
	return nil
}
func (m *mockResolvedStream) AddPath(path SubscriptionID, dest *subscribedSpan, area ...dynstream.AreaSettings) error {
	return nil
}
func (m *mockResolvedStream) RemovePath(path SubscriptionID) error { return nil }
func (m *mockResolvedStream) Release(path SubscriptionID)          {}
func (m *mockResolvedStream) SetAreaSettings(area int, settings dynstream.AreaSettings) {
}
func (m *mockResolvedStream) GetMetrics() dynstream.Metrics[int, SubscriptionID] {
	return dynstream.Metrics[int, SubscriptionID]{}
}

func (m *mockResolvedStream) pushCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pushTotal
}

func (m *mockResolvedStream) lastBatch() batchResolvedTsEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastEvent
}
