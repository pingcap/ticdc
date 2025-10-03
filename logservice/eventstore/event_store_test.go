// Copyright 2025 PingCAP, Inc.
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

package eventstore

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

type mockSubscriptionStat struct {
	span    heartbeatpb.TableSpan
	startTs uint64
}

type mockSubscriptionClient struct {
	nextID        atomic.Uint64
	mu            sync.Mutex
	subscriptions map[logpuller.SubscriptionID]*mockSubscriptionStat
}

func NewMockSubscriptionClient() logpuller.SubscriptionClient {
	return &mockSubscriptionClient{
		subscriptions: make(map[logpuller.SubscriptionID]*mockSubscriptionStat),
	}
}

func (s *mockSubscriptionClient) Name() string {
	return "mockSubscriptionClient"
}

func (s *mockSubscriptionClient) Run(ctx context.Context) error {
	return nil
}

func (s *mockSubscriptionClient) Close(ctx context.Context) error {
	return nil
}

func (s *mockSubscriptionClient) AllocSubscriptionID() logpuller.SubscriptionID {
	nextID := s.nextID.Add(1)
	return logpuller.SubscriptionID(nextID)
}

func (s *mockSubscriptionClient) Subscribe(
	subID logpuller.SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	bdrMode bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[subID] = &mockSubscriptionStat{
		span:    span,
		startTs: startTs,
	}
}

func (s *mockSubscriptionClient) Unsubscribe(subID logpuller.SubscriptionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, subID)
}

func newEventStoreForTest(path string) (logpuller.SubscriptionClient, EventStore) {
	ctx := context.Background()
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	subClient := NewMockSubscriptionClient()
	store := New(ctx, path, subClient)
	return subClient, store
}

func TestEventStoreInteractionWithSubClient(t *testing.T) {
	subClient, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))
	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	dispatcherID3 := common.NewDispatcherID()
	cfID := common.NewChangefeedID4Test("default", "test-cf")

	{
		span := &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("e"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID1, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// add a dispatcher with the same span
	{
		span := &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("e"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID2, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// check there is only one subscription in subClient
	{
		mockSubClient := subClient.(*mockSubscriptionClient)
		mockSubClient.mu.Lock()
		require.Equal(t, 1, len(mockSubClient.subscriptions))
		mockSubClient.mu.Unlock()
	}
	// add a dispatcher with a containing span
	{
		span := &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID3, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// check a new subscription is created in subClient
	{
		mockSubClient := subClient.(*mockSubscriptionClient)
		mockSubClient.mu.Lock()
		require.Equal(t, 2, len(mockSubClient.subscriptions))
		mockSubClient.mu.Unlock()
	}
}

func TestEventStoreOnlyReuseDispatcher(t *testing.T) {
	_, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))

	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	dispatcherID3 := common.NewDispatcherID()
	tableID := int64(1)
	cfID := common.NewChangefeedID4Test("default", "test-cf")
	// add a dispatcher to create a subscription
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID1, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// add a dispatcher(onlyReuse=true) with a non-containing span which should fail
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("b"),
			EndKey:   []byte("i"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID2, span, 100, func(watermark uint64, latestCommitTs uint64) {}, true, false)
		require.False(t, ok)
	}
	// add a dispatcher(onlyReuse=true) with a containing span which should success
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("b"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID3, span, 100, func(watermark uint64, latestCommitTs uint64) {}, true, false)
		require.True(t, ok)
	}
	{
		store.UnregisterDispatcher(cfID, dispatcherID1)
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		require.Equal(t, 1, len(subStats))
		// because there is only one subStat, we know its subID is 1
		subStat := subStats[logpuller.SubscriptionID(1)]
		require.NotNil(t, subStat)
		subData := subStat.subscribers.Load()
		require.NotNil(t, subData)
		require.Equal(t, 1, len(subData.subscribers))
		require.Equal(t, int64(0), subData.idleTime)
		store.UnregisterDispatcher(cfID, dispatcherID3)
		subData = subStat.subscribers.Load()
		require.NotNil(t, subData)
		require.Equal(t, 0, len(subData.subscribers))
		require.NotEqual(t, int64(0), subData.idleTime)
	}
}

func TestEventStoreOnlyReuseDispatcherSuccess(t *testing.T) {
	_, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))
	es := store.(*eventStore)

	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	dispatcherID3 := common.NewDispatcherID()
	tableID := int64(1)
	cfID := common.NewChangefeedID4Test("default", "test-cf")

	// 1. Register a dispatcher to create a large subscription.
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		}
		ok := es.RegisterDispatcher(cfID, dispatcherID1, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}

	// 2. Register a second dispatcher with onlyReuse=true, whose span is contained
	//    by the first subscription. This registration should succeed.
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("b"),
			EndKey:   []byte("y"),
		}
		ok := es.RegisterDispatcher(cfID, dispatcherID2, span, 100, func(watermark uint64, latestCommitTs uint64) {}, true, false)
		require.True(t, ok)
	}

	// 3. Register a third dispatcher with onlyReuse=true, whose span is an exact match
	//    to the first subscription. This registration should also succeed.
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		}
		ok := es.RegisterDispatcher(cfID, dispatcherID3, span, 100, func(watermark uint64, latestCommitTs uint64) {}, true, false)
		require.True(t, ok)
	}

	// 4. Verify that dispatcherID2 and dispatcherID3 are included in the changefeedStat, confirming
	//    that the defer cleanup logic was not incorrectly triggered.
	v, ok := es.changefeedMeta.Load(cfID)
	require.True(t, ok)
	cfStat := v.(*changefeedStat)
	cfStat.mutex.Lock()
	defer cfStat.mutex.Unlock()
	require.Len(t, cfStat.dispatchers, 3, "dispatcher2 and dispatcher3 should be registered successfully")
	_, dispatcher2Exists := cfStat.dispatchers[dispatcherID2]
	require.True(t, dispatcher2Exists, "dispatcher2 should exist in changefeedStat")
	_, dispatcher3Exists := cfStat.dispatchers[dispatcherID3]
	require.True(t, dispatcher3Exists, "dispatcher3 should exist in changefeedStat")
}

func TestEventStoreNonOnlyReuseDispatcher(t *testing.T) {
	_, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))

	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	dispatcherID3 := common.NewDispatcherID()
	dispatcherID4 := common.NewDispatcherID()
	tableID := int64(1)
	cfID := common.NewChangefeedID4Test("default", "test-cf")
	// add a subscription to create a subscription
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID1, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// add a dispatcher(onlyReuse=false) with a non-containing span
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("c"),
			EndKey:   []byte("i"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID2, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// do some check
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		// 2 = 1 dispatcher for dispatcherID1 + 1 dispatcher for dispatcherID2
		require.Equal(t, 2, len(subStats))
	}
	// add a dispatcher(onlyReuse=false) with a containing span
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("b"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID3, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// do some check
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		// 3 = 1 dispatcher for dispatcherID1 + 1 dispatcher for dispatcherID2 + 1 dispatcher for dispatcherID3
		require.Equal(t, 3, len(subStats))
		// subStat with subID 1 should have two dispatchers
		subStat := subStats[logpuller.SubscriptionID(1)]
		require.NotNil(t, subStat)
		subData := subStat.subscribers.Load()
		require.NotNil(t, subData)
		require.Equal(t, 2, len(subData.subscribers))
	}
	// add a dispatcher(onlyReuse=false) with the same span
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID4, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		require.Equal(t, 3, len(subStats))
		// subStat with subID 1 should have three dispatchers
		subStat := subStats[logpuller.SubscriptionID(1)]
		require.NotNil(t, subStat)
		subData := subStat.subscribers.Load()
		require.NotNil(t, subData)
		require.Equal(t, 3, len(subData.subscribers))
	}
	// test unregister dispatcherID3 can remove its dependency on two subscriptions
	{
		store.UnregisterDispatcher(cfID, dispatcherID3)
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		require.Equal(t, 3, len(subStats))
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			subData := subStat.subscribers.Load()
			require.NotNil(t, subData)
			require.Equal(t, 2, len(subData.subscribers))
		}
		{
			subStat := subStats[logpuller.SubscriptionID(3)]
			require.NotNil(t, subStat)
			subData := subStat.subscribers.Load()
			require.NotNil(t, subData)
			require.Equal(t, 0, len(subData.subscribers))
		}
	}
}

func TestEventStoreUpdateCheckpointTs(t *testing.T) {
	_, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))

	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	tableID := int64(1)
	cfID := common.NewChangefeedID4Test("default", "test-cf")
	// add first dispatcher
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID1, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// add a dispatcher(onlyReuse=false) with a containing span
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("b"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID2, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// check subStat checkpointTs cannot advance when their resolved ts is not advanced
	{
		store.UpdateDispatcherCheckpointTs(dispatcherID1, 110)
		store.UpdateDispatcherCheckpointTs(dispatcherID2, 120)
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			require.Equal(t, uint64(100), subStat.checkpointTs.Load())
		}
		{
			subStat := subStats[logpuller.SubscriptionID(2)]
			require.NotNil(t, subStat)
			require.Equal(t, uint64(100), subStat.checkpointTs.Load())
		}
	}
	// update subStat resolvedTs
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			subStat.resolvedTs.Store(200)
		}
		{
			subStat := subStats[logpuller.SubscriptionID(2)]
			require.NotNil(t, subStat)
			subStat.resolvedTs.Store(300)
		}
	}
	// check subStat checkpointTs can advance normally
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		store.UpdateDispatcherCheckpointTs(dispatcherID1, 130)
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			require.Equal(t, uint64(120), subStat.checkpointTs.Load())
		}
		store.UpdateDispatcherCheckpointTs(dispatcherID2, 140)
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			require.Equal(t, uint64(130), subStat.checkpointTs.Load())
		}
		{
			subStat := subStats[logpuller.SubscriptionID(2)]
			require.NotNil(t, subStat)
			require.Equal(t, uint64(140), subStat.checkpointTs.Load())
		}
	}
}

func TestEventStoreSwitchSubStat(t *testing.T) {
	_, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))

	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	tableID := int64(1)
	cfID := common.NewChangefeedID4Test("default", "test-cf")

	updateSubStatResolvedTs := func(subID logpuller.SubscriptionID, ts uint64) {
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		subStat := subStats[subID]
		require.NotNil(t, subStat)
		subStat.resolvedTs.Store(ts)
	}
	// ============ prepare two subscriptions ============
	// add a dispatcher to create an subscription
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID1, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}
	// add a dispatcher(onlyReuse=false) with a containing span
	// it will reuse the first subscription and create a new one
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("b"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID2, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
	}

	// =========== check two subscriptions are created ============
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		require.Equal(t, 2, len(subStats))
	}

	// case 1: dispatcher 2 use data from subStat 1
	updateSubStatResolvedTs(1, 200)
	{
		iter := store.GetIterator(dispatcherID2, common.DataRange{
			Span: &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: []byte("b"),
				EndKey:   []byte("h"),
			},
			CommitTsStart: 100,
			CommitTsEnd:   150,
		})
		iterImpl := iter.(*eventStoreIter)
		require.True(t, iterImpl.needCheckSpan)
	}

	// case 2: subStat 2 is ready, dispatcher 2 read data from subStat 2 and stop listen subStat 1
	updateSubStatResolvedTs(2, 200)
	{
		iter := store.GetIterator(dispatcherID2, common.DataRange{
			Span: &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: []byte("b"),
				EndKey:   []byte("h"),
			},
			CommitTsStart: 100,
			CommitTsEnd:   150,
		})
		iterImpl := iter.(*eventStoreIter)
		require.False(t, iterImpl.needCheckSpan)
	}
	// check dispatcher 2 is no longer receive event from subStat 1
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			subData := subStat.subscribers.Load()
			require.NotNil(t, subData)
			require.Equal(t, 2, len(subData.subscribers))
			require.Equal(t, true, subData.subscribers[dispatcherID2].isStopped)
		}
	}

	// case 3: subStat 1 advance quicker than subStat 2, dispatcher 2 can still read data from subStat 1
	updateSubStatResolvedTs(1, 220)
	{
		iter := store.GetIterator(dispatcherID2, common.DataRange{
			Span: &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: []byte("b"),
				EndKey:   []byte("h"),
			},
			CommitTsStart: 100,
			CommitTsEnd:   220,
		})
		iterImpl := iter.(*eventStoreIter)
		require.True(t, iterImpl.needCheckSpan)
	}
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			subData := subStat.subscribers.Load()
			require.NotNil(t, subData)
			require.Equal(t, 2, len(subData.subscribers))
			require.Equal(t, true, subData.subscribers[dispatcherID2].isStopped)
		}
	}
	{
		dispatcherStat := store.(*eventStore).dispatcherMeta.dispatcherStats[dispatcherID2]
		require.NotNil(t, dispatcherStat)
		require.Equal(t, logpuller.SubscriptionID(2), dispatcherStat.subStat.subID)
		require.Equal(t, logpuller.SubscriptionID(1), dispatcherStat.removingSubStat.subID)
	}

	// case 4: subStat 2 advance quicker or the same as subStat 1,
	// dispatcher 2 read data from subStat 2 and totally remove itself from the subsriber list of subStat 1
	updateSubStatResolvedTs(2, 220)
	{
		iter := store.GetIterator(dispatcherID2, common.DataRange{
			Span: &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: []byte("b"),
				EndKey:   []byte("h"),
			},
			CommitTsStart: 100,
			CommitTsEnd:   220,
		})
		iterImpl := iter.(*eventStoreIter)
		require.False(t, iterImpl.needCheckSpan)
	}
	{
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			subData := subStat.subscribers.Load()
			require.NotNil(t, subData)
			require.Equal(t, 1, len(subData.subscribers))
		}
	}
	{
		dispatcherStat := store.(*eventStore).dispatcherMeta.dispatcherStats[dispatcherID2]
		require.NotNil(t, dispatcherStat)
		require.Equal(t, logpuller.SubscriptionID(2), dispatcherStat.subStat.subID)
		require.Nil(t, dispatcherStat.removingSubStat)
	}
}

func TestChangefeedStatManagement(t *testing.T) {
	_, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))
	es := store.(*eventStore)

	cfID1 := common.NewChangefeedID4Test("default", "test-cf-1")
	cfID2 := common.NewChangefeedID4Test("default", "test-cf-2")

	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	dispatcherID3 := common.NewDispatcherID()

	span1 := &heartbeatpb.TableSpan{TableID: 1}
	span2 := &heartbeatpb.TableSpan{TableID: 2}
	span3 := &heartbeatpb.TableSpan{TableID: 3}

	// 1. Register dispatcher1 for cfID1.
	ok := es.RegisterDispatcher(cfID1, dispatcherID1, span1, 100, func(u uint64, u2 uint64) {}, false, false)
	require.True(t, ok)

	// Check if changefeedStat for cfID1 is created.
	v, ok := es.changefeedMeta.Load(cfID1)
	require.True(t, ok)
	cfStat1 := v.(*changefeedStat)
	require.NotNil(t, cfStat1)
	cfStat1.mutex.Lock()
	require.Len(t, cfStat1.dispatchers, 1)
	_, ok = cfStat1.dispatchers[dispatcherID1]
	cfStat1.mutex.Unlock()
	require.True(t, ok)

	// 2. Register dispatcher2 for cfID1.
	ok = es.RegisterDispatcher(cfID1, dispatcherID2, span2, 110, func(u uint64, u2 uint64) {}, false, false)
	require.True(t, ok)

	// Check if dispatcher2 is added to the same changefeedStat.
	v, ok = es.changefeedMeta.Load(cfID1)
	require.True(t, ok)
	cfStat1 = v.(*changefeedStat)
	cfStat1.mutex.Lock()
	require.Len(t, cfStat1.dispatchers, 2)
	_, ok = cfStat1.dispatchers[dispatcherID2]
	cfStat1.mutex.Unlock()
	require.True(t, ok)

	// 3. Register dispatcher3 for cfID2.
	ok = es.RegisterDispatcher(cfID2, dispatcherID3, span3, 120, func(u uint64, u2 uint64) {}, false, false)
	require.True(t, ok)

	// Check if changefeedStat for cfID2 is created.
	v, ok = es.changefeedMeta.Load(cfID2)
	require.True(t, ok)
	cfStat2 := v.(*changefeedStat)
	require.NotNil(t, cfStat2)
	cfStat2.mutex.Lock()
	require.Len(t, cfStat2.dispatchers, 1)
	cfStat2.mutex.Unlock()

	// 4. Unregister dispatcher1 from cfID1.
	es.UnregisterDispatcher(cfID1, dispatcherID1)
	v, ok = es.changefeedMeta.Load(cfID1)
	require.True(t, ok)
	cfStat1 = v.(*changefeedStat)
	cfStat1.mutex.Lock()
	require.Len(t, cfStat1.dispatchers, 1)
	_, ok = cfStat1.dispatchers[dispatcherID1]
	cfStat1.mutex.Unlock()
	require.False(t, ok)

	// 5. Unregister dispatcher2 from cfID1 (the last one).
	es.UnregisterDispatcher(cfID1, dispatcherID2)
	// Check if changefeedStat for cfID1 is removed.
	_, ok = es.changefeedMeta.Load(cfID1)
	require.False(t, ok)

	// 6. Check cfID2 is not affected.
	v, ok = es.changefeedMeta.Load(cfID2)
	require.True(t, ok)
	cfStat2 = v.(*changefeedStat)
	cfStat2.mutex.Lock()
	require.Len(t, cfStat2.dispatchers, 1)
	cfStat2.mutex.Unlock()
}

func TestChangefeedStatManagementConcurrent(t *testing.T) {
	_, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))
	es := store.(*eventStore)

	cfID1 := common.NewChangefeedID4Test("default", "test-cf-1")
	cfID2 := common.NewChangefeedID4Test("default", "test-cf-2")
	cfIDs := []common.ChangeFeedID{cfID1, cfID2}

	concurrency := 100
	dispatchersPerRoutine := 20
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < dispatchersPerRoutine; j++ {
				dispatcherID := common.NewDispatcherID()
				cfID := cfIDs[(routineID*dispatchersPerRoutine+j)%len(cfIDs)]
				span := &heartbeatpb.TableSpan{TableID: int64(j)}

				ok := es.RegisterDispatcher(cfID, dispatcherID, span, 100, func(u uint64, u2 uint64) {}, false, false)
				require.True(t, ok)

				es.UnregisterDispatcher(cfID, dispatcherID)
			}
		}(i)
	}

	wg.Wait()

	// After all operations, the changefeedMeta should be empty because all dispatchers are unregistered.
	isEmpty := true
	es.changefeedMeta.Range(func(key, value interface{}) bool {
		isEmpty = false
		return false // stop iteration
	})
	require.True(t, isEmpty, "changefeedMeta should be empty after all dispatchers are unregistered")
}

func TestWriteToEventStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	dir := t.TempDir()
	store := New(ctx, dir, nil).(*eventStore)
	defer store.Close(ctx)

	smallEntryKey := []byte("small-key")
	smallEntryValue := []byte("small-value")
	// A value smaller than the threshold.
	smallEntry := &common.RawKVEntry{
		OpType:   common.OpTypePut,
		StartTs:  200,
		CRTs:     210,
		KeyLen:   uint32(len(smallEntryKey)),
		ValueLen: uint32(len(smallEntryValue)),
		Key:      smallEntryKey,
		Value:    smallEntryValue,
		OldValue: nil,
	}

	largeEntryKey := []byte("large-key")
	largeEntryValue := []byte("large-value")
	// A value larger than the threshold.
	largeEntry := &common.RawKVEntry{
		OpType:   common.OpTypePut,
		StartTs:  200,
		CRTs:     211, // Note: must be different from smallEntry's CRTs to avoid key collision if key is same
		KeyLen:   uint32(len(largeEntryKey)),
		ValueLen: uint32(len(largeEntryValue)) * uint32(store.compressionThreshold/10),
		Key:      []byte(largeEntryKey),
		Value:    bytes.Repeat(largeEntryValue, store.compressionThreshold/10),
		OldValue: nil,
	}
	events := []eventWithCallback{
		{
			subID:   1,
			tableID: 1,
			kvs:     []common.RawKVEntry{*smallEntry, *largeEntry},
			callback: func() {
			},
		},
	}
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer encoder.Close()

	err = store.writeEvents(store.dbs[0], events, encoder)
	require.NoError(t, err)

	// Read events back and verify.
	iter, err := store.dbs[0].NewIter(&pebble.IterOptions{})
	require.NoError(t, err)
	defer iter.Close()

	var readEntries []*common.RawKVEntry
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		_, compressionType := DecodeKeyMetas(key)

		var decodedValue []byte
		if compressionType == CompressionZSTD {
			decodedValue, err = decoder.DecodeAll(value, nil)
			require.NoError(t, err)
		} else {
			require.Equal(t, CompressionNone, compressionType)
			decodedValue = value
		}

		entry := &common.RawKVEntry{}
		err = entry.Decode(decodedValue)
		require.NoError(t, err)
		readEntries = append(readEntries, entry)
	}

	require.Len(t, readEntries, 2)

	// The order of keys might be "large-key" then "small-key" due to lexicographical sorting.
	var foundSmall, foundLarge bool
	for _, entry := range readEntries {
		if bytes.Equal(entry.Key, smallEntry.Key) {
			require.Equal(t, smallEntry, entry)
			foundSmall = true
		} else if bytes.Equal(entry.Key, largeEntry.Key) {
			require.Equal(t, largeEntry, entry)
			foundLarge = true
		}
	}
	require.True(t, foundSmall, "small value entry not found")
	require.True(t, foundLarge, "large value entry not found")
}

func TestEventStoreGetIteratorConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	_, store := newEventStoreForTest(dir)
	defer store.Close(ctx)

	// 1. Register a dispatcher.
	dispatcherID := common.NewDispatcherID()
	cfID := common.NewChangefeedID4Test("default", "test-cf")
	span := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	startTs := uint64(100)
	var resolvedTs atomic.Uint64
	resolvedTs.Store(startTs)
	ok := store.RegisterDispatcher(cfID, dispatcherID, span, startTs, func(watermark, latestCommitTs uint64) {
		resolvedTs.Store(watermark)
	}, false, false)
	require.True(t, ok)

	// 2. Write some data.
	var events []eventWithCallback
	var lastCommitTs uint64
	for i := 0; i < 10; i++ {
		lastCommitTs = startTs + uint64(i*10) + 5
		entry := &common.RawKVEntry{
			OpType:  common.OpTypePut,
			StartTs: startTs + uint64(i*10),
			CRTs:    lastCommitTs,
			Key:     []byte(fmt.Sprintf("key-%d", i)),
			// Make value large enough to trigger compression.
			Value: bytes.Repeat([]byte("value"), store.(*eventStore).compressionThreshold),
		}
		events = append(events, eventWithCallback{
			subID:    1,
			tableID:  1,
			kvs:      []common.RawKVEntry{*entry},
			callback: func() {},
		})
	}
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer encoder.Close()
	err = store.(*eventStore).writeEvents(store.(*eventStore).dbs[0], events, encoder)
	require.NoError(t, err)

	// 3. Advance resolved ts for the subscription.
	dispatcherStat := store.(*eventStore).dispatcherMeta.dispatcherStats[dispatcherID]
	require.NotNil(t, dispatcherStat)
	subStat := dispatcherStat.subStat
	require.NotNil(t, subStat)
	subStat.resolvedTs.Store(lastCommitTs + 1)

	// 4. Concurrently get iterators and read data.
	concurrency := 10
	iterations := 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				dataRange := common.DataRange{
					Span:          span,
					CommitTsStart: startTs,
					CommitTsEnd:   lastCommitTs + 1,
				}
				iter := store.GetIterator(dispatcherID, dataRange)
				require.NotNil(t, iter, "iterator should not be nil")

				var receivedEvents []*common.RawKVEntry
				for {
					ev, ok := iter.Next()
					if !ok {
						break
					}
					receivedEvents = append(receivedEvents, ev)
				}
				require.Len(t, receivedEvents, 10, "should receive 10 events")
				_, err := iter.Close()
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
}
