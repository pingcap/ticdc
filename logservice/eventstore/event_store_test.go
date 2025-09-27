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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

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
		require.Equal(t, 1, len(subStat.dispatchers.subscribers))
		require.Equal(t, int64(0), subStat.idleTime.Load())
		store.UnregisterDispatcher(cfID, dispatcherID3)
		require.Equal(t, 0, len(subStat.dispatchers.subscribers))
		require.NotEqual(t, int64(0), subStat.idleTime.Load())
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
		require.Equal(t, 2, len(subStat.dispatchers.subscribers))
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
		require.Equal(t, 3, len(subStat.dispatchers.subscribers))
	}
	// test unregister dispatcherID3 can remove its dependency on two subscriptions
	{
		store.UnregisterDispatcher(cfID, dispatcherID3)
		subStats := store.(*eventStore).dispatcherMeta.tableStats[tableID]
		require.Equal(t, 3, len(subStats))
		{
			subStat := subStats[logpuller.SubscriptionID(1)]
			require.NotNil(t, subStat)
			require.Equal(t, 2, len(subStat.dispatchers.subscribers))
		}
		{
			subStat := subStats[logpuller.SubscriptionID(3)]
			require.NotNil(t, subStat)
			require.Equal(t, 0, len(subStat.dispatchers.subscribers))
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
			require.Equal(t, 2, len(subStat.dispatchers.subscribers))
			require.Equal(t, true, subStat.dispatchers.subscribers[dispatcherID2].isStopped.Load())
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
			require.Equal(t, 2, len(subStat.dispatchers.subscribers))
			require.Equal(t, true, subStat.dispatchers.subscribers[dispatcherID2].isStopped.Load())
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
			require.Equal(t, 1, len(subStat.dispatchers.subscribers))
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
