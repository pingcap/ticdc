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
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
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
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	subClient := NewMockSubscriptionClient()
	store := New(path, subClient)
	return subClient, store
}

func setDataSharingForTest(t *testing.T, enable bool) func() {
	t.Helper()
	originalCfg := config.GetGlobalServerConfig().Clone()
	updatedCfg := originalCfg.Clone()
	updatedCfg.Debug.EventStore.EnableDataSharing = enable
	config.StoreGlobalServerConfig(updatedCfg)
	return func() {
		config.StoreGlobalServerConfig(originalCfg)
	}
}

func TestEventStoreInteractionWithSubClient(t *testing.T) {
	restoreCfg := setDataSharingForTest(t, true)
	defer restoreCfg()

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

func markSubStatsInitializedForTest(store EventStore, tableID int64) {
	es := store.(*eventStore)
	subStats := es.dispatcherMeta.tableStats[tableID]
	for _, subStat := range subStats {
		subStat.initialized.Store(true)
	}
}

func TestEventStoreOnlyReuseDispatcher(t *testing.T) {
	restoreCfg := setDataSharingForTest(t, true)
	defer restoreCfg()

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
	// when the existing subscription is not initialized, add a dispatcher(onlyReuse=true) should fail
	{
		span := &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("b"),
			EndKey:   []byte("h"),
		}
		ok := store.RegisterDispatcher(cfID, dispatcherID3, span, 100, func(watermark uint64, latestCommitTs uint64) {}, true, false)
		require.False(t, ok)
	}
	// mark existing subscription as initialized
	markSubStatsInitializedForTest(store, tableID)
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
	restoreCfg := setDataSharingForTest(t, true)
	defer restoreCfg()

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
	markSubStatsInitializedForTest(store, tableID)

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
}

func TestEventStoreNonOnlyReuseDispatcher(t *testing.T) {
	restoreCfg := setDataSharingForTest(t, true)
	defer restoreCfg()

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

func TestEventStoreRegisterDispatcherWithoutDataSharing(t *testing.T) {
	restoreCfg := setDataSharingForTest(t, false)
	defer restoreCfg()

	subClient, store := newEventStoreForTest(fmt.Sprintf("/tmp/%s", t.Name()))
	es := store.(*eventStore)

	tableID := int64(1)
	cfID := common.NewChangefeedID4Test("default", "test-cf")
	dispatcherID1 := common.NewDispatcherID()
	dispatcherID2 := common.NewDispatcherID()
	dispatcherID3 := common.NewDispatcherID()
	dispatcherID4 := common.NewDispatcherID()

	spanFull := &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: []byte("a"),
		EndKey:   []byte("h"),
	}
	require.True(t, store.RegisterDispatcher(cfID, dispatcherID1, spanFull, 100, func(uint64, uint64) {}, false, false))

	require.True(t, store.RegisterDispatcher(cfID, dispatcherID2, spanFull, 100, func(uint64, uint64) {}, false, false))

	spanSubset := &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: []byte("b"),
		EndKey:   []byte("g"),
	}
	require.True(t, store.RegisterDispatcher(cfID, dispatcherID3, spanSubset, 100, func(uint64, uint64) {}, false, false))

	mockSubClient := subClient.(*mockSubscriptionClient)
	mockSubClient.mu.Lock()
	require.Equal(t, 3, len(mockSubClient.subscriptions))
	mockSubClient.mu.Unlock()

	es.dispatcherMeta.RLock()
	subStats := es.dispatcherMeta.tableStats[tableID]
	require.Equal(t, 3, len(subStats))
	require.Nil(t, es.dispatcherMeta.dispatcherStats[dispatcherID2].pendingSubStat)
	require.Nil(t, es.dispatcherMeta.dispatcherStats[dispatcherID3].pendingSubStat)
	es.dispatcherMeta.RUnlock()

	ok := store.RegisterDispatcher(cfID, dispatcherID4, spanFull, 100, func(uint64, uint64) {}, true, false)
	require.False(t, ok)

	es.dispatcherMeta.RLock()
	_, exists := es.dispatcherMeta.dispatcherStats[dispatcherID4]
	require.False(t, exists)
	es.dispatcherMeta.RUnlock()

	mockSubClient.mu.Lock()
	require.Equal(t, 3, len(mockSubClient.subscriptions))
	mockSubClient.mu.Unlock()
}

func TestEventStoreUpdateCheckpointTs(t *testing.T) {
	restoreCfg := setDataSharingForTest(t, true)
	defer restoreCfg()

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
	restoreCfg := setDataSharingForTest(t, true)
	defer restoreCfg()

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

func TestWriteToEventStore(t *testing.T) {
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	dir := t.TempDir()
	store := New(dir, nil).(*eventStore)
	defer store.Close(context.Background())

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

func TestEventStoreIter_NextWithFiltering(t *testing.T) {
	t.Parallel()

	// Define a set of reusable events for different test cases.
	// The span for the iterator will be [keyB, keyD).
	// Filtered events (outside the span)
	filteredInsert := &common.RawKVEntry{OpType: common.OpTypePut, Key: []byte("keyA1"), Value: []byte("valA1"), StartTs: 300, CRTs: 301}
	filteredDelete := &common.RawKVEntry{OpType: common.OpTypeDelete, Key: []byte("keyA2"), OldValue: []byte("valA2"), StartTs: 302, CRTs: 303}
	filteredUpdate := &common.RawKVEntry{OpType: common.OpTypePut, Key: []byte("keyA3"), Value: []byte("valA3"), OldValue: []byte("oldValA3"), StartTs: 304, CRTs: 305}
	filteredAtEnd := &common.RawKVEntry{OpType: common.OpTypePut, Key: []byte("keyD"), Value: []byte("valD"), StartTs: 310, CRTs: 311}

	// Kept events (inside the span)
	keptInsert := &common.RawKVEntry{OpType: common.OpTypePut, Key: []byte("keyB1"), Value: []byte("valB1"), StartTs: 400, CRTs: 401}
	keptDelete := &common.RawKVEntry{OpType: common.OpTypeDelete, Key: []byte("keyB2"), OldValue: []byte("valB2"), StartTs: 402, CRTs: 403}
	keptUpdate := &common.RawKVEntry{OpType: common.OpTypePut, Key: []byte("keyB3"), Value: []byte("valB3"), OldValue: []byte("oldValB3"), StartTs: 404, CRTs: 405}

	testCases := []struct {
		name           string
		allEvents      []*common.RawKVEntry
		expectedEvents []*common.RawKVEntry
	}{
		{
			name:           "FilteredInsert-then-KeptDelete",
			allEvents:      []*common.RawKVEntry{filteredInsert, keptDelete},
			expectedEvents: []*common.RawKVEntry{keptDelete},
		},
		{
			name:           "FilteredDelete-then-KeptInsert",
			allEvents:      []*common.RawKVEntry{filteredDelete, keptInsert},
			expectedEvents: []*common.RawKVEntry{keptInsert},
		},
		{
			name:           "FilteredUpdate-then-KeptInsert",
			allEvents:      []*common.RawKVEntry{filteredUpdate, keptInsert},
			expectedEvents: []*common.RawKVEntry{keptInsert},
		},
		{
			name:           "FilteredUpdate-then-KeptDelete",
			allEvents:      []*common.RawKVEntry{filteredUpdate, keptDelete},
			expectedEvents: []*common.RawKVEntry{keptDelete},
		},
		{
			name:           "KeptInsert-then-FilteredAtEnd",
			allEvents:      []*common.RawKVEntry{keptInsert, filteredAtEnd},
			expectedEvents: []*common.RawKVEntry{keptInsert},
		},
		{
			name:           "MultipleFiltered-then-KeptUpdate",
			allEvents:      []*common.RawKVEntry{filteredInsert, filteredDelete, keptUpdate},
			expectedEvents: []*common.RawKVEntry{keptUpdate},
		},
	}

	var subID uint64 = 1
	var tableID int64 = 42
	iteratorSpan := &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: []byte("keyB"),
		EndKey:   []byte("keyD"),
	}

	// This test now focuses on a single, more comprehensive scenario.
	for _, tc := range testCases {
		dir := t.TempDir()
		db, err := pebble.Open(dir, &pebble.Options{})
		require.NoError(t, err)
		t.Run(tc.name, func(t *testing.T) {
			// Write test data
			batch := db.NewBatch()
			for _, event := range tc.allEvents {
				key := EncodeKey(subID, tableID, event, CompressionNone)
				value := event.Encode()
				require.NoError(t, batch.Set(key, value, pebble.NoSync))
			}
			require.NoError(t, batch.Commit(pebble.NoSync))

			// Create iterator with a wider range to ensure it sees all keys,
			// so we can test the internal filtering logic.
			start := EncodeKeyPrefix(subID, tableID, 0)
			end := EncodeKeyPrefix(subID, tableID, 500)
			innerIter, err := db.NewIter(&pebble.IterOptions{
				LowerBound: start,
				UpperBound: end,
			})
			require.NoError(t, err)
			_ = innerIter.First()

			decoder, err := zstd.NewReader(nil)
			require.NoError(t, err)

			iter := &eventStoreIter{
				tableSpan:     iteratorSpan,
				needCheckSpan: true, // Enable span checking logic
				innerIter:     innerIter,
				decoder:       decoder,
				decoderPool:   nil, // Not needed for this test
			}

			// Collect results
			var results []*common.RawKVEntry
			for {
				rawKV, _ := iter.Next()
				if rawKV == nil {
					break
				}
				// Make a copy to verify against later, as the original pointer's content might be overwritten if reused.
				kvCopy := *rawKV
				results = append(results, &kvCopy)
			}
			require.NoError(t, iter.innerIter.Close())

			// Verify results
			require.Len(t, results, len(tc.expectedEvents), "Should only read events within the span")

			for i, res := range results {
				// Check content correctness
				require.True(t, bytes.Equal(tc.expectedEvents[i].Key, res.Key))
				require.True(t, bytes.Equal(tc.expectedEvents[i].Value, res.Value))
				require.True(t, bytes.Equal(tc.expectedEvents[i].OldValue, res.OldValue))
				require.Equal(t, tc.expectedEvents[i].OpType, res.OpType)
				require.Equal(t, tc.expectedEvents[i].StartTs, res.StartTs)
				require.Equal(t, tc.expectedEvents[i].CRTs, res.CRTs)
			}
		})
		require.NoError(t, db.Close())
		require.NoError(t, os.RemoveAll(dir))
	}
}
