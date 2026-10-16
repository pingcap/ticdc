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

package eventstore

import (
	"context"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

// newRedoSharingStore disables general sharing so tests exercise only the opt-in path.
func newRedoSharingStore(t *testing.T, enabled bool) (*mockSubscriptionClient, *eventStore) {
	t.Helper()
	original := config.GetGlobalServerConfig().Clone()
	cfg := original.Clone()
	cfg.Debug.EventStore.EnableDataSharing = false
	cfg.Debug.EventStore.EnableRedoDataSharing = enabled
	config.StoreGlobalServerConfig(cfg)
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	client, store := newEventStoreForTest(t.TempDir())
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	return client.(*mockSubscriptionClient), store.(*eventStore)
}

// TestRedoSharingLifecycle registers both modes in either order before initialization,
// advances their checkpoints independently, then removes each consumer in turn.
func TestRedoSharingLifecycle(t *testing.T) {
	for _, firstMode := range []int64{common.DefaultMode, common.RedoMode} {
		t.Run(map[int64]string{common.DefaultMode: "normal-first", common.RedoMode: "redo-first"}[firstMode], func(t *testing.T) {
			client, store := newRedoSharingStore(t, true)
			cf := common.NewChangefeedID4Test("default", "pair")
			span := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
			ids := []common.DispatcherID{common.NewDispatcherID(), common.NewDispatcherID()}
			secondMode := int64(common.DefaultMode)
			if firstMode == common.DefaultMode {
				secondMode = common.RedoMode
			}
			var notified [2]uint64
			require.True(t, store.RegisterDispatcher(cf, ids[0], span, 100, func(ts, _ uint64) { notified[0] = ts }, false, false, false, firstMode))
			require.True(t, store.RegisterDispatcher(cf, ids[1], span, 100, func(ts, _ uint64) { notified[1] = ts }, false, false, false, secondMode))
			require.Len(t, client.subscriptions, 1)
			sub := store.dispatcherMeta.dispatcherStats[ids[0]].subStat
			require.Same(t, sub, store.dispatcherMeta.dispatcherStats[ids[1]].subStat)
			require.False(t, sub.initialized.Load())
			// The existing immutable subscriber snapshot fans out to both consumers.
			sub.resolvedTs.Store(200)
			for _, subscriber := range sub.subscribers.Load().subscribers {
				subscriber.notifyFunc(200, 190)
			}
			require.Equal(t, [2]uint64{200, 200}, notified)
			// Write one physical copy and read it through two independent iterators.
			encoder, err := zstd.NewWriter(nil)
			require.NoError(t, err)
			defer encoder.Close()
			kv := common.RawKVEntry{
				OpType: common.OpTypePut, StartTs: 140, CRTs: 150,
				Key: []byte("b"), KeyLen: 1, Value: []byte("value"), ValueLen: 5,
			}
			var compressed, raw []byte
			require.NoError(t, store.writeEvents(store.dbs[sub.dbIndex], []eventWithCallback{{
				subID: sub.subID, tableID: span.TableID, kvs: []common.RawKVEntry{kv}, callback: func() {},
			}}, encoder, &compressed, &raw))
			for _, id := range ids {
				iter := requireEventIterator(t, store, id, common.DataRange{Span: span, CommitTsStart: 100, CommitTsEnd: 200})
				entry, _ := iter.Next()
				require.Equal(t, &kv, entry)
				entry, _ = iter.Next()
				require.Nil(t, entry)
				_, err := iter.Close()
				require.NoError(t, err)
			}
			store.UpdateDispatcherCheckpointTs(ids[0], 180)
			require.Equal(t, uint64(100), sub.checkpointTs.Load())
			store.UpdateDispatcherCheckpointTs(ids[1], 120)
			require.Equal(t, uint64(120), sub.checkpointTs.Load())
			require.Equal(t, uint64(180), store.dispatcherMeta.dispatcherStats[ids[0]].checkpointTs.Load())
			store.UnregisterDispatcher(cf, ids[0])
			store.cleanObsoleteSubscriptionsOnce(0)
			require.Len(t, client.subscriptions, 1)
			store.UpdateDispatcherCheckpointTs(ids[1], 190)
			require.Equal(t, uint64(190), sub.checkpointTs.Load())
			store.UnregisterDispatcher(cf, ids[1])
			store.cleanObsoleteSubscriptionsOnce(0)
			require.Empty(t, client.subscriptions)
		})
	}
}

// TestRedoSharingIsolation changes one compatibility condition for each second
// registration and verifies that it falls back to a separate subscription.
func TestRedoSharingIsolation(t *testing.T) {
	for _, scenario := range []string{"disabled", "same-mode", "changefeed", "keyspace", "bdr", "low-latency", "span", "expired", "future", "remote-probe", "stopped"} {
		t.Run(scenario, func(t *testing.T) {
			client, store := newRedoSharingStore(t, scenario != "disabled")
			cf := common.NewChangefeedID4Test("default", "pair")
			span := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
			first, second := common.NewDispatcherID(), common.NewDispatcherID()
			notify := func(uint64, uint64) {}
			require.True(t, store.RegisterDispatcher(cf, first, span, 100, notify, false, false, false, common.DefaultMode))
			nextSpan := *span
			lowLatency := false
			mode, startTs, bdr, onlyReuse := int64(common.RedoMode), uint64(100), false, false
			switch scenario {
			case "same-mode":
				mode = common.DefaultMode
			case "changefeed":
				cf.Id = common.NewGID() // Same display name must not cross changefeed identities.
			case "keyspace":
				nextSpan.KeyspaceID++
			case "bdr":
				bdr = true
			case "low-latency":
				lowLatency = true
			case "span":
				nextSpan.StartKey = []byte("b") // Containment is intentionally insufficient.
			case "expired":
				startTs = 99
			case "future":
				startTs = 101
			case "remote-probe":
				onlyReuse = true
			case "stopped":
				store.dispatcherMeta.dispatcherStats[first].subStat.subscribers.Load().subscribers[first].isStopped = true
			}
			ok := store.RegisterDispatcher(cf, second, &nextSpan, startTs, notify, onlyReuse, bdr, lowLatency, mode)
			if onlyReuse {
				require.False(t, ok)
				require.Len(t, client.subscriptions, 1)
			} else {
				require.True(t, ok)
				require.Len(t, client.subscriptions, 2)
			}
		})
	}
}

// TestRedoSharingConcurrentRegistration starts both registrations together and
// verifies lookup/create serialization prevents duplicate upstream subscriptions.
func TestRedoSharingConcurrentRegistration(t *testing.T) {
	client, store := newRedoSharingStore(t, true)
	cf := common.NewChangefeedID4Test("default", "pair")
	span := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	start := make(chan struct{})
	results := make(chan bool, 2)
	var wg sync.WaitGroup
	for _, mode := range []int64{common.DefaultMode, common.RedoMode} {
		wg.Add(1)
		go func(mode int64) {
			defer wg.Done()
			<-start
			results <- store.RegisterDispatcher(cf, common.NewDispatcherID(), span, 100, func(uint64, uint64) {}, false, false, false, mode)
		}(mode)
	}
	close(start)
	wg.Wait()
	require.True(t, <-results)
	require.True(t, <-results)
	require.Len(t, client.subscriptions, 1)
}

// TestRedoSharingRetainedRange registers a late peer at each retained-range
// boundary, removes it, and verifies a replacement still reuses the live source.
func TestRedoSharingRetainedRange(t *testing.T) {
	client, store := newRedoSharingStore(t, true)
	cf := common.NewChangefeedID4Test("default", "pair")
	span := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	first := common.NewDispatcherID()
	notify := func(uint64, uint64) {}
	require.True(t, store.RegisterDispatcher(cf, first, span, 100, notify, false, false, false, common.DefaultMode))
	sub := store.dispatcherMeta.dispatcherStats[first].subStat
	sub.resolvedTs.Store(200)
	sub.initialized.Store(true)
	store.UpdateDispatcherCheckpointTs(first, 120)
	for _, startTs := range []uint64{120, 200, 150} {
		peer := common.NewDispatcherID()
		require.True(t, store.RegisterDispatcher(cf, peer, span, startTs, notify, false, false, false, common.RedoMode))
		require.Same(t, sub, store.dispatcherMeta.dispatcherStats[peer].subStat)
		require.Equal(t, startTs, store.dispatcherMeta.dispatcherStats[peer].checkpointTs.Load())
		require.Len(t, client.subscriptions, 1)
		store.UnregisterDispatcher(cf, peer)
		store.cleanObsoleteSubscriptionsOnce(0)
		require.Len(t, client.subscriptions, 1)
	}
}
