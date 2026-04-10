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
	"errors"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRegionRuntimeRegistryAllocKey(t *testing.T) {
	registry := newRegionRuntimeRegistry()

	key1 := registry.allocKey(1, 101)
	key2 := registry.allocKey(1, 101)
	key3 := registry.allocKey(1, 102)

	require.Equal(t, regionRuntimeKey{subID: 1, regionID: 101, generation: 1}, key1)
	require.Equal(t, regionRuntimeKey{subID: 1, regionID: 101, generation: 2}, key2)
	require.Equal(t, regionRuntimeKey{subID: 1, regionID: 102, generation: 1}, key3)
}

func TestRegionRuntimeRegistryUpdateAndSnapshot(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	key := registry.allocKey(1, 101)
	now := time.Unix(1700000000, 0)

	subSpan := &subscribedSpan{
		subID: 1,
		span: heartbeatpb.TableSpan{
			TableID:  42,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}
	region := regionInfo{
		verID: tikv.NewRegionVerID(101, 2, 3),
		span: heartbeatpb.TableSpan{
			TableID:  42,
			StartKey: []byte("b"),
			EndKey:   []byte("c"),
		},
		rpcCtx: &tikv.RPCContext{
			Addr: "tikv-1:20160",
			Peer: &metapb.Peer{Id: 11, StoreId: 22},
		},
		subscribedSpan: subSpan,
	}

	registry.updateRegionInfo(key, region)
	registry.transition(key, regionPhaseQueued, now)
	registry.updateWorker(key, 7)
	registry.updateResolvedTs(key, 12345, now.Add(time.Second))
	registry.recordError(key, errors.New("store busy"), now.Add(2*time.Second))
	registry.incRetry(key)
	registry.setRequestEnqueueTime(key, now.Add(3*time.Second))

	state, ok := registry.get(key)
	require.True(t, ok)
	require.Equal(t, int64(42), state.tableID)
	require.Equal(t, regionPhaseQueued, state.phase)
	require.Equal(t, uint64(22), state.leaderStoreID)
	require.Equal(t, uint64(11), state.leaderPeerID)
	require.Equal(t, "tikv-1:20160", state.storeAddr)
	require.Equal(t, uint64(7), state.workerID)
	require.Equal(t, uint64(12345), state.lastResolvedTs)
	require.Equal(t, "store busy", state.lastError)
	require.Equal(t, 1, state.retryCount)
	require.Equal(t, now.Add(3*time.Second), state.requestEnqueueTime)

	snapshots := registry.snapshot()
	require.Len(t, snapshots, 1)
	require.Equal(t, state, snapshots[0])

	snapshots[0].span.StartKey[0] = 'x'
	updated, ok := registry.get(key)
	require.True(t, ok)
	require.Equal(t, []byte("b"), updated.span.StartKey)
}

func TestRegionRuntimeRegistryRemoveBySubscription(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	key1 := registry.allocKey(1, 101)
	key2 := registry.allocKey(1, 102)
	key3 := registry.allocKey(2, 201)

	registry.transition(key1, regionPhaseDiscovered, time.Unix(1, 0))
	registry.transition(key2, regionPhaseRemoved, time.Unix(2, 0))
	registry.transition(key3, regionPhaseReplicating, time.Unix(3, 0))

	require.Len(t, registry.snapshot(), 3)
	require.Equal(t, 2, registry.removeBySubscription(1))

	_, ok := registry.get(key1)
	require.False(t, ok)
	_, ok = registry.get(key2)
	require.False(t, ok)

	state, ok := registry.get(key3)
	require.True(t, ok)
	require.Equal(t, regionPhaseReplicating, state.phase)
	require.Len(t, registry.snapshot(), 1)
}

func TestRegionRuntimeRegistryPhaseCounts(t *testing.T) {
	registry := newRegionRuntimeRegistry()
	now := time.Unix(1700000000, 0)

	key1 := registry.allocKey(1, 101)
	key2 := registry.allocKey(1, 102)
	key3 := registry.allocKey(2, 201)

	registry.transition(key1, regionPhaseQueued, now)
	registry.transition(key2, regionPhaseQueued, now.Add(time.Second))
	registry.transition(key3, regionPhaseWaitInitialized, now.Add(2*time.Second))

	counts := registry.phaseCounts()
	require.Equal(t, 2, counts[regionPhaseQueued])
	require.Equal(t, 1, counts[regionPhaseWaitInitialized])
	require.Equal(t, 0, counts[regionPhaseReplicating])
}
