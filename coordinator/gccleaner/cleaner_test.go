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

package gccleaner

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type errorPdClient struct {
	pd.Client
	err error
}

func (c *errorPdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return 0, c.err
}

func (c *errorPdClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &errorGCStatesClient{err: c.err}
}

type errorGCStatesClient struct {
	pdgc.GCStatesClient
	err error
}

func (c *errorGCStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), c.err
}

func (c *errorGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	return nil, c.err
}

func (c *errorGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	return pdgc.GCState{}, c.err
}

type countingPdClient struct {
	pd.Client
	callCount atomic.Int32
}

func (c *countingPdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	c.callCount.Add(1)
	return safePoint, nil
}

func (c *countingPdClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &countingGCStatesClient{parent: c}
}

type countingGCStatesClient struct {
	pdgc.GCStatesClient
	parent *countingPdClient
}

func (c *countingGCStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), nil
}

func (c *countingGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	c.parent.callCount.Add(1)
	return nil, nil
}

func (c *countingGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	return pdgc.GCState{}, nil
}

func TestCleanerGating(t *testing.T) {
	client := &countingPdClient{}
	cleaner := New(client, "test-gc-service")

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)

	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(0), client.callCount.Load())

	cleaner.BeginGCTick()
	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(0), client.callCount.Load())

	cleaner.OnUpdateGCSafepointSucceeded()
	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(1), client.callCount.Load())
}

func TestTryClearDoesNotUndoTaskAddedInSameTick(t *testing.T) {
	client := &countingPdClient{}
	cleaner := New(client, "test-gc-service")

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)

	cleaner.BeginGCTick()
	cleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)
	cleaner.OnUpdateGCSafepointSucceeded()

	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(0), client.callCount.Load())
	require.Len(t, cleaner.pending, 1)
	require.True(t, cleaner.pending[cfID].creating)

	cleaner.BeginGCTick()
	cleaner.OnUpdateGCSafepointSucceeded()
	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(1), client.callCount.Load())
	require.Len(t, cleaner.pending, 0)
}

func TestTryClearRequeuesRemainingTasksOnError(t *testing.T) {
	client := &errorPdClient{err: context.Canceled}
	cleaner := New(client, "test-gc-service")

	cfID1 := common.NewChangeFeedIDWithName("test1", common.DefaultKeyspaceName)
	cfID2 := common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName)
	cleaner.Add(cfID1, 1, gc.EnsureGCServiceCreating)
	cleaner.Add(cfID2, 1, gc.EnsureGCServiceCreating)
	cleaner.BeginGCTick()
	cleaner.OnUpdateGCSafepointSucceeded()

	require.False(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))

	entry1 := cleaner.pending[cfID1]
	require.NotNil(t, entry1)
	require.True(t, entry1.creating)

	entry2 := cleaner.pending[cfID2]
	require.NotNil(t, entry2)
	require.True(t, entry2.creating)
}

func TestTryClearOnlyProcessesLimitedTasksPerTick(t *testing.T) {
	client := &countingPdClient{}
	cleaner := New(client, "test-gc-service")

	for i := 0; i < 5; i++ {
		cfID := common.NewChangeFeedIDWithName("test"+strconv.Itoa(i), common.DefaultKeyspaceName)
		cleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)
	}
	cleaner.BeginGCTick()
	cleaner.OnUpdateGCSafepointSucceeded()

	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(4), client.callCount.Load())
	require.Len(t, cleaner.pending, 1)
	for _, entry := range cleaner.pending {
		require.True(t, entry.creating)
		require.False(t, entry.resuming)
	}
}
