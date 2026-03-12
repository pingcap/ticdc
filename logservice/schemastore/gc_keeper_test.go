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

package schemastore

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

func TestSchemaStoreGCKeeperLifecycle(t *testing.T) {
	originalConfig := config.GetGlobalServerConfig()
	cfg := originalConfig.Clone()
	cfg.AdvertiseAddr = "127.0.0.1:8300"
	config.StoreGlobalServerConfig(cfg)
	defer config.StoreGlobalServerConfig(originalConfig)

	pdCli := &mockPdClientForSchemaStoreGC{
		serviceSafePoint: make(map[string]uint64),
		gcBarriers:       make(map[string]uint64),
		txnSafePoint:     100,
	}
	keeper := newSchemaStoreGCKeeper(pdCli, common.DefaultKeyspace)
	serviceID := keeper.serviceID()

	require.Contains(t, serviceID, "node_127_0_0_1_8300")

	ctx := context.Background()
	require.NoError(t, keeper.initialize(ctx, 100))
	assertSchemaStoreBarrierTS(t, pdCli, serviceID, 101)

	require.NoError(t, keeper.refresh(ctx, 130))
	assertSchemaStoreBarrierTS(t, pdCli, serviceID, 131)

	require.NoError(t, keeper.close(ctx))
	if kerneltype.IsClassic() {
		require.Equal(t, uint64(math.MaxUint64), pdCli.serviceSafePoint[serviceID])
		return
	}
	_, ok := pdCli.gcBarriers[serviceID]
	require.False(t, ok)
}

func assertSchemaStoreBarrierTS(t *testing.T, pdCli *mockPdClientForSchemaStoreGC, serviceID string, expected uint64) {
	t.Helper()
	if kerneltype.IsClassic() {
		require.Equal(t, expected, pdCli.serviceSafePoint[serviceID])
		return
	}
	require.Equal(t, expected, pdCli.gcBarriers[serviceID])
}

type mockPdClientForSchemaStoreGC struct {
	pd.Client
	serviceSafePoint map[string]uint64
	gcBarriers       map[string]uint64
	txnSafePoint     uint64
}

func (m *mockPdClientForSchemaStoreGC) UpdateServiceGCSafePoint(
	ctx context.Context, serviceID string, ttl int64, safePoint uint64,
) (uint64, error) {
	minSafePoint := uint64(math.MaxUint64)
	for _, ts := range m.serviceSafePoint {
		if ts < minSafePoint {
			minSafePoint = ts
		}
	}
	if len(m.serviceSafePoint) != 0 && safePoint < minSafePoint {
		return minSafePoint, nil
	}
	m.serviceSafePoint[serviceID] = safePoint
	return minSafePoint, nil
}

func (m *mockPdClientForSchemaStoreGC) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &mockSchemaStoreGCStatesClient{
		keyspaceID: keyspaceID,
		parent:     m,
	}
}

type mockSchemaStoreGCStatesClient struct {
	keyspaceID uint32
	parent     *mockPdClientForSchemaStoreGC
}

func (m *mockSchemaStoreGCStatesClient) SetGCBarrier(
	ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration,
) (*pdgc.GCBarrierInfo, error) {
	if barrierTS < m.parent.txnSafePoint {
		return nil, errors.New("ErrGCBarrierTSBehindTxnSafePoint")
	}
	m.parent.gcBarriers[barrierID] = barrierTS
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), nil
}

func (m *mockSchemaStoreGCStatesClient) DeleteGCBarrier(
	ctx context.Context, barrierID string,
) (*pdgc.GCBarrierInfo, error) {
	barrierTS, ok := m.parent.gcBarriers[barrierID]
	if !ok {
		return nil, nil
	}
	delete(m.parent.gcBarriers, barrierID)
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, 0, time.Now()), nil
}

func (m *mockSchemaStoreGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	gcBarriers := make([]*pdgc.GCBarrierInfo, 0, len(m.parent.gcBarriers))
	for id, ts := range m.parent.gcBarriers {
		gcBarriers = append(gcBarriers, pdgc.NewGCBarrierInfo(id, ts, 0, time.Now()))
	}

	return pdgc.GCState{
		KeyspaceID:   m.keyspaceID,
		TxnSafePoint: m.parent.txnSafePoint,
		GCBarriers:   gcBarriers,
	}, nil
}
