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
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

func TestSchemaStoreGCKeeperLifecycle(t *testing.T) {
	originalConfig := config.GetGlobalServerConfig()
	cfg := originalConfig.Clone()
	cfg.AdvertiseAddr = "127.0.0.1:8300"
	cfg.EnableLegacySafePoint = false
	config.StoreGlobalServerConfig(cfg)
	defer config.StoreGlobalServerConfig(originalConfig)

	pdCli, state := newMockGCServiceClientForSchemaStoreGC(t)
	keeper := newSchemaStoreGCKeeper(pdCli, common.DefaultKeyspace)
	serviceID := keeper.serviceID()

	require.Contains(t, serviceID, "node_127_0_0_1_8300")

	ctx := context.Background()
	require.NoError(t, keeper.initialize(ctx, 100))
	assertSchemaStoreBarrierTS(t, state, serviceID, 101)

	require.NoError(t, keeper.refresh(ctx, 130))
	assertSchemaStoreBarrierTS(t, state, serviceID, 131)

	require.NoError(t, keeper.close(ctx))
	if kerneltype.IsClassic() {
		require.Equal(t, uint64(math.MaxUint64), state.serviceSafePoint[serviceID])
		return
	}
	if config.GetGlobalServerConfig().EnableLegacySafePoint {
		_, ok := state.legacyServiceSafePoint[serviceID]
		require.False(t, ok)
		return
	}
	_, ok := state.gcBarriers[serviceID]
	require.False(t, ok)
}

func TestCloseSchemaStoreGCKeeperUsesFreshContext(t *testing.T) {
	originalConfig := config.GetGlobalServerConfig()
	cfg := originalConfig.Clone()
	cfg.AdvertiseAddr = "127.0.0.1:8300"
	cfg.EnableLegacySafePoint = false
	config.StoreGlobalServerConfig(cfg)
	defer config.StoreGlobalServerConfig(originalConfig)

	pdCli, state := newMockGCServiceClientForSchemaStoreGC(t)
	keeper := newSchemaStoreGCKeeper(pdCli, common.DefaultKeyspace)
	serviceID := keeper.serviceID()

	ctx := context.Background()
	require.NoError(t, keeper.initialize(ctx, 100))
	assertSchemaStoreBarrierTS(t, state, serviceID, 101)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, keeper.close(canceledCtx), context.Canceled)

	require.NoError(t, closeSchemaStoreGCKeeper(common.DefaultKeyspace.ID, keeper))
	if kerneltype.IsClassic() {
		require.Equal(t, uint64(math.MaxUint64), state.serviceSafePoint[serviceID])
		return
	}
	if config.GetGlobalServerConfig().EnableLegacySafePoint {
		_, ok := state.legacyServiceSafePoint[serviceID]
		require.False(t, ok)
		return
	}
	_, ok := state.gcBarriers[serviceID]
	require.False(t, ok)
}

func TestSanitizeSchemaStoreNodeID(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "unknown",
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: "unknown",
		},
		{
			name:     "advertise address",
			input:    "127.0.0.1:8300",
			expected: "127_0_0_1_8300",
		},
		{
			name:     "path like value",
			input:    "node/a:b",
			expected: "node_a_b",
		},
		{
			name:     "keep allowed characters",
			input:    "node-1_abcXYZ",
			expected: "node-1_abcXYZ",
		},
		{
			name:     "trim surrounding spaces",
			input:    "  node-1  ",
			expected: "node-1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, sanitizeSchemaStoreNodeID(tc.input))
		})
	}
}

func assertSchemaStoreBarrierTS(t *testing.T, state *schemaStoreGCMockState, serviceID string, expected uint64) {
	t.Helper()
	if kerneltype.IsClassic() {
		require.Equal(t, expected, state.serviceSafePoint[serviceID])
		return
	}
	if config.GetGlobalServerConfig().EnableLegacySafePoint {
		require.Equal(t, expected, state.legacyServiceSafePoint[serviceID])
		return
	}
	require.Equal(t, expected, state.gcBarriers[serviceID])
}

type schemaStoreGCMockState struct {
	serviceSafePoint       map[string]uint64
	legacyServiceSafePoint map[string]uint64
	gcBarriers             map[string]uint64
	txnSafePoint           uint64
}

func newMockGCServiceClientForSchemaStoreGC(t *testing.T) (*gc.MockPDClient, *schemaStoreGCMockState) {
	t.Helper()

	state := &schemaStoreGCMockState{
		serviceSafePoint:       make(map[string]uint64),
		legacyServiceSafePoint: make(map[string]uint64),
		gcBarriers:             make(map[string]uint64),
		txnSafePoint:           100,
	}
	gcStatesCli := &mockSchemaStoreGCStatesClient{state: state}
	pdCli := &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			minSafePoint := uint64(math.MaxUint64)
			for _, ts := range state.serviceSafePoint {
				if ts < minSafePoint {
					minSafePoint = ts
				}
			}
			if len(state.serviceSafePoint) != 0 && safePoint < minSafePoint {
				return minSafePoint, nil
			}
			state.serviceSafePoint[serviceID] = safePoint
			return minSafePoint, nil
		},
		GetGCStatesClientFunc: func(keyspaceID uint32) pdgc.GCStatesClient {
			return gcStatesCli
		},
		GetMinServiceSafePointV2Func: func(ctx context.Context, keyspaceID uint32) (uint64, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			return state.txnSafePoint, nil
		},
		SetServiceSafePointV2Func: func(
			ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64,
		) (uint64, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			state.legacyServiceSafePoint[serviceID] = safePoint
			return state.txnSafePoint, nil
		},
		DeleteServiceSafePointV2Func: func(ctx context.Context, keyspaceID uint32, serviceID string) (uint64, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			delete(state.legacyServiceSafePoint, serviceID)
			return state.txnSafePoint, nil
		},
	}

	return pdCli, state
}

type mockSchemaStoreGCStatesClient struct {
	state *schemaStoreGCMockState
}

func (m *mockSchemaStoreGCStatesClient) SetGCBarrier(
	ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration,
) (*pdgc.GCBarrierInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if barrierTS < m.state.txnSafePoint {
		return nil, errors.New("ErrGCBarrierTSBehindTxnSafePoint")
	}
	m.state.gcBarriers[barrierID] = barrierTS
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), nil
}

func (m *mockSchemaStoreGCStatesClient) DeleteGCBarrier(
	ctx context.Context, barrierID string,
) (*pdgc.GCBarrierInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	barrierTS, ok := m.state.gcBarriers[barrierID]
	if !ok {
		return nil, nil
	}
	delete(m.state.gcBarriers, barrierID)
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, 0, time.Now()), nil
}

func (m *mockSchemaStoreGCStatesClient) GetGCState(ctx context.Context, opts ...pdgc.GCStatesAPIOption) (pdgc.GCState, error) {
	if err := ctx.Err(); err != nil {
		return pdgc.GCState{}, err
	}
	gcBarriers := make([]*pdgc.GCBarrierInfo, 0, len(m.state.gcBarriers))
	for id, ts := range m.state.gcBarriers {
		gcBarriers = append(gcBarriers, pdgc.NewGCBarrierInfo(id, ts, 0, time.Now()))
	}
	return pdgc.NewGCStateWithGCBarriers(0, m.state.txnSafePoint, 0, gcBarriers), nil
}

func (m *mockSchemaStoreGCStatesClient) SetGlobalGCBarrier(
	ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration,
) (*pdgc.GlobalGCBarrierInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return pdgc.NewGlobalGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), nil
}

func (m *mockSchemaStoreGCStatesClient) DeleteGlobalGCBarrier(
	ctx context.Context, barrierID string,
) (*pdgc.GlobalGCBarrierInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (m *mockSchemaStoreGCStatesClient) GetAllKeyspacesGCStates(
	ctx context.Context, opts ...pdgc.GCStatesAPIOption,
) (pdgc.ClusterGCStates, error) {
	gcState, err := m.GetGCState(ctx, opts...)
	if err != nil {
		return pdgc.ClusterGCStates{}, err
	}
	return pdgc.NewClusterGCStatesWithoutGlobalGCBarriers(map[uint32]pdgc.GCState{
		0: gcState,
	}), nil
}
