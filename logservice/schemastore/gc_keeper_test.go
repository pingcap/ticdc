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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
)

func TestSchemaStoreGCKeeperLifecycle(t *testing.T) {
	originalConfig := config.GetGlobalServerConfig()
	cfg := originalConfig.Clone()
	cfg.AdvertiseAddr = "127.0.0.1:8300"
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
	_, ok := state.serviceSafePointV2[serviceID]
	require.False(t, ok)
}

func TestCloseSchemaStoreGCKeeperUsesFreshContext(t *testing.T) {
	originalConfig := config.GetGlobalServerConfig()
	cfg := originalConfig.Clone()
	cfg.AdvertiseAddr = "127.0.0.1:8300"
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
	_, ok := state.serviceSafePointV2[serviceID]
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
	require.Equal(t, expected, state.serviceSafePointV2[serviceID])
}

type schemaStoreGCMockState struct {
	serviceSafePoint   map[string]uint64
	serviceSafePointV2 map[string]uint64
}

func newMockGCServiceClientForSchemaStoreGC(t *testing.T) (*gc.MockGCServiceClient, *schemaStoreGCMockState) {
	t.Helper()

	ctrl := gomock.NewController(t)
	state := &schemaStoreGCMockState{
		serviceSafePoint:   make(map[string]uint64),
		serviceSafePointV2: make(map[string]uint64),
	}
	pdCli := gc.NewMockGCServiceClient(ctrl)

	pdCli.EXPECT().
		UpdateServiceGCSafePoint(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
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
		}).
		AnyTimes()

	pdCli.EXPECT().
		UpdateServiceSafePointV2(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if ttl == 0 {
				delete(state.serviceSafePointV2, serviceID)
				minSafePoint := uint64(math.MaxUint64)
				for _, ts := range state.serviceSafePointV2 {
					if ts < minSafePoint {
						minSafePoint = ts
					}
				}
				return minSafePoint, nil
			}
			minSafePoint := uint64(math.MaxUint64)
			for _, ts := range state.serviceSafePointV2 {
				if ts < minSafePoint {
					minSafePoint = ts
				}
			}
			if len(state.serviceSafePointV2) != 0 && safePoint < minSafePoint {
				return minSafePoint, nil
			}
			state.serviceSafePointV2[serviceID] = safePoint
			return minSafePoint, nil
		}).
		AnyTimes()

	return pdCli, state
}
