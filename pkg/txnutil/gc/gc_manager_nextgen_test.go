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

//go:build nextgen
// +build nextgen

package gc

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	pdgc "github.com/tikv/pd/client/clients/gc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTryUpdateKeyspaceGCBarrierDoesNotReturnSnapshotLost(t *testing.T) {
	if kerneltype.UseLegacySafePointInNextGen {
		t.Skip("GC Barrier path is disabled with legacy_safepoint tag")
	}
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	keyspaceID := uint32(1)
	keyspaceName := "test"
	checkpointTs := common.Ts(100)
	txnSafePoint := uint64(200)

	pdCliForGCStates := &mockPdClientForServiceGCSafePoint{
		serviceSafePoint: make(map[string]uint64),
		gcBarriers:       make(map[string]uint64),
		txnSafePoint:     txnSafePoint,
	}
	gcStatesClient := &mockGCStatesClient{
		keyspaceID: keyspaceID,
		parent:     pdCliForGCStates,
	}
	pdClient := &MockPDClient{
		GetGCStatesClientFunc: func(id uint32) pdgc.GCStatesClient {
			require.Equal(t, keyspaceID, id)
			return gcStatesClient
		},
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return 0, stderrors.New("not used")
		},
	}

	m := NewManager("test-service", pdClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, m.TryUpdateKeyspaceGCBarrier(ctx, keyspaceID, keyspaceName, checkpointTs))

	cfID := common.NewChangeFeedIDWithName("test-changefeed", keyspaceName)
	err := m.CheckStaleCheckpointTs(keyspaceID, cfID, checkpointTs)
	require.Error(t, err)
	errCode, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrSnapshotLostByGC.RFCCode(), errCode)
}

func TestTryUpdateKeyspaceGCBarrierUsesServiceSafePointV2WhenEnabled(t *testing.T) {
	if !kerneltype.UseLegacySafePointInNextGen {
		t.Skip("legacy safepoint path requires legacy_safepoint tag")
	}
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	keyspaceID := uint32(1)
	keyspaceName := "test"
	checkpointTs := common.Ts(100)

	setServiceSafePointV2Calls := 0
	getMinServiceSafePointV2Calls := 0
	var manager *gcManager
	pdClient := &MockPDClient{
		GetGCStatesClientFunc: func(id uint32) pdgc.GCStatesClient {
			require.FailNow(t, "GetGCStatesClient should not be called when service safe point v2 is enabled")
			return nil
		},
		GetMinServiceSafePointV2Func: func(ctx context.Context, id uint32) (uint64, error) {
			getMinServiceSafePointV2Calls++
			require.Equal(t, keyspaceID, id)
			return uint64(checkpointTs), nil
		},
		SetServiceSafePointV2Func: func(
			ctx context.Context, id uint32, serviceID string, ttl int64, safePoint uint64,
		) (uint64, error) {
			setServiceSafePointV2Calls++
			require.Equal(t, keyspaceID, id)
			require.Equal(t, "test-service", serviceID)
			require.Equal(t, manager.gcTTL, ttl)
			require.Equal(t, uint64(checkpointTs), safePoint)
			return safePoint, nil
		},
	}

	manager = NewManager("test-service", pdClient).(*gcManager)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, manager.TryUpdateKeyspaceGCBarrier(ctx, keyspaceID, keyspaceName, checkpointTs))
	require.Equal(t, 1, setServiceSafePointV2Calls)
	require.Equal(t, 1, getMinServiceSafePointV2Calls)

	barrierInfo, ok := manager.keyspaceGCBarrierInfoMap.Load(keyspaceID)
	require.True(t, ok)
	require.Equal(t, &keyspaceGCBarrierInfo{
		lastSafePointTs: uint64(checkpointTs),
		isTiCDCBlockGC:  true,
	}, barrierInfo)
}

func TestEnsureChangefeedStartTsSafetyUsesServiceSafePointV2WhenEnabled(t *testing.T) {
	if !kerneltype.UseLegacySafePointInNextGen {
		t.Skip("legacy safepoint path requires legacy_safepoint tag")
	}

	const (
		keyspaceID = uint32(1)
		startTs    = uint64(100)
	)
	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", "default")

	testCases := []struct {
		name         string
		minSafePoint uint64
		expectedErr  string
	}{
		{
			name:         "reject start ts behind minimum safe point",
			minSafePoint: 200,
			expectedErr: "[CDC:ErrStartTsBeforeGC]fail to create or maintain changefeed " +
				"because start-ts 100 is earlier than or equal to GC safepoint at 200",
		},
		{
			name:         "accept start ts equal to minimum safe point",
			minSafePoint: startTs,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pdClient := &MockPDClient{
				GetGCStatesClientFunc: func(id uint32) pdgc.GCStatesClient {
					require.FailNow(t, "GetGCStatesClient should not be called when service safe point v2 is enabled")
					return nil
				},
				SetServiceSafePointV2Func: func(
					ctx context.Context, id uint32, serviceID string, ttl int64, safePoint uint64,
				) (uint64, error) {
					require.Equal(t, keyspaceID, id)
					require.Equal(t, "test-service-default_test-changefeed", serviceID)
					require.Equal(t, int64(60), ttl)
					require.Equal(t, startTs, safePoint)
					return tc.minSafePoint, nil
				},
			}

			err := EnsureChangefeedStartTsSafety(
				context.Background(), pdClient, "test-service-", keyspaceID, changefeedID, 60, startTs)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}

func TestTryUpdateKeyspaceGCBarrierDoesNotUseServiceSafePointV2WhenDisabled(t *testing.T) {
	if kerneltype.UseLegacySafePointInNextGen {
		t.Skip("GC Barrier path is disabled with legacy_safepoint tag")
	}
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	keyspaceID := uint32(1)
	keyspaceName := "test"
	checkpointTs := common.Ts(100)

	pdClient := &MockPDClient{
		GetGCStatesClientFunc: func(id uint32) pdgc.GCStatesClient {
			require.Equal(t, keyspaceID, id)
			return unsupportedGCStatesClient{}
		},
		SetServiceSafePointV2Func: func(
			ctx context.Context, id uint32, serviceID string, ttl int64, safePoint uint64,
		) (uint64, error) {
			require.FailNow(t, "SetServiceSafePointV2 should not be called when service safe point v2 is disabled")
			return 0, nil
		},
	}

	manager := NewManager("test-service", pdClient).(*gcManager)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := manager.TryUpdateKeyspaceGCBarrier(ctx, keyspaceID, keyspaceName, checkpointTs)
	require.Error(t, err)
	errCode, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrUpdateGCBarrierFailed.RFCCode(), errCode)
}

func TestUnifyGetAndDeleteGcSafepointUseServiceSafePointV2WhenEnabled(t *testing.T) {
	if !kerneltype.UseLegacySafePointInNextGen {
		t.Skip("legacy safepoint path requires legacy_safepoint tag")
	}

	keyspaceID := uint32(1)
	serviceID := "test-service"
	minSafePoint := uint64(120)

	getServiceSafePointV2Calls := 0
	deleteServiceSafePointV2Calls := 0
	pdClient := &MockPDClient{
		GetGCStatesClientFunc: func(id uint32) pdgc.GCStatesClient {
			require.FailNow(t, "GetGCStatesClient should not be called when service safe point v2 is enabled")
			return nil
		},
		GetMinServiceSafePointV2Func: func(ctx context.Context, id uint32) (uint64, error) {
			getServiceSafePointV2Calls++
			require.Equal(t, keyspaceID, id)
			return minSafePoint, nil
		},
		DeleteServiceSafePointV2Func: func(ctx context.Context, id uint32, sid string) (uint64, error) {
			deleteServiceSafePointV2Calls++
			require.Equal(t, keyspaceID, id)
			require.Equal(t, serviceID, sid)
			return minSafePoint, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	gotMinSafePoint, err := UnifyGetServiceGCSafepoint(ctx, pdClient, keyspaceID, serviceID)
	require.NoError(t, err)
	require.Equal(t, minSafePoint, gotMinSafePoint)
	require.Equal(t, 1, getServiceSafePointV2Calls)

	require.NoError(t, UnifyDeleteGcSafepoint(ctx, pdClient, keyspaceID, serviceID))
	require.Equal(t, 1, deleteServiceSafePointV2Calls)
}

type unsupportedGCStatesClient struct{}

func (unsupportedGCStatesClient) SetGCBarrier(
	ctx context.Context,
	barrierID string,
	barrierTS uint64,
	ttl time.Duration,
) (*pdgc.GCBarrierInfo, error) {
	return nil, status.Error(codes.Unimplemented, "unknown method SetGCBarrier")
}

func (unsupportedGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	return nil, status.Error(codes.Unimplemented, "unknown method DeleteGCBarrier")
}

func (unsupportedGCStatesClient) GetGCState(ctx context.Context, opts ...pdgc.GCStatesAPIOption) (pdgc.GCState, error) {
	return pdgc.GCState{}, status.Error(codes.Unimplemented, "unknown method GetGCState")
}

func (unsupportedGCStatesClient) SetGlobalGCBarrier(
	ctx context.Context,
	barrierID string,
	barrierTS uint64,
	ttl time.Duration,
) (*pdgc.GlobalGCBarrierInfo, error) {
	return nil, status.Error(codes.Unimplemented, "unknown method SetGlobalGCBarrier")
}

func (unsupportedGCStatesClient) DeleteGlobalGCBarrier(
	ctx context.Context,
	barrierID string,
) (*pdgc.GlobalGCBarrierInfo, error) {
	return nil, status.Error(codes.Unimplemented, "unknown method DeleteGlobalGCBarrier")
}

func (unsupportedGCStatesClient) GetAllKeyspacesGCStates(
	ctx context.Context,
	opts ...pdgc.GCStatesAPIOption,
) (pdgc.ClusterGCStates, error) {
	return pdgc.ClusterGCStates{}, status.Error(codes.Unimplemented, "unknown method GetAllKeyspacesGCStates")
}
