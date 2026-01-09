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

package gc

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type mockGCStatesClient struct {
	setCalls int
	setArgs  struct {
		barrierID string
		barrierTS uint64
		ttl       time.Duration
	}
	setErr error
}

func (m *mockGCStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	m.setCalls++
	m.setArgs.barrierID = barrierID
	m.setArgs.barrierTS = barrierTS
	m.setArgs.ttl = ttl
	return nil, m.setErr
}

func (m *mockGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	return nil, nil
}

func (m *mockGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	return pdgc.GCState{}, nil
}

type mockPDClientWithGCStates struct {
	pd.Client
	gcStatesClient pdgc.GCStatesClient
	gotKeyspaceID  uint32
}

func (m *mockPDClientWithGCStates) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	m.gotKeyspaceID = keyspaceID
	return m.gcStatesClient
}

func TestEnsureChangefeedStartTsSafetyNextGenSetsGCBarrier(t *testing.T) {
	mockGCClient := &mockGCStatesClient{}
	mockPDClient := &mockPDClientWithGCStates{gcStatesClient: mockGCClient}

	err := ensureChangefeedStartTsSafetyNextGen(
		context.Background(),
		mockPDClient,
		10,
		"gc-service",
		3600,
		123,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(10), mockPDClient.gotKeyspaceID)
	require.Equal(t, 1, mockGCClient.setCalls)
	require.Equal(t, "gc-service", mockGCClient.setArgs.barrierID)
	require.Equal(t, uint64(123), mockGCClient.setArgs.barrierTS)
	require.Equal(t, time.Hour, mockGCClient.setArgs.ttl)
}

func TestEnsureChangefeedStartTsSafetyNextGenErrorIsWrapped(t *testing.T) {
	mockGCClient := &mockGCStatesClient{
		setErr: fmt.Errorf("ErrGCBarrierTSBehindTxnSafePoint: %w", context.Canceled),
	}
	mockPDClient := &mockPDClientWithGCStates{gcStatesClient: mockGCClient}

	err := ensureChangefeedStartTsSafetyNextGen(
		context.Background(),
		mockPDClient,
		11,
		"gc-service",
		3600,
		124,
	)
	require.Error(t, err)
	code, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrStartTsBeforeGC.RFCCode(), code)
	require.Contains(t, err.Error(), "ErrGCBarrierTSBehindTxnSafePoint")
}

func TestGetServiceGCSafepointUsesDummyServiceIDAndReadOnlyArgs(t *testing.T) {
	var (
		callCount int
		gotID     string
		gotTTL    int64
		gotTS     uint64
	)
	pdCli := &MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			callCount++
			gotID, gotTTL, gotTS = serviceID, ttl, safePoint
			return 123, nil
		},
	}

	sp, err := getServiceGCSafepoint(context.Background(), pdCli)
	require.NoError(t, err)
	require.Equal(t, uint64(123), sp)
	require.Equal(t, 1, callCount)
	require.Equal(t, "min-service-gc-safepoint-reader", gotID)
	require.Equal(t, int64(0), gotTTL)
	require.Equal(t, uint64(0), gotTS)
}

func TestRemoveServiceGCSafepointUsesTTL0AndMaxUint64(t *testing.T) {
	var (
		callCount int
		gotID     string
		gotTTL    int64
		gotTS     uint64
	)
	pdCli := &MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			callCount++
			gotID, gotTTL, gotTS = serviceID, ttl, safePoint
			return 0, nil
		},
	}

	err := removeServiceGCSafepoint(context.Background(), pdCli, "svc")
	require.NoError(t, err)
	require.Equal(t, 1, callCount)
	require.Equal(t, "svc", gotID)
	require.Equal(t, int64(0), gotTTL)
	require.Equal(t, uint64(math.MaxUint64), gotTS)
}

func TestCheckStaleCheckpointTsSnapshotLostByGC(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "ks")
	checkpointTs := uint64(200)
	minServiceGCSafepoint := uint64(201)

	err := checkStaleCheckpointTs(
		cfID,
		checkpointTs,
		pdutil.NewClockWithValue4Test(time.Now()),
		minServiceGCSafepoint,
		60,
	)
	require.Error(t, err)
	code, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrSnapshotLostByGC.RFCCode(), code)
}

func TestCheckStaleCheckpointTsGCTTLExceeded(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("cf", "ks")

	now := time.Now()
	checkpointTime := now.Add(-61 * time.Second)
	checkpointTs := oracle.GoTimeToTS(checkpointTime)

	err := checkStaleCheckpointTs(
		cfID,
		checkpointTs,
		pdutil.NewClockWithValue4Test(now),
		checkpointTs,
		60,
	)
	require.Error(t, err)
	code, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrGCTTLExceeded.RFCCode(), code)
}
