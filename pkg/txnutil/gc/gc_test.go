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
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	gcmock "github.com/pingcap/ticdc/pkg/txnutil/gc/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

func TestEnsureChangefeedStartTsSafetyNextGenSetsGCBarrier(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCClient := gcmock.NewMockClient(ctrl)
	mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
	mockGCClient.EXPECT().GetGCStatesClient(uint32(10)).Return(mockStatesClient).Times(1)
	mockStatesClient.EXPECT().SetGCBarrier(gomock.Any(), "gc-service", uint64(123), time.Hour).Return(nil, nil).Times(1)

	err := ensureChangefeedStartTsSafetyNextGen(
		context.Background(),
		mockGCClient,
		10,
		"gc-service",
		3600,
		123,
	)
	require.NoError(t, err)
}

func TestEnsureChangefeedStartTsSafetyNextGenErrorIsWrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCClient := gcmock.NewMockClient(ctrl)
	mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)

	setErr := perrors.Annotate(context.Canceled, "ErrGCBarrierTSBehindTxnSafePoint")
	mockGCClient.EXPECT().GetGCStatesClient(uint32(11)).Return(mockStatesClient).Times(2)
	mockStatesClient.EXPECT().SetGCBarrier(gomock.Any(), "gc-service", uint64(124), time.Hour).Return(nil, setErr).Times(1)
	mockStatesClient.EXPECT().GetGCState(gomock.Any()).Return(pdgc.GCState{}, nil).Times(1)

	err := ensureChangefeedStartTsSafetyNextGen(
		context.Background(),
		mockGCClient,
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCClient := gcmock.NewMockClient(ctrl)
	mockGCClient.EXPECT().
		UpdateServiceGCSafePoint(gomock.Any(), "min-service-gc-safepoint-reader", int64(0), uint64(0)).
		Return(uint64(123), nil).
		Times(1)

	sp, err := getServiceGCSafepoint(context.Background(), mockGCClient)
	require.NoError(t, err)
	require.Equal(t, uint64(123), sp)
}

func TestRemoveServiceGCSafepointUsesTTL0AndMaxUint64(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockGCClient := gcmock.NewMockClient(ctrl)
	mockGCClient.EXPECT().
		UpdateServiceGCSafePoint(gomock.Any(), "svc", int64(0), uint64(math.MaxUint64)).
		Return(uint64(0), nil).
		Times(1)

	err := removeServiceGCSafepoint(context.Background(), mockGCClient, "svc")
	require.NoError(t, err)
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
