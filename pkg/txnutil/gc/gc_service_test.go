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
	stderrors "errors"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	gcmock "github.com/pingcap/ticdc/pkg/txnutil/gc/mock"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type mockPDClientAdapter struct {
	pd.Client
	gcClient *gcmock.MockClient
}

func (m *mockPDClientAdapter) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return m.gcClient.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
}

func (m *mockPDClientAdapter) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return m.gcClient.GetGCStatesClient(keyspaceID)
}

func TestEnsureChangefeedStartTsSafetySuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		ttl        int64  = 1
		keyspaceID uint32 = 11
		startTs    uint64 = 65
	)
	changefeedID := common.NewChangeFeedIDWithName("changefeed1", "default")
	gcServiceID := "ticdc-creating-default_changefeed1"

	mockGCClient := gcmock.NewMockClient(ctrl)
	pdClient := &mockPDClientAdapter{gcClient: mockGCClient}

	if kerneltype.IsClassic() {
		mockGCClient.EXPECT().
			UpdateServiceGCSafePoint(gomock.Any(), gcServiceID, ttl, startTs).
			Return(uint64(60), nil).
			Times(1)
	} else {
		mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
		mockGCClient.EXPECT().GetGCStatesClient(keyspaceID).Return(mockStatesClient).Times(1)
		mockStatesClient.EXPECT().
			SetGCBarrier(gomock.Any(), gcServiceID, startTs, time.Second).
			Return(nil, nil).
			Times(1)
	}

	err := EnsureChangefeedStartTsSafety(
		context.Background(),
		pdClient,
		"ticdc-creating-",
		keyspaceID,
		changefeedID,
		ttl,
		startTs,
	)
	require.NoError(t, err)
}

func TestEnsureChangefeedStartTsSafetyBeforeGC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		ttl        int64  = 1
		keyspaceID uint32 = 12
		startTs    uint64 = 65
	)
	changefeedID := common.NewChangeFeedIDWithName("changefeed1", "default")
	gcServiceID := "ticdc-creating-default_changefeed1"

	mockGCClient := gcmock.NewMockClient(ctrl)
	pdClient := &mockPDClientAdapter{gcClient: mockGCClient}

	if kerneltype.IsClassic() {
		mockGCClient.EXPECT().
			UpdateServiceGCSafePoint(gomock.Any(), gcServiceID, ttl, startTs).
			Return(uint64(70), nil).
			Times(1)
	} else {
		mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
		mockGCClient.EXPECT().GetGCStatesClient(keyspaceID).Return(mockStatesClient).Times(2)
		mockStatesClient.EXPECT().
			SetGCBarrier(gomock.Any(), gcServiceID, startTs, time.Second).
			Return(nil, perrors.Annotate(context.Canceled, "ErrGCBarrierTSBehindTxnSafePoint")).
			Times(1)
		mockStatesClient.EXPECT().
			GetGCState(gomock.Any()).
			Return(pdgc.GCState{TxnSafePoint: 70}, nil).
			Times(1)
	}

	err := EnsureChangefeedStartTsSafety(
		context.Background(),
		pdClient,
		"ticdc-creating-",
		keyspaceID,
		changefeedID,
		ttl,
		startTs,
	)
	require.Error(t, err)
	code, ok := cerror.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerror.ErrStartTsBeforeGC.RFCCode(), code)
}

func TestEnsureChangefeedStartTsSafetyReturnsReachMaxTry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		ttl        int64  = 1
		keyspaceID uint32 = 13
		startTs    uint64 = 65
	)
	changefeedID := common.NewChangeFeedIDWithName("changefeed1", "default")
	gcServiceID := "ticdc-creating-default_changefeed1"

	mockGCClient := gcmock.NewMockClient(ctrl)
	pdClient := &mockPDClientAdapter{gcClient: mockGCClient}

	if kerneltype.IsClassic() {
		mockGCClient.EXPECT().
			UpdateServiceGCSafePoint(gomock.Any(), gcServiceID, ttl, startTs).
			Return(uint64(0), stderrors.New("not pd leader")).
			Times(gcServiceMaxRetries)
	} else {
		mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
		mockGCClient.EXPECT().GetGCStatesClient(keyspaceID).Return(mockStatesClient).Times(1)
		mockStatesClient.EXPECT().
			SetGCBarrier(gomock.Any(), gcServiceID, startTs, time.Second).
			Return(nil, stderrors.New("not pd leader")).
			Times(gcServiceMaxRetries)
	}

	err := EnsureChangefeedStartTsSafety(
		context.Background(),
		pdClient,
		"ticdc-creating-",
		keyspaceID,
		changefeedID,
		ttl,
		startTs,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ErrReachMaxTry")
}

func TestUndoEnsureChangefeedStartTsSafety(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const keyspaceID uint32 = 14
	changefeedID := common.NewChangeFeedIDWithName("changefeed1", "default")
	gcServiceID := "ticdc-creating-default_changefeed1"

	mockGCClient := gcmock.NewMockClient(ctrl)
	pdClient := &mockPDClientAdapter{gcClient: mockGCClient}

	if kerneltype.IsClassic() {
		mockGCClient.EXPECT().
			UpdateServiceGCSafePoint(gomock.Any(), gcServiceID, int64(0), uint64(math.MaxUint64)).
			Return(uint64(0), nil).
			Times(1)
	} else {
		mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
		mockGCClient.EXPECT().GetGCStatesClient(keyspaceID).Return(mockStatesClient).Times(1)
		mockStatesClient.EXPECT().
			DeleteGCBarrier(gomock.Any(), gcServiceID).
			Return(nil, nil).
			Times(1)
	}

	err := UndoEnsureChangefeedStartTsSafety(
		context.Background(),
		pdClient,
		keyspaceID,
		"ticdc-creating-",
		changefeedID,
	)
	require.NoError(t, err)
}
