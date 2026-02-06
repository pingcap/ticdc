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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	gcmock "github.com/pingcap/ticdc/pkg/txnutil/gc/mock"
	"github.com/stretchr/testify/require"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

func TestTryUpdateKeyspaceGCBarrierDoesNotReturnSnapshotLost(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	keyspaceID := uint32(1)
	keyspaceName := "test"
	checkpointTs := common.Ts(100)
	txnSafePoint := uint64(200)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGCClient := gcmock.NewMockClient(ctrl)
	mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
	mockGCClient.EXPECT().GetGCStatesClient(keyspaceID).Return(mockStatesClient).Times(2)
	mockStatesClient.EXPECT().
		SetGCBarrier(gomock.Any(), "test-service", uint64(checkpointTs), time.Minute).
		Return(nil, nil).
		Times(1)
	mockStatesClient.EXPECT().
		GetGCState(gomock.Any()).
		Return(pdgc.GCState{TxnSafePoint: txnSafePoint}, nil).
		Times(1)
	m := &gcManager{
		gcServiceID: "test-service",
		gcClient:    mockGCClient,
		pdClock:     appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		gcTTL:       60,
	}

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
