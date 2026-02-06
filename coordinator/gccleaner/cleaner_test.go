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
	"math"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	gcmock "github.com/pingcap/ticdc/pkg/txnutil/gc/mock"
	"github.com/stretchr/testify/require"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

func expectUndoCalls(
	mockGCClient *gcmock.MockClient,
	ctrl *gomock.Controller,
	keyspaceID uint32,
	callCount *atomic.Int32,
	times int,
	retErr error,
) {
	if kerneltype.IsClassic() {
		mockGCClient.EXPECT().
			UpdateServiceGCSafePoint(gomock.Any(), gomock.Any(), int64(0), uint64(math.MaxUint64)).
			DoAndReturn(func(_ context.Context, _ string, _ int64, safePoint uint64) (uint64, error) {
				callCount.Add(1)
				if retErr != nil {
					return 0, retErr
				}
				return safePoint, nil
			}).
			Times(times)
		return
	}

	mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
	mockGCClient.EXPECT().GetGCStatesClient(keyspaceID).Return(mockStatesClient).Times(times)
	mockStatesClient.EXPECT().
		DeleteGCBarrier(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string) (*pdgc.GCBarrierInfo, error) {
			callCount.Add(1)
			return nil, retErr
		}).
		Times(times)
}

func TestCleanerGating(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callCount atomic.Int32
	mockGCClient := gcmock.NewMockClient(ctrl)
	expectUndoCalls(mockGCClient, ctrl, 1, &callCount, 1, nil)
	cleaner := New(mockGCClient, "test-gc-service")

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)

	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(0), callCount.Load())

	cleaner.BeginGCTick()
	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(0), callCount.Load())

	cleaner.OnUpdateGCSafepointSucceeded()
	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(1), callCount.Load())
}

func TestTryClearDoesNotUndoTaskAddedInSameTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callCount atomic.Int32
	mockGCClient := gcmock.NewMockClient(ctrl)
	expectUndoCalls(mockGCClient, ctrl, 1, &callCount, 1, nil)
	cleaner := New(mockGCClient, "test-gc-service")

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)

	cleaner.BeginGCTick()
	cleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)
	cleaner.OnUpdateGCSafepointSucceeded()

	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(0), callCount.Load())
	require.Len(t, cleaner.pending, 1)
	require.True(t, cleaner.pending[cfID].creating)

	cleaner.BeginGCTick()
	cleaner.OnUpdateGCSafepointSucceeded()
	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(1), callCount.Load())
	require.Len(t, cleaner.pending, 0)
}

func TestTryClearRequeuesRemainingTasksOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callCount atomic.Int32
	mockGCClient := gcmock.NewMockClient(ctrl)
	expectUndoCalls(mockGCClient, ctrl, 1, &callCount, 1, context.Canceled)
	cleaner := New(mockGCClient, "test-gc-service")

	cfID1 := common.NewChangeFeedIDWithName("test1", common.DefaultKeyspaceName)
	cfID2 := common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName)
	cleaner.Add(cfID1, 1, gc.EnsureGCServiceCreating)
	cleaner.Add(cfID2, 1, gc.EnsureGCServiceCreating)
	cleaner.BeginGCTick()
	cleaner.OnUpdateGCSafepointSucceeded()

	require.False(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(1), callCount.Load())

	entry1 := cleaner.pending[cfID1]
	require.NotNil(t, entry1)
	require.True(t, entry1.creating)

	entry2 := cleaner.pending[cfID2]
	require.NotNil(t, entry2)
	require.True(t, entry2.creating)
}

func TestTryClearOnlyProcessesLimitedTasksPerTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callCount atomic.Int32
	mockGCClient := gcmock.NewMockClient(ctrl)
	expectUndoCalls(mockGCClient, ctrl, 1, &callCount, 4, nil)
	cleaner := New(mockGCClient, "test-gc-service")

	for i := 0; i < 5; i++ {
		cfID := common.NewChangeFeedIDWithName("test"+strconv.Itoa(i), common.DefaultKeyspaceName)
		cleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)
	}
	cleaner.BeginGCTick()
	cleaner.OnUpdateGCSafepointSucceeded()

	require.True(t, cleaner.tryClearEnsureGCSafepoint(context.Background()))
	require.Equal(t, int32(4), callCount.Load())
	require.Len(t, cleaner.pending, 1)
	for _, entry := range cleaner.pending {
		require.True(t, entry.creating)
		require.False(t, entry.resuming)
	}
}
