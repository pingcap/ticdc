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

package coordinator

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/gccleaner"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	gcmock "github.com/pingcap/ticdc/pkg/txnutil/gc/mock"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
	"go.uber.org/atomic"
)

type pdClientAdapter struct {
	pd.Client
	gcClient *gcmock.MockClient
}

func (c *pdClientAdapter) UpdateServiceGCSafePoint(
	ctx context.Context, serviceID string, ttl int64, safePoint uint64,
) (uint64, error) {
	return c.gcClient.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
}

func (c *pdClientAdapter) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return c.gcClient.GetGCStatesClient(keyspaceID)
}

func TestTryClearEnsureGCSafepointDoesNotBlockChangefeedChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	undoStarted := make(chan struct{})
	releaseUndo := make(chan struct{})
	var startedOnce sync.Once

	mockGCManager := gcmock.NewMockManager(ctrl)
	mockGCManager.EXPECT().TryUpdateServiceGCSafepoint(gomock.Any(), gomock.Any()).Times(0)
	mockGCManager.EXPECT().TryUpdateKeyspaceGCBarrier(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	expectedServiceID := "test-gc-service" + gc.EnsureGCServiceCreating + common.DefaultKeyspaceName + "_test"
	mockGCClient := gcmock.NewMockClient(ctrl)
	if kerneltype.IsClassic() {
		mockGCClient.EXPECT().
			UpdateServiceGCSafePoint(gomock.Any(), expectedServiceID, int64(0), uint64(math.MaxUint64)).
			DoAndReturn(func(ctx context.Context, _ string, _ int64, safePoint uint64) (uint64, error) {
				startedOnce.Do(func() { close(undoStarted) })
				select {
				case <-releaseUndo:
					return safePoint, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}).
			Times(1)
	} else {
		mockStatesClient := gcmock.NewMockGCStatesClient(ctrl)
		mockGCClient.EXPECT().GetGCStatesClient(uint32(1)).Return(mockStatesClient).Times(1)
		mockStatesClient.EXPECT().
			DeleteGCBarrier(gomock.Any(), expectedServiceID).
			DoAndReturn(func(ctx context.Context, _ string) (*pdgc.GCBarrierInfo, error) {
				startedOnce.Do(func() { close(undoStarted) })
				select {
				case <-releaseUndo:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}).
			Times(1)
	}
	pdClient := &pdClientAdapter{gcClient: mockGCClient}

	co := &coordinator{
		controller: &Controller{
			changefeedDB: changefeed.NewChangefeedDB(1),
			initialized:  atomic.NewBool(false),
		},
		gcServiceID:        "test-gc-service",
		gcManager:          mockGCManager,
		pdClient:           pdClient,
		pdClock:            pdutil.NewClock4Test(),
		changefeedChangeCh: make(chan []*changefeedChange),
		gcCleaner:          gccleaner.New(pdClient, "test-gc-service"),
		lastTickTime:       time.Now(),
		gcTickInterval:     100 * time.Millisecond,
	}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	co.gcCleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)

	errCh := make(chan error, 2)
	ctx, cancel := context.WithCancel(context.Background())

	go func() { errCh <- co.gcCleaner.Run(ctx) }()
	go func() { errCh <- co.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		close(releaseUndo)
		<-errCh
		<-errCh
	})
	select {
	case <-undoStarted:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timeout waiting for undo ensure gc safepoint")
	}

	select {
	case co.changefeedChangeCh <- nil:
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "changefeedChangeCh send blocked by tryClearEnsureGCSafepoint")
	}
}
