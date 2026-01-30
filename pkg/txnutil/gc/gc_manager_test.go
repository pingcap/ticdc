// Copyright 2021 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/errors"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
)

func TestUpdateGCSafePoint(t *testing.T) {
	t.Parallel()

	mockPDClient := &MockPDClient{}
	pdClock := pdutil.NewClock4Test()
	gcManager := &gcManager{
		gcServiceID:       etcd.GcServiceIDForTest(),
		pdClient:          mockPDClient,
		pdClock:           pdClock,
		gcTTL:             60,
		lastUpdatedTime:   atomic.NewTime(time.Now().Add(-gcSafepointUpdateInterval)),
		lastSucceededTime: atomic.NewTime(time.Now()),
	}
	ctx := context.Background()

	startTs := oracle.GoTimeToTS(time.Now())
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, startTs, safePoint)
		require.Equal(t, gcManager.gcTTL, ttl)
		require.Equal(t, etcd.GcServiceIDForTest(), serviceID)
		return 0, nil
	}
	err := gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	require.Nil(t, err)

	// gcManager must not update frequent.
	gcManager.lastUpdatedTime.Store(time.Now())
	startTs++
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	require.Nil(t, err)

	// Assume that the gc safe point updated gcSafepointUpdateInterval ago.
	gcManager.lastUpdatedTime.Store(time.Now().Add(-gcSafepointUpdateInterval))
	startTs++
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, startTs, safePoint)
		require.Equal(t, gcManager.gcTTL, ttl)
		require.Equal(t, etcd.GcServiceIDForTest(), serviceID)
		return 0, nil
	}
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	require.Nil(t, err)

	// Force update
	startTs++
	ch := make(chan struct{}, 1)
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, startTs, safePoint)
		require.Equal(t, gcManager.gcTTL, ttl)
		require.Equal(t, etcd.GcServiceIDForTest(), serviceID)
		ch <- struct{}{}
		return 0, nil
	}
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, true /* forceUpdate */)
	require.Nil(t, err)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	case <-ch:
	}
}

func TestCheckStaleCheckpointTs(t *testing.T) {
	t.Parallel()

	mockPDClient := &MockPDClient{}
	pdClock := pdutil.NewClock4Test()
	gcManager := &gcManager{
		gcServiceID:       etcd.GcServiceIDForTest(),
		pdClient:          mockPDClient,
		pdClock:           pdClock,
		gcTTL:             60,
		lastUpdatedTime:   atomic.NewTime(time.Now().Add(-gcSafepointUpdateInterval)),
		lastSucceededTime: atomic.NewTime(time.Now()),
	}
	ctx := context.Background()

	cfID := commonType.NewChangeFeedIDWithName("cfID", "")
	err := gcManager.CheckStaleCheckpointTs(ctx, 0, cfID, oracle.GoTimeToTS(time.Now()))
	require.Nil(t, err)

	gcManager.lastSafePointTs.Store(20)
	if !kerneltype.IsClassic() {
		gcManager.keyspaceGCBarrierInfoMap.Store(uint32(0), &keyspaceGCBarrierInfo{
			lastSafePointTs: 20,
		})
	}
	err = gcManager.CheckStaleCheckpointTs(ctx, 0, cfID, 10)
	require.True(t, cerror.ErrSnapshotLostByGC.Equal(errors.Cause(err)))
	rfcCode, ok := cerror.RFCCode(err)
	require.True(t, ok)
	require.True(t, cerror.IsChangefeedGCFastFailErrorCode(rfcCode))
}
