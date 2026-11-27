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

package txnutil

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func TestDirectSharedLockWrapperResolveFails(t *testing.T) {
	wrapper := &kvrpcpb.LockInfo{
		Key:         []byte("shared-wrapper"),
		LockVersion: 100,
		LockType:    kvrpcpb.Op_SharedLock,
		SharedLockInfos: []*kvrpcpb.LockInfo{
			{
				Key:         []byte("shared-wrapper"),
				PrimaryLock: []byte("primary-1"),
				LockVersion: 101,
				LockType:    kvrpcpb.Op_Lock,
			},
		},
	}
	lock := txnkv.NewLock(wrapper)

	_, err := txnlock.NewLockResolver(nil).ResolveLocks(
		tikv.NewBackoffer(context.Background(), 1),
		0,
		[]*txnkv.Lock{lock},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "trying to resolve a shared lock directly")
}

// lockScanClient supplies ScanLock pages while using client-go's actual resolver.
type lockScanClient struct {
	tikv.Client
	scan func(*kvrpcpb.ScanLockRequest) []*kvrpcpb.LockInfo
}

func (c lockScanClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdScanLock {
		return &tikvrpc.Response{Resp: &kvrpcpb.ScanLockResponse{Locks: c.scan(req.ScanLock())}}, nil
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func TestProgressRepairScansPastSharedMetadata(t *testing.T) {
	wrapper := &kvrpcpb.LockInfo{LockType: kvrpcpb.Op_SharedLock}
	for i := range scanLockLimit {
		wrapper.SharedLockInfos = append(wrapper.SharedLockInfos, &kvrpcpb.LockInfo{
			Key: []byte("m"), LockType: kvrpcpb.Op_Lock, LockVersion: uint64(i + 1),
		})
	}
	ctx := context.Background()
	dataKey := []byte{'m', 0}
	expired := &kvrpcpb.LockInfo{Key: dataKey, PrimaryLock: dataKey, LockType: kvrpcpb.Op_Put, LockVersion: 10}
	var regionID uint64
	var starts [][]byte
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) { _, _, regionID = mockstore.BootstrapWithSingleStore(c) }),
		mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client {
			return lockScanClient{c, func(scan *kvrpcpb.ScanLockRequest) []*kvrpcpb.LockInfo {
				starts = append(starts, scan.StartKey)
				if len(scan.StartKey) == 0 || string(scan.StartKey) == "z" {
					return []*kvrpcpb.LockInfo{wrapper}
				}
				return []*kvrpcpb.LockInfo{expired}
			}}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	meta := &keyspacepb.KeyspaceMeta{Name: "test"}
	manager := keyspace.NewMockManager(gomock.NewController(t))
	manager.EXPECT().GetKeyspaceByID(ctx, uint32(1)).Return(meta, nil).AnyTimes()
	manager.EXPECT().GetStorage(ctx, meta.Name).Return(store, nil).AnyTimes()
	previous, _ := appcontext.TryGetService[keyspace.Manager](appcontext.KeyspaceManager)
	defer appcontext.SetService(appcontext.KeyspaceManager, previous)
	appcontext.SetService[keyspace.Manager](appcontext.KeyspaceManager, manager)

	found := testutil.ToFloat64(metricResolveLockFoundCounter)
	resolved := testutil.ToFloat64(metricResolveLockResolvedCounter)
	r := NewDataLockResolver()
	next, err := r.ResolveDataLocks(ctx, 1, regionID, 2000, nil)
	require.NoError(t, err)
	require.Equal(t, dataKey, next)
	next, err = r.ResolveDataLocks(ctx, 1, regionID, 2000, next)
	require.NoError(t, err)
	require.Nil(t, next)
	require.Len(t, starts, 2)
	require.Empty(t, starts[0])
	require.Equal(t, dataKey, starts[1])
	require.Equal(t, found+1025, testutil.ToFloat64(metricResolveLockFoundCounter))
	require.Equal(t, resolved+1, testutil.ToFloat64(metricResolveLockResolvedCounter))

	_, err = r.ResolveDataLocks(ctx, 1, regionID, 2000, []byte("z"))
	require.ErrorContains(t, err, "did not advance")
}

func TestProgressRepairSelectsOnlyDataLocks(t *testing.T) {
	infos := []*kvrpcpb.LockInfo{
		{LockType: kvrpcpb.Op_PessimisticLock}, {LockType: kvrpcpb.Op_SharedPessimisticLock},
		{LockType: kvrpcpb.Op_Lock}, {LockType: kvrpcpb.Op_SharedLock},
		{LockType: kvrpcpb.Op_Put}, {LockType: kvrpcpb.Op_Del},
	}
	locks := locksToResolve(infos, true)[0]
	require.Len(t, locks, 2)
	require.Equal(t, kvrpcpb.Op_Put, locks[0].LockType)
	require.Equal(t, kvrpcpb.Op_Del, locks[1].LockType)
}
