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

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
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

func TestProgressRepairScansPastSharedMetadata(t *testing.T) {
	key := []byte("shared")
	wrapper := &kvrpcpb.LockInfo{
		Key: key, LockType: kvrpcpb.Op_SharedLock,
		SharedLockInfos: make([]*kvrpcpb.LockInfo, scanLockLimit),
	}
	for i := range wrapper.SharedLockInfos {
		wrapper.SharedLockInfos[i] = &kvrpcpb.LockInfo{
			Key: key, LockVersion: uint64(i + 1), LockType: kvrpcpb.Op_Lock,
		}
	}
	dataKey := append(append([]byte{}, key...), 0)
	expired := &kvrpcpb.LockInfo{Key: dataKey, LockType: kvrpcpb.Op_Put, LockVersion: 10}
	regionEnd := []byte("z")
	page := []*kvrpcpb.LockInfo{wrapper}
	locks, _, err := resolveScanLockInfos(page, func(_ []*txnkv.Lock) (int64, error) {
		t.Fatal("a shared member cannot hold back resolved-ts")
		return 0, nil
	})
	require.NoError(t, err)
	require.Empty(t, locks)
	require.Equal(t, dataKey, nextScanLockKey(regionEnd, page))

	page = []*kvrpcpb.LockInfo{expired, {Key: []byte("u"), LockType: kvrpcpb.Op_PessimisticLock}}
	locks, _, err = resolveScanLockInfos(page, func(locks []*txnkv.Lock) (int64, error) {
		require.Len(t, locks, 1)
		require.Equal(t, expired.Key, locks[0].Key)
		return 0, nil
	})
	require.NoError(t, err)
	require.Len(t, locks, 1)
	require.Equal(t, regionEnd, nextScanLockKey(regionEnd, page))
}

func TestProgressRepairContinuesPastAttemptedDataLocks(t *testing.T) {
	page := make([]*kvrpcpb.LockInfo, scanLockLimit)
	for i := range page {
		page[i] = &kvrpcpb.LockInfo{Key: []byte("k"), LockType: kvrpcpb.Op_Put}
	}
	page[len(page)-1].Key = []byte("x")
	locks, ttl, err := resolveScanLockInfos(page, func(locks []*txnkv.Lock) (int64, error) {
		return 1000, nil
	})
	require.NoError(t, err)
	require.Len(t, locks, scanLockLimit)
	require.Positive(t, ttl)
	require.Equal(t, []byte{'x', 0}, nextScanLockKey([]byte("z"), page))
	require.Equal(t, []byte("z"), nextScanLockKey([]byte("z"), nil))
}
