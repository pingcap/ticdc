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
	"fmt"
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

func TestResolveScanLockInfosExpandsSharedLockWrapper(t *testing.T) {
	sharedLock1 := &kvrpcpb.LockInfo{
		Key:         []byte("shared-key"),
		PrimaryLock: []byte("primary-1"),
		LockVersion: 101,
		LockType:    kvrpcpb.Op_Lock,
	}
	sharedLock2 := &kvrpcpb.LockInfo{
		Key:         []byte("shared-key"),
		PrimaryLock: []byte("primary-2"),
		LockVersion: 102,
		LockType:    kvrpcpb.Op_PessimisticLock,
	}
	ordinaryLock := &kvrpcpb.LockInfo{
		Key:         []byte("ordinary-key"),
		PrimaryLock: []byte("ordinary-primary"),
		LockVersion: 103,
		LockType:    kvrpcpb.Op_Lock,
	}
	wrapper := &kvrpcpb.LockInfo{
		Key:             []byte("shared-key"),
		LockVersion:     100,
		LockType:        kvrpcpb.Op_SharedLock,
		SharedLockInfos: []*kvrpcpb.LockInfo{sharedLock1, sharedLock2},
	}
	var resolvedLocks []*txnkv.Lock

	locks, ttl, err := resolveScanLockInfos([]*kvrpcpb.LockInfo{wrapper, ordinaryLock}, func(locks []*txnkv.Lock) (int64, error) {
		for _, lock := range locks {
			if lock.LockType == kvrpcpb.Op_SharedLock {
				return 0, fmt.Errorf("unexpected shared lock wrapper")
			}
		}
		resolvedLocks = append(resolvedLocks, locks...)
		return 42, nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(42), ttl)
	require.Len(t, locks, 3)
	require.Equal(t, locks, resolvedLocks)
	require.Equal(t, []uint64{101, 102, 103}, []uint64{locks[0].TxnID, locks[1].TxnID, locks[2].TxnID})
	require.Equal(t, []kvrpcpb.Op{kvrpcpb.Op_Lock, kvrpcpb.Op_PessimisticLock, kvrpcpb.Op_Lock},
		[]kvrpcpb.Op{locks[0].LockType, locks[1].LockType, locks[2].LockType})
	require.Equal(t, [][]byte{[]byte("shared-key"), []byte("shared-key"), []byte("ordinary-key")},
		[][]byte{locks[0].Key, locks[1].Key, locks[2].Key})
}

func TestResolveScanLockInfosKeepsOrdinaryLocks(t *testing.T) {
	ordinaryLock := &kvrpcpb.LockInfo{
		Key:         []byte("ordinary-key"),
		PrimaryLock: []byte("ordinary-primary"),
		LockVersion: 103,
		LockType:    kvrpcpb.Op_Lock,
	}
	var resolvedLocks []*txnkv.Lock

	locks, ttl, err := resolveScanLockInfos([]*kvrpcpb.LockInfo{ordinaryLock}, func(locks []*txnkv.Lock) (int64, error) {
		resolvedLocks = append(resolvedLocks, locks...)
		return 0, nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(0), ttl)
	require.Len(t, locks, 1)
	require.Equal(t, locks, resolvedLocks)
	require.Equal(t, uint64(103), locks[0].TxnID)
	require.Equal(t, []byte("ordinary-key"), locks[0].Key)
	require.Equal(t, kvrpcpb.Op_Lock, locks[0].LockType)
}

func TestNextScanLockKeyStopsAtRegionEndWhenFullPageHasLiveLocks(t *testing.T) {
	locks := make([]*txnkv.Lock, scanLockLimit)
	for i := range locks {
		locks[i] = &txnkv.Lock{Key: []byte("shared-key")}
	}

	key := nextScanLockKey([]byte("region-end"), locks, 1)

	require.Equal(t, []byte("region-end"), key)
}

func TestNextScanLockKeyContinuesFromLastLockWhenFullPageHasNoLiveLocks(t *testing.T) {
	locks := make([]*txnkv.Lock, scanLockLimit)
	for i := range locks {
		locks[i] = &txnkv.Lock{Key: []byte("shared-key")}
	}
	locks[len(locks)-1].Key = []byte("last-lock")

	key := nextScanLockKey([]byte("region-end"), locks, 0)

	require.Equal(t, []byte("last-lock"), key)
}

func TestNextScanLockKeyStopsAtRegionEndWhenPageIsNotFull(t *testing.T) {
	locks := []*txnkv.Lock{{Key: []byte("last-lock")}}

	key := nextScanLockKey([]byte("region-end"), locks, 0)

	require.Equal(t, []byte("region-end"), key)
}

func TestSharedLockFullPageUsesResolveTTLForNextScanKey(t *testing.T) {
	sharedLockInfos := make([]*kvrpcpb.LockInfo, scanLockLimit)
	for i := range sharedLockInfos {
		sharedLockInfos[i] = &kvrpcpb.LockInfo{
			Key:         []byte("shared-key"),
			PrimaryLock: []byte(fmt.Sprintf("primary-%d", i)),
			LockVersion: uint64(i + 1),
			LockType:    kvrpcpb.Op_Lock,
		}
	}
	wrapper := &kvrpcpb.LockInfo{
		Key:             []byte("shared-key"),
		LockType:        kvrpcpb.Op_SharedLock,
		SharedLockInfos: sharedLockInfos,
	}

	locks, ttl, err := resolveScanLockInfos([]*kvrpcpb.LockInfo{wrapper}, func(locks []*txnkv.Lock) (int64, error) {
		require.Len(t, locks, scanLockLimit)
		return 1, nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), ttl)
	require.Equal(t, []byte("region-end"), nextScanLockKey([]byte("region-end"), locks, ttl))
	require.Equal(t, []byte("shared-key"), nextScanLockKey([]byte("region-end"), locks, 0))
}
