// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/util"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
)

// LockResolver resolves all kinds of locks in a region.
type LockResolver interface {
	Resolve(ctx context.Context, keyspaceID uint32, regionID, maxVersion uint64) error
}

// DataLockResolver repairs one ScanLock page for checkpoint progress. A non-nil
// cursor resumes the same region and maxVersion on the worker's next turn.
type DataLockResolver interface {
	ResolveDataLocks(ctx context.Context, keyspaceID uint32, regionID, maxVersion uint64, startKey []byte) ([]byte, error)
}

type resolver struct{}

// NewLockerResolver returns a LockResolver.
func NewLockerResolver() LockResolver {
	return &resolver{}
}

// NewDataLockResolver returns a DataLockResolver for checkpoint repair.
func NewDataLockResolver() DataLockResolver {
	return &resolver{}
}

const scanLockLimit = 1024

var (
	metricResolveLockFoundCounter      = subscriptionClientResolveLockProcessedLockCounter.WithLabelValues("found")
	metricResolveLockResolvedCounter   = subscriptionClientResolveLockProcessedLockCounter.WithLabelValues("resolved")
	metricScanLockOperationDuration    = subscriptionClientResolveLockOperationDuration.WithLabelValues("scan")
	metricResolveLockOperationDuration = subscriptionClientResolveLockOperationDuration.WithLabelValues("resolve")
)

// Resolve attempts to resolve every lock through maxVersion, including lock-only
// prewrites, pessimistic primaries, and individual shared holders.
func (r *resolver) Resolve(ctx context.Context, keyspaceID uint32, regionID, maxVersion uint64) error {
	_, err := r.resolve(ctx, keyspaceID, regionID, maxVersion, nil, false)
	return err
}

func (r *resolver) ResolveDataLocks(ctx context.Context, keyspaceID uint32, regionID, maxVersion uint64, startKey []byte) ([]byte, error) {
	return r.resolve(ctx, keyspaceID, regionID, maxVersion, startKey, true)
}

func physicalLockKey(info *kvrpcpb.LockInfo) []byte {
	if members := info.GetSharedLockInfos(); len(members) > 0 {
		return members[0].GetKey()
	}
	return info.GetKey()
}

// ResolveLocks returns a single TTL for its entire batch. Keep each
// transaction's locks together so a live transaction cannot mask the outcome
// for another one, and client-go can still batch its secondary-key rollbacks.
func locksToResolve(infos []*kvrpcpb.LockInfo, dataOnly bool) map[uint64][]*txnkv.Lock {
	byTxn := make(map[uint64][]*txnkv.Lock)
	add := func(info *kvrpcpb.LockInfo) {
		id := info.GetLockVersion()
		byTxn[id] = append(byTxn[id], txnkv.NewLock(info))
	}
	for _, info := range infos {
		if members := info.GetSharedLockInfos(); len(members) > 0 {
			if !dataOnly {
				for _, member := range members {
					add(member)
				}
			}
			continue
		}
		switch info.GetLockType() {
		case kvrpcpb.Op_SharedLock:
			continue // An empty wrapper has no holder to resolve.
		case kvrpcpb.Op_Lock, kvrpcpb.Op_PessimisticLock, kvrpcpb.Op_SharedPessimisticLock:
			if dataOnly {
				continue
			}
		}
		add(info)
	}
	return byTxn
}

func (r *resolver) resolve(ctx context.Context, keyspaceID uint32, regionID, maxVersion uint64, key []byte, dataOnly bool) (_ []byte, err error) {
	var totalLockCount int
	var sampleLocks []string
	start := time.Now()
	defer func() {
		if totalLockCount != 0 || err != nil {
			log.Debug("resolve lock finishes",
				zap.Uint64("regionID", regionID),
				zap.Int("lockCount", totalLockCount),
				zap.Strings("sampleLocks", sampleLocks),
				zap.Uint64("maxVersion", maxVersion),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		}
	}()

	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.GetKeyspaceByID(ctx, keyspaceID)
	if err != nil {
		return nil, err
	}
	storage, err := keyspaceManager.GetStorage(ctx, keyspaceMeta.Name)
	if err != nil {
		return nil, err
	}
	kvStorage := storage.(tikv.Storage)

	for ctx.Err() == nil {
		bo := tikv.NewGcResolveLockMaxBackoffer(ctx)
		page := &kvrpcpb.ScanLockRequest{StartKey: key, MaxVersion: maxVersion, Limit: scanLockLimit}
		infos, scannedCount, err := scanRegionLocks(bo, kvStorage, regionID, page)
		if err != nil {
			return nil, err
		}
		var nextKey []byte
		if scannedCount >= scanLockLimit {
			last := infos[len(infos)-1]
			lastKey := physicalLockKey(last)
			nextKey = kv.NextKey(lastKey)
			if bytes.Compare(nextKey, page.StartKey) <= 0 {
				return nil, errors.ErrUnexpected.GenWithStackByArgs("scan lock page did not advance")
			}
			if !dataOnly && len(last.GetSharedLockInfos()) > 0 {
				// TiKV can truncate the last wrapper even when live holders
				// hide later expired ones. Read just this physical key with
				// no holder limit before advancing the manual scan.
				point := &kvrpcpb.ScanLockRequest{StartKey: lastKey, EndKey: nextKey, MaxVersion: maxVersion}
				holders, _, err := scanRegionLocks(bo, kvStorage, regionID, point)
				if err != nil {
					return nil, err
				}
				if len(holders) > 0 {
					infos = append(infos[:len(infos)-1], holders...)
				}
			}
			if len(page.EndKey) > 0 && bytes.Compare(nextKey, page.EndKey) >= 0 {
				nextKey = nil
			}
		}

		resolveStart := time.Now()
		byTxn := locksToResolve(infos, dataOnly)
		for _, locks := range byTxn {
			totalLockCount += len(locks)
			for _, lock := range locks[:min(len(locks), 5-len(sampleLocks))] {
				sampleLocks = append(sampleLocks, fmt.Sprintf("key: %s, primary: %s, txnID: %d, ttl: %d",
					util.RedactKey(lock.Key), util.RedactKey(lock.Primary), lock.TxnID, lock.TTL))
			}
			ttl, err := kvStorage.GetLockResolver().ResolveLocks(bo, 0, locks)
			if err != nil {
				return nil, errors.WrapError(errors.ErrUnexpected, err, "resolve scanned locks")
			}
			if ttl == 0 {
				metricResolveLockResolvedCounter.Add(float64(len(locks)))
			}
		}
		if len(byTxn) > 0 {
			metricResolveLockOperationDuration.Observe(time.Since(resolveStart).Seconds())
		}
		if nextKey == nil || dataOnly {
			return nextKey, nil
		}
		key = nextKey
	}
	return nil, ctx.Err()
}

// scanRegionLocks clips the request to the current region bounds, including
// after region errors. The caller uses the actual bounds to advance its cursor.
func scanRegionLocks(bo *tikv.Backoffer, kvStorage tikv.Storage, regionID uint64, page *kvrpcpb.ScanLockRequest) ([]*kvrpcpb.LockInfo, int, error) {
	endKey := page.EndKey
	for {
		loc, err := kvStorage.GetRegionCache().LocateRegionByID(bo, regionID)
		if err != nil {
			return nil, 0, errors.WrapError(errors.ErrUnexpected, err, "locate region for lock scan")
		}
		if bytes.Compare(page.StartKey, loc.StartKey) < 0 {
			page.StartKey = loc.StartKey
		}
		page.EndKey = endKey
		if len(loc.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(loc.EndKey, endKey) < 0) {
			page.EndKey = loc.EndKey
		}
		if len(page.EndKey) > 0 && bytes.Compare(page.StartKey, page.EndKey) >= 0 {
			return nil, 0, nil
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, page)
		scanLockStart := time.Now()
		resp, err := kvStorage.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			return nil, 0, errors.WrapError(errors.ErrUnexpected, err, "scan region locks")
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, 0, errors.WrapError(errors.ErrUnexpected, err, "read lock scan response")
		}
		if regionErr != nil {
			if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String())); err != nil {
				return nil, 0, errors.WrapError(errors.ErrUnexpected, err, "retry region lock scan")
			}
			continue
		}
		if resp.Resp == nil {
			return nil, 0, errors.WrapError(errors.ErrUnexpected, tikverr.ErrBodyMissing, "read lock scan response")
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return nil, 0, errors.ErrUnexpected.GenWithStackByArgs("TiKV returned a scan lock key error")
		}
		metricScanLockOperationDuration.Observe(time.Since(scanLockStart).Seconds())
		infos := locksResp.GetLocks()
		scannedCount := 0
		for _, info := range infos {
			scannedCount += max(1, len(info.GetSharedLockInfos()))
		}
		metricResolveLockFoundCounter.Add(float64(scannedCount))
		return infos, scannedCount, nil
	}
}
