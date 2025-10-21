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
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/keyspace"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
)

// LockResolver resolves lock in the given region.
type LockResolver interface {
	Resolve(ctx context.Context, keyspaceID uint32, regionID uint64, maxVersion uint64) error
}

type resolver struct{}

// NewLockerResolver returns a LockResolver.
func NewLockerResolver() LockResolver {
	return &resolver{}
}

const scanLockLimit = 1024

func (r *resolver) Resolve(ctx context.Context, keyspaceID uint32, regionID uint64, maxVersion uint64) (err error) {
	var totalLocks []*txnkv.Lock

	start := time.Now()

	defer func() {
		// Only log when there are locks or error to avoid log flooding.
		if len(totalLocks) != 0 || err != nil {
			cost := time.Since(start)
			log.Info("resolve lock finishes",
				zap.Uint64("regionID", regionID),
				zap.Int("lockCount", len(totalLocks)),
				zap.Any("locks", totalLocks),
				zap.Uint64("maxVersion", maxVersion),
				zap.Duration("duration", cost),
				zap.Error(err))
		}
	}()

	// TODO test whether this function will kill active transaction
	req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
		MaxVersion: maxVersion,
		Limit:      scanLockLimit,
	})

	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.GetKeyspaceByID(ctx, keyspaceID)
	if err != nil {
		return err
	}

	storage, err := keyspaceManager.GetStorage(keyspaceMeta.Name)
	if err != nil {
		return err
	}
	kvStorage := storage.(tikv.Storage)

	bo := tikv.NewGcResolveLockMaxBackoffer(ctx)
	var loc *tikv.KeyLocation
	var key []byte
	flushRegion := func() error {
		var err error
		loc, err = kvStorage.GetRegionCache().LocateRegionByID(bo, regionID)
		if err != nil {
			return err
		}
		key = loc.StartKey
		return nil
	}
	if err = flushRegion(); err != nil {
		return errors.Trace(err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		req.ScanLock().StartKey = key
		resp, err := kvStorage.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			if err := flushRegion(); err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return errors.Trace(tikverr.ErrBodyMissing)
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*txnkv.Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = txnkv.NewLock(locksInfo[i])
		}
		totalLocks = append(totalLocks, locks...)

		_, err1 := kvStorage.GetLockResolver().ResolveLocks(bo, 0, locks)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if len(locks) < scanLockLimit {
			key = loc.EndKey
		} else {
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(loc.EndKey) != 0 && bytes.Compare(key, loc.EndKey) >= 0) {
			break
		}
		bo = tikv.NewGcResolveLockMaxBackoffer(ctx)
	}
	return nil
}
