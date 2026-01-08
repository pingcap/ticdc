// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"go.uber.org/zap"
)

func ensureChangefeedStartTsSafetyNextGen(ctx context.Context, pdCli pd.Client, keyspaceID uint32, gcServiceID string, ttl int64, startTs uint64) error {
	err := setGCBarrier(ctx, pdCli, keyspaceID, gcServiceID, startTs, ttl)
	if err == nil {
		return nil
	}
	if !errors.IsGCBarrierTSBehindTxnSafePointError(err) {
		return errors.WrapError(errors.ErrUpdateGCBarrierFailed, err)
	}

	state, err := getGCState(ctx, pdCli, keyspaceID)
	if err != nil {
		log.Error("get gc barrier failed when try to get the current gc state",
			zap.Uint64("startTs", startTs),
			zap.Uint32("keyspaceID", keyspaceID), zap.String("gcServiceID", gcServiceID),
			zap.Error(err))
		return err
	}

	if startTs < state.TxnSafePoint {
		return errors.ErrStartTsBeforeGC.GenWithStackByArgs(startTs, state.TxnSafePoint)
	}
	return err
}

// SetGCBarrier Set a GC Barrier of a keyspace
func setGCBarrier(ctx context.Context, pdCli pd.Client, keyspaceID uint32, serviceID string, ts uint64, TTL int64) error {
	ttl := time.Duration(TTL) * time.Second
	cli := pdCli.GetGCStatesClient(keyspaceID)
	err := retry.Do(ctx, func() error {
		barrierInfo, err1 := cli.SetGCBarrier(ctx, serviceID, ts, ttl)
		if err1 != nil {
			log.Warn("set gc barrier failed, retry later",
				zap.String("serviceID", serviceID),
				zap.Uint64("ts", ts),
				zap.Duration("ttl", ttl),
				zap.Any("barrierInfo", barrierInfo),
				zap.Error(err1))
			return err1
		}
		return nil
	}, retry.WithBackoffBaseDelay(gcServiceBackoffDelay),
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
	return errors.WrapError(errors.ErrUpdateGCBarrierFailed, err)
}

func getGCState(ctx context.Context, pdCli pd.Client, keyspaceID uint32) (gc.GCState, error) {
	state, err := pdCli.GetGCStatesClient(keyspaceID).GetGCState(ctx)
	return state, errors.WrapError(errors.ErrGetGCBarrierFailed, err)
}

// deleteGCBarrier Delete a GC barrier of a keyspace
func deleteGCBarrier(ctx context.Context, pdCli pd.Client, keyspaceID uint32, serviceID string) error {
	cli := pdCli.GetGCStatesClient(keyspaceID)
	err := retry.Do(ctx, func() error {
		_, err1 := cli.DeleteGCBarrier(ctx, serviceID)
		if err1 != nil {
			log.Warn("delete gc barrier failed, retry later",
				zap.String("serviceID", serviceID),
				zap.Error(err1))
			return err1
		}
		return nil
	}, retry.WithBackoffBaseDelay(gcServiceBackoffDelay),
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
	return errors.WrapError(errors.ErrDeleteGCBarrierFailed, err)
}
