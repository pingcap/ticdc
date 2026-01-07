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

func ensureChangefeedStartTsSafetyNextGen(ctx context.Context, pdCli pd.Client, gcServiceID string, keyspaceID uint32, ttl int64, startTs uint64) error {
	gcCli := pdCli.GetGCStatesClient(keyspaceID)
	_, err := SetGCBarrier(ctx, gcCli, gcServiceID, startTs, time.Duration(ttl)*time.Second)
	if err != nil {
		return errors.ErrStartTsBeforeGC.GenWithStackByArgs(startTs)
	}
	return nil
}

// SetGCBarrier Set a GC Barrier of a keyspace
func SetGCBarrier(ctx context.Context, gcCli gc.GCStatesClient, serviceID string, ts uint64, ttl time.Duration) (barrierTS uint64, err error) {
	err = retry.Do(ctx, func() error {
		barrierInfo, err1 := gcCli.SetGCBarrier(ctx, serviceID, ts, ttl)
		if err1 != nil {
			log.Warn("set gc barrier failed, retry later",
				zap.String("serviceID", serviceID),
				zap.Uint64("ts", ts),
				zap.Duration("ttl", ttl),
				zap.Any("barrierInfo", barrierInfo),
				zap.Error(err1))
			return err1
		}
		barrierTS = barrierInfo.BarrierTS
		return nil
	}, retry.WithBackoffBaseDelay(gcServiceBackoffDelay),
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
	return barrierTS, err
}

func getGCState(ctx context.Context, gcCli gc.GCStatesClient) (gc.GCState, error) {
	state, err := gcCli.GetGCState(ctx)
	if err != nil {
		return state, errors.WrapError(errors.ErrGetGCBarrierFailed, err)
	}
	return state, nil
}

// DeleteGCBarrier Delete a GC barrier of a keyspace
func DeleteGCBarrier(ctx context.Context, gcCli gc.GCStatesClient, serviceID string) (barrierInfo *gc.GCBarrierInfo, err error) {
	err = retry.Do(ctx, func() error {
		info, err1 := gcCli.DeleteGCBarrier(ctx, serviceID)
		if err1 != nil {
			log.Warn("delete gc barrier failed, retry later",
				zap.String("serviceID", serviceID),
				zap.Error(err1))
			return err1
		}
		barrierInfo = info
		return nil
	}, retry.WithBackoffBaseDelay(gcServiceBackoffDelay),
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
	return barrierInfo, err
}
