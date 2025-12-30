// Copyright 2025 PingCAP, Inc.
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

//go:build !nextgen

package gc

import (
	"context"
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

func ensureChangefeedStartTsSafety(ctx context.Context, pdCli pd.Client, gcServiceID string, keyspaceID uint32, ttl int64, startTs uint64) error {
	// set gc safepoint for the changefeed gc service
	minServiceGCTs, err := SetServiceGCSafepoint(ctx, pdCli, common.DefaultKeyspace.ID, gcServiceID, ttl, startTs)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("set gc safepoint for changefeed",
		zap.String("gcServiceID", gcServiceID),
		zap.Uint64("expectedGCSafepoint", startTs),
		zap.Uint64("actualGCSafepoint", minServiceGCTs),
		zap.Int64("ttl", ttl))

	// startTs should be greater than or equal to minServiceGCTs + 1, otherwise gcManager
	// would return a ErrSnapshotLostByGC even though the changefeed would appear to be successfully
	// created/resumed. See issue #6350 for more detail.
	if startTs > 0 && startTs < minServiceGCTs+1 {
		return errors.ErrStartTsBeforeGC.GenWithStackByArgs(startTs, minServiceGCTs)
	}
	return nil
}

// SetServiceGCSafepoint set a service safepoint to PD.
func SetServiceGCSafepoint(
	ctx context.Context, pdCli pd.Client, keyspaceID uint32, serviceID string, TTL int64, safePoint uint64,
) (minServiceGCTs uint64, err error) {
	err = retry.Do(ctx,
		func() error {
			var err1 error
			minServiceGCTs, err1 = pdCli.UpdateServiceGCSafePoint(ctx, serviceID, TTL, safePoint)
			if err1 != nil {
				log.Warn("Set GC safepoint failed, retry later", zap.Error(err1))
			}
			return err1
		},
		retry.WithBackoffBaseDelay(gcServiceBackoffDelay),
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
	return minServiceGCTs, err
}

// removeServiceGCSafepoint removes a service safepoint from PD.
func removeServiceGCSafepoint(
	ctx context.Context,
	pdCli pd.Client,
	keyspaceID uint32,
	serviceID string,
) error {
	// Set TTL to 0 second to delete the service safe point.
	TTL := 0
	return retry.Do(ctx,
		func() error {
			_, err := pdCli.UpdateServiceGCSafePoint(ctx, serviceID, int64(TTL), math.MaxUint64)
			if err != nil {
				log.Warn("Remove GC safepoint failed, retry later", zap.Error(err))
			}
			return err
		},
		retry.WithBackoffBaseDelay(gcServiceBackoffDelay), // 1s
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
}

func getServiceGCSafepoint(ctx context.Context, pdCli pd.Client, keyspaceID uint32, serviceID string) (uint64, error) {
	return SetServiceGCSafepoint(ctx, pdCli, keyspaceID, serviceID, 0, 0)
}
