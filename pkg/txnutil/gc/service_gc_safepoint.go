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

package gc

import (
	"context"
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

func ensureChangefeedStartTsSafetyClassic(ctx context.Context, gcCli Client, gcServiceID string, ttl int64, startTs uint64) error {
	minServiceGcSafepoint, err := setServiceGCSafepoint(ctx, gcCli, gcServiceID, ttl, startTs)
	if err != nil {
		return err
	}
	// the TiKV snapshot at the minServiceGcSafepoint is reserved,
	// so startTs >= minServiceGcSafepoint is safe.
	// else set the startTs as the service gc safepoint failed.
	if startTs < minServiceGcSafepoint {
		return errors.ErrStartTsBeforeGC.GenWithStackByArgs(startTs, minServiceGcSafepoint)
	}

	log.Info("ensure changefeed start ts safety",
		zap.String("gcServiceID", gcServiceID),
		zap.Uint64("startTs", startTs),
		zap.Bool("isBlockingGC", startTs == minServiceGcSafepoint),
		zap.Int64("ttl", ttl))
	return nil
}

func getServiceGCSafepoint(ctx context.Context, gcCli Client) (minServiceGCSafepoint uint64, err error) {
	// NOTE: In classic mode, PD does not expose a dedicated "get service GC safepoint" API.
	// Calling `UpdateServiceGCSafePoint` with safePoint=0 is used
	// as a read-only operation to get the current minimum service GC safepoint.
	// use a dummy service id for reading
	return setServiceGCSafepoint(ctx, gcCli, "min-service-gc-safepoint-reader", 0, 0)
}

// SetServiceGCSafepoint set a service safepoint to PD.
func setServiceGCSafepoint(
	ctx context.Context, gcCli Client, serviceID string, TTL int64, safePoint uint64,
) (minServiceGCTs uint64, err error) {
	err = retry.Do(ctx,
		func() error {
			var err1 error
			minServiceGCTs, err1 = gcCli.UpdateServiceGCSafePoint(ctx, serviceID, TTL, safePoint)
			if err1 != nil {
				log.Warn("set gc safepoint failed, retry later", zap.Error(err1))
			}
			return err1
		},
		retry.WithBackoffBaseDelay(gcServiceBackoffDelay),
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
	return minServiceGCTs, errors.WrapError(errors.ErrUpdateServiceSafepointFailed, err)
}

// removeServiceGCSafepoint removes a service safepoint from PD.
func removeServiceGCSafepoint(ctx context.Context, gcCli Client, serviceID string) error {
	err := retry.Do(ctx,
		func() error {
			// Set TTL to 0 second to delete the service safe point.
			_, err := gcCli.UpdateServiceGCSafePoint(ctx, serviceID, 0, math.MaxUint64)
			if err != nil {
				log.Warn("remove gc safepoint failed, retry it", zap.Error(err))
			}
			return err
		},
		retry.WithBackoffBaseDelay(gcServiceBackoffDelay), // 1s
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(errors.IsRetryableError))
	return errors.WrapError(errors.ErrUpdateServiceSafepointFailed, err)
}
