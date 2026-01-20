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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func (m *gcManager) TryUpdateGCSafePoint(
	ctx context.Context,
	keyspaceID uint32,
	keyspaceName string,
	checkpointTs common.Ts,
	forceUpdate bool,
) error {
	if time.Since(m.lastUpdatedTime.Load()) < gcSafepointUpdateInterval && !forceUpdate {
		return nil
	}
	m.lastUpdatedTime.Store(time.Now())

	actual, err := SetServiceGCSafepoint(ctx, m.pdClient, common.DefaultKeyspaceID, m.gcServiceID, m.gcTTL, checkpointTs)
	if err != nil {
		log.Warn("updateGCSafePoint failed",
			zap.Uint64("safePointTs", checkpointTs),
			zap.Error(err))
		if time.Since(m.lastSucceededTime.Load()) >= time.Second*time.Duration(m.gcTTL) {
			return errors.ErrUpdateServiceSafepointFailed.Wrap(err)
		}
		return nil
	}
	failpoint.Inject("InjectActualGCSafePoint", func(val failpoint.Value) {
		actual = uint64(val.(int))
	})

	log.Debug("update gc safe point",
		zap.String("gcServiceID", m.gcServiceID),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("actual", actual))

	if actual == checkpointTs {
		log.Info("update gc safe point success", zap.Uint64("gcSafePointTs", checkpointTs))
	}
	if actual > checkpointTs {
		log.Warn("update gc safe point failed, the gc safe point is larger than checkpointTs",
			zap.Uint64("actual", actual), zap.Uint64("checkpointTs", checkpointTs))
	}
	// if the min checkpoint ts is equal to the current gc safe point, it
	// means that the service gc safe point set by TiCDC is the min service
	// gc safe point
	m.isTiCDCBlockGC.Store(actual == checkpointTs)
	m.lastSafePointTs.Store(actual)
	m.lastSucceededTime.Store(time.Now())
	minServiceGCSafePointGauge.Set(float64(oracle.ExtractPhysical(actual)))
	cdcGCSafePointGauge.Set(float64(oracle.ExtractPhysical(checkpointTs)))
	return nil
}
