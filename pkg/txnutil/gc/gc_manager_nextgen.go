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

//go:build nextgen

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
	var lastUpdatedTime time.Time
	if lastUpdatedTimeResult, ok := m.keyspaceLastUpdatedTimeMap.Load(keyspaceID); ok {
		lastUpdatedTime = lastUpdatedTimeResult.(time.Time)
	}
	if time.Since(lastUpdatedTime) < gcSafepointUpdateInterval && !forceUpdate {
		return nil
	}
	m.keyspaceLastUpdatedTimeMap.Store(keyspaceID, time.Now())

	gcCli := m.pdClient.GetGCStatesClient(keyspaceID)
	ttl := time.Duration(m.gcTTL) * time.Second
	_, err := SetGCBarrier(ctx, gcCli, m.gcServiceID, checkpointTs, ttl)
	// align to classic mode, if the checkpointTs is less than TxnSafePoint,
	// we can also use the TxnSafePoint as the lastSafePointTs
	if err != nil && !errors.IsGCBarrierTSBehindTxnSafePointError(err) {
		log.Warn("updateKeyspaceGCBarrier failed",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint64("safePointTs", checkpointTs),
			zap.Error(err))
		var lastSucceededTime time.Time
		if barrierInfoObj, ok := m.keyspaceGCBarrierInfoMap.Load(keyspaceID); ok {
			barrierInfo := barrierInfoObj.(*keyspaceGCBarrierInfo)
			lastSucceededTime = barrierInfo.lastSucceededTime
		}
		if time.Since(lastSucceededTime) >= time.Duration(m.gcTTL)*time.Second {
			return errors.ErrUpdateGCBarrierFailed.Wrap(err)
		}
		return nil
	}

	actual, err := UnifyGetServiceGCSafepoint(ctx, m.pdClient, keyspaceID, m.gcServiceID)
	if err != nil {
		return err
	}

	failpoint.Inject("InjectActualGCSafePoint", func(val failpoint.Value) {
		actual = uint64(val.(int))
	})

	if actual > checkpointTs {
		log.Warn("update gc barrier failed, the gc barrier is larger than checkpointTs",
			zap.Uint64("actual", actual), zap.Uint64("checkpointTs", checkpointTs))
	}

	// if the min checkpoint ts is equal to the current gc barrier ts, it means
	// that the service gc barrier ts set by TiCDC is the min service gc barrier ts
	newBarrierInfo := &keyspaceGCBarrierInfo{
		lastSucceededTime: time.Now(),
		lastSafePointTs:   actual,
		isTiCDCBlockGC:    actual == checkpointTs,
	}
	m.keyspaceGCBarrierInfoMap.Store(keyspaceID, newBarrierInfo)

	minGCBarrierMetric := minGCBarrierGauge.WithLabelValues(keyspaceName)
	minGCBarrierMetric.Set(float64(oracle.ExtractPhysical(actual)))

	cdcGcBarrierMetric := cdcGCBarrierGauge.WithLabelValues(keyspaceName)
	cdcGcBarrierMetric.Set(float64(oracle.ExtractPhysical(checkpointTs)))

	return nil
}
