// Copyright 2021 PingCAP, Inc.
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
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// gcSafepointUpdateInterval is the minimum interval that CDC can update gc safepoint
var gcSafepointUpdateInterval = 1 * time.Minute

// Manager is an interface for gc manager
type Manager interface {
	// TryUpdateGCSafePoint tries to update TiCDC service GC safepoint.
	// Manager may skip update when it thinks it is too frequent.
	// Set `forceUpdate` to force Manager update.
	TryUpdateGCSafePoint(ctx context.Context, checkpointTs common.Ts, forceUpdate bool) error
	CheckStaleCheckpointTs(ctx context.Context, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error
	// TryUpdateKeyspaceGCBarrier tries to update gc barrier of a keyspace
	TryUpdateKeyspaceGCBarrier(ctx context.Context, keyspaceID uint32, keyspaceName string, checkpointTs common.Ts, forceUpdate bool) error
}

// keyspaceGCBarrierInfo is the gc info for a keyspace
type keyspaceGCBarrierInfo struct {
	lastSucceededTime time.Time
	lastSafePointTs   uint64
	isTiCDCBlockGC    bool
}

type gcManager struct {
	gcServiceID string
	pdClient    pd.Client
	pdClock     pdutil.Clock
	gcTTL       int64

	lastUpdatedTime   *atomic.Time
	lastSucceededTime *atomic.Time
	lastSafePointTs   atomic.Uint64
	isTiCDCBlockGC    atomic.Bool

	// keyspaceLastUpdatedTimeMap store last updated time of each keyspace
	// key => keyspaceID
	// value => time.Time
	keyspaceLastUpdatedTimeMap sync.Map

	// keyspaceGCBarrierInfoMap store gc info of each keyspace
	// key => keyspaceID
	// value => keyspaceGcInfo
	keyspaceGCBarrierInfoMap sync.Map
}

// NewManager creates a new Manager.
func NewManager(gcServiceID string, pdClient pd.Client) Manager {
	serverConfig := config.GetGlobalServerConfig()
	failpoint.Inject("InjectGcSafepointUpdateInterval", func(val failpoint.Value) {
		gcSafepointUpdateInterval = time.Duration(val.(int) * int(time.Millisecond))
	})
	return &gcManager{
		gcServiceID:       gcServiceID,
		pdClient:          pdClient,
		pdClock:           appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		lastUpdatedTime:   atomic.NewTime(time.Now()),
		lastSucceededTime: atomic.NewTime(time.Now()),
		gcTTL:             serverConfig.GcTTL,
	}
}

func (m *gcManager) TryUpdateGCSafePoint(
	ctx context.Context, checkpointTs common.Ts, forceUpdate bool,
) error {
	if time.Since(m.lastUpdatedTime.Load()) < gcSafepointUpdateInterval && !forceUpdate {
		return nil
	}
	m.lastUpdatedTime.Store(time.Now())

	actual, err := SetServiceGCSafepoint(ctx, m.pdClient, m.gcServiceID, m.gcTTL, checkpointTs)
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

func (m *gcManager) CheckStaleCheckpointTs(
	ctx context.Context, changefeedID common.ChangeFeedID, checkpointTs common.Ts,
) error {
	if kerneltype.IsClassic() {
		return m.checkStaleCheckPointTsGlobal(changefeedID, checkpointTs)
	}
	return m.checkStaleCheckpointTsKeyspace(ctx, changefeedID, checkpointTs)
}

func checkStaleCheckpointTs(
	changefeedID common.ChangeFeedID,
	checkpointTs common.Ts,
	pdClock pdutil.Clock,
	isTiCDCBlockGC bool,
	lastSafePointTs uint64,
	gcTTL int64,
) error {
	gcSafepointUpperBound := checkpointTs - 1
	if isTiCDCBlockGC {
		pdTime := pdClock.CurrentTime()
		if pdTime.Sub(
			oracle.GetTimeFromTS(gcSafepointUpperBound),
		) > time.Duration(gcTTL)*time.Second {
			return errors.ErrGCTTLExceeded.
				GenWithStackByArgs(
					checkpointTs,
					changefeedID,
				)
		}
	} else {
		// if `isTiCDCBlockGC` is false, it means there is another service gc
		// point less than the min checkpoint ts.
		if gcSafepointUpperBound < lastSafePointTs {
			return errors.ErrSnapshotLostByGC.
				GenWithStackByArgs(
					checkpointTs,
					lastSafePointTs,
				)
		}
	}
	return nil
}

func (m *gcManager) checkStaleCheckpointTsKeyspace(ctx context.Context, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.LoadKeyspace(ctx, changefeedID.Keyspace())
	if err != nil {
		return err
	}

	barrierInfo := new(keyspaceGCBarrierInfo)
	o, ok := m.keyspaceGCBarrierInfoMap.Load(keyspaceMeta.Id)
	if ok {
		barrierInfo = o.(*keyspaceGCBarrierInfo)
	}

	return checkStaleCheckpointTs(changefeedID, checkpointTs, m.pdClock, barrierInfo.isTiCDCBlockGC, barrierInfo.lastSafePointTs, m.gcTTL)
}

func (m *gcManager) checkStaleCheckPointTsGlobal(changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	return checkStaleCheckpointTs(changefeedID, checkpointTs, m.pdClock, m.isTiCDCBlockGC.Load(), m.lastSafePointTs.Load(), m.gcTTL)
}

func (m *gcManager) TryUpdateKeyspaceGCBarrier(ctx context.Context, keyspaceID uint32, keyspaceName string, checkpointTs common.Ts, forceUpdate bool) error {
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
