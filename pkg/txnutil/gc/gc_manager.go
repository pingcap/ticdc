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
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Manager is an interface for gc manager
type Manager interface {
<<<<<<< HEAD
	// TryUpdateGCSafePoint tries to update TiCDC service GC safepoint.
	// Manager may skip update when it thinks it is too frequent.
	// Set `forceUpdate` to force Manager update.
	TryUpdateGCSafePoint(ctx context.Context, checkpointTs common.Ts, forceUpdate bool) error
	CheckStaleCheckpointTs(ctx context.Context, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error
=======
	// TryUpdateServiceGCSafepoint tries to update TiCDC service GC safepoint.
	TryUpdateServiceGCSafepoint(ctx context.Context, checkpointTs common.Ts) error
	CheckStaleCheckpointTs(keyspaceID uint32, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error
	// TryUpdateKeyspaceGCBarrier tries to update gc barrier of a keyspace
	TryUpdateKeyspaceGCBarrier(ctx context.Context, keyspaceID uint32, keyspaceName string, checkpointTs common.Ts) error
}

// keyspaceGCBarrierInfo is the gc info for a keyspace
type keyspaceGCBarrierInfo struct {
	lastSafePointTs uint64
	isTiCDCBlockGC  bool
>>>>>>> 6a0ae936a (coordinator: make the gc manager always report error if meet (#4119))
}

type gcManager struct {
	gcServiceID string
	pdClient    pd.Client
	pdClock     pdutil.Clock
	gcTTL       int64

<<<<<<< HEAD
	lastUpdatedTime   *atomic.Time
	lastSucceededTime *atomic.Time
	lastSafePointTs   atomic.Uint64
	isTiCDCBlockGC    atomic.Bool

	// keyspaceLastUpdatedTimeMap store last updated time of each keyspace
	// key => keyspaceID
	// value => time.Time
	keyspaceLastUpdatedTimeMap sync.Map
=======
	lastSafePointTs atomic.Uint64
	isTiCDCBlockGC  atomic.Bool

	// keyspaceGCBarrierInfoMap store gc info of each keyspace
	// key => keyspaceID
	// value => keyspaceGcInfo
	keyspaceGCBarrierInfoMap sync.Map
>>>>>>> 6a0ae936a (coordinator: make the gc manager always report error if meet (#4119))
}

// NewManager creates a new Manager.
func NewManager(gcServiceID string, pdClient pd.Client) Manager {
	return &gcManager{
		gcServiceID: gcServiceID,
		pdClient:    pdClient,
		pdClock:     appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		gcTTL:       config.GetGlobalServerConfig().GcTTL,
	}
}

func (m *gcManager) TryUpdateServiceGCSafepoint(
	ctx context.Context, checkpointTs common.Ts,
) error {
	minServiceGCSafepoint, err := SetServiceGCSafepoint(ctx, m.pdClient, m.gcServiceID, m.gcTTL, checkpointTs)
	if err != nil {
		log.Warn("update service gc safepoint failed", zap.Uint64("checkpointTs", checkpointTs),
			zap.String("serviceID", m.gcServiceID), zap.Error(err))
		return errors.WrapError(errors.ErrUpdateServiceSafepointFailed, err)
	}
	failpoint.Inject("InjectActualGCSafePoint", func(val failpoint.Value) {
		minServiceGCSafepoint = uint64(val.(int))
	})

	log.Debug("update gc safe point",
		zap.String("gcServiceID", m.gcServiceID),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("minServiceGCSafepoint", minServiceGCSafepoint))

	if minServiceGCSafepoint == checkpointTs {
		log.Info("update gc safe point success, cdc is blocking gc", zap.Uint64("minServiceGCSafepoint", checkpointTs))
	}

	if checkpointTs < minServiceGCSafepoint {
		log.Warn("checkpointTs is smaller than the minimum service gc safepoint",
			zap.Uint64("minServiceGCSafepoint", minServiceGCSafepoint), zap.Uint64("checkpointTs", checkpointTs),
			zap.String("serviceID", m.gcServiceID))
	}

	// if the min checkpoint ts is equal to the current gc safe point, it
	// means that the service gc safe point set by TiCDC is the min service
	// gc safe point
	m.isTiCDCBlockGC.Store(minServiceGCSafepoint == checkpointTs)
	m.lastSafePointTs.Store(minServiceGCSafepoint)
	minServiceGCSafePointGauge.Set(float64(oracle.ExtractPhysical(minServiceGCSafepoint)))
	cdcGCSafePointGauge.Set(float64(oracle.ExtractPhysical(checkpointTs)))
	return nil
}

func (m *gcManager) CheckStaleCheckpointTs(
<<<<<<< HEAD
	ctx context.Context, changefeedID common.ChangeFeedID, checkpointTs common.Ts,
) error {
	return m.checkStaleCheckPointTsGlobal(changefeedID, checkpointTs)
=======
	keyspaceID uint32, changefeedID common.ChangeFeedID, checkpointTs common.Ts,
) error {
	if kerneltype.IsClassic() {
		return m.checkStaleCheckPointTsGlobal(changefeedID, checkpointTs)
	}
	return m.checkStaleCheckpointTsKeyspace(keyspaceID, changefeedID, checkpointTs)
>>>>>>> 6a0ae936a (coordinator: make the gc manager always report error if meet (#4119))
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

<<<<<<< HEAD
func (m *gcManager) checkStaleCheckPointTsGlobal(changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	return checkStaleCheckpointTs(changefeedID, checkpointTs, m.pdClock, m.isTiCDCBlockGC.Load(), m.lastSafePointTs.Load(), m.gcTTL)
}
=======
func (m *gcManager) checkStaleCheckpointTsKeyspace(keyspaceID uint32, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	barrierInfo := new(keyspaceGCBarrierInfo)
	o, ok := m.keyspaceGCBarrierInfoMap.Load(keyspaceID)
	if ok {
		barrierInfo = o.(*keyspaceGCBarrierInfo)
	}

	return checkStaleCheckpointTs(changefeedID, checkpointTs, m.pdClock, barrierInfo.isTiCDCBlockGC, barrierInfo.lastSafePointTs, m.gcTTL)
}

func (m *gcManager) checkStaleCheckPointTsGlobal(changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	return checkStaleCheckpointTs(changefeedID, checkpointTs, m.pdClock, m.isTiCDCBlockGC.Load(), m.lastSafePointTs.Load(), m.gcTTL)
}

func (m *gcManager) TryUpdateKeyspaceGCBarrier(ctx context.Context, keyspaceID uint32, keyspaceName string, checkpointTs common.Ts) error {
	gcCli := m.pdClient.GetGCStatesClient(keyspaceID)
	ttl := time.Duration(m.gcTTL) * time.Second
	_, setBarrierErr := SetGCBarrier(ctx, gcCli, m.gcServiceID, checkpointTs, ttl)
	if setBarrierErr != nil && !errors.IsGCBarrierTSBehindTxnSafePointError(setBarrierErr) {
		log.Warn("update keyspace gc barrier failed",
			zap.Uint32("keyspaceID", keyspaceID), zap.Uint64("checkpointTs", checkpointTs),
			zap.String("serviceID", m.gcServiceID), zap.Error(setBarrierErr))
		return errors.WrapError(errors.ErrUpdateGCBarrierFailed, setBarrierErr)
	}

	minGCBarrier, err := UnifyGetServiceGCSafepoint(ctx, m.pdClient, keyspaceID, m.gcServiceID)
	if err != nil {
		return err
	}

	failpoint.Inject("InjectActualGCSafePoint", func(val failpoint.Value) {
		minGCBarrier = uint64(val.(int))
	})

	if setBarrierErr != nil {
		log.Warn("update keyspace gc barrier failed, checkpointTs smaller than the minimum gc barrier",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("minGCBarrier", minGCBarrier),
			zap.String("serviceID", m.gcServiceID),
			zap.Error(setBarrierErr))
	}
	// if the min checkpoint ts is equal to the current gc barrier ts, it means
	// that the service gc barrier ts set by TiCDC is the min service gc barrier ts
	newBarrierInfo := &keyspaceGCBarrierInfo{
		lastSafePointTs: minGCBarrier,
		isTiCDCBlockGC:  minGCBarrier == checkpointTs,
	}
	m.keyspaceGCBarrierInfoMap.Store(keyspaceID, newBarrierInfo)

	minGCBarrierMetric := minGCBarrierGauge.WithLabelValues(keyspaceName)
	minGCBarrierMetric.Set(float64(oracle.ExtractPhysical(minGCBarrier)))

	cdcGcBarrierMetric := cdcGCBarrierGauge.WithLabelValues(keyspaceName)
	cdcGcBarrierMetric.Set(float64(oracle.ExtractPhysical(checkpointTs)))

	return nil
}
>>>>>>> 6a0ae936a (coordinator: make the gc manager always report error if meet (#4119))
