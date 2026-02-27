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
	// TryUpdateServiceGCSafepoint tries to update TiCDC service GC safepoint.
	TryUpdateServiceGCSafepoint(ctx context.Context, checkpointTs common.Ts) error
	CheckStaleCheckpointTs(changefeedID common.ChangeFeedID, checkpointTs common.Ts) error
}

type gcManager struct {
	gcServiceID string
	pdClient    pd.Client
	pdClock     pdutil.Clock
	gcTTL       int64

	lastSafePointTs atomic.Uint64
	isTiCDCBlockGC  atomic.Bool
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
	changefeedID common.ChangeFeedID, checkpointTs common.Ts,
) error {
	return m.checkStaleCheckPointTsGlobal(changefeedID, checkpointTs)
}

func (m *gcManager) checkStaleCheckPointTsGlobal(changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	return checkStaleCheckpointTs(changefeedID, checkpointTs, m.pdClock, m.isTiCDCBlockGC.Load(), m.lastSafePointTs.Load(), m.gcTTL)
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
