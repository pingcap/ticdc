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
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
)

// gcSafepointUpdateInterval is the minimum interval that CDC can update gc safepoint
var gcSafepointUpdateInterval = 1 * time.Minute

// Manager is an interface for gc manager
type Manager interface {
	// TryUpdateGCSafePoint tries to update TiCDC service GC safepoint.
	// Manager may skip update when it thinks it is too frequent.
	// Set `forceUpdate` to force Manager update.
	TryUpdateGCSafePoint(ctx context.Context, keyspaceID uint32, keyspaceName string, checkpointTs common.Ts, forceUpdate bool) error
	CheckStaleCheckpointTs(ctx context.Context, keyspaceID uint32, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error
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

func (m *gcManager) CheckStaleCheckpointTs(
	ctx context.Context, keyspaceID uint32, changefeedID common.ChangeFeedID, checkpointTs common.Ts,
) error {
	if kerneltype.IsClassic() {
		return m.checkStaleCheckPointTsGlobal(changefeedID, checkpointTs)
	}
	return m.checkStaleCheckpointTsKeyspace(ctx, keyspaceID, changefeedID, checkpointTs)
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

func (m *gcManager) checkStaleCheckpointTsKeyspace(ctx context.Context, keyspaceID uint32, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
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
