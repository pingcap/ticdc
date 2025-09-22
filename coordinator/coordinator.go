// Copyright 2024 PingCAP, Inc.
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

package coordinator

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Message Flow in Coordinator:
// (from maintainer)
// External Messages         Coordinator                Controller              Storage
//      |                        |                          |                     |
//      |   ----message----->    |                          |                     |
//      |                        |                          |                     |
//      |                        |  ---event.message---->   |                     |
//      |                        |                          |                     |
//      |                        |  <---state change-----   |                     |
//      |                        |                          |                     |
//      |                        |  ----update state---------------->             |
//      |                        |                          |                     |
//      |                        |  <---checkpoint ts----   |                     |
//      |                        |                          |                     |
//      |                        |  ----save checkpoint ts------------->          |
//      |                        |                          |                     |
//
// Flow Description:
// 1. External messages arrive at Coordinator via MessageCenter
// 2. Coordinator forwards messages as events to Controller
// 3. Controller processes events and reports state changes back
// 4. Coordinator updates state in meta store
// 5. Controller reports checkpoint TS
// 6. Coordinator saves checkpoint TS to meta store

var updateGCTickerInterval = 1 * time.Minute

// coordinator implements the Coordinator interface
type coordinator struct {
	nodeInfo     *node.Info
	version      int64
	gcServiceID  string
	lastTickTime time.Time

	controller *Controller
	backend    changefeed.Backend

	mc        messaging.MessageCenter
	gcManager gc.Manager
	pdClient  pd.Client
	pdClock   pdutil.Clock

	// eventCh is used to receive the event from message center, basically these messages
	// are from maintainer.
	eventCh *chann.DrainableChann[*Event]
	// changefeedChangeCh is used to receive the changefeed change from the controller
	changefeedChangeCh chan []*ChangefeedChange

	cancel func()
	closed atomic.Bool
}

func New(node *node.Info,
	pdClient pd.Client,
	backend changefeed.Backend,
	gcServiceID string,
	version int64,
	batchSize int,
	balanceCheckInterval time.Duration,
) server.Coordinator {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &coordinator{
		version:            version,
		nodeInfo:           node,
		gcServiceID:        gcServiceID,
		lastTickTime:       time.Now(),
		gcManager:          gc.NewManager(gcServiceID, pdClient),
		eventCh:            chann.NewAutoDrainChann[*Event](),
		pdClient:           pdClient,
		pdClock:            appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		mc:                 mc,
		changefeedChangeCh: make(chan []*ChangefeedChange, 1024),
		backend:            backend,
	}
	// handle messages from message center
	mc.RegisterHandler(messaging.CoordinatorTopic, c.recvMessages)
	c.controller = NewController(
		c.version,
		c.nodeInfo,
		c.changefeedChangeCh,
		c.backend,
		c.eventCh,
		batchSize,
		balanceCheckInterval,
		c.pdClient,
	)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.RegisterOwnerChangeHandler(
		string(c.nodeInfo.ID),
		func(newCoordinatorID string) {
			if newCoordinatorID != string(c.nodeInfo.ID) {
				log.Info("Coordinator changed, and I am not the coordinator, stop myself",
					zap.String("selfID", string(c.nodeInfo.ID)),
					zap.String("newCoordinatorID", newCoordinatorID))
				c.Stop()
			}
		})

	return c
}

func (c *coordinator) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	if c.closed.Load() {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			// There is chance that:
			// 1. Just before the c.eventCh is closed, the recvMessages is called
			// 2. Then the goroutine(call it g1) that calls recvMessages is scheduled out by runtime, and the msg is in flight
			// 3. The c.eventCh is closed by another goroutine(call it g2)
			// 4. g1 is scheduled back by runtime, and the msg is sent to the closed channel
			// 5. g1 panics
			// To avoid the panic, we have two choices:
			// 1. Use a mutex to protect this function, but it will reduce the throughput
			// 2. Recover the panic, and log the error
			// We choose the second option here.
			log.Error("panic in recvMessages", zap.Any("msg", msg), zap.Any("panic", r))
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.eventCh.In() <- &Event{message: msg}
	}

	return nil
}

// Run spawns two goroutines to handle messages and run the coordinator.
func (c *coordinator) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return c.run(ctx)
	})
	eg.Go(func() error {
		return c.runHandleEvent(ctx)
	})

	eg.Go(func() error {
		return c.controller.collectMetrics(ctx)
	})

	return eg.Wait()
}

// run handles the following:
// 1. update the gc safepoint to PD
// 2. store the changefeed checkpointTs to meta store
// 3. handle the state changed event
func (c *coordinator) run(ctx context.Context) error {
	failpoint.Inject("InjectUpdateGCTickerInterval", func(val failpoint.Value) {
		updateGCTickerInterval = time.Duration(val.(int) * int(time.Millisecond))
	})

	gcTicker := time.NewTicker(updateGCTickerInterval)
	defer gcTicker.Stop()

	failpoint.Inject("coordinator-run-with-error", func() error {
		return errors.New("coordinator run with error")
	})
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTicker.C:
			if err := c.updateGCSafepoint(ctx); err != nil {
				log.Warn("update gc safepoint failed",
					zap.Error(err))
			}
			now := time.Now()
			metrics.CoordinatorCounter.Add(float64(now.Sub(c.lastTickTime)) / float64(time.Second))
			c.lastTickTime = now
		case changes := <-c.changefeedChangeCh:
			if err := c.saveCheckpointTs(ctx, changes); err != nil {
				return errors.Trace(err)
			}
			for _, change := range changes {
				if change.changeType == ChangeState || change.changeType == ChangeStateAndTs {
					if err := c.handleStateChange(ctx, change); err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
	}
}

// runHandleEvent handles messages from the other modules.
func (c *coordinator) runHandleEvent(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-c.eventCh.Out():
			c.controller.HandleEvent(event)
		}
	}
}

func (c *coordinator) handleStateChange(
	ctx context.Context,
	event *ChangefeedChange,
) error {
	cf := c.controller.getChangefeed(event.changefeedID)
	if cf == nil {
		log.Warn("changefeed not found", zap.String("changefeed", event.changefeedID.String()))
		return nil
	}
	cfInfo, err := cf.GetInfo().Clone()
	if err != nil {
		return errors.Trace(err)
	}
	cfInfo.State = event.state
	cfInfo.Error = event.err
	progress := config.ProgressNone
	if event.state == config.StateFailed || event.state == config.StateFinished {
		progress = config.ProgressStopping
	}
	if err := c.backend.UpdateChangefeed(context.Background(), cfInfo, cf.GetStatus().CheckpointTs, progress); err != nil {
		log.Error("failed to update changefeed state",
			zap.Error(err))
		return errors.Trace(err)
	}
	cf.SetInfo(cfInfo)

	switch event.state {
	case config.StateWarning:
		c.controller.operatorController.StopChangefeed(ctx, event.changefeedID, false)
		c.controller.updateChangefeedEpoch(ctx, event.changefeedID)
		c.controller.moveChangefeedToSchedulingQueue(event.changefeedID, false, false)
	case config.StateFailed, config.StateFinished:
		c.controller.operatorController.StopChangefeed(ctx, event.changefeedID, false)
	case config.StateNormal:
		log.Info("changefeed is resumed or created successfully, try to delete its safeguard gc safepoint",
			zap.String("changefeed", event.changefeedID.String()))
		// We need to clean its gc safepoint when changefeed is resumed or created
		gcServiceID := c.getEnsureGCServiceID(gc.EnsureGCServiceCreating)
		err := gc.UndoEnsureChangefeedStartTsSafety(ctx, c.pdClient, gcServiceID, event.changefeedID)
		if err != nil {
			log.Warn("failed to delete create changefeed gc safepoint", zap.Error(err))
		}
		gcServiceID = c.getEnsureGCServiceID(gc.EnsureGCServiceResuming)
		err = gc.UndoEnsureChangefeedStartTsSafety(ctx, c.pdClient, gcServiceID, event.changefeedID)
		if err != nil {
			log.Warn("failed to delete resume changefeed gc safepoint", zap.Error(err))
		}
	default:
	}
	return nil
}

// checkStaleCheckpointTs checks if the checkpointTs is stale, if it is, it will send a state change event to the stateChangedCh
func (c *coordinator) checkStaleCheckpointTs(ctx context.Context, id common.ChangeFeedID, reportedCheckpointTs uint64) {
	err := c.gcManager.CheckStaleCheckpointTs(ctx, id, reportedCheckpointTs)
	if err == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCode, _ := errors.RFCCode(err)
	state := config.StateWarning
	if errors.IsChangefeedGCFastFailErrorCode(errCode) {
		state = config.StateFailed
	}

	change := &ChangefeedChange{
		changefeedID: id,
		state:        state,
		err: &config.RunningError{
			Code:    string(errCode),
			Message: err.Error(),
		},
		changeType: ChangeState,
	}

	select {
	case <-ctx.Done():
		log.Warn("Failed to send state change event to stateChangedCh since context timeout, "+
			"there may be a lot of state need to be handled. Try next time",
			zap.String("changefeed", id.String()),
			zap.Error(ctx.Err()))
		return
	case c.changefeedChangeCh <- []*ChangefeedChange{change}:
	}
}

func (c *coordinator) saveCheckpointTs(ctx context.Context, changes []*ChangefeedChange) error {
	statusMap := make(map[common.ChangeFeedID]uint64)
	cfsMap := make(map[common.ChangeFeedID]*changefeed.Changefeed)
	for _, change := range changes {
		if change.changeType == ChangeState {
			continue
		}
		upCf := change.changefeed
		reportedCheckpointTs := upCf.GetStatus().CheckpointTs
		if upCf.GetLastSavedCheckPointTs() < reportedCheckpointTs {
			statusMap[upCf.ID] = reportedCheckpointTs
			cfsMap[upCf.ID] = upCf
			c.checkStaleCheckpointTs(ctx, upCf.ID, reportedCheckpointTs)
		}
	}
	if len(statusMap) == 0 {
		return nil
	}
	err := c.controller.backend.UpdateChangefeedCheckpointTs(ctx, statusMap)
	if err != nil {
		log.Error("failed to update checkpointTs", zap.Error(err))
		return errors.Trace(err)
	}
	// update the last saved checkpoint ts and send checkpointTs to maintainer
	for id, cp := range statusMap {
		cf := cfsMap[id]
		cf.SetLastSavedCheckPointTs(cp)
		if cf.NeedCheckpointTsMessage() {
			msg := cf.NewCheckpointTsMessage(cf.GetLastSavedCheckPointTs())
			c.sendMessages([]*messaging.TargetMessage{msg})
		}
	}
	return nil
}

func (c *coordinator) CreateChangefeed(ctx context.Context, info *config.ChangeFeedInfo) error {
	err := c.controller.CreateChangefeed(ctx, info)
	if err != nil {
		return errors.Trace(err)
	}
	// update gc safepoint after create changefeed
	return c.updateGCSafepointByChangefeed(ctx, info.ChangefeedID)
}

func (c *coordinator) RemoveChangefeed(ctx context.Context, id common.ChangeFeedID) (uint64, error) {
	return c.controller.RemoveChangefeed(ctx, id)
}

func (c *coordinator) PauseChangefeed(ctx context.Context, id common.ChangeFeedID) error {
	return c.controller.PauseChangefeed(ctx, id)
}

func (c *coordinator) ResumeChangefeed(ctx context.Context, id common.ChangeFeedID, newCheckpointTs uint64, overwriteCheckpointTs bool) error {
	return c.controller.ResumeChangefeed(ctx, id, newCheckpointTs, overwriteCheckpointTs)
}

func (c *coordinator) UpdateChangefeed(ctx context.Context, change *config.ChangeFeedInfo) error {
	return c.controller.UpdateChangefeed(ctx, change)
}

func (c *coordinator) ListChangefeeds(ctx context.Context, keyspace string) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return c.controller.ListChangefeeds(ctx, keyspace)
}

func (c *coordinator) GetChangefeed(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return c.controller.GetChangefeed(ctx, changefeedDisplayName)
}

func (c *coordinator) Stop() {
	if c.closed.CompareAndSwap(false, true) {
		c.mc.DeRegisterHandler(messaging.CoordinatorTopic)
		c.controller.Stop()
		c.cancel()
		// close eventCh after cancel, to avoid send or get event from the channel
		c.eventCh.CloseAndDrain()
	}
}

func (c *coordinator) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		err := c.mc.SendCommand(msg)
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (c *coordinator) updateGlobalGcSafepoint(ctx context.Context) error {
	minCheckpointTs := c.controller.calculateGlobalGCSafepoint()
	// check if the upstream has a changefeed, if not we should update the gc safepoint
	if minCheckpointTs == math.MaxUint64 {
		ts := c.pdClock.CurrentTime()
		minCheckpointTs = oracle.GoTimeToTS(ts)
	}
	// When the changefeed starts up, CDC will do a snapshot read at
	// (checkpointTs - 1) from TiKV, so (checkpointTs - 1) should be an upper
	// bound for the GC safepoint.
	gcSafepointUpperBound := minCheckpointTs - 1
	err := c.gcManager.TryUpdateGCSafePoint(ctx, gcSafepointUpperBound, false)
	return errors.Trace(err)
}

func (c *coordinator) updateAllKeyspaceGcBarriers(ctx context.Context) error {
	barrierMap := c.controller.calculateKeyspaceGCBarrier()

	for keyspaceName := range barrierMap {
		err := c.updateKeyspaceGcBarrier(ctx, barrierMap, keyspaceName)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *coordinator) updateKeyspaceGcBarrier(ctx context.Context, barrierMap map[string]uint64, keyspaceName string) error {
	// Obtain keyspace metadata from PD
	keyspaceManager := appcontext.GetService[keyspace.KeyspaceManager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.LoadKeyspace(ctx, keyspaceName)
	if err != nil {
		return cerror.WrapError(cerror.ErrLoadKeyspaceFailed, err)
	}
	keyspaceID := keyspaceMeta.Id

	barrierTS, ok := barrierMap[keyspaceName]
	if !ok || barrierTS == math.MaxUint64 {
		ts := c.pdClock.CurrentTime()
		barrierTS = oracle.GoTimeToTS(ts)
	}

	barrierTsUpperBound := barrierTS - 1
	err = c.gcManager.TryUpdateKeyspaceGCBarrier(ctx, keyspaceID, keyspaceName, barrierTsUpperBound, false)
	return errors.Trace(err)
}

// updateGCSafepointByChangefeed update the gc safepoint by changefeed
// On next gen, we should update the gc barrier for the specific keyspace
// Otherwise we should update the global gc safepoint
func (c *coordinator) updateGCSafepointByChangefeed(ctx context.Context, changefeedID common.ChangeFeedID) error {
	if kerneltype.IsNextGen() {
		barrierMap := c.controller.calculateKeyspaceGCBarrier()
		return c.updateKeyspaceGcBarrier(ctx, barrierMap, changefeedID.Keyspace())
	}
	return c.updateGlobalGcSafepoint(ctx)
}

// updateGCSafepoint update the gc safepoint
// On next gen, we should update the gc barrier for all keyspaces
// Otherwise we should update the global gc safepoint
func (c *coordinator) updateGCSafepoint(ctx context.Context) error {
	if kerneltype.IsNextGen() {
		return c.updateAllKeyspaceGcBarriers(ctx)
	}
	return c.updateGlobalGcSafepoint(ctx)
}

// GetEnsureGCServiceID return the prefix for the gc service id when changefeed is creating
func (c *coordinator) getEnsureGCServiceID(tag string) string {
	return c.gcServiceID + tag
}
