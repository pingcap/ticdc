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
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/pingcap/tiflow/cdc/model"
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

	mc            messaging.MessageCenter
	taskScheduler threadpool.ThreadPool

	gcManager gc.Manager
	pdClient  pd.Client
	pdClock   pdutil.Clock

	// eventCh is used to receive the event from message center, basically these messages
	// are from maintainer.
	eventCh *chann.DrainableChann[*Event]
	// changefeedProgressReportCh is used to receive the changefeed progress report from the controller
	changefeedProgressReportCh chan map[common.ChangeFeedID]*changefeed.Changefeed
	// changefeedStateChangedCh is used to receive the changefeed state changed event from the controller
	changefeedStateChangedCh chan *ChangefeedStateChangeEvent

	cancel func()
	closed atomic.Bool
}

func New(node *node.Info,
	pdClient pd.Client,
	pdClock pdutil.Clock,
	backend changefeed.Backend,
	gcServiceID string,
	version int64,
	batchSize int,
	balanceCheckInterval time.Duration,
) server.Coordinator {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &coordinator{
		version:                    version,
		nodeInfo:                   node,
		gcServiceID:                gcServiceID,
		lastTickTime:               time.Now(),
		gcManager:                  gc.NewManager(gcServiceID, pdClient, pdClock),
		eventCh:                    chann.NewAutoDrainChann[*Event](),
		pdClient:                   pdClient,
		pdClock:                    pdClock,
		mc:                         mc,
		changefeedProgressReportCh: make(chan map[common.ChangeFeedID]*changefeed.Changefeed, 1024),
		changefeedStateChangedCh:   make(chan *ChangefeedStateChangeEvent, 1024),
		backend:                    backend,
	}
	// handle messages from message center
	mc.RegisterHandler(messaging.CoordinatorTopic, c.recvMessages)

	c.taskScheduler = threadpool.NewThreadPoolDefault()
	c.closed.Store(false)

	controller := NewController(
		c.version,
		c.nodeInfo,
		c.changefeedProgressReportCh,
		c.changefeedStateChangedCh,
		c.backend,
		c.eventCh,
		c.taskScheduler,
		batchSize,
		balanceCheckInterval,
		c.pdClient,
	)

	c.controller = controller

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.RegisterOwnerChangeHandler(
		string(c.nodeInfo.ID),
		func(newCoordinatorID string) {
			if newCoordinatorID != string(c.nodeInfo.ID) {
				log.Info("Coordinator changed, and I am not the coordinator, stop myself",
					zap.String("selfID", string(c.nodeInfo.ID)),
					zap.String("newCoordinatorID", newCoordinatorID))
				c.AsyncStop()
			}
		})

	return c
}

func (c *coordinator) recvMessages(_ context.Context, msg *messaging.TargetMessage) error {
	if c.closed.Load() {
		return nil
	}
	c.eventCh.In() <- &Event{message: msg}
	return nil
}

// Run spawns two goroutines to handle messages and run the coordinator.
func (c *coordinator) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	eg, cctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return c.run(cctx)
	})
	eg.Go(func() error {
		return c.runHandleEvent(cctx)
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
	gcTick := time.NewTicker(updateGCTickerInterval)

	defer gcTick.Stop()
	updateMetricsTicker := time.NewTicker(time.Second * 1)
	defer updateMetricsTicker.Stop()

	failpoint.Inject("coordinator-run-with-error", func() error {
		return errors.New("coordinator run with error")
	})
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTick.C:
			if err := c.updateGCSafepoint(ctx); err != nil {
				log.Warn("update gc safepoint failed",
					zap.Error(err))
			}
			now := time.Now()
			metrics.CoordinatorCounter.Add(float64(now.Sub(c.lastTickTime)) / float64(time.Second))
			c.lastTickTime = now
		case cfs := <-c.changefeedProgressReportCh:
			if err := c.saveCheckpointTs(ctx, cfs); err != nil {
				return errors.Trace(err)
			}
		case event := <-c.changefeedStateChangedCh:
			if err := c.handleStateChangedEvent(ctx, event); err != nil {
				return errors.Trace(err)
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

func (c *coordinator) handleStateChangedEvent(
	ctx context.Context,
	event *ChangefeedStateChangeEvent,
) error {
	cf := c.controller.getChangefeed(event.ChangefeedID)
	if cf == nil {
		log.Warn("changefeed not found", zap.String("changefeed", event.ChangefeedID.String()))
		return nil
	}
	cfInfo, err := cf.GetInfo().Clone()
	if err != nil {
		return errors.Trace(err)
	}
	cfInfo.State = event.State
	cfInfo.Error = event.err
	progress := config.ProgressNone
	if event.State == model.StateFailed || event.State == model.StateFinished {
		progress = config.ProgressStopping
	}
	if err := c.backend.UpdateChangefeed(context.Background(), cfInfo, cf.GetStatus().CheckpointTs, progress); err != nil {
		log.Error("failed to update changefeed state",
			zap.Error(err))
		return errors.Trace(err)
	}
	cf.SetInfo(cfInfo)

	switch event.State {
	case model.StateWarning:
		c.controller.operatorController.StopChangefeed(ctx, event.ChangefeedID, false)
		c.controller.updateChangefeedEpoch(ctx, event.ChangefeedID)
		c.controller.moveChangefeedToSchedulingQueue(event.ChangefeedID, false, false)
	case model.StateFailed, model.StateFinished:
		c.controller.operatorController.StopChangefeed(ctx, event.ChangefeedID, false)
	case model.StateNormal:
		log.Info("changefeed is resumed or created successfully, try to delete its safeguard gc safepoint",
			zap.String("changefeed", event.ChangefeedID.String()))
		// We need to clean its gc safepoint when changefeed is resumed or created
		gcServiceID := c.getEnsureGCServiceID(gc.EnsureGCServiceCreating)
		err := gc.UndoEnsureChangefeedStartTsSafety(ctx, c.pdClient, gcServiceID, event.ChangefeedID)
		if err != nil {
			log.Warn("failed to delete create changefeed gc safepoint", zap.Error(err))
		}
		gcServiceID = c.getEnsureGCServiceID(gc.EnsureGCServiceResuming)
		err = gc.UndoEnsureChangefeedStartTsSafety(ctx, c.pdClient, gcServiceID, event.ChangefeedID)
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
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err != nil {
		errCode, _ := errors.RFCCode(err)
		state := model.StateFailed
		if !errors.IsChangefeedGCFastFailErrorCode(errCode) {
			state = model.StateWarning
		}
		select {
		case <-ctx.Done():
			log.Warn("Failed to send state change event to stateChangedCh since context timeout, "+
				"there may be a lot of state need to be handled. Try next time",
				zap.String("changefeed", id.String()),
				zap.Error(ctx.Err()))
			return
		case c.changefeedStateChangedCh <- &ChangefeedStateChangeEvent{
			ChangefeedID: id,
			State:        state,
			err: &model.RunningError{
				Code:    string(errCode),
				Message: err.Error(),
			},
		}:
		}
	}
}

func (c *coordinator) saveCheckpointTs(ctx context.Context, cfs map[common.ChangeFeedID]*changefeed.Changefeed) error {
	statusMap := make(map[common.ChangeFeedID]uint64)
	for _, upCf := range cfs {
		reportedCheckpointTs := upCf.GetStatus().CheckpointTs
		if upCf.GetLastSavedCheckPointTs() < reportedCheckpointTs {
			statusMap[upCf.ID] = reportedCheckpointTs
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
		cf, ok := cfs[id]
		if !ok {
			continue
		}
		cf.SetLastSavedCheckPointTs(cp)
		if cf.IsMQSink() {
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
	return c.updateGCSafepoint(ctx)
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

func (c *coordinator) ListChangefeeds(ctx context.Context) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return c.controller.ListChangefeeds(ctx)
}

func (c *coordinator) GetChangefeed(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return c.controller.GetChangefeed(ctx, changefeedDisplayName)
}

func (c *coordinator) AsyncStop() {
	if c.closed.CompareAndSwap(false, true) {
		c.mc.DeRegisterHandler(messaging.CoordinatorTopic)
		c.controller.Stop()
		c.taskScheduler.Stop()
		c.eventCh.CloseAndDrain()
		c.cancel()
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

func (c *coordinator) updateGCSafepoint(
	ctx context.Context,
) error {
	minCheckpointTs := c.controller.calculateGCSafepoint()
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

// GetEnsureGCServiceID return the prefix for the gc service id when changefeed is creating
func (c *coordinator) getEnsureGCServiceID(tag string) string {
	return c.gcServiceID + tag
}
