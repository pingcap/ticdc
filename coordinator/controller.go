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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	coscheduler "github.com/pingcap/ticdc/coordinator/scheduler"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/bootstrap"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	bootstrapperID                = "coordinator"
	nodeChangeHandlerID           = "coordinator-controller"
	createChangefeedMaxRetry      = 10
	createChangefeedRetryInterval = 5 * time.Second
)

// Controller schedules and balance changefeeds, there are 3 main components:
//  1. scheduler: generate operators for handling different scheduling tasks.
//  2. operatorController: manage all operators and execute them periodically.
//  3. changefeedDB: store all changefeeds info and their status in memory.
//  4. backend: the durable storage for storing changefeed metadata.
type Controller struct {
	version int64

	selfNode *node.Info

	pdClient           pd.Client
	pdClock            pdutil.Clock
	scheduler          *scheduler.Controller
	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	backend            changefeed.Backend
	eventCh            *chann.DrainableChann[*Event]

	// initialized is true after all necessary resources ready,
	// it's not affected by new node join the cluster.
	initialized  *atomic.Bool
	bootstrapper *bootstrap.Bootstrapper[heartbeatpb.CoordinatorBootstrapResponse]

	nodeChanged struct {
		sync.Mutex
		changed bool
	}
	nodeManager *watcher.NodeManager

	taskScheduler    threadpool.ThreadPool
	taskHandlerMutex sync.Mutex // protect taskHandlers
	taskHandlers     []*threadpool.TaskHandle
	messageCenter    messaging.MessageCenter

	changefeedChangeCh chan []*changefeedChange
	apiLock            sync.RWMutex
}

type changefeedChange struct {
	changefeedID common.ChangeFeedID
	changefeed   *changefeed.Changefeed
	state        config.FeedState
	changeType   ChangeType
	err          *config.RunningError
}

func newChangefeedChange(changefeed *changefeed.Changefeed, state config.FeedState, changeType ChangeType, err *config.RunningError) *changefeedChange {
	return &changefeedChange{
		changefeedID: changefeed.ID,
		changefeed:   changefeed,
		state:        state,
		changeType:   changeType,
		err:          err,
	}
}

func NewController(
	version int64,
	selfNode *node.Info,
	changefeedChangeCh chan []*changefeedChange,
	backend changefeed.Backend,
	eventCh *chann.DrainableChann[*Event],
	batchSize int,
	balanceInterval time.Duration,
	pdClient pd.Client,
) *Controller {
	changefeedDB := changefeed.NewChangefeedDB(version)

	oc := operator.NewOperatorController(selfNode, changefeedDB, backend, batchSize)
	c := &Controller{
		version:     version,
		selfNode:    selfNode,
		initialized: atomic.NewBool(false),
		scheduler: scheduler.NewController(map[string]scheduler.Scheduler{
			scheduler.BasicScheduler: coscheduler.NewBasicScheduler(
				selfNode.ID.String(),
				batchSize,
				oc,
				changefeedDB,
			),
			scheduler.BalanceScheduler: coscheduler.NewBalanceScheduler(
				selfNode.ID.String(),
				batchSize,
				oc,
				changefeedDB,
				balanceInterval,
			),
		}),
		eventCh:            eventCh,
		operatorController: oc,
		messageCenter:      appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		changefeedDB:       changefeedDB,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		taskScheduler:      threadpool.NewThreadPoolDefault(),
		backend:            backend,
		changefeedChangeCh: changefeedChangeCh,
		pdClient:           pdClient,
		pdClock:            appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
	}
	c.nodeChanged.changed = false

	c.bootstrapper = bootstrap.NewBootstrapper[heartbeatpb.CoordinatorBootstrapResponse](
		bootstrapperID,
		c.newBootstrapMessage,
	)

	// detect the capture changes
	c.nodeManager.RegisterNodeChangeHandler(
		nodeChangeHandlerID,
		func(allNodes map[node.ID]*node.Info) {
			c.nodeChanged.Lock()
			defer c.nodeChanged.Unlock()
			c.nodeChanged.changed = true
		},
	)

	nodes := c.nodeManager.GetAliveNodes()
	added, _, requests, _ := c.bootstrapper.HandleNodesChange(nodes)
	log.Info("coordinator bootstrap initial nodes",
		zap.Int("addedCount", len(added)), zap.Any("addedNodes", nodes))

	for _, req := range requests {
		err := c.messageCenter.SendCommand(req)
		if err != nil {
			log.Warn("send request failed when boostrapping initial node, will be resent later",
				zap.Any("targetNode", req.To), zap.Error(err))
		}
	}

	c.submitPeriodTask()
	return c
}

func (c *Controller) collectMetrics(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			metrics.ChangefeedStateGauge.WithLabelValues("Total").Set(float64(c.changefeedDB.GetSize()))
			metrics.ChangefeedStateGauge.WithLabelValues("Working").Set(float64(c.changefeedDB.GetReplicatingSize()))
			metrics.ChangefeedStateGauge.WithLabelValues("Scheduling").Set(float64(c.operatorController.OperatorSize()))
			metrics.ChangefeedStateGauge.WithLabelValues("Absent").Set(float64(c.changefeedDB.GetAbsentSize()))
			metrics.ChangefeedStateGauge.WithLabelValues("Stopped").Set(float64(c.changefeedDB.GetStoppedSize()))

			c.changefeedDB.Foreach(func(cf *changefeed.Changefeed) {
				info := cf.GetInfo()
				keyspace := info.ChangefeedID.Keyspace()
				name := info.ChangefeedID.Name()
				metrics.ChangefeedStatusGauge.WithLabelValues(keyspace, name).Set(float64(info.State.ToInt()))

				// don't update checkpoint ts and checkpoint ts lag for stopped changefeed
				if info.State == config.StateStopped {
					return
				}

				pdPhysicalTime := oracle.GetPhysical(c.pdClock.CurrentTime())
				phyCkpTs := oracle.ExtractPhysical(cf.GetLastSavedCheckPointTs())
				lag := float64(pdPhysicalTime-phyCkpTs) / 1e3
				metrics.ChangefeedCheckpointTsGauge.WithLabelValues(keyspace, name).Set(float64(phyCkpTs))
				metrics.ChangefeedCheckpointTsLagGauge.WithLabelValues(keyspace, name).Set(lag)
			})
		}
	}
}

// HandleEvent implements the event-driven process mode
func (c *Controller) HandleEvent(event *Event) {
	if event == nil {
		return
	}

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > time.Second {
			log.Info("coordinator is slow, handle a event takes too long",
				zap.Int("type", event.eventType),
				zap.Duration("duration", duration))
		}
	}()

	// Before processing the event, we need to check the online/offline nodes,
	// the following logic is based on whether the node changed.
	c.checkOnNodeChanged()

	switch event.eventType {
	case EventMessage:
		c.onMessage(event.message)
	case EventPeriod:
		c.onPeriodTask()
	}
}

func (c *Controller) checkOnNodeChanged() {
	c.nodeChanged.Lock()
	defer c.nodeChanged.Unlock()

	if c.nodeChanged.changed {
		c.onNodeChanged()
		c.nodeChanged.changed = false
	}
}

func (c *Controller) onPeriodTask() {
	// resend bootstrap message
	requests := c.bootstrapper.ResendBootstrapMessage()
	for _, req := range requests {
		_ = c.messageCenter.SendCommand(req)
	}
}

func (c *Controller) onMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeCoordinatorBootstrapResponse:
		c.onMaintainerBootstrapResponse(msg)
	case messaging.TypeMaintainerHeartbeatRequest:
		if c.bootstrapper.AllNodesReady() {
			req := msg.Message[0].(*heartbeatpb.MaintainerHeartbeat)
			c.handleMaintainerStatus(msg.From, req.Statuses)
		}
	case messaging.TypeLogCoordinatorResolvedTsResponse:
		c.onLogCoordinatorReportResolvedTs(msg)
	default:
		log.Warn("unknown message type, ignore it",
			zap.String("type", msg.Type.String()),
			zap.Any("message", msg.Message))
	}
}

func (c *Controller) onLogCoordinatorReportResolvedTs(msg *messaging.TargetMessage) {
	resp := msg.Message[0].(*heartbeatpb.LogCoordinatorResolvedTsResponse)
	c.changefeedDB.UpdateLogCoordinatorResolvedTsByID(common.NewChangefeedIDFromPB(resp.ChangefeedID), resp.ResolvedTs)
}

func (c *Controller) RequestResolvedTsFromLogCoordinator(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) {
	// get the old resolved ts
	oldTs := c.changefeedDB.GetLogCoordinatorResolvedTsByName(changefeedDisplayName)

	// request all log coordinators to report resolved ts
	changefeedID := c.changefeedDB.GetChangefeedIDByName(changefeedDisplayName)
	ids := c.nodeManager.GetAliveNodeIDs()
	for _, id := range ids {
		c.messageCenter.SendEvent(messaging.NewSingleTargetMessage(id, messaging.LogCoordinatorTopic, &heartbeatpb.LogCoordinatorResolvedTsRequest{
			ChangefeedID: changefeedID.ToPB(),
		}))
	}

	// wait for some time to get the resolved ts
	waitTimer := time.NewTimer(2 * time.Second)
	defer waitTimer.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waitTimer.C:
			log.Warn("timeout waiting for log coordinator resolved ts",
				zap.String("changefeed", changefeedDisplayName.String()),
				zap.Uint64("oldTs", oldTs))
			return
		case <-ticker.C:
			newTs := c.changefeedDB.GetLogCoordinatorResolvedTsByName(changefeedDisplayName)
			if newTs != oldTs {
				log.Debug("received log coordinator resolved ts",
					zap.String("changefeed", changefeedDisplayName.String()),
					zap.Uint64("oldTs", oldTs),
					zap.Uint64("newTs", newTs))
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Controller) onNodeChanged() {
	addedNodes, removedNodes, requests, responses := c.bootstrapper.HandleNodesChange(c.nodeManager.GetAliveNodes())
	log.Info("controller detects node changed",
		zap.Int("addedCount", len(addedNodes)),
		zap.Int("removedCount", len(removedNodes)),
		zap.Any("addedNodes", addedNodes),
		zap.Any("removedNodes", removedNodes))

	for _, n := range removedNodes {
		c.RemoveNode(n)
	}
	for _, req := range requests {
		err := c.messageCenter.SendCommand(req)
		if err != nil {
			log.Warn("send request failed when boostrapping newly added node, will be resent later",
				zap.Any("targetNode", req.To), zap.Error(err))
		}
	}
	c.handleBootstrapResponses(responses)
}

func (c *Controller) onMaintainerBootstrapResponse(req *messaging.TargetMessage) {
	response := req.Message[0].(*heartbeatpb.CoordinatorBootstrapResponse)
	log.Info("controller received maintainer bootstrap response",
		zap.Stringer("node", req.From),
		zap.Int("maintainerCount", len(response.Statuses)))
	responses := c.bootstrapper.HandleBootstrapResponse(req.From, response)
	c.handleBootstrapResponses(responses)
}

type remoteMaintainer struct {
	nodeID node.ID
	status *heartbeatpb.MaintainerStatus
}

func (c *Controller) handleBootstrapResponses(responses map[node.ID]*heartbeatpb.CoordinatorBootstrapResponse) {
	if c.initialized.Load() || responses == nil {
		return
	}
	log.Info("all new nodes bootstrap response received",
		zap.Int("newNodeCount", len(responses)))
	// runningCfs are changefeeds that already running on other nodes
	runningCfs := make(map[common.ChangeFeedID]remoteMaintainer)
	for nodeID, resp := range responses {
		for _, status := range resp.Statuses {
			changeFeedID := common.NewChangefeedIDFromPB(status.ChangefeedID)
			if old, ok := runningCfs[changeFeedID]; ok {
				log.Panic("maintainer runs on multiple node",
					zap.Stringer("changefeedID", changeFeedID),
					zap.Stringer("oldNode", old.nodeID),
					zap.Stringer("newNode", nodeID))
			}
			runningCfs[changeFeedID] = remoteMaintainer{
				nodeID: nodeID,
				status: status,
			}
		}
	}
	c.finishBootstrap(runningCfs)
}

// handleMaintainerStatus handle the status report from the maintainers
func (c *Controller) handleMaintainerStatus(from node.ID, statusList []*heartbeatpb.MaintainerStatus) {
	changes := make([]*changefeedChange, 0, len(statusList))
	for _, status := range statusList {
		cfID := common.NewChangefeedIDFromPB(status.ChangefeedID)
		change := c.handleSingleMaintainerStatus(from, status, cfID)
		if change != nil {
			changes = append(changes, change)
		}
	}

	// Try to send updated changefeeds without blocking
	select {
	case c.changefeedChangeCh <- changes:
	default:
	}
}

func (c *Controller) handleSingleMaintainerStatus(
	from node.ID,
	status *heartbeatpb.MaintainerStatus,
	cfID common.ChangeFeedID,
) *changefeedChange {
	// Update the operator status first
	c.operatorController.UpdateOperatorStatus(cfID, from, status)

	cf := c.getChangefeed(cfID)
	if cf == nil {
		c.handleNonExistentChangefeed(cfID, from, status)
		return nil
	}

	if !c.validateMaintainerNode(cf, from, cfID) {
		return nil
	}

	change := c.updateChangefeedStatus(cf, cfID, status)
	return change
}

func (c *Controller) handleNonExistentChangefeed(
	cfID common.ChangeFeedID,
	from node.ID,
	status *heartbeatpb.MaintainerStatus,
) {
	// If the changefeed is not in changefeedDB, and the maintainer is not working, just ignore it
	if status.State != heartbeatpb.ComponentState_Working {
		return
	}

	if op := c.operatorController.GetOperator(cfID); op == nil {
		log.Warn("no changefeed found and no operator for it, removing from maintainer",
			zap.Stringer("changefeed", cfID),
			zap.Stringer("sourceNode", from),
			zap.String("status", common.FormatMaintainerStatus(status)))

		keyspaceID := c.getChangefeed(cfID).GetKeyspaceID()

		// Remove working changefeed from maintainer if it's not in changefeedDB
		_ = c.messageCenter.SendCommand(changefeed.RemoveMaintainerMessage(keyspaceID, cfID, from, true, true))
	}
}

func (c *Controller) validateMaintainerNode(
	cf *changefeed.Changefeed,
	from node.ID,
	cfID common.ChangeFeedID,
) bool {
	nodeID := cf.GetNodeID()
	if nodeID == "" {
		return false
	}

	if nodeID != from {
		log.Warn("remote changefeed maintainer nodeID mismatch with local record",
			zap.Stringer("changefeed", cfID),
			zap.Stringer("localNode", nodeID),
			zap.Stringer("remoteNode", from))
		return false
	}
	return true
}

func (c *Controller) updateChangefeedStatus(
	cf *changefeed.Changefeed,
	cfID common.ChangeFeedID,
	status *heartbeatpb.MaintainerStatus,
) *changefeedChange {
	changed, state, err := cf.UpdateStatus(status)
	var runningErr *config.RunningError
	if err != nil {
		runningErr = &config.RunningError{
			Time:    time.Now(),
			Addr:    err.Node,
			Code:    err.Code,
			Message: err.Message,
		}
	}

	changeType := ChangeTs
	if changed {
		changeType = ChangeStateAndTs
		log.Info("changefeed status changed",
			zap.Stringer("changefeed", cfID),
			zap.String("state", string(state)),
			zap.Stringer("error", err))
	}

	change := newChangefeedChange(cf, state, changeType, runningErr)
	return change
}

// finishBootstrap is called when all nodes have sent bootstrap response
// It will load all changefeeds from metastore, and compare with running changefeeds
// Then initialize the changefeeds that are not running on other nodes
// And construct all changefeeds state in memory.
func (c *Controller) finishBootstrap(runningChangefeeds map[common.ChangeFeedID]remoteMaintainer) {
	// load all changefeeds from metastore, and check if the changefeed is already in workingMap
	allChangefeeds, err := c.backend.GetAllChangefeeds(context.Background())
	if err != nil {
		log.Panic("load all changefeeds failed", zap.Error(err))
	}

	// Register keyspace
	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	registeredKeyspace := make(map[string]struct{})
	ctx := context.Background()
	for id := range allChangefeeds {
		if _, ok := registeredKeyspace[id.Keyspace()]; ok {
			continue
		}

		cfInfo, _, err := c.GetChangefeed(ctx, id.DisplayName)
		if err != nil {
			log.Error("get changefeed failed", zap.Any("changefeed", id), zap.Error(err))
			continue
		}

		err = schemaStore.RegisterKeyspace(ctx, common.KeyspaceMeta{
			ID:   cfInfo.KeyspaceID,
			Name: id.Keyspace(),
		})
		if err != nil {
			log.Error("RegisterKeyspace failed", zap.String("keyspace", id.Keyspace()), zap.Error(err))
		}
		registeredKeyspace[id.Keyspace()] = struct{}{}
	}

	log.Info("load all changefeeds", zap.Int("size", len(allChangefeeds)))
	// Compare all changefeeds and running changefeeds, and add them to changefeedDB
	for cfID, cfMeta := range allChangefeeds {
		rm, ok := runningChangefeeds[cfID]
		if !ok {
			// Configuration items for compatibility with older versions
			cfMeta.Info.VerifyAndComplete()
			// The changefeed is not running on other nodes, add it to changefeedDB.
			// We will create this changefeed later.
			cf := changefeed.NewChangefeed(cfID, cfMeta.Info, cfMeta.Status.CheckpointTs, false)
			if shouldRunChangefeed(cf.GetInfo().State) {
				c.changefeedDB.AddAbsentChangefeed(cf)
			} else {
				c.changefeedDB.AddStoppedChangefeed(cf)
			}
		} else {
			log.Info("changefeed maintainer already running in other server",
				zap.String("changefeed", cfID.String()),
				zap.String("node", rm.nodeID.String()),
				zap.String("status", common.FormatMaintainerStatus(rm.status)))
			cf := changefeed.NewChangefeed(cfID, cfMeta.Info, rm.status.CheckpointTs, false)
			c.changefeedDB.AddReplicatingMaintainer(cf, rm.nodeID)
			delete(runningChangefeeds, cfID)
		}

		// check if the changefeed is stopping or removing, we need to stop all dispatchers completely
		switch cfMeta.Status.Progress {
		case config.ProgressStopping, config.ProgressRemoving:
			remove := cfMeta.Status.Progress == config.ProgressRemoving
			c.operatorController.StopChangefeed(context.Background(), cfID, remove)
			log.Info("stop changefeed when bootstrapping", zap.String("changefeed", cfID.String()), zap.Any("meta", cfMeta))
		}
	}

	// Remove the changefeeds that are not in allChangefeeds, there are stale changefeeds.
	for id, rm := range runningChangefeeds {
		log.Warn("maintainer not found in local, remove it",
			zap.String("changefeed", id.Name()),
			zap.String("node", rm.nodeID.String()),
		)
		keyspaceID := c.getChangefeed(id).GetKeyspaceID()
		_ = c.messageCenter.SendCommand(changefeed.RemoveMaintainerMessage(keyspaceID, id, rm.nodeID, true, true))
	}

	// start operator and scheduler
	c.taskHandlerMutex.Lock()
	defer c.taskHandlerMutex.Unlock()
	c.taskHandlers = append(c.taskHandlers, c.scheduler.Start(c.taskScheduler)...)
	operatorControllerHandle := c.taskScheduler.Submit(c.operatorController, time.Now())
	c.taskHandlers = append(c.taskHandlers, operatorControllerHandle)
	c.initialized.Store(true)
	log.Info("coordinator bootstrapped", zap.Any("nodeID", c.selfNode.ID))
}

func (c *Controller) Stop() {
	c.taskHandlerMutex.Lock()
	for _, h := range c.taskHandlers {
		h.Cancel()
	}
	c.taskHandlerMutex.Unlock()
	c.taskScheduler.Stop()
}

func (c *Controller) CreateChangefeed(ctx context.Context, info *config.ChangeFeedInfo) error {
	if !c.initialized.Load() {
		return errors.New("not initialized, wait a moment")
	}

	c.apiLock.Lock()
	defer c.apiLock.Unlock()
	old := c.changefeedDB.GetByChangefeedDisplayName(info.ChangefeedID.DisplayName)
	if old != nil {
		return errors.New("changefeed already exists")
	}

	// remove changefeed is async action, so when we create the same changefeed just when we remove the changefeed
	// the remove changefeed may not finished, so we need to wait a moment
	count := 0
	for count < createChangefeedMaxRetry {
		ok := c.operatorController.HasOperator(info.ChangefeedID.DisplayName)
		if !ok {
			break
		}
		log.Warn("changefeed is in scheduling, wait a moment", zap.String("changefeed", info.ChangefeedID.DisplayName.String()))
		time.Sleep(createChangefeedRetryInterval)
		count += 1
	}
	if count >= createChangefeedMaxRetry {
		return errors.New("changefeed is still in scheduling, please try again later")
	}

	// generate a unique changefeed epoch
	info.Epoch = pdutil.GenerateChangefeedEpoch(ctx, c.pdClient)
	err := c.backend.CreateChangefeed(ctx, info)
	if err != nil {
		return errors.Trace(err)
	}
	c.changefeedDB.AddAbsentChangefeed(changefeed.NewChangefeed(info.ChangefeedID, info, info.StartTs, true))
	return nil
}

func (c *Controller) RemoveChangefeed(ctx context.Context, id common.ChangeFeedID) (uint64, error) {
	c.apiLock.Lock()
	defer c.apiLock.Unlock()

	cf := c.changefeedDB.GetByID(id)
	if cf == nil {
		return 0, errors.New("changefeed not found")
	}
	err := c.backend.SetChangefeedProgress(ctx, id, config.ProgressRemoving)
	if err != nil {
		return 0, errors.Trace(err)
	}
	op := c.operatorController.StopChangefeed(ctx, id, true)
	count := 0
	for {
		if op.IsFinished() {
			break
		}

		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for stop changefeed operator finished", zap.Int("count", count), zap.Any("id", id))
	}
	return cf.GetStatus().CheckpointTs, nil
}

func (c *Controller) PauseChangefeed(ctx context.Context, id common.ChangeFeedID) error {
	c.apiLock.Lock()
	defer c.apiLock.Unlock()

	cf := c.changefeedDB.GetByID(id)
	if cf == nil {
		return errors.New("changefeed not found")
	}
	if err := c.backend.PauseChangefeed(ctx, id); err != nil {
		return err
	}

	clone, err := cf.GetInfo().Clone()
	if err != nil {
		return err
	}
	clone.State = config.StateStopped
	cf.SetInfo(clone)
	op := c.operatorController.StopChangefeed(ctx, id, false)

	count := 0
	for {
		if op.IsFinished() {
			break
		}

		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for stop changefeed operator finished", zap.Int("count", count), zap.Any("id", id))
	}
	return nil
}

// ResumeChangefeed resumes a changefeed, it will be call by HTTP API
func (c *Controller) ResumeChangefeed(
	ctx context.Context,
	id common.ChangeFeedID,
	newCheckpointTs uint64,
	overwriteCheckpointTs bool,
) error {
	c.apiLock.Lock()
	defer c.apiLock.Unlock()

	cf := c.changefeedDB.GetByID(id)
	if cf == nil {
		return errors.New("changefeed not found")
	}
	if err := c.backend.ResumeChangefeed(ctx, id, newCheckpointTs); err != nil {
		return err
	}

	clone, err := cf.GetInfo().Clone()
	if err != nil {
		return err
	}

	clone.State = config.StateNormal
	clone.Epoch = pdutil.GenerateChangefeedEpoch(ctx, c.pdClient)
	cf.SetInfo(clone)

	status := cf.GetClonedStatus()
	status.CheckpointTs = newCheckpointTs
	_, _, runningErr := cf.ForceUpdateStatus(status)
	if runningErr != nil {
		return errors.New(runningErr.Message)
	}
	if overwriteCheckpointTs {
		cf.SetLastSavedCheckPointTs(newCheckpointTs)
	}
	c.moveChangefeedToSchedulingQueue(id, true, overwriteCheckpointTs)
	return nil
}

func (c *Controller) UpdateChangefeed(ctx context.Context, change *config.ChangeFeedInfo) error {
	c.apiLock.Lock()
	defer c.apiLock.Unlock()

	cf := c.changefeedDB.GetByID(change.ChangefeedID)
	if cf == nil {
		return errors.New("changefeed not found")
	}
	progress := config.ProgressNone
	state := cf.GetInfo().State
	if state == config.StateFailed || state == config.StateFinished {
		progress = config.ProgressStopping
	}
	if err := c.backend.UpdateChangefeed(ctx, change, cf.GetStatus().CheckpointTs, progress); err != nil {
		return errors.Trace(err)
	}
	c.changefeedDB.ReplaceStoppedChangefeed(change)
	return nil
}

func (c *Controller) ListChangefeeds(_ context.Context, keyspace string) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	c.apiLock.RLock()
	defer c.apiLock.RUnlock()

	cfs := c.changefeedDB.GetAllChangefeedsByKeyspace(keyspace)
	infos := make([]*config.ChangeFeedInfo, 0, len(cfs))
	statuses := make([]*config.ChangeFeedStatus, 0, len(cfs))
	for _, cf := range cfs {
		infos = append(infos, cf.GetInfo())
		statuses = append(statuses, &config.ChangeFeedStatus{CheckpointTs: cf.GetStatus().CheckpointTs})
	}
	return infos, statuses, nil
}

func (c *Controller) GetChangefeed(
	_ context.Context,
	changefeedDisplayName common.ChangeFeedDisplayName,
) (
	*config.ChangeFeedInfo,
	*config.ChangeFeedStatus,
	error,
) {
	c.apiLock.RLock()
	defer c.apiLock.RUnlock()

	cf := c.changefeedDB.GetByChangefeedDisplayName(changefeedDisplayName)
	if cf == nil {
		return nil, nil, errors.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedDisplayName.Name)
	}

	maintainerID := cf.GetNodeID()
	nodeInfo := c.nodeManager.GetNodeInfo(maintainerID)
	maintainerAddr := ""
	if nodeInfo != nil {
		maintainerAddr = nodeInfo.AdvertiseAddr
	}
	status := &config.ChangeFeedStatus{CheckpointTs: cf.GetStatus().CheckpointTs, LastSyncedTs: cf.GetStatus().LastSyncedTs, LogCoordinatorResolvedTs: cf.GetLogCoordinatorResolvedTs()}
	status.SetMaintainerAddr(maintainerAddr)
	return cf.GetInfo(), status, nil
}

// getChangefeed returns the changefeed by id, return nil if not found
func (c *Controller) getChangefeed(id common.ChangeFeedID) *changefeed.Changefeed {
	return c.changefeedDB.GetByID(id)
}

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	c.operatorController.OnNodeRemoved(id)
}

func (c *Controller) submitPeriodTask() {
	task := func() time.Time {
		c.eventCh.In() <- &Event{eventType: EventPeriod}
		return time.Now().Add(time.Millisecond * 500)
	}
	periodTaskhandler := c.taskScheduler.SubmitFunc(task, time.Now().Add(time.Millisecond*500))
	c.taskHandlers = append(c.taskHandlers, periodTaskhandler)
}

func (c *Controller) newBootstrapMessage(id node.ID, addr string) *messaging.TargetMessage {
	log.Info("send coordinator bootstrap request", zap.Any("nodeID", id), zap.String("nodeAddr", addr))
	return messaging.NewSingleTargetMessage(
		id,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

func (c *Controller) updateChangefeedEpoch(ctx context.Context, id common.ChangeFeedID) {
	cf := c.changefeedDB.GetByID(id)
	if cf == nil {
		log.Warn("changefeed not found, skip updating epoch", zap.String("changefeed", id.String()))
		return
	}
	clonedInfo, err := cf.GetInfo().Clone()
	if err != nil {
		log.Panic("clone changefeed info failed", zap.String("changefeed", id.String()), zap.Error(err))
	}
	clonedInfo.Epoch = pdutil.GenerateChangefeedEpoch(ctx, c.pdClient)
	cf.SetInfo(clonedInfo)
}

// moveChangefeedToSchedulingQueue moves a changefeed to scheduling queue
// It will set a new epoch for the changefeed before moving it to scheduling queue
func (c *Controller) moveChangefeedToSchedulingQueue(
	id common.ChangeFeedID,
	resetBackoff bool,
	overwriteCheckpointTs bool,
) {
	c.changefeedDB.MoveToSchedulingQueue(id, resetBackoff, overwriteCheckpointTs)
}

func (c *Controller) calculateGlobalGCSafepoint() uint64 {
	return c.changefeedDB.CalculateGlobalGCSafepoint()
}

func (c *Controller) calculateKeyspaceGCBarrier() map[common.KeyspaceMeta]uint64 {
	return c.changefeedDB.CalculateKeyspaceGCBarrier()
}

func shouldRunChangefeed(state config.FeedState) bool {
	switch state {
	case config.StateStopped, config.StateFailed, config.StateFinished:
		return false
	}
	return true
}
