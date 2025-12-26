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

package maintainer

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/bootstrap"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/redo"
	pkgReplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	periodEventInterval = time.Millisecond * 100
	periodRedoInterval  = time.Second * 1
)

// Maintainer is response for handle changefeed replication tasks. Maintainer should:
// 1. schedule tables to dispatcher manager
// 2. calculate changefeed checkpoint ts
// 3. send changefeed status to coordinator
// 4. handle heartbeat reported by dispatcher
//
// There are four threads in maintainer:
// 1. controller thread , handled in dynstream, it handles the main logic of the maintainer, like barrier, heartbeat
// 2. scheduler thread, handled in threadpool, it schedules the tables to dispatcher manager
// 3. operator controller thread, handled in threadpool, it runs the operators
// 4. checker controller, handled in threadpool, it runs the checkers to dynamically adjust the schedule
// all threads are read/write information from/to the ReplicationDB
type Maintainer struct {
	changefeedID common.ChangeFeedID
	info         *config.ChangeFeedInfo
	selfNode     *node.Info
	controller   *Controller

	pdClock pdutil.Clock
	eventCh *chann.DrainableChann[*Event]

	mc messaging.MessageCenter

	watermark struct {
		mu sync.RWMutex
		*heartbeatpb.Watermark
	}

	checkpointTsByCapture *WatermarkCaptureMap

	scheduleState atomic.Int32
	bootstrapper  *bootstrap.Bootstrapper[heartbeatpb.MaintainerBootstrapResponse]

	removed *atomic.Bool

	// initialized is true after all necessary resources ready,
	// it's not affected by new node join the cluster.
	initialized      atomic.Bool
	postBootstrapMsg *heartbeatpb.MaintainerPostBootstrapRequest

	// startCheckpointTs is the initial checkpointTs when the maintainer is created.
	// It is sent to dispatcher managers during bootstrap to initialize their
	// checkpointTs.
	startCheckpointTs uint64
	enableRedo        bool
	// redoMetaTs is the redo meta unflushed ts to forward
	redoMetaTs *heartbeatpb.RedoMetaMessage
	// redoResolvedTs is the redo meta flushed resolvedTs
	redoResolvedTs  uint64
	redoDDLSpan     *replica.SpanReplication
	redoTsByCapture *WatermarkCaptureMap

	// ddlSpan represents the table trigger event dispatcher that handles DDL events.
	// This dispatcher is always created on the same node as the maintainer and has a
	// 1:1 relationship with it. If a maintainer fails and is recreated on another node,
	// a new table trigger event dispatcher must also be created on that node.
	ddlSpan *replica.SpanReplication

	nodeManager *watcher.NodeManager
	// closedNodes is used to record the nodes that dispatcherManager is closed
	closedNodes map[node.ID]struct{}

	// statusChanged is used to notify the maintainer manager to send heartbeat to coordinator
	// to report the changefeed's status.
	statusChanged *atomic.Bool

	nodeChanged struct {
		// We use a mutex to protect the nodeChanged field
		// because it is accessed by multiple goroutines.
		// Note: a atomic.Bool is not enough here, because we need to
		// protect the nodeChange from being changed when onNodeChanged() is running.
		sync.Mutex
		changed bool
	}

	lastReportTime time.Time

	removing        atomic.Bool
	cascadeRemoving atomic.Bool
	// the changefeed is removed, notify the dispatcher manager to clear ddl_ts table
	changefeedRemoved atomic.Bool

	lastPrintStatusTime time.Time
	// lastCheckpointTsTime time.Time

	// newChangefeed indicates if this is a fresh changefeed instance:
	// - true: when the changefeed is newly created or resumed with an overwritten checkpointTs
	// - false: when the changefeed is moved between nodes or restarted normally
	newChangefeed bool

	runningErrors struct {
		sync.Mutex
		m map[node.ID]*heartbeatpb.RunningError
	}

	cancel context.CancelFunc

	checkpointTsGauge    prometheus.Gauge
	checkpointTsLagGauge prometheus.Gauge

	resolvedTsGauge    prometheus.Gauge
	resolvedTsLagGauge prometheus.Gauge

	scheduledTaskGauge  prometheus.Gauge
	spanCountGauge      prometheus.Gauge
	tableCountGauge     prometheus.Gauge
	handleEventDuration prometheus.Observer

	redoScheduledTaskGauge prometheus.Gauge
	redoSpanCountGauge     prometheus.Gauge
	redoTableCountGauge    prometheus.Gauge
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID common.ChangeFeedID,
	conf *config.SchedulerConfig,
	info *config.ChangeFeedInfo,
	selfNode *node.Info,
	taskScheduler threadpool.ThreadPool,
	checkpointTs uint64,
	newChangefeed bool,
	keyspaceID uint32,
) *Maintainer {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	tableTriggerEventDispatcherID, ddlSpan := newDDLSpan(keyspaceID, cfID, checkpointTs, selfNode, common.DefaultMode)
	var redoDDLSpan *replica.SpanReplication
	enableRedo := redo.IsConsistentEnabled(util.GetOrZero(info.Config.Consistent.Level))
	if enableRedo {
		_, redoDDLSpan = newDDLSpan(keyspaceID, cfID, checkpointTs, selfNode, common.RedoMode)
	}

	refresher := replica.NewRegionCountRefresher(cfID, util.GetOrZero(info.Config.Scheduler.RegionCountRefreshInterval))

	var (
		keyspaceName = cfID.Keyspace()
		name         = cfID.Name()
	)
	keyspaceMeta := common.KeyspaceMeta{
		ID:   keyspaceID,
		Name: keyspaceName,
	}
	m := &Maintainer{
		changefeedID:      cfID,
		selfNode:          selfNode,
		eventCh:           chann.NewAutoDrainChann[*Event](),
		startCheckpointTs: checkpointTs,
		controller: NewController(cfID, checkpointTs, taskScheduler,
			info.Config, ddlSpan, redoDDLSpan, conf.AddTableBatchSize, time.Duration(conf.CheckBalanceInterval), refresher, keyspaceMeta, enableRedo),
		mc:                    mc,
		removed:               atomic.NewBool(false),
		nodeManager:           nodeManager,
		closedNodes:           make(map[node.ID]struct{}),
		statusChanged:         atomic.NewBool(true),
		info:                  info,
		pdClock:               appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		ddlSpan:               ddlSpan,
		redoDDLSpan:           redoDDLSpan,
		checkpointTsByCapture: newWatermarkCaptureMap(),
		redoTsByCapture:       newWatermarkCaptureMap(),
		newChangefeed:         newChangefeed,
		enableRedo:            enableRedo,

		checkpointTsGauge:    metrics.MaintainerCheckpointTsGauge.WithLabelValues(keyspaceName, name),
		checkpointTsLagGauge: metrics.MaintainerCheckpointTsLagGauge.WithLabelValues(keyspaceName, name),
		resolvedTsGauge:      metrics.MaintainerResolvedTsGauge.WithLabelValues(keyspaceName, name),
		resolvedTsLagGauge:   metrics.MaintainerResolvedTsLagGauge.WithLabelValues(keyspaceName, name),

		scheduledTaskGauge:  metrics.ScheduleTaskGauge.WithLabelValues(keyspaceName, name, "default"),
		spanCountGauge:      metrics.SpanCountGauge.WithLabelValues(keyspaceName, name, "default"),
		tableCountGauge:     metrics.TableCountGauge.WithLabelValues(keyspaceName, name, "default"),
		handleEventDuration: metrics.MaintainerHandleEventDuration.WithLabelValues(keyspaceName, name),

		redoScheduledTaskGauge: metrics.ScheduleTaskGauge.WithLabelValues(keyspaceName, name, "redo"),
		redoSpanCountGauge:     metrics.SpanCountGauge.WithLabelValues(keyspaceName, name, "redo"),
		redoTableCountGauge:    metrics.TableCountGauge.WithLabelValues(keyspaceName, name, "redo"),
	}
	m.nodeChanged.changed = false
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)

	m.watermark.Watermark = &heartbeatpb.Watermark{
		CheckpointTs: checkpointTs,
		ResolvedTs:   checkpointTs,
	}
	m.redoMetaTs = &heartbeatpb.RedoMetaMessage{
		ChangefeedID: cfID.ToPB(),
		CheckpointTs: checkpointTs,
		ResolvedTs:   checkpointTs,
	}
	m.redoResolvedTs = checkpointTs
	m.scheduleState.Store(int32(heartbeatpb.ComponentState_Working))
	m.bootstrapper = bootstrap.NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse](
		m.changefeedID.Name(),
		m.createBootstrapMessageFactory(),
	)

	metrics.MaintainerGauge.WithLabelValues(keyspaceName, name).Inc()
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go m.runHandleEvents(ctx)
	go m.calCheckpointTs(ctx)
	if enableRedo {
		go m.handleRedoMetaTsMessage(ctx)
	}

	if util.GetOrZero(info.Config.Scheduler.EnableTableAcrossNodes) &&
		util.GetOrZero(info.Config.Scheduler.RegionThreshold) > 0 {
		go refresher.Run(ctx)
	}

	log.Info("changefeed maintainer started",
		zap.Stringer("changefeedID", cfID),
		zap.String("state", string(info.State)),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.String("ddlDispatcherID", tableTriggerEventDispatcherID.String()),
		zap.String("redoTs", m.redoMetaTs.String()),
		zap.Bool("newChangefeed", newChangefeed),
	)

	return m
}

func NewMaintainerForRemove(cfID common.ChangeFeedID,
	conf *config.SchedulerConfig,
	selfNode *node.Info,
	taskScheduler threadpool.ThreadPool,
	keyspaceID uint32,
) *Maintainer {
	unused := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		SinkURI:      "",
		Config:       config.GetDefaultReplicaConfig(),
	}
	m := NewMaintainer(cfID, conf, unused, selfNode, taskScheduler, 1, false, keyspaceID)
	m.cascadeRemoving.Store(true)
	return m
}

// HandleEvent implements the event-driven process mode
// it's the entrance of the Maintainer, it handles all types of Events
// note: the EventPeriod is a special event that submitted when initializing maintainer
// , and it will be re-submitted at the end of onPeriodTask
func (m *Maintainer) HandleEvent(event *Event) bool {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > time.Second {
			// add a log for debug an occasional slow bootstrap problem
			if event.eventType == EventMessage {
				log.Info("maintainer is too slow",
					zap.Stringer("changefeedID", m.changefeedID),
					zap.Int("eventType", event.eventType),
					zap.Duration("duration", duration),
					zap.Any("Message", event.message),
				)
			} else {
				log.Info("maintainer is too slow",
					zap.Stringer("changefeedID", m.changefeedID),
					zap.Int("eventType", event.eventType),
					zap.Duration("duration", duration))
			}
		}
		m.handleEventDuration.Observe(duration.Seconds())
	}()

	if m.scheduleState.Load() == int32(heartbeatpb.ComponentState_Stopped) {
		log.Warn("maintainer is stopped, stop handling event",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
			zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs),
		)
		return false
	}

	// first check the online/offline nodes
	m.checkNodeChanged()

	// TODO:use a better way
	switch event.eventType {
	case EventInit:
		return m.onInit()
	case EventMessage:
		m.onMessage(event.message)
	case EventPeriod:
		m.onPeriodTask()
	}
	return false
}

func (m *Maintainer) checkNodeChanged() {
	m.nodeChanged.Lock()
	defer m.nodeChanged.Unlock()
	if m.nodeChanged.changed {
		m.onNodeChanged()
		m.nodeChanged.changed = false
	}
}

// Close cleanup resources
func (m *Maintainer) Close() {
	m.cancel()
	m.controller.Stop()
	m.cleanupMetrics()
}

func (m *Maintainer) GetMaintainerStatus() *heartbeatpb.MaintainerStatus {
	m.runningErrors.Lock()
	defer m.runningErrors.Unlock()
	var runningErrors []*heartbeatpb.RunningError
	if len(m.runningErrors.m) > 0 {
		runningErrors = make([]*heartbeatpb.RunningError, 0, len(m.runningErrors.m))
		for _, e := range m.runningErrors.m {
			runningErrors = append(runningErrors, e)
		}
		m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
	}

	status := &heartbeatpb.MaintainerStatus{
		ChangefeedID:  m.changefeedID.ToPB(),
		State:         heartbeatpb.ComponentState(m.scheduleState.Load()),
		CheckpointTs:  m.getWatermark().CheckpointTs,
		Err:           runningErrors,
		BootstrapDone: m.initialized.Load(),
		LastSyncedTs:  m.getWatermark().LastSyncedTs,
	}
	return status
}

func (m *Maintainer) initialize() error {
	start := time.Now()
	log.Info("start to initialize changefeed maintainer",
		zap.Stringer("changefeedID", m.changefeedID))

	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(errors.New("failpoint injected retriable error"))
	})

	// register node change handler, it will be called when nodes change(eg. online/offline) in the cluster
	m.nodeManager.RegisterNodeChangeHandler(node.ID("maintainer-"+m.changefeedID.Name()), func(allNodes map[node.ID]*node.Info) {
		m.nodeChanged.Lock()
		defer m.nodeChanged.Unlock()
		m.nodeChanged.changed = true
	})

	// register all nodes to bootstrapper
	nodes := m.nodeManager.GetAliveNodes()
	log.Info("changefeed bootstrap initial nodes",
		zap.Stringer("selfNodeID", m.selfNode.ID),
		zap.Stringer("changefeedID", m.changefeedID),
		zap.Int("nodeCount", len(nodes)))

	_, _, requests, _ := m.bootstrapper.HandleNodesChange(nodes)
	m.sendMessages(requests)

	log.Info("changefeed maintainer initialized",
		zap.Stringer("changefeedID", m.changefeedID),
		zap.String("status", common.FormatMaintainerStatus(m.GetMaintainerStatus())),
		zap.String("info", m.info.String()),
		zap.Duration("duration", time.Since(start)))
	m.statusChanged.Store(true)
	return nil
}

func (m *Maintainer) cleanupMetrics() {
	keyspace := m.changefeedID.Keyspace()
	name := m.changefeedID.Name()
	metrics.MaintainerCheckpointTsGauge.DeleteLabelValues(keyspace, name)
	metrics.MaintainerCheckpointTsLagGauge.DeleteLabelValues(keyspace, name)
	metrics.MaintainerHandleEventDuration.DeleteLabelValues(keyspace, name)
	metrics.MaintainerResolvedTsGauge.DeleteLabelValues(keyspace, name)
	metrics.MaintainerResolvedTsLagGauge.DeleteLabelValues(keyspace, name)

	metrics.TableStateGauge.DeleteLabelValues(keyspace, name, "Absent", "default")
	metrics.TableStateGauge.DeleteLabelValues(keyspace, name, "Absent", "redo")
	metrics.TableStateGauge.DeleteLabelValues(keyspace, name, "Working", "default")
	metrics.TableStateGauge.DeleteLabelValues(keyspace, name, "Working", "redo")

	metrics.ScheduleTaskGauge.DeleteLabelValues(keyspace, name, "default")
	metrics.ScheduleTaskGauge.DeleteLabelValues(keyspace, name, "redo")
	metrics.SpanCountGauge.DeleteLabelValues(keyspace, name, "default")
	metrics.SpanCountGauge.DeleteLabelValues(keyspace, name, "redo")
	metrics.TableCountGauge.DeleteLabelValues(keyspace, name, "default")
	metrics.TableCountGauge.DeleteLabelValues(keyspace, name, "redo")
}

func (m *Maintainer) onInit() bool {
	err := m.initialize()
	if err != nil {
		m.handleError(err)
	}
	return false
}

// onMessage handles incoming messages from various components(eg. dispatcher manager, coordinator):
// Dispatcher Manager:
//   - HeartbeatRequest: Status updates and watermarks from dispatcher managers
//   - BlockStatusRequest: Barrier-related status from dispatcher managers
//   - MaintainerBootstrapResponse: Bootstrap completion status from dispatcher managers
//   - MaintainerPostBootstrapResponse: Post-bootstrap initialization status from dispatcher managers
//   - MaintainerCloseResponse: Shutdown confirmation from dispatcher managers
//
// Coordinator:
// - RemoveMaintainerRequest: Changefeed removal commands from coordinator
// - CheckpointTsMessage: CheckpointTs from coordinator, need to send to all dispatcher managers
func (m *Maintainer) onMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeHeartBeatRequest:
		m.onHeartbeatRequest(msg)
	case messaging.TypeBlockStatusRequest:
		m.onBlockStateRequest(msg)
	case messaging.TypeMaintainerBootstrapResponse:
		m.onMaintainerBootstrapResponse(msg)
	case messaging.TypeMaintainerPostBootstrapResponse:
		m.onMaintainerPostBootstrapResponse(msg)
	case messaging.TypeMaintainerCloseResponse:
		resp := msg.Message[0].(*heartbeatpb.MaintainerCloseResponse)
		m.onMaintainerCloseResponse(msg.From, resp)
	case messaging.TypeRemoveMaintainerRequest:
		req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
		m.onRemoveMaintainer(req.Cascade, req.Removed)
	case messaging.TypeCheckpointTsMessage:
		req := msg.Message[0].(*heartbeatpb.CheckpointTsMessage)
		m.onCheckpointTsPersisted(req)
	case messaging.TypeRedoResolvedTsProgressMessage:
		req := msg.Message[0].(*heartbeatpb.RedoResolvedTsProgressMessage)
		m.onRedoPersisted(req)
	default:
		log.Warn("unknown message type, ignore it",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.String("type", msg.Type.String()),
			zap.Any("message", msg.Message))
	}
}

func (m *Maintainer) onRemoveMaintainer(cascade, changefeedRemoved bool) {
	m.removing.Store(true)
	m.cascadeRemoving.Store(cascade)
	m.changefeedRemoved.Store(changefeedRemoved)
	closed := m.tryCloseChangefeed()
	if closed {
		m.removed.Store(true)
		m.scheduleState.Store(int32(heartbeatpb.ComponentState_Stopped))
		metrics.MaintainerGauge.WithLabelValues(m.changefeedID.Keyspace(), m.changefeedID.Name()).Dec()
		log.Info("changefeed maintainer closed", zap.Stringer("changefeedID", m.changefeedID),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs), zap.Bool("removed", m.removed.Load()))
	}
}

// onCheckpointTsPersisted forwards the checkpoint message to the table trigger dispatcher,
// which is co-located on the same node as the maintainer. The dispatcher will propagate
// the watermark information to downstream sinks.
func (m *Maintainer) onCheckpointTsPersisted(msg *heartbeatpb.CheckpointTsMessage) {
	m.sendMessages([]*messaging.TargetMessage{
		messaging.NewSingleTargetMessage(m.selfNode.ID, messaging.HeartbeatCollectorTopic, msg),
	})
}

func (m *Maintainer) onRedoPersisted(req *heartbeatpb.RedoResolvedTsProgressMessage) {
	if m.redoResolvedTs < req.ResolvedTs {
		m.redoResolvedTs = req.ResolvedTs
		msgs := make([]*messaging.TargetMessage, 0, len(m.bootstrapper.GetAllNodeIDs()))
		for _, id := range m.bootstrapper.GetAllNodeIDs() {
			msgs = append(msgs, messaging.NewSingleTargetMessage(id, messaging.HeartbeatCollectorTopic, &heartbeatpb.RedoResolvedTsForwardMessage{
				ChangefeedID: req.ChangefeedID,
				ResolvedTs:   m.redoResolvedTs,
			}))
		}
		m.sendMessages(msgs)
	}
}

func (m *Maintainer) onNodeChanged() {
	addedNodes, removedNodes, requests, responses := m.bootstrapper.HandleNodesChange(m.nodeManager.GetAliveNodes())
	log.Info("maintainer node changed", zap.Stringer("changefeedID", m.changefeedID),
		zap.Int("addedCount", len(addedNodes)),
		zap.Int("removedCount", len(removedNodes)),
		zap.Any("addedNodes", addedNodes),
		zap.Any("removedNodes", removedNodes))

	for _, id := range removedNodes {
		m.controller.RemoveNode(id)
		m.checkpointTsByCapture.Delete(id)
		m.redoTsByCapture.Delete(id)
	}

	m.sendMessages(requests)
	m.onBootstrapResponses(responses)
}

// handleRedoMetaTsMessage forwards the redo meta ts message to the dispatcher manager.
// - ResolvedTs: The commit-ts of the transaction that was finally confirmed to have been fully uploaded to external storage.
func (m *Maintainer) handleRedoMetaTsMessage(ctx context.Context) {
	ticker := time.NewTicker(periodRedoInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.initialized.Load() {
				log.Warn("can not advance redoTs since not bootstrapped",
					zap.Stringer("changefeedID", m.changefeedID))
				break
			}
			needUpdate := false
			updateCheckpointTs := true

			newWatermark := heartbeatpb.NewMaxWatermark()
			// Calculate operator and barrier constraints first to ensure atomicity.
			// This prevents a race condition where checkpointTsByCapture contains old heartbeat data
			// while operators have completed based on newer heartbeat processing.
			// For more detailed comments, please refer to `calculateNewCheckpointTs`.
			minRedoCheckpointTsForScheduler := m.controller.GetMinRedoCheckpointTs(newWatermark.CheckpointTs)
			minRedoCheckpointTsForBarrier := m.controller.redoBarrier.GetMinBlockedCheckpointTsForNewTables(newWatermark.CheckpointTs)

			// if there is no tables, there must be a table trigger dispatcher
			for _, id := range m.bootstrapper.GetAllNodeIDs() {
				// maintainer node has the table trigger dispatcher
				if id != m.selfNode.ID && m.controller.redoSpanController.GetTaskSizeByNodeID(id) <= 0 {
					continue
				}
				// node level watermark reported, ignore this round
				watermark, ok := m.redoTsByCapture.Get(id)
				if !ok {
					updateCheckpointTs = false
					log.Warn("redo checkpointTs can not be advanced, since missing capture heartbeat",
						zap.Stringer("changefeedID", m.changefeedID),
						zap.Any("node", id))
					continue
				}
				newWatermark.UpdateMin(watermark)
			}

			newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minRedoCheckpointTsForScheduler, ResolvedTs: minRedoCheckpointTsForScheduler})
			newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minRedoCheckpointTsForBarrier, ResolvedTs: minRedoCheckpointTsForBarrier})

			if m.redoMetaTs.ResolvedTs < newWatermark.CheckpointTs && updateCheckpointTs {
				m.redoMetaTs.ResolvedTs = newWatermark.CheckpointTs
				needUpdate = true
			}
			if m.redoMetaTs.CheckpointTs < m.getWatermark().CheckpointTs {
				m.redoMetaTs.CheckpointTs = m.getWatermark().CheckpointTs
				needUpdate = true
			}
			log.Debug("handle redo message",
				zap.Any("needUpdate", needUpdate),
				zap.Any("redoMetaTs", m.redoMetaTs),
				zap.Any("checkpointTs", m.getWatermark().CheckpointTs),
				zap.Any("resolvedTs", newWatermark.CheckpointTs),
			)
			if needUpdate {
				m.sendMessages([]*messaging.TargetMessage{
					messaging.NewSingleTargetMessage(m.selfNode.ID, messaging.HeartbeatCollectorTopic, &heartbeatpb.RedoMetaMessage{
						ChangefeedID: m.redoMetaTs.ChangefeedID,
						CheckpointTs: m.redoMetaTs.CheckpointTs,
						ResolvedTs:   m.redoMetaTs.ResolvedTs,
					}),
				})
			}
		}
	}
}

// calCheckpointTs will be a little expensive when there are a large number of operators or absent tasks
// so we use a single goroutine to calculate the checkpointTs, instead of blocking event handling
func (m *Maintainer) calCheckpointTs(ctx context.Context) {
	ticker := time.NewTicker(periodEventInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.initialized.Load() {
				log.Warn("can not advance checkpointTs since not bootstrapped",
					zap.Stringer("changefeedID", m.changefeedID),
					zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
					zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs))
				break
			}

			// first check the online/offline nodes
			// we need to check node changed before calculating checkpointTs
			// to avoid the case when a node is offline, the node's heartbeat is missing
			// while the span in this node still not set to absent, which may cause
			// the checkpointTs be advanced incorrectly
			m.checkNodeChanged()

			// CRITICAL SECTION: Calculate checkpointTs with proper ordering to prevent race condition
			newWatermark, canUpdate := m.calculateNewCheckpointTs()
			if canUpdate {
				m.setWatermark(*newWatermark)
				m.updateMetrics()
			}
		}
	}
}

// calculateNewCheckpointTs calculates the new checkpoint with proper ordering to prevent race condition.
//
// Race Condition Problem:
// 1. DDL creates operator_add for new dispatcher (startTs=150)
// 2. Old heartbeat (calculated before dispatcher creation) sent with checkpointTs=200
// 3. New heartbeat (with new dispatcher status) sent with checkpointTs=200
// 4. onHeartBeatRequest processes old heartbeat first -> updates checkpointTsByCapture to 200
// 5. onHeartBeatRequest processes new heartbeat -> operator_add completes -> no longer blocks
// 6. calCheckpointTs uses checkpointTsByCapture=200 but operator no longer blocks
// 7. RESULT: checkpointTs advances to 200 > newDispatcherStartTs=150 -> DATA LOSS on crash
//
// Solution: Two-part atomic fix in onHeartBeatRequest + calculateNewCheckpointTs:
//
// Part 1 (onHeartBeatRequest): Update checkpointTsByCapture BEFORE processing operator status
//   - This ensures when operator completes, checkpointTsByCapture contains complete heartbeat
//   - Eliminates the window where old heartbeat data exists with completed operator
//
// Part 2 (calculateNewCheckpointTs): Calculate operator constraints first, then apply heartbeat constraints
//   - Operator constraints represent current safe limits regardless of heartbeat timing
//   - Heartbeat constraints can only further restrict, never relax operator limits
//
// Returns: (newWatermark, canUpdate)
func (m *Maintainer) calculateNewCheckpointTs() (*heartbeatpb.Watermark, bool) {
	// Step 1: Get current operator and barrier constraints first
	newWatermark := heartbeatpb.NewMaxWatermark()
	minCheckpointTsForScheduler := m.controller.GetMinCheckpointTs(newWatermark.CheckpointTs)
	minCheckpointTsForBarrier := m.controller.barrier.GetMinBlockedCheckpointTsForNewTables(newWatermark.CheckpointTs)

	// Step 2: Apply heartbeat constraints from all nodes
	updateCheckpointTs := true
	for _, id := range m.bootstrapper.GetAllNodeIDs() {
		// maintainer node has the table trigger dispatcher
		if id != m.selfNode.ID && m.controller.spanController.GetTaskSizeByNodeID(id) <= 0 {
			continue
		}
		// node level watermark reported, ignore this round
		watermark, ok := m.checkpointTsByCapture.Get(id)
		if !ok {
			updateCheckpointTs = false
			log.Warn("checkpointTs can not be advanced, since missing capture heartbeat",
				zap.Stringer("changefeedID", m.changefeedID), zap.Any("node", id),
				zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
				zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs))
			continue
		}
		// Apply heartbeat constraint - can only make checkpointTs smaller (safer)
		newWatermark.UpdateMin(watermark)
	}

	if !updateCheckpointTs {
		return nil, false
	}

	newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minCheckpointTsForBarrier, ResolvedTs: minCheckpointTsForBarrier})
	newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minCheckpointTsForScheduler, ResolvedTs: minCheckpointTsForScheduler})

	log.Debug("can advance checkpointTs",
		zap.Stringer("changefeedID", m.changefeedID),
		zap.Uint64("newCheckpointTs", newWatermark.CheckpointTs),
		zap.Uint64("newResolvedTs", newWatermark.ResolvedTs),
		zap.Uint64("minCheckpointTsForScheduler", minCheckpointTsForScheduler),
		zap.Uint64("minCheckpointTsForBarrier", minCheckpointTsForBarrier),
	)

	return newWatermark, true
}

func (m *Maintainer) updateMetrics() {
	watermark := m.getWatermark()

	pdPhysicalTime := oracle.GetPhysical(m.pdClock.CurrentTime())
	phyCkpTs := oracle.ExtractPhysical(watermark.CheckpointTs)
	m.checkpointTsGauge.Set(float64(phyCkpTs))
	lag := float64(pdPhysicalTime-phyCkpTs) / 1e3
	m.checkpointTsLagGauge.Set(lag)

	phyResolvedTs := oracle.ExtractPhysical(watermark.ResolvedTs)
	m.resolvedTsGauge.Set(float64(phyResolvedTs))
	lag = float64(pdPhysicalTime-phyResolvedTs) / 1e3
	m.resolvedTsLagGauge.Set(lag)
}

// send message to other components
func (m *Maintainer) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		err := m.mc.SendCommand(msg)
		if err != nil {
			log.Debug("failed to send maintainer request",
				zap.Stringer("changefeedID", m.changefeedID),
				zap.Any("msg", msg), zap.Error(err))
		}
	}
}

func (m *Maintainer) onHeartbeatRequest(msg *messaging.TargetMessage) {
	// ignore the heartbeat if the maintainer not bootstrapped
	if !m.initialized.Load() {
		return
	}
	req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)

	// ATOMIC CHECKPOINT UPDATE: Part 1 of race condition fix
	// Update checkpointTsByCapture BEFORE processing operator status to ensure atomicity
	// This works together with calCheckpointTs to prevent incorrect checkpoint advancement
	if req.Watermark != nil {
		// The sequence increases when a dispatcher status changes, so accept the new watermark
		// even if the reported checkpoint regresses (new dispatcher might replay from
		// an earlier startTs). For the same sequence we still keep checkpoint monotonic
		// to ignore reordered or duplicated heartbeats.
		old, ok := m.checkpointTsByCapture.Get(msg.From)
		if !ok || req.Watermark.Seq > old.Seq || (req.Watermark.Seq == old.Seq && req.Watermark.CheckpointTs > old.CheckpointTs) {
			m.checkpointTsByCapture.Set(msg.From, *req.Watermark)
		}
		// Update last synced ts from all dispatchers.
		// We don't care about the checkpoint ts of scheduler or barrier here,
		// we just want to know the max time that all dispatchers have synced.
		m.watermark.mu.Lock()
		if m.watermark.LastSyncedTs < req.Watermark.LastSyncedTs {
			m.watermark.LastSyncedTs = req.Watermark.LastSyncedTs
		}
		m.watermark.mu.Unlock()
	}

	if req.RedoWatermark != nil {
		// Apply the same rule for redo checkpoint: newer sequence wins even if checkpoint
		// moves backwards, while identical sequence updates must be strictly forward only.
		old, ok := m.redoTsByCapture.Get(msg.From)
		if !ok || req.RedoWatermark.Seq > old.Seq || (req.RedoWatermark.Seq == old.Seq && req.RedoWatermark.CheckpointTs > old.CheckpointTs) {
			m.redoTsByCapture.Set(msg.From, *req.RedoWatermark)
		}
	}
	if req.Err != nil {
		log.Error("dispatcher report an error",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.Stringer("sourceNode", msg.From),
			zap.String("error", req.Err.Message))
		m.onError(msg.From, req.Err)
	}

	// ATOMIC CHECKPOINT UPDATE: Part 2 of race condition fix
	// Process operator status updates AFTER checkpointTsByCapture is updated
	// This ensures when operators complete, checkpointTsByCapture already contains the complete heartbeat
	// Works with calCheckpointTs constraint ordering to prevent checkpoint advancing past new dispatcher startTs
	m.controller.HandleStatus(msg.From, req.Statuses)
}

func (m *Maintainer) onError(from node.ID, err *heartbeatpb.RunningError) {
	err.Node = from.String()
	if info, ok := m.nodeManager.GetAliveNodes()[from]; ok {
		err.Node = info.AdvertiseAddr
	}
	m.runningErrors.Lock()
	m.statusChanged.Store(true)
	m.runningErrors.m[from] = err
	m.runningErrors.Unlock()
}

func (m *Maintainer) onBlockStateRequest(msg *messaging.TargetMessage) {
	// the barrier is not initialized
	if !m.initialized.Load() {
		return
	}
	req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)

	var ackMsg []*messaging.TargetMessage
	if common.IsDefaultMode(req.Mode) {
		ackMsg = m.controller.barrier.HandleStatus(msg.From, req)
	} else {
		ackMsg = m.controller.redoBarrier.HandleStatus(msg.From, req)
	}
	if ackMsg != nil {
		m.sendMessages(ackMsg)
	}
}

// onMaintainerBootstrapResponse is called when a maintainer bootstrap response(send by dispatcher manager) is received.
func (m *Maintainer) onMaintainerBootstrapResponse(msg *messaging.TargetMessage) {
	log.Info("maintainer received bootstrap response",
		zap.Stringer("changefeedID", m.changefeedID),
		zap.Stringer("sourceNodeID", msg.From))

	resp := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
	if resp.Err != nil {
		log.Warn("maintainer bootstrap failed",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.String("error", resp.Err.Message))
		m.onError(msg.From, resp.Err)
		return
	}

	responses := m.bootstrapper.HandleBootstrapResponse(msg.From, resp)
	m.onBootstrapResponses(responses)

	// When receiving a bootstrap response from our own node's dispatcher manager
	// (which handles table trigger events), mark this changefeed is not new created.
	if msg.From == m.selfNode.ID {
		m.newChangefeed = false
	}
}

func (m *Maintainer) onMaintainerPostBootstrapResponse(msg *messaging.TargetMessage) {
	log.Info("received maintainer post bootstrap response",
		zap.Stringer("changefeedID", m.changefeedID),
		zap.Any("server", msg.From))
	resp := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapResponse)
	if resp.Err != nil {
		log.Warn("maintainer post bootstrap failed",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.String("error", resp.Err.Message))
		m.onError(msg.From, resp.Err)
		return
	}
	// disable resend post bootstrap message
	m.postBootstrapMsg = nil
}

// isMysqlCompatible returns true if the sinkURIStr is mysql compatible.
func isMysqlCompatible(sinkURIStr string) (bool, error) {
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return false, errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	scheme := config.GetScheme(sinkURI)
	return config.IsMySQLCompatibleScheme(scheme), nil
}

func newDDLSpan(keyspaceID uint32, cfID common.ChangeFeedID, checkpointTs uint64, selfNode *node.Info, mode int64) (common.DispatcherID, *replica.SpanReplication) {
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(keyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    checkpointTs,
			Mode:            mode,
		}, selfNode.ID, false)
	return tableTriggerEventDispatcherID, ddlSpan
}

func (m *Maintainer) onBootstrapResponses(responses map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	// calCheckpointTs() skips advancing checkpoint when bootstrapped is false, so it won't call
	// onNodeChanged() before FinishBootstrap succeeds. All other callers of onNodeChanged() and
	// onMaintainerBootstrapResponse() run in the same event loop goroutine as onRemoveMaintainer(),
	// hence guarding with m.removing is sufficient to avoid accessing a removed DDL span leading to panic.
	if responses == nil || m.removing.Load() {
		return
	}
	isMySQLSinkCompatible, err := isMysqlCompatible(m.info.SinkURI)
	if err != nil {
		m.handleError(err)
		return
	}
	postBootstrapRequest, err := m.controller.FinishBootstrap(responses, isMySQLSinkCompatible)
	if err != nil {
		m.handleError(err)
		return
	}

	if postBootstrapRequest == nil {
		return
	}

	m.initialized.Store(true)
	log.Info("changefeed maintainer bootstrapped", zap.Stringer("changefeedID", m.changefeedID))
	// Memory Consumption is 64(tableName/schemaName limit) * 4(utf8.UTFMax) * 2(tableName+schemaName) * tableNum
	// For an extreme case(100w tables, and 64 utf8 characters for each name), the memory consumption is about 488MB.
	// For a normal case(100w tables, and 16 ascii characters for each name), the memory consumption is about 30MB.
	m.postBootstrapMsg = postBootstrapRequest
	m.sendPostBootstrapRequest()
	m.statusChanged.Store(true)
}

func (m *Maintainer) sendPostBootstrapRequest() {
	if m.postBootstrapMsg != nil {
		msg := messaging.NewSingleTargetMessage(
			m.selfNode.ID,
			messaging.DispatcherManagerManagerTopic,
			m.postBootstrapMsg,
		)
		m.sendMessages([]*messaging.TargetMessage{msg})
	}
}

func (m *Maintainer) onMaintainerCloseResponse(from node.ID, response *heartbeatpb.MaintainerCloseResponse) {
	if response.Success {
		m.closedNodes[from] = struct{}{}
		m.onRemoveMaintainer(m.cascadeRemoving.Load(), m.changefeedRemoved.Load())
	}
}

func (m *Maintainer) handleResendMessage() {
	// resend closing message
	if m.removing.Load() && m.cascadeRemoving.Load() {
		m.trySendMaintainerCloseRequestToAllNode()
		return
	}
	// resend bootstrap message
	m.sendMessages(m.bootstrapper.ResendBootstrapMessage())
	m.sendPostBootstrapRequest()
	if m.controller.redoBarrier != nil {
		// resend redo barrier ack messages
		m.sendMessages(m.controller.redoBarrier.Resend())
	}
	if m.controller.barrier != nil {
		// resend barrier ack messages
		m.sendMessages(m.controller.barrier.Resend())
	}
}

func (m *Maintainer) tryCloseChangefeed() bool {
	if m.scheduleState.Load() != int32(heartbeatpb.ComponentState_Stopped) {
		m.statusChanged.Store(true)
	}
	if !m.cascadeRemoving.Load() {
		m.controller.operatorController.RemoveTasksByTableIDs(m.ddlSpan.Span.TableID)
		if m.enableRedo {
			m.controller.redoOperatorController.RemoveTasksByTableIDs(m.redoDDLSpan.Span.TableID)
			return !m.ddlSpan.IsWorking() && !m.redoDDLSpan.IsWorking()
		}
		return !m.ddlSpan.IsWorking()
	}
	return m.trySendMaintainerCloseRequestToAllNode()
}

// trySendMaintainerCloseRequestToAllNode is used to send maintainer close request to all nodes
// if all nodes are closed, return true, otherwise return false.
func (m *Maintainer) trySendMaintainerCloseRequestToAllNode() bool {
	msgs := make([]*messaging.TargetMessage, 0)
	for n := range m.nodeManager.GetAliveNodes() {
		// Check if the node is already closed.
		if _, ok := m.closedNodes[n]; !ok {
			msgs = append(msgs, messaging.NewSingleTargetMessage(
				n,
				messaging.DispatcherManagerManagerTopic,
				&heartbeatpb.MaintainerCloseRequest{
					ChangefeedID: m.changefeedID.ToPB(),
					Removed:      m.changefeedRemoved.Load(),
				}))
		}
	}
	if len(msgs) > 0 {
		m.sendMessages(msgs)
		log.Info("send maintainer close request to all dispatcher managers",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
			zap.Int("nodeCount", len(msgs)))
	}
	return len(msgs) == 0
}

// handleError set the caches the error, the error will be reported to coordinator
// and coordinator remove this maintainer
func (m *Maintainer) handleError(err error) {
	log.Error("an error occurred in maintainer",
		zap.Stringer("changefeedID", m.changefeedID), zap.Error(err))
	var code string
	if rfcCode, ok := errors.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(errors.ErrOwnerUnknown.RFCCode())
	}

	m.runningErrors.Lock()
	defer m.runningErrors.Unlock()
	m.runningErrors.m[m.selfNode.ID] = &heartbeatpb.RunningError{
		Time:    time.Now().String(),
		Node:    m.selfNode.AdvertiseAddr,
		Code:    code,
		Message: err.Error(),
	}
	m.statusChanged.Store(true)
}

// createBootstrapMessageFactory returns a function that generates bootstrap messages
// for initializing dispatcher managers. The returned function takes a node ID and
// returns a message containing:
// - Serialized changefeed configuration
// - Starting checkpoint timestamp
// - Table trigger event dispatcher ID (only for dispatcher manager on same node)
// - Flag indicating if this is a new changefeed
func (m *Maintainer) createBootstrapMessageFactory() bootstrap.NewBootstrapRequestFn {
	// cfgBytes only holds necessary fields to initialize a changefeed dispatcher manager.
	cfgBytes, err := json.Marshal(m.info.ToChangefeedConfig())
	if err != nil {
		log.Panic("marshal changefeed info failed",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.Error(err))
	}
	return func(targetNodeID node.ID, targetAddr string) *messaging.TargetMessage {
		msg := &heartbeatpb.MaintainerBootstrapRequest{
			ChangefeedID:                      m.changefeedID.ToPB(),
			Config:                            cfgBytes,
			StartTs:                           m.startCheckpointTs,
			TableTriggerEventDispatcherId:     nil,
			RedoTableTriggerEventDispatcherId: nil,
			IsNewChangefeed:                   false,
			KeyspaceId:                        m.info.KeyspaceID,
		}

		// only send dispatcher targetNodeID to dispatcher manager on the same node
		if targetNodeID == m.selfNode.ID {
			log.Info("create table event trigger dispatcher bootstrap message",
				zap.Stringer("changefeedID", m.changefeedID),
				zap.String("nodeAddr", targetAddr),
				zap.Any("nodeID", targetNodeID),
				zap.String("dispatcherID", m.ddlSpan.ID.String()),
				zap.Uint64("startTs", m.startCheckpointTs),
			)
			msg.TableTriggerEventDispatcherId = m.ddlSpan.ID.ToPB()
			if m.enableRedo {
				msg.RedoTableTriggerEventDispatcherId = m.redoDDLSpan.ID.ToPB()
			}
			msg.IsNewChangefeed = m.newChangefeed
		}

		log.Info("maintainer new bootstrap message to dispatcher orchestrator",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.String("nodeAddr", targetAddr),
			zap.Any("nodeID", targetNodeID),
			zap.Uint64("startTs", m.startCheckpointTs))

		return messaging.NewSingleTargetMessage(targetNodeID, messaging.DispatcherManagerManagerTopic, msg)
	}
}

func (m *Maintainer) onPeriodTask() {
	// send scheduling messages
	m.handleResendMessage()
	m.collectMetrics()
}

func (m *Maintainer) collectMetrics() {
	if !m.initialized.Load() {
		return
	}
	updateMetric := func(mode int64) {
		// exclude the table trigger
		spanController := m.controller.getSpanController(mode)
		totalSpanCount := spanController.TaskSize() - 1
		totalTableCount := 0
		groupSize := spanController.GetGroupSize()
		if groupSize == 1 {
			totalTableCount = spanController.GetTaskSizeByGroup(pkgReplica.DefaultGroupID)
		} else {
			totalTableCount = groupSize - 1 + spanController.GetTaskSizeByGroup(pkgReplica.DefaultGroupID)
		}
		scheduling := spanController.GetSchedulingSize()
		working := spanController.GetReplicatingSize()
		absent := spanController.GetAbsentSize()

		if common.IsDefaultMode(mode) {
			m.spanCountGauge.Set(float64(totalSpanCount))
			m.tableCountGauge.Set(float64(totalTableCount))
			m.scheduledTaskGauge.Set(float64(scheduling))
		} else {
			m.redoSpanCountGauge.Set(float64(totalSpanCount))
			m.redoTableCountGauge.Set(float64(totalTableCount))
			m.redoScheduledTaskGauge.Set(float64(scheduling))
		}
		metrics.TableStateGauge.WithLabelValues(m.changefeedID.Keyspace(), m.changefeedID.Name(), "Absent", common.StringMode(mode)).Set(float64(absent))
		metrics.TableStateGauge.WithLabelValues(m.changefeedID.Keyspace(), m.changefeedID.Name(), "Working", common.StringMode(mode)).Set(float64(working))
	}
	if time.Since(m.lastPrintStatusTime) > time.Second*20 {
		updateMetric(common.DefaultMode)
		if m.enableRedo {
			updateMetric(common.RedoMode)
		}
		m.lastPrintStatusTime = time.Now()
	}
}

func (m *Maintainer) runHandleEvents(ctx context.Context) {
	ticker := time.NewTicker(periodEventInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-m.eventCh.Out():
			m.HandleEvent(event)
		case <-ticker.C:
			m.HandleEvent(&Event{
				changefeedID: m.changefeedID,
				eventType:    EventPeriod,
			})
		}
	}
}

// pushEvent is used to push event to maintainer's event channel
// event will be handled by maintainer's main loop
func (m *Maintainer) pushEvent(event *Event) {
	m.eventCh.In() <- event
}

func (m *Maintainer) getWatermark() heartbeatpb.Watermark {
	m.watermark.mu.RLock()
	defer m.watermark.mu.RUnlock()
	res := heartbeatpb.Watermark{
		CheckpointTs: m.watermark.CheckpointTs,
		ResolvedTs:   m.watermark.ResolvedTs,
		LastSyncedTs: m.watermark.LastSyncedTs,
	}
	return res
}

func (m *Maintainer) setWatermark(newWatermark heartbeatpb.Watermark) {
	m.watermark.mu.Lock()
	defer m.watermark.mu.Unlock()
	if newWatermark.CheckpointTs != math.MaxUint64 {
		m.watermark.CheckpointTs = newWatermark.CheckpointTs
	}
	if newWatermark.ResolvedTs != math.MaxUint64 {
		m.watermark.ResolvedTs = newWatermark.ResolvedTs
	}
}
