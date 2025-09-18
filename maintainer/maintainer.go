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
	id         common.ChangeFeedID
	config     *config.ChangeFeedInfo
	selfNode   *node.Info
	controller *Controller

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

	bootstrapped     atomic.Bool
	postBootstrapMsg *heartbeatpb.MaintainerPostBootstrapRequest

	// startCheckpointTs is the initial checkpointTs when the maintainer is created.
	// It is sent to dispatcher managers during bootstrap to initialize their
	// checkpointTs.
	startCheckpointTs uint64
	enableRedo        bool
	// redoTs is global ts to forward
	redoTs          *heartbeatpb.RedoMessage
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

	cancel                         context.CancelFunc
	changefeedCheckpointTsGauge    prometheus.Gauge
	changefeedCheckpointTsLagGauge prometheus.Gauge
	changefeedResolvedTsGauge      prometheus.Gauge
	changefeedResolvedTsLagGauge   prometheus.Gauge
	changefeedStatusGauge          prometheus.Gauge
	scheduledTaskGauge             prometheus.Gauge
	spanCountGauge                 prometheus.Gauge
	tableCountGauge                prometheus.Gauge
	handleEventDuration            prometheus.Observer

	redoScheduledTaskGauge prometheus.Gauge
	redoSpanCountGauge     prometheus.Gauge
	redoTableCountGauge    prometheus.Gauge
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID common.ChangeFeedID,
	conf *config.SchedulerConfig,
	cfg *config.ChangeFeedInfo,
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
	enableRedo := redo.IsConsistentEnabled(cfg.Config.Consistent.Level)
	if enableRedo {
		_, redoDDLSpan = newDDLSpan(keyspaceID, cfID, checkpointTs, selfNode, common.RedoMode)
	}
	m := &Maintainer{
		id:                cfID,
		selfNode:          selfNode,
		eventCh:           chann.NewAutoDrainChann[*Event](),
		startCheckpointTs: checkpointTs,
		controller: NewController(cfID, checkpointTs, taskScheduler,
			cfg.Config, ddlSpan, redoDDLSpan, conf.AddTableBatchSize, time.Duration(conf.CheckBalanceInterval), keyspaceID, enableRedo),
		mc:                    mc,
		removed:               atomic.NewBool(false),
		nodeManager:           nodeManager,
		closedNodes:           make(map[node.ID]struct{}),
		statusChanged:         atomic.NewBool(true),
		config:                cfg,
		pdClock:               appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		ddlSpan:               ddlSpan,
		redoDDLSpan:           redoDDLSpan,
		checkpointTsByCapture: newWatermarkCaptureMap(),
		redoTsByCapture:       newWatermarkCaptureMap(),
		newChangefeed:         newChangefeed,
		enableRedo:            enableRedo,

		changefeedCheckpointTsGauge:    metrics.ChangefeedCheckpointTsGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()),
		changefeedCheckpointTsLagGauge: metrics.ChangefeedCheckpointTsLagGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()),
		changefeedResolvedTsGauge:      metrics.ChangefeedResolvedTsGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()),
		changefeedResolvedTsLagGauge:   metrics.ChangefeedResolvedTsLagGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()),
		changefeedStatusGauge:          metrics.ChangefeedStatusGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()),
		scheduledTaskGauge:             metrics.ScheduleTaskGauge.WithLabelValues(cfID.Keyspace(), cfID.Name(), "default"),
		spanCountGauge:                 metrics.SpanCountGauge.WithLabelValues(cfID.Keyspace(), cfID.Name(), "default"),
		tableCountGauge:                metrics.TableCountGauge.WithLabelValues(cfID.Keyspace(), cfID.Name(), "default"),
		handleEventDuration:            metrics.MaintainerHandleEventDuration.WithLabelValues(cfID.Keyspace(), cfID.Name()),

		redoScheduledTaskGauge: metrics.ScheduleTaskGauge.WithLabelValues(cfID.Keyspace(), cfID.Name(), "redo"),
		redoSpanCountGauge:     metrics.SpanCountGauge.WithLabelValues(cfID.Keyspace(), cfID.Name(), "redo"),
		redoTableCountGauge:    metrics.TableCountGauge.WithLabelValues(cfID.Keyspace(), cfID.Name(), "redo"),
	}
	m.nodeChanged.changed = false
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)

	m.watermark.Watermark = &heartbeatpb.Watermark{
		CheckpointTs: checkpointTs,
		ResolvedTs:   checkpointTs,
	}
	m.redoTs = &heartbeatpb.RedoMessage{
		ChangefeedID: cfID.ToPB(),
		CheckpointTs: checkpointTs,
		ResolvedTs:   checkpointTs,
	}
	m.scheduleState.Store(int32(heartbeatpb.ComponentState_Working))
	m.bootstrapper = bootstrap.NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse](
		m.id.Name(),
		m.createBootstrapMessageFactory(),
	)

	metrics.MaintainerGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()).Inc()
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go m.runHandleEvents(ctx)
	go m.calCheckpointTs(ctx)
	if enableRedo {
		go m.handleRedoMessage(ctx)
	}

	log.Info("changefeed maintainer is created", zap.String("id", cfID.String()),
		zap.String("state", string(cfg.State)),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.String("ddlDispatcherID", tableTriggerEventDispatcherID.String()),
		zap.String("redoTs", m.redoTs.String()),
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
			log.Info("maintainer is too slow",
				zap.String("changefeed", m.id.String()),
				zap.Int("eventType", event.eventType),
				zap.Duration("duration", duration))
		}
		m.handleEventDuration.Observe(duration.Seconds())
	}()

	if m.scheduleState.Load() == int32(heartbeatpb.ComponentState_Stopped) {
		log.Warn("maintainer is stopped, stop handling event",
			zap.String("changefeed", m.id.String()),
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
	m.cleanupMetrics()
	m.controller.Stop()
	log.Info("changefeed maintainer closed",
		zap.String("changefeed", m.id.String()),
		zap.Bool("removed", m.removed.Load()),
		zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs))
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
		ChangefeedID:  m.id.ToPB(),
		State:         heartbeatpb.ComponentState(m.scheduleState.Load()),
		CheckpointTs:  m.getWatermark().CheckpointTs,
		Err:           runningErrors,
		BootstrapDone: m.bootstrapped.Load(),
	}
	return status
}

func (m *Maintainer) initialize() error {
	start := time.Now()
	log.Info("start to initialize changefeed maintainer",
		zap.String("changefeed", m.id.String()))

	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(errors.New("failpoint injected retriable error"))
	})

	// register node change handler, it will be called when nodes change(eg. online/offline) in the cluster
	m.nodeManager.RegisterNodeChangeHandler(node.ID("maintainer-"+m.id.Name()), func(allNodes map[node.ID]*node.Info) {
		m.nodeChanged.Lock()
		defer m.nodeChanged.Unlock()
		m.nodeChanged.changed = true
	})

	// register all nodes to bootstrapper
	nodes := m.nodeManager.GetAliveNodes()
	log.Info("changefeed bootstrap initial nodes",
		zap.Stringer("selfNodeID", m.selfNode.ID),
		zap.Stringer("changefeedID", m.id),
		zap.Int("nodeCount", len(nodes)))

	newNodes := make([]*node.Info, 0, len(nodes))
	for _, n := range nodes {
		newNodes = append(newNodes, n)
	}
	m.sendMessages(m.bootstrapper.HandleNewNodes(newNodes))

	log.Info("changefeed maintainer initialized",
		zap.String("info", m.config.String()),
		zap.String("id", m.id.String()),
		zap.String("status", common.FormatMaintainerStatus(m.GetMaintainerStatus())),
		zap.Duration("duration", time.Since(start)))
	m.statusChanged.Store(true)
	return nil
}

func (m *Maintainer) cleanupMetrics() {
	metrics.ChangefeedCheckpointTsGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.ChangefeedCheckpointTsLagGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.ChangefeedResolvedTsGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.ChangefeedResolvedTsLagGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.ChangefeedStatusGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.ScheduleTaskGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.SpanCountGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.TableCountGauge.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
	metrics.MaintainerHandleEventDuration.DeleteLabelValues(m.id.Keyspace(), m.id.Name())
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
		m.onHeartBeatRequest(msg)
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
	default:
		log.Panic("unexpected message type",
			zap.String("changefeed", m.id.Name()),
			zap.String("messageType", msg.Type.String()))
	}
}

func (m *Maintainer) onRemoveMaintainer(cascade, changefeedRemoved bool) {
	m.removing.Store(true)
	m.cascadeRemoving.Store(cascade)
	m.changefeedRemoved.Store(changefeedRemoved)
	closed := m.tryCloseChangefeed()
	if closed {
		log.Info("changefeed maintainer closed",
			zap.Stringer("changefeed", m.id),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs))
		m.removed.Store(true)
		m.scheduleState.Store(int32(heartbeatpb.ComponentState_Stopped))
		metrics.MaintainerGauge.WithLabelValues(m.id.Keyspace(), m.id.Name()).Dec()
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

func (m *Maintainer) onNodeChanged() {
	currentNodes := m.bootstrapper.GetAllNodeIDs()

	activeNodes := m.nodeManager.GetAliveNodes()
	newNodes := make([]*node.Info, 0, len(activeNodes))
	for id, n := range activeNodes {
		if _, ok := currentNodes[id]; !ok {
			newNodes = append(newNodes, n)
		}
	}
	var removedNodes []node.ID
	for id := range currentNodes {
		if _, ok := activeNodes[id]; !ok {
			removedNodes = append(removedNodes, id)
			m.checkpointTsByCapture.Delete(id)
			m.redoTsByCapture.Delete(id)
			m.controller.RemoveNode(id)
		}
	}
	log.Info("maintainer node changed", zap.String("id", m.id.String()),
		zap.Int("new", len(newNodes)),
		zap.Int("removed", len(removedNodes)))
	m.sendMessages(m.bootstrapper.HandleNewNodes(newNodes))
	cachedResponse := m.bootstrapper.HandleRemoveNodes(removedNodes)
	if cachedResponse != nil {
		log.Info("bootstrap done after removed some nodes", zap.String("id", m.id.String()))
		m.onBootstrapDone(cachedResponse)
	}
}

// handleRedoMessage forwards the redoTs message to the dispatcher manager.
// - ResolvedTs: The commit-ts of the transaction that was finally confirmed to have been fully uploaded to external storage.
func (m *Maintainer) handleRedoMessage(ctx context.Context) {
	ticker := time.NewTicker(periodRedoInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.bootstrapped.Load() {
				log.Warn("can not advance redoTs since not bootstrapped",
					zap.String("changefeed", m.id.Name()))
				break
			}
			needUpdate := false
			updateCheckpointTs := true

			newWatermark := heartbeatpb.NewMaxWatermark()
			minRedoCheckpointTsForScheduler := m.controller.GetMinRedoCheckpointTs()
			minRedoCheckpointTsForBarrier := m.controller.redoBarrier.GetMinBlockedCheckpointTsForNewTables()
			// if there is no tables, there must be a table trigger dispatcher
			for id := range m.bootstrapper.GetAllNodeIDs() {
				// maintainer node has the table trigger dispatcher
				if id != m.selfNode.ID && m.controller.redoSpanController.GetTaskSizeByNodeID(id) <= 0 {
					continue
				}
				// node level watermark reported, ignore this round
				watermark, ok := m.redoTsByCapture.Get(id)
				if !ok {
					updateCheckpointTs = false
					log.Warn("redo checkpointTs can not be advanced, since missing capture heartbeat",
						zap.String("changefeed", m.id.Name()),
						zap.Any("node", id))
					continue
				}
				newWatermark.UpdateMin(watermark)
			}

			if minRedoCheckpointTsForScheduler != uint64(math.MaxUint64) {
				newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minRedoCheckpointTsForScheduler, ResolvedTs: minRedoCheckpointTsForScheduler})
			}
			if minRedoCheckpointTsForBarrier != uint64(math.MaxUint64) {
				newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minRedoCheckpointTsForBarrier, ResolvedTs: minRedoCheckpointTsForBarrier})
			}

			if m.redoTs.ResolvedTs < newWatermark.CheckpointTs && updateCheckpointTs {
				m.redoTs.ResolvedTs = newWatermark.CheckpointTs
				needUpdate = true
			}
			if m.redoTs.CheckpointTs < m.getWatermark().CheckpointTs {
				m.redoTs.CheckpointTs = m.getWatermark().CheckpointTs
				needUpdate = true
			}
			log.Debug("handle redo message",
				zap.Any("needUpdate", needUpdate),
				zap.Any("updateCheckpointTs", updateCheckpointTs),
				zap.Any("globalRedoTs", m.redoTs),
				zap.Any("checkpointTs", m.getWatermark().CheckpointTs),
				zap.Any("resolvedTs", newWatermark.CheckpointTs),
			)
			if needUpdate {
				msgs := make([]*messaging.TargetMessage, 0, len(m.bootstrapper.GetAllNodeIDs()))
				for id := range m.bootstrapper.GetAllNodeIDs() {
					msgs = append(msgs, messaging.NewSingleTargetMessage(id, messaging.HeartbeatCollectorTopic, &heartbeatpb.RedoMessage{
						ChangefeedID: m.redoTs.ChangefeedID,
						CheckpointTs: m.redoTs.CheckpointTs,
						ResolvedTs:   m.redoTs.ResolvedTs,
					}))
				}
				m.sendMessages(msgs)
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
			updateCheckpointTs := true
			if !m.bootstrapped.Load() {
				log.Warn("can not advance checkpointTs since not bootstrapped",
					zap.String("changefeed", m.id.Name()),
					zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
					zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs))
				break
			}

			// the min checkpointTs is taken from the minimum of three sources: dispatcher reported checkpointTs,
			// minimum checkpointTs of new tables undergoing DDL, and checkpointTs of tasks in scheduling.

			minCheckpointTsForScheduler := m.controller.GetMinCheckpointTs()
			minCheckpointTsForBarrier := m.controller.barrier.GetMinBlockedCheckpointTsForNewTables()

			newWatermark := heartbeatpb.NewMaxWatermark()
			// if there is no tables, there must be a table trigger dispatcher
			for id := range m.bootstrapper.GetAllNodeIDs() {
				// maintainer node has the table trigger dispatcher
				if id != m.selfNode.ID && m.controller.spanController.GetTaskSizeByNodeID(id) <= 0 {
					continue
				}
				// node level watermark reported, ignore this round
				watermark, ok := m.checkpointTsByCapture.Get(id)
				if !ok {
					updateCheckpointTs = false
					log.Warn("checkpointTs can not be advanced, since missing capture heartbeat",
						zap.String("changefeed", m.id.Name()),
						zap.Any("node", id),
						zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
						zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs))
					continue
				}
				newWatermark.UpdateMin(watermark)
			}

			if !updateCheckpointTs {
				break
			}

			if minCheckpointTsForBarrier != uint64(math.MaxUint64) {
				newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minCheckpointTsForBarrier, ResolvedTs: minCheckpointTsForBarrier})
			}
			if minCheckpointTsForScheduler != uint64(math.MaxUint64) {
				newWatermark.UpdateMin(heartbeatpb.Watermark{CheckpointTs: minCheckpointTsForScheduler, ResolvedTs: minCheckpointTsForScheduler})
			}

			log.Debug("can advance checkpointTs",
				zap.String("changefeed", m.id.Name()),
				zap.Uint64("newCheckpointTs", newWatermark.CheckpointTs),
				zap.Uint64("newResolvedTs", newWatermark.ResolvedTs),
			)

			m.setWatermark(*newWatermark)
			m.updateMetrics()
		}
	}
}

func (m *Maintainer) updateMetrics() {
	watermark := m.getWatermark()

	pdTime := m.pdClock.CurrentTime()
	phyCkpTs := oracle.ExtractPhysical(watermark.CheckpointTs)
	m.changefeedCheckpointTsGauge.Set(float64(phyCkpTs))
	lag := float64(oracle.GetPhysical(pdTime)-phyCkpTs) / 1e3
	m.changefeedCheckpointTsLagGauge.Set(lag)

	phyResolvedTs := oracle.ExtractPhysical(watermark.ResolvedTs)
	m.changefeedResolvedTsGauge.Set(float64(phyResolvedTs))
	lag = float64(oracle.GetPhysical(pdTime)-phyResolvedTs) / 1e3
	m.changefeedResolvedTsLagGauge.Set(lag)

	m.changefeedStatusGauge.Set(float64(m.scheduleState.Load()))
}

// send message to other components
func (m *Maintainer) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		err := m.mc.SendCommand(msg)
		if err != nil {
			log.Debug("failed to send maintainer request",
				zap.String("changefeed", m.id.Name()),
				zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (m *Maintainer) onHeartBeatRequest(msg *messaging.TargetMessage) {
	// ignore the heartbeat if the maintainer not bootstrapped
	if !m.bootstrapped.Load() {
		return
	}
	req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
	// TODO:add comment and test to ensure the operator will get first before calculate checkpointTs
	m.controller.HandleStatus(msg.From, req.Statuses)
	if req.Watermark != nil {
		old, ok := m.checkpointTsByCapture.Get(msg.From)
		if !ok || req.Watermark.Seq >= old.Seq {
			m.checkpointTsByCapture.Set(msg.From, *req.Watermark)
		}
	}
	if req.RedoWatermark != nil {
		old, ok := m.redoTsByCapture.Get(msg.From)
		if !ok || req.RedoWatermark.Seq >= old.Seq {
			m.redoTsByCapture.Set(msg.From, *req.RedoWatermark)
		}
	}
	if req.Err != nil {
		log.Error("dispatcher report an error",
			zap.Stringer("changefeed", m.id),
			zap.Stringer("sourceNode", msg.From),
			zap.String("error", req.Err.Message))
		m.onError(msg.From, req.Err)
	}
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
	if !m.bootstrapped.Load() {
		return
	}
	req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)

	var ackMsg *messaging.TargetMessage
	if common.IsDefaultMode(req.Mode) {
		ackMsg = m.controller.barrier.HandleStatus(msg.From, req)
	} else {
		ackMsg = m.controller.redoBarrier.HandleStatus(msg.From, req)
	}
	if ackMsg != nil {
		m.sendMessages([]*messaging.TargetMessage{ackMsg})
	}
}

// onMaintainerBootstrapResponse is called when a maintainer bootstrap response(send by dispatcher manager) is received.
func (m *Maintainer) onMaintainerBootstrapResponse(msg *messaging.TargetMessage) {
	log.Info("received maintainer bootstrap response",
		zap.String("changefeed", m.id.Name()),
		zap.Stringer("sourceNodeID", msg.From))

	resp := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
	if resp.Err != nil {
		log.Warn("maintainer bootstrap failed",
			zap.String("changefeed", m.id.Name()),
			zap.String("error", resp.Err.Message))
		m.onError(msg.From, resp.Err)
		return
	}

	cachedResp := m.bootstrapper.HandleBootstrapResponse(
		msg.From,
		resp,
	)

	if cachedResp != nil {
		m.onBootstrapDone(cachedResp)
	}

	// When receiving a bootstrap response from our own node's dispatcher manager
	// (which handles table trigger events), mark this changefeed is not new created.
	if msg.From == m.selfNode.ID {
		m.newChangefeed = false
	}
}

func (m *Maintainer) onMaintainerPostBootstrapResponse(msg *messaging.TargetMessage) {
	log.Info("received maintainer post bootstrap response",
		zap.String("changefeed", m.id.Name()),
		zap.Any("server", msg.From))
	resp := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapResponse)
	if resp.Err != nil {
		log.Warn("maintainer post bootstrap failed",
			zap.String("changefeed", m.id.Name()),
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
		}, selfNode.ID)
	return tableTriggerEventDispatcherID, ddlSpan
}

func (m *Maintainer) onBootstrapDone(cachedResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	if cachedResp == nil {
		return
	}
	isMySQLSinkCompatible, err := isMysqlCompatible(m.config.SinkURI)
	if err != nil {
		m.handleError(err)
		return
	}
	msg, err := m.controller.FinishBootstrap(cachedResp, isMySQLSinkCompatible)
	if err != nil {
		m.handleError(err)
		return
	}
	m.bootstrapped.Store(true)

	// Memory Consumption is 64(tableName/schemaName limit) * 4(utf8.UTFMax) * 2(tableName+schemaName) * tableNum
	// For an extreme case(100w tables, and 64 utf8 characters for each name), the memory consumption is about 488MB.
	// For a normal case(100w tables, and 16 ascii characters for each name), the memory consumption is about 30MB.
	m.postBootstrapMsg = msg
	m.sendPostBootstrapRequest()
	// set status changed to true, trigger the maintainer manager to send heartbeat to coordinator
	// to report the this changefeed's status
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
	if m.postBootstrapMsg != nil {
		m.sendPostBootstrapRequest()
	}
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
					ChangefeedID: m.id.ToPB(),
					Removed:      m.changefeedRemoved.Load(),
				}))
		}
	}
	if len(msgs) > 0 {
		m.sendMessages(msgs)
		log.Info("send maintainer close request to all dispatcher managers",
			zap.Stringer("changefeed", m.id),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
			zap.Int("nodeCount", len(msgs)))
	}
	return len(msgs) == 0
}

// handleError set the caches the error, the error will be reported to coordinator
// and coordinator remove this maintainer
func (m *Maintainer) handleError(err error) {
	log.Error("an error occurred in maintainer",
		zap.String("changefeed", m.id.Name()), zap.Error(err))
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
func (m *Maintainer) createBootstrapMessageFactory() bootstrap.NewBootstrapMessageFn {
	// cfgBytes only holds necessary fields to initialize a changefeed dispatcher manager.
	cfgBytes, err := json.Marshal(m.config.ToChangefeedConfig())
	if err != nil {
		log.Panic("marshal changefeed config failed",
			zap.String("changefeed", m.id.Name()),
			zap.Error(err))
	}
	return func(id node.ID) *messaging.TargetMessage {
		msg := &heartbeatpb.MaintainerBootstrapRequest{
			ChangefeedID:                      m.id.ToPB(),
			Config:                            cfgBytes,
			StartTs:                           m.startCheckpointTs,
			TableTriggerEventDispatcherId:     nil,
			RedoTableTriggerEventDispatcherId: nil,
			IsNewChangefeed:                   false,
		}

		// only send dispatcher id to dispatcher manager on the same node
		if id == m.selfNode.ID {
			log.Info("create table event trigger dispatcher bootstrap message",
				zap.String("changefeed", m.id.String()),
				zap.String("server", id.String()),
				zap.String("dispatcherID", m.ddlSpan.ID.String()),
				zap.Uint64("startTs", m.startCheckpointTs),
			)
			msg.TableTriggerEventDispatcherId = m.ddlSpan.ID.ToPB()
			if m.enableRedo {
				msg.RedoTableTriggerEventDispatcherId = m.redoDDLSpan.ID.ToPB()
			}
			msg.IsNewChangefeed = m.newChangefeed
		}

		log.Info("New maintainer bootstrap message to dispatcher manager",
			zap.String("changefeed", m.id.String()),
			zap.String("server", id.String()),
			zap.Uint64("startTs", m.startCheckpointTs))

		return messaging.NewSingleTargetMessage(id, messaging.DispatcherManagerManagerTopic, msg)
	}
}

func (m *Maintainer) onPeriodTask() {
	// send scheduling messages
	m.handleResendMessage()
	m.collectMetrics()
}

func (m *Maintainer) collectMetrics() {
	if !m.bootstrapped.Load() {
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
		metrics.TableStateGauge.WithLabelValues(m.id.Keyspace(), m.id.Name(), "Absent", common.StringMode(mode)).Set(float64(absent))
		metrics.TableStateGauge.WithLabelValues(m.id.Keyspace(), m.id.Name(), "Working", common.StringMode(mode)).Set(float64(working))
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
				changefeedID: m.id,
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
