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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/bootstrap"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	periodEventInterval = time.Millisecond * 200
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
	barrier    *Barrier

	pdClock pdutil.Clock

	eventCh *chann.DrainableChann[*Event]

	taskScheduler threadpool.ThreadPool
	mc            messaging.MessageCenter

	watermark struct {
		mu sync.RWMutex
		*heartbeatpb.Watermark
	}

	checkpointTsByCapture map[node.ID]heartbeatpb.Watermark

	state        atomic.Int32
	bootstrapper *bootstrap.Bootstrapper[heartbeatpb.MaintainerBootstrapResponse]

	changefeedSate model.FeedState

	removed *atomic.Bool

	bootstrapped     bool
	postBootstrapMsg *heartbeatpb.MaintainerPostBootstrapRequest

	// startCheckpointTs is the check point ts when the maintainer is created
	// it's will be sent to dispatcher manager to initialize the checkpoint ts and get the real checkpoint ts
	startCheckpointTs uint64
	// the dispatcher id of table trigger event dispatcher, it's generated by maintainer
	// table trigger event dispatcher runs on the same node with maintainer,
	// so when a maintainer is created, that means the dispatcher is gone and must be recreated.
	ddlSpan *replica.SpanReplication

	pdEndpoints []string
	nodeManager *watcher.NodeManager
	// closedNodes is used to record the nodes that dispatcherManager is closed
	closedNodes map[node.ID]struct{}

	statusChanged *atomic.Bool

	mutex       sync.Mutex // protect nodeChanged and do onNodeChanged()
	nodeChanged bool

	lastReportTime time.Time

	removing        atomic.Bool
	cascadeRemoving bool
	// the changefeed is removed, notify the dispatcher manager to clear ddl_ts table
	changefeedRemoved atomic.Bool

	lastPrintStatusTime time.Time
	// lastCheckpointTsTime time.Time

	// true when the changefeed is new created, or resume by overwriteCheckpointTs
	// false when otherwise, such as maintainer move to different nodes.
	newChangefeed bool

	errLock             sync.Mutex
	runningErrors       map[node.ID]*heartbeatpb.RunningError
	cancelUpdateMetrics context.CancelFunc

	changefeedCheckpointTsGauge    prometheus.Gauge
	changefeedCheckpointTsLagGauge prometheus.Gauge
	changefeedResolvedTsGauge      prometheus.Gauge
	changefeedResolvedTsLagGauge   prometheus.Gauge
	changefeedStatusGauge          prometheus.Gauge
	scheduledTaskGauge             prometheus.Gauge
	runningTaskGauge               prometheus.Gauge
	tableCountGauge                prometheus.Gauge
	handleEventDuration            prometheus.Observer
}

// NewMaintainer create the maintainer for the changefeed
func NewMaintainer(cfID common.ChangeFeedID,
	conf *config.SchedulerConfig,
	cfg *config.ChangeFeedInfo,
	selfNode *node.Info,
	taskScheduler threadpool.ThreadPool,
	pdAPI pdutil.PDAPIClient,
	tsoClient replica.TSOClient,
	regionCache split.RegionCache,
	checkpointTs uint64,
	newChangfeed bool,
) *Maintainer {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingReplicaSet(cfID, tableTriggerEventDispatcherID, tsoClient,
		heartbeatpb.DDLSpanSchemaID,
		heartbeatpb.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    checkpointTs,
		}, selfNode.ID)

	// TODO: Retrieve the correct pdClock from the context once multiple upstreams are supported.
	// For now, since there is only one upstream, using the default pdClock is sufficient.
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)
	m := &Maintainer{
		id:                cfID,
		pdClock:           pdClock,
		selfNode:          selfNode,
		eventCh:           chann.NewAutoDrainChann[*Event](),
		taskScheduler:     taskScheduler,
		startCheckpointTs: checkpointTs,
		controller: NewController(cfID, checkpointTs, pdAPI, tsoClient, regionCache, taskScheduler,
			cfg.Config, ddlSpan, conf.AddTableBatchSize, time.Duration(conf.CheckBalanceInterval)),
		mc:              mc,
		removed:         atomic.NewBool(false),
		nodeManager:     nodeManager,
		closedNodes:     make(map[node.ID]struct{}),
		statusChanged:   atomic.NewBool(true),
		nodeChanged:     false,
		cascadeRemoving: false,
		config:          cfg,

		ddlSpan:               ddlSpan,
		checkpointTsByCapture: make(map[node.ID]heartbeatpb.Watermark),
		runningErrors:         map[node.ID]*heartbeatpb.RunningError{},
		newChangefeed:         newChangfeed,

		changefeedCheckpointTsGauge:    metrics.ChangefeedCheckpointTsGauge.WithLabelValues(cfID.Namespace(), cfID.Name()),
		changefeedCheckpointTsLagGauge: metrics.ChangefeedCheckpointTsLagGauge.WithLabelValues(cfID.Namespace(), cfID.Name()),
		changefeedResolvedTsGauge:      metrics.ChangefeedResolvedTsGauge.WithLabelValues(cfID.Namespace(), cfID.Name()),
		changefeedResolvedTsLagGauge:   metrics.ChangefeedResolvedTsLagGauge.WithLabelValues(cfID.Namespace(), cfID.Name()),
		changefeedStatusGauge:          metrics.ChangefeedStatusGauge.WithLabelValues(cfID.Namespace(), cfID.Name()),
		scheduledTaskGauge:             metrics.ScheduleTaskGuage.WithLabelValues(cfID.Namespace(), cfID.Name()),
		runningTaskGauge:               metrics.RunningScheduleTaskGauge.WithLabelValues(cfID.Namespace(), cfID.Name()),
		tableCountGauge:                metrics.TableGauge.WithLabelValues(cfID.Namespace(), cfID.Name()),
		handleEventDuration:            metrics.MaintainerHandleEventDuration.WithLabelValues(cfID.Namespace(), cfID.Name()),
	}

	m.watermark.Watermark = &heartbeatpb.Watermark{
		CheckpointTs: checkpointTs,
		ResolvedTs:   checkpointTs,
	}
	m.state.Store(int32(heartbeatpb.ComponentState_Working))
	m.bootstrapper = bootstrap.NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse](m.id.Name(), m.getNewBootstrapFn())
	log.Info("changefeed maintainer is created", zap.String("id", cfID.String()),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.String("ddlDispatcherID", tableTriggerEventDispatcherID.String()))
	metrics.MaintainerGauge.WithLabelValues(cfID.Namespace(), cfID.Name()).Inc()
	// Should update metrics immediately when maintainer is created
	// FIXME: Use a correct context
	ctx, cancel := context.WithCancel(context.Background())
	m.cancelUpdateMetrics = cancel
	go m.runUpdateMetrics(ctx)
	go m.runHandleEvents(ctx)
	return m
}

func NewMaintainerForRemove(cfID common.ChangeFeedID,
	conf *config.SchedulerConfig,
	selfNode *node.Info,
	taskScheduler threadpool.ThreadPool,
	pdAPI pdutil.PDAPIClient,
	tsoClient replica.TSOClient,
	regionCache split.RegionCache,
) *Maintainer {
	unused := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		SinkURI:      "",
		Config:       config.GetDefaultReplicaConfig(),
	}
	m := NewMaintainer(cfID, conf, unused, selfNode, taskScheduler, pdAPI,
		tsoClient, regionCache, 1, false)
	m.cascadeRemoving = true
	// setup period event
	m.submitScheduledEvent(m.taskScheduler, &Event{
		changefeedID: m.id,
		eventType:    EventPeriod,
	}, time.Now().Add(periodEventInterval))
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
				zap.String("id", m.id.String()),
				zap.Int("type", event.eventType),
				zap.Duration("duration", duration))
		}
		m.handleEventDuration.Observe(duration.Seconds())
	}()
	if m.state.Load() == int32(heartbeatpb.ComponentState_Stopped) {
		log.Warn("maintainer is stopped, ignore",
			zap.String("changefeed", m.id.String()))
		return false
	}

	// first check the online/offline nodes
	m.checkOnNodeChanged()

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

func (m *Maintainer) checkOnNodeChanged() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.nodeChanged {
		m.onNodeChanged()
		m.nodeChanged = false
	}
}

// Close cleanup resources
func (m *Maintainer) Close() {
	m.cancelUpdateMetrics()
	m.cleanupMetrics()
	m.controller.Stop()
	log.Info("changefeed maintainer closed",
		zap.String("id", m.id.String()),
		zap.Bool("removed", m.removed.Load()),
		zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs))
}

func (m *Maintainer) GetMaintainerStatus() *heartbeatpb.MaintainerStatus {
	m.errLock.Lock()
	defer m.errLock.Unlock()
	var runningErrors []*heartbeatpb.RunningError
	if len(m.runningErrors) > 0 {
		runningErrors = make([]*heartbeatpb.RunningError, 0, len(m.runningErrors))
		for _, e := range m.runningErrors {
			runningErrors = append(runningErrors, e)
		}
		clear(m.runningErrors)
	}

	status := &heartbeatpb.MaintainerStatus{
		ChangefeedID: m.id.ToPB(),
		FeedState:    string(m.changefeedSate),
		State:        heartbeatpb.ComponentState(m.state.Load()),
		CheckpointTs: m.getWatermark().CheckpointTs,
		Err:          runningErrors,
	}
	return status
}

func (m *Maintainer) initialize() error {
	start := time.Now()
	log.Info("start to initialize changefeed maintainer",
		zap.String("id", m.id.String()))

	// detect the capture changes
	m.nodeManager.RegisterNodeChangeHandler(node.ID("maintainer-"+m.id.Name()), func(allNodes map[node.ID]*node.Info) {
		m.mutex.Lock()
		m.nodeChanged = true
		m.mutex.Unlock()
	})
	// init bootstrapper nodes
	nodes := m.nodeManager.GetAliveNodes()
	log.Info("changefeed bootstrap initial nodes",
		zap.Int("nodes", len(nodes)))
	newNodes := make([]*node.Info, 0, len(nodes))
	for _, n := range nodes {
		newNodes = append(newNodes, n)
	}
	m.sendMessages(m.bootstrapper.HandleNewNodes(newNodes))
	// setup period event
	m.submitScheduledEvent(m.taskScheduler, &Event{
		changefeedID: m.id,
		eventType:    EventPeriod,
	}, time.Now().Add(periodEventInterval))
	log.Info("changefeed maintainer initialized",
		zap.String("id", m.id.String()),
		zap.Duration("duration", time.Since(start)))
	m.state.Store(int32(heartbeatpb.ComponentState_Working))
	m.statusChanged.Store(true)
	return nil
}

func (m *Maintainer) cleanupMetrics() {
	metrics.ChangefeedCheckpointTsGauge.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.ChangefeedCheckpointTsLagGauge.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.ChangefeedResolvedTsGauge.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.ChangefeedResolvedTsLagGauge.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.ChangefeedStatusGauge.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.ScheduleTaskGuage.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.RunningScheduleTaskGauge.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.TableGauge.DeleteLabelValues(m.id.Namespace(), m.id.Name())
	metrics.MaintainerHandleEventDuration.DeleteLabelValues(m.id.Namespace(), m.id.Name())
}

func (m *Maintainer) onInit() bool {
	err := m.initialize()
	if err != nil {
		m.handleError(err)
	}
	return false
}

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
		m.onMaintainerCloseResponse(msg.From, msg.Message[0].(*heartbeatpb.MaintainerCloseResponse))
	case messaging.TypeRemoveMaintainerRequest:
		req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
		m.onRemoveMaintainer(req.Cascade, req.Removed)
	case messaging.TypeCheckpointTsMessage:
		m.onCheckpointTsPersisted(msg.Message[0].(*heartbeatpb.CheckpointTsMessage))
	default:
		log.Panic("unexpected message type",
			zap.String("changefeed", m.id.Name()),
			zap.String("type", msg.Type.String()))
	}
}

func (m *Maintainer) onRemoveMaintainer(cascade, changefeedRemoved bool) {
	m.removing.Store(true)
	m.cascadeRemoving = cascade
	m.changefeedRemoved.Store(changefeedRemoved)
	closed := m.tryCloseChangefeed()
	if closed {
		m.removed.Store(true)
		m.state.Store(int32(heartbeatpb.ComponentState_Stopped))
		metrics.MaintainerGauge.WithLabelValues(m.id.Namespace(), m.id.Name()).Dec()
	}
}

func (m *Maintainer) onCheckpointTsPersisted(msg *heartbeatpb.CheckpointTsMessage) {
	// the table trigger dispatcher is on the same node with maintainer, and it will send the water marker to downstream
	m.sendMessages([]*messaging.TargetMessage{
		messaging.NewSingleTargetMessage(m.selfNode.ID, messaging.HeartbeatCollectorTopic, msg),
	})
}

func (m *Maintainer) onNodeChanged() {
	currentNodes := m.bootstrapper.GetAllNodes()

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
			delete(m.checkpointTsByCapture, id)
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

func (m *Maintainer) calCheckpointTs() {
	defer m.updateMetrics()
	if !m.bootstrapped {
		log.Warn("can not advance checkpointTs since not bootstrapped",
			zap.String("changefeed", m.id.Name()),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
			zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs))
		return
	}
	// make sure there is no task running
	// the dispatcher changing come from:
	// 1. node change
	// 2. ddl
	// 3. interval scheduling, like balance, split
	if !m.controller.ScheduleFinished() {
		log.Warn("can not advance checkpointTs since schedule is not finished",
			zap.String("changefeed", m.id.Name()),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
			zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs),
		)
		return
	}
	if m.barrier.ShouldBlockCheckpointTs() {
		log.Warn("can not advance checkpointTs since barrier is blocking",
			zap.String("changefeed", m.id.Name()),
			zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
			zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs),
		)
		return
	}
	newWatermark := heartbeatpb.NewMaxWatermark()
	// if there is no tables, there must be a table trigger dispatcher
	for id := range m.bootstrapper.GetAllNodes() {
		// maintainer node has the table trigger dispatcher
		if id != m.selfNode.ID && m.controller.GetTaskSizeByNodeID(id) <= 0 {
			continue
		}
		// node level watermark reported, ignore this round
		if _, ok := m.checkpointTsByCapture[id]; !ok {
			log.Warn("checkpointTs can not be advanced, since missing capture heartbeat",
				zap.String("changefeed", m.id.Name()),
				zap.Any("node", id),
				zap.Uint64("checkpointTs", m.getWatermark().CheckpointTs),
				zap.Uint64("resolvedTs", m.getWatermark().ResolvedTs))
			return
		}
		newWatermark.UpdateMin(m.checkpointTsByCapture[id])
	}

	m.setWatermark(*newWatermark)
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

	m.changefeedStatusGauge.Set(float64(m.state.Load()))
}

// send message to remote
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
	// ignore the heartbeat if the maintianer not bootstrapped
	if !m.bootstrapped {
		return
	}
	req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
	if req.Watermark != nil {
		old, ok := m.checkpointTsByCapture[msg.From]
		if !ok || req.Watermark.Seq >= old.Seq {
			m.checkpointTsByCapture[msg.From] = *req.Watermark
		}
	}
	m.controller.HandleStatus(msg.From, req.Statuses)
	if req.Err != nil {
		log.Warn("dispatcher report an error",
			zap.String("changefeed", m.id.Name()),
			zap.String("from", msg.From.String()),
			zap.String("error", req.Err.Message))
		m.onError(msg.From, req.Err)
	}
}

func (m *Maintainer) onError(from node.ID, err *heartbeatpb.RunningError) {
	err.Node = from.String()
	if info, ok := m.nodeManager.GetAliveNodes()[from]; ok {
		err.Node = info.AdvertiseAddr
	}
	m.errLock.Lock()
	m.statusChanged.Store(true)
	m.runningErrors[from] = err
	m.errLock.Unlock()
}

func (m *Maintainer) onBlockStateRequest(msg *messaging.TargetMessage) {
	// the barrier is not initialized
	if !m.bootstrapped {
		return
	}
	req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)
	ackMsg := m.barrier.HandleStatus(msg.From, req)
	m.sendMessages([]*messaging.TargetMessage{ackMsg})
}

func (m *Maintainer) onMaintainerBootstrapResponse(msg *messaging.TargetMessage) {
	log.Info("received maintainer bootstrap response",
		zap.String("changefeed", m.id.Name()),
		zap.Any("server", msg.From))
	resp := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
	if resp.Err != nil {
		log.Warn("maintainer bootstrap failed",
			zap.String("changefeed", m.id.Name()),
			zap.String("error", resp.Err.Message))
		m.onError(msg.From, resp.Err)
		return
	}
	cachedResp := m.bootstrapper.HandleBootstrapResponse(msg.From, msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse))
	m.onBootstrapDone(cachedResp)

	// when get the bootstrap response from the event dispatcher manager with table trigger event,
	// set new changefeed to false, which means the changefeed is no longer new created.
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

func (m *Maintainer) onBootstrapDone(cachedResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	if cachedResp == nil {
		return
	}
	isMysqlCompatibleBackend, err := util.IsMysqlCompatibleBackend(m.config.SinkURI)
	if err != nil {
		m.handleError(err)
		return
	}
	barrier, msg, err := m.controller.FinishBootstrap(cachedResp, isMysqlCompatibleBackend)
	if err != nil {
		m.handleError(err)
		return
	}
	m.barrier = barrier
	m.bootstrapped = true

	// Memory Consumption is 64(tableName/schemaName limit) * 4(utf8.UTFMax) * 2(tableName+schemaName) * tableNum
	// For an extreme case(100w tables, and 64 utf8 characters for each name), the memory consumption is about 488MB.
	// For a normal case(100w tables, and 16 ascii characters for each name), the memory consumption is about 30MB.
	m.postBootstrapMsg = msg
	m.sendPostBootstrapRequest()
}

func (m *Maintainer) sendPostBootstrapRequest() {
	if m.postBootstrapMsg != nil {
		msg := messaging.NewSingleTargetMessage(m.selfNode.ID, messaging.DispatcherManagerManagerTopic, m.postBootstrapMsg)
		m.sendMessages([]*messaging.TargetMessage{msg})
	}
}

func (m *Maintainer) onMaintainerCloseResponse(from node.ID, response *heartbeatpb.MaintainerCloseResponse) {
	if response.Success {
		m.closedNodes[from] = struct{}{}
		m.onRemoveMaintainer(m.cascadeRemoving, m.changefeedRemoved.Load())
	}
}

func (m *Maintainer) handleResendMessage() {
	// resend closing message
	if m.removing.Load() {
		m.trySendMaintainerCloseRequestToAllNode()
		return
	}
	// resend bootstrap message
	m.sendMessages(m.bootstrapper.ResendBootstrapMessage())
	if m.postBootstrapMsg != nil {
		m.sendPostBootstrapRequest()
	}
	if m.barrier != nil {
		// resend barrier ack messages
		m.sendMessages(m.barrier.Resend())
	}
}

func (m *Maintainer) tryCloseChangefeed() bool {
	if m.state.Load() != int32(heartbeatpb.ComponentState_Stopped) {
		m.statusChanged.Store(true)
	}
	if !m.cascadeRemoving {
		m.controller.RemoveTasksByTableIDs(m.ddlSpan.Span.TableID)
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
		log.Info("send maintainer close request",
			zap.String("changefeed", m.id.Name()),
			zap.Int("count", len(msgs)))
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
	m.runningErrors = map[node.ID]*heartbeatpb.RunningError{
		m.selfNode.ID: {
			Time:    time.Now().String(),
			Node:    m.selfNode.AdvertiseAddr,
			Code:    code,
			Message: err.Error(),
		},
	}
	m.statusChanged.Store(true)
}

// getNewBootstrapFn returns a function that creates a new bootstrap message to initialize
// a changefeed dispatcher manager.
func (m *Maintainer) getNewBootstrapFn() bootstrap.NewBootstrapMessageFn {
	// cfgBytes only holds necessary fields to initialize a changefeed dispatcher.
	cfgBytes, err := json.Marshal(m.config.ToChangefeedConfig())
	if err != nil {
		log.Panic("marshal changefeed config failed",
			zap.String("changefeed", m.id.Name()),
			zap.Error(err))
	}
	return func(id node.ID) *messaging.TargetMessage {
		msg := &heartbeatpb.MaintainerBootstrapRequest{
			ChangefeedID:                  m.id.ToPB(),
			Config:                        cfgBytes,
			StartTs:                       m.startCheckpointTs,
			TableTriggerEventDispatcherId: nil,
			IsNewChangfeed:                false,
		}

		// only send dispatcher id to dispatcher manager on the same node
		if id == m.selfNode.ID {
			log.Info("create table event trigger dispatcher", zap.String("changefeed", m.id.String()),
				zap.String("server", id.String()),
				zap.String("dispatcherID", m.ddlSpan.ID.String()))
			msg.TableTriggerEventDispatcherId = m.ddlSpan.ID.ToPB()
			msg.IsNewChangfeed = m.newChangefeed
		}
		log.Info("send maintainer bootstrap message",
			zap.String("changefeed", m.id.String()),
			zap.String("server", id.String()),
			zap.Uint64("startTs", m.startCheckpointTs),
			zap.Bool("isNewChangefed", msg.IsNewChangfeed))
		return messaging.NewSingleTargetMessage(id, messaging.DispatcherManagerManagerTopic, msg)
	}
}

func (m *Maintainer) onPeriodTask() {
	// send scheduling messages
	m.handleResendMessage()
	m.collectMetrics()
	m.calCheckpointTs()
	m.submitScheduledEvent(m.taskScheduler, &Event{
		changefeedID: m.id,
		eventType:    EventPeriod,
	}, time.Now().Add(periodEventInterval))
}

func (m *Maintainer) collectMetrics() {
	if !m.bootstrapped {
		return
	}
	if time.Since(m.lastPrintStatusTime) > time.Second*20 {
		// exclude the table trigger
		total := m.controller.TaskSize() - 1
		scheduling := m.controller.replicationDB.GetSchedulingSize()
		working := m.controller.replicationDB.GetReplicatingSize()
		absent := m.controller.replicationDB.GetAbsentSize()

		m.tableCountGauge.Set(float64(total))
		m.scheduledTaskGauge.Set(float64(scheduling))
		metrics.TableStateGauge.WithLabelValues(m.id.Namespace(), m.id.Name(), "Absent").Set(float64(absent))
		metrics.TableStateGauge.WithLabelValues(m.id.Namespace(), m.id.Name(), "Working").Set(float64(working))
		m.lastPrintStatusTime = time.Now()
	}
}

func (m *Maintainer) runUpdateMetrics(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("stop update metrics")
			return
		case <-ticker.C:
			m.updateMetrics()
		}
	}
}

func (m *Maintainer) runHandleEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-m.eventCh.Out():
			m.HandleEvent(event)
		}
	}
}

// for test only
func (m *Maintainer) MoveTable(tableId int64, targetNode node.ID) error {
	return m.controller.moveTable(tableId, targetNode)
}

func (m *Maintainer) GetTables() []*replica.SpanReplication {
	return m.controller.replicationDB.GetAllTasks()
}

// SubmitScheduledEvent submits a task to controller pool to send a future event
func (m *Maintainer) submitScheduledEvent(
	scheduler threadpool.ThreadPool,
	event *Event,
	scheduleTime time.Time,
) {
	task := func() time.Time {
		m.pushEvent(event)
		return time.Time{}
	}
	scheduler.SubmitFunc(task, scheduleTime)
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

func (m *Maintainer) GetDispatcherCount() int {
	return len(m.controller.GetAllTasks())
}
