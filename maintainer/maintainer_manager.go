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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

const nodeHeartbeatInterval = 5 * time.Second

// Manager is the manager of all changefeed maintainer in a ticdc server, each ticdc server will
// start a Manager when the ticdc server is startup. It responsible for:
// 1. Handle bootstrap command from coordinator and report all changefeed maintainer status.
// 2. Handle other commands from coordinator: like add or remove changefeed maintainer
// 3. Manage maintainers lifetime
type Manager struct {
	mc   messaging.MessageCenter
	conf *config.SchedulerConfig

	liveness  *api.Liveness
	nodeEpoch uint64

	// changefeedID -> maintainer
	maintainers sync.Map

	coordinatorID      node.ID
	coordinatorVersion int64

	nodeInfo *node.Info

	// msgCh is used to cache messages from coordinator
	msgCh chan *messaging.TargetMessage

	taskScheduler threadpool.ThreadPool

	lastNodeHeartbeatSentAt time.Time
}

// NewMaintainerManager create a changefeed maintainer manager instance
// and register message handler to message center
func NewMaintainerManager(
	nodeInfo *node.Info,
	conf *config.SchedulerConfig,
	liveness *api.Liveness,
) *Manager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	if liveness == nil {
		liveness = new(api.Liveness)
	}
	nodeEpoch := uint64(time.Now().UnixNano())
	if nodeEpoch == 0 {
		nodeEpoch = 1
	}
	m := &Manager{
		mc:            mc,
		conf:          conf,
		liveness:      liveness,
		nodeEpoch:     nodeEpoch,
		maintainers:   sync.Map{},
		nodeInfo:      nodeInfo,
		msgCh:         make(chan *messaging.TargetMessage, 1024),
		taskScheduler: threadpool.NewThreadPoolDefault(),
	}

	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.recvMessages)
	mc.RegisterHandler(messaging.MaintainerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			req := msg.Message[0].(*heartbeatpb.MaintainerCloseResponse)
			return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
		})
	return m
}

// recvMessages is the message handler for maintainer manager
func (m *Manager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// Coordinator related messages
	case messaging.TypeAddMaintainerRequest,
		messaging.TypeRemoveMaintainerRequest,
		messaging.TypeCoordinatorBootstrapRequest,
		messaging.TypeSetNodeLivenessRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	// receive bootstrap response message from the dispatcher manager
	case messaging.TypeMaintainerBootstrapResponse:
		req := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeMaintainerPostBootstrapResponse:
		req := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	// receive heartbeat message from dispatchers
	case messaging.TypeHeartBeatRequest:
		req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeBlockStatusRequest:
		req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeCheckpointTsMessage:
		req := msg.Message[0].(*heartbeatpb.CheckpointTsMessage)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeRedoResolvedTsProgressMessage:
		req := msg.Message[0].(*heartbeatpb.RedoResolvedTsProgressMessage)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	default:
		log.Warn("unknown message type, ignore it",
			zap.String("type", msg.Type.String()),
			zap.Any("message", msg.Message))
	}
	return nil
}

func (m *Manager) Name() string {
	return appcontext.MaintainerManager
}

func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-ticker.C:
			m.sendNodeHeartbeat(false)
			// 1.  try to send heartbeat to coordinator
			m.sendHeartbeat()
			// 2. cleanup removed maintainers
			m.maintainers.Range(func(key, value interface{}) bool {
				cf := value.(*Maintainer)
				if cf.removed.Load() {
					cf.Close()
					log.Info("maintainer removed, remove it from dynamic stream",
						zap.Stringer("changefeedID", cf.changefeedID),
						zap.Uint64("checkpointTs", cf.getWatermark().CheckpointTs),
					)
					m.maintainers.Delete(key)
				}
				return true
			})
		}
	}
}

func (m *Manager) newCoordinatorTopicMessage(msg messaging.IOTypeT) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		msg,
	)
}

func (m *Manager) sendMessages(msg *heartbeatpb.MaintainerHeartbeat) {
	target := m.newCoordinatorTopicMessage(msg)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
	}
}

func (m *Manager) sendNodeHeartbeat(force bool) {
	if m.coordinatorVersion <= 0 || m.coordinatorID.IsEmpty() {
		return
	}

	now := time.Now()
	if !force && now.Sub(m.lastNodeHeartbeatSentAt) < nodeHeartbeatInterval {
		return
	}

	hb := &heartbeatpb.NodeHeartbeat{
		Liveness:  m.toNodeLivenessPB(m.liveness.Load()),
		NodeEpoch: m.nodeEpoch,
	}
	target := m.newCoordinatorTopicMessage(hb)
	if err := m.mc.SendCommand(target); err != nil {
		log.Warn("send node heartbeat failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
		return
	}
	m.lastNodeHeartbeatSentAt = now
}

// Close closes, it's a block call
func (m *Manager) Close(_ context.Context) error {
	m.maintainers.Range(func(key, value interface{}) bool {
		maintainer := value.(*Maintainer)
		maintainer.Close()
		return true
	})
	return nil
}

func (m *Manager) onCoordinatorBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.CoordinatorBootstrapRequest)
	if m.coordinatorVersion > req.Version {
		log.Warn("ignore invalid coordinator version",
			zap.Int64("coordinatorVersion", m.coordinatorVersion),
			zap.Int64("version", req.Version))
		return
	}
	m.coordinatorID = msg.From
	m.coordinatorVersion = req.Version

	response := &heartbeatpb.CoordinatorBootstrapResponse{}
	m.maintainers.Range(func(key, value interface{}) bool {
		maintainer := value.(*Maintainer)
		status := maintainer.GetMaintainerStatus()
		response.Statuses = append(response.Statuses, status)
		maintainer.statusChanged.Store(false)
		maintainer.lastReportTime = time.Now()
		return true
	})

	msg = m.newCoordinatorTopicMessage(response)
	err := m.mc.SendCommand(msg)
	if err != nil {
		log.Warn("send bootstrap response failed",
			zap.Stringer("coordinatorID", m.coordinatorID),
			zap.Int64("coordinatorVersion", m.coordinatorVersion),
			zap.Error(err))
	}

	log.Info("new coordinator online, bootstrap response already sent",
		zap.Stringer("coordinatorID", m.coordinatorID),
		zap.Int64("version", m.coordinatorVersion))

	m.sendNodeHeartbeat(true)
}

func (m *Manager) onAddMaintainerRequest(req *heartbeatpb.AddMaintainerRequest) *heartbeatpb.MaintainerStatus {
	changefeedID := common.NewChangefeedIDFromPB(req.Id)
	_, ok := m.maintainers.Load(changefeedID)
	if ok {
		return nil
	}

	if m.liveness.Load() == api.LivenessCaptureStopping {
		log.Info("reject add maintainer request because node is stopping",
			zap.Stringer("nodeID", m.nodeInfo.ID),
			zap.Stringer("changefeedID", changefeedID))
		return nil
	}

	info := &config.ChangeFeedInfo{}
	err := json.Unmarshal(req.Config, info)
	if err != nil {
		log.Panic("decode changefeed fail", zap.Error(err))
	}
	if req.CheckpointTs == 0 {
		log.Panic("add maintainer with invalid checkpointTs",
			zap.Stringer("changefeedID", changefeedID),
			zap.Uint64("checkpointTs", req.CheckpointTs),
			zap.Any("info", info))
	}

	maintainer := NewMaintainer(changefeedID, m.conf, info, m.nodeInfo, m.taskScheduler, req.CheckpointTs, req.IsNewChangefeed, req.KeyspaceId)
	m.maintainers.Store(changefeedID, maintainer)
	maintainer.pushEvent(&Event{changefeedID: changefeedID, eventType: EventInit})
	return nil
}

func (m *Manager) onRemoveMaintainerRequest(msg *messaging.TargetMessage) *heartbeatpb.MaintainerStatus {
	req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	changefeedID := common.NewChangefeedIDFromPB(req.GetId())
	maintainer, ok := m.maintainers.Load(changefeedID)
	if !ok {
		if !req.Cascade {
			log.Warn("ignore remove maintainer request, "+
				"since the maintainer not found",
				zap.Stringer("changefeedID", changefeedID),
				zap.Any("request", req))
			return &heartbeatpb.MaintainerStatus{
				ChangefeedID: req.GetId(),
				State:        heartbeatpb.ComponentState_Stopped,
			}
		}

		// it's cascade remove, we should remove the dispatcher from all node
		// here we create a maintainer to run the remove the dispatcher logic
		maintainer = NewMaintainerForRemove(changefeedID, m.conf, m.nodeInfo, m.taskScheduler, req.KeyspaceId)
		m.maintainers.Store(changefeedID, maintainer)
	}
	maintainer.(*Maintainer).pushEvent(&Event{
		changefeedID: changefeedID,
		eventType:    EventMessage,
		message:      msg,
	})
	log.Info("received remove maintainer request",
		zap.Stringer("changefeedID", changefeedID))
	return nil
}

func (m *Manager) onDispatchMaintainerRequest(
	msg *messaging.TargetMessage,
) *heartbeatpb.MaintainerStatus {
	if m.coordinatorID != msg.From {
		log.Warn("ignore invalid coordinator id",
			zap.Any("request", msg),
			zap.Any("coordinatorID", m.coordinatorID),
			zap.Stringer("from", msg.From))
		return nil
	}
	switch msg.Type {
	case messaging.TypeAddMaintainerRequest:
		req := msg.Message[0].(*heartbeatpb.AddMaintainerRequest)
		return m.onAddMaintainerRequest(req)
	case messaging.TypeRemoveMaintainerRequest:
		return m.onRemoveMaintainerRequest(msg)
	default:
		log.Warn("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (m *Manager) sendHeartbeat() {
	if m.isBootstrap() {
		response := &heartbeatpb.MaintainerHeartbeat{}
		m.maintainers.Range(func(key, value interface{}) bool {
			cfMaintainer := value.(*Maintainer)
			if cfMaintainer.statusChanged.Load() ||
				time.Since(cfMaintainer.lastReportTime) > time.Second {
				mStatus := cfMaintainer.GetMaintainerStatus()
				response.Statuses = append(response.Statuses, mStatus)
				cfMaintainer.statusChanged.Store(false)
				cfMaintainer.lastReportTime = time.Now()
			}
			return true
		})
		if len(response.Statuses) != 0 {
			m.sendMessages(response)
		}
	}
}

func (m *Manager) handleMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeCoordinatorBootstrapRequest:
		m.onCoordinatorBootstrapRequest(msg)
	case messaging.TypeAddMaintainerRequest,
		messaging.TypeRemoveMaintainerRequest:
		if m.isBootstrap() {
			status := m.onDispatchMaintainerRequest(msg)
			if status == nil {
				return
			}
			response := &heartbeatpb.MaintainerHeartbeat{
				Statuses: []*heartbeatpb.MaintainerStatus{status},
			}
			m.sendMessages(response)
		}
	case messaging.TypeSetNodeLivenessRequest:
		m.onSetNodeLivenessRequest(msg)
	default:
	}
}

func (m *Manager) onSetNodeLivenessRequest(msg *messaging.TargetMessage) {
	if m.coordinatorID != msg.From {
		log.Warn("ignore set node liveness request from non coordinator",
			zap.Stringer("from", msg.From),
			zap.Stringer("coordinatorID", m.coordinatorID))
		return
	}

	req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	if req.NodeEpoch != m.nodeEpoch {
		log.Info("reject set node liveness request due to epoch mismatch",
			zap.Stringer("nodeID", m.nodeInfo.ID),
			zap.Uint64("localEpoch", m.nodeEpoch),
			zap.Uint64("requestEpoch", req.NodeEpoch))
		m.sendSetNodeLivenessResponse(m.liveness.Load())
		return
	}

	target := m.fromNodeLivenessPB(req.Target)
	current := m.liveness.Load()
	if target > current {
		if m.liveness.Store(target) {
			log.Info("node liveness transition applied",
				zap.Stringer("nodeID", m.nodeInfo.ID),
				zap.String("from", current.String()),
				zap.String("to", target.String()),
				zap.Uint64("epoch", m.nodeEpoch))
			current = target
			m.sendNodeHeartbeat(true)
		}
	}

	m.sendSetNodeLivenessResponse(current)
}

func (m *Manager) sendSetNodeLivenessResponse(applied api.Liveness) {
	resp := &heartbeatpb.SetNodeLivenessResponse{
		Applied:   m.toNodeLivenessPB(applied),
		NodeEpoch: m.nodeEpoch,
	}
	target := m.newCoordinatorTopicMessage(resp)
	if err := m.mc.SendCommand(target); err != nil {
		log.Warn("send set node liveness response failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
	}
}

func (m *Manager) fromNodeLivenessPB(l heartbeatpb.NodeLiveness) api.Liveness {
	switch l {
	case heartbeatpb.NodeLiveness_ALIVE:
		return api.LivenessCaptureAlive
	case heartbeatpb.NodeLiveness_DRAINING:
		return api.LivenessCaptureDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return api.LivenessCaptureStopping
	default:
		return api.LivenessCaptureAlive
	}
}

func (m *Manager) toNodeLivenessPB(l api.Liveness) heartbeatpb.NodeLiveness {
	switch l {
	case api.LivenessCaptureAlive:
		return heartbeatpb.NodeLiveness_ALIVE
	case api.LivenessCaptureDraining:
		return heartbeatpb.NodeLiveness_DRAINING
	case api.LivenessCaptureStopping:
		return heartbeatpb.NodeLiveness_STOPPING
	default:
		return heartbeatpb.NodeLiveness_ALIVE
	}
}

func (m *Manager) dispatcherMaintainerMessage(
	ctx context.Context, changefeed common.ChangeFeedID, msg *messaging.TargetMessage,
) error {
	c, ok := m.maintainers.Load(changefeed)
	if !ok {
		log.Warn("maintainer is not found",
			zap.Stringer("changefeedID", changefeed),
			zap.String("message", msg.String()))
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		maintainer := c.(*Maintainer)
		maintainer.pushEvent(&Event{
			changefeedID: changefeed,
			eventType:    EventMessage,
			message:      msg,
		})
	}
	return nil
}

func (m *Manager) GetMaintainerForChangefeed(changefeedID common.ChangeFeedID) (*Maintainer, bool) {
	c, ok := m.maintainers.Load(changefeedID)

	m.maintainers.Range(func(key, value interface{}) bool {
		return true
	})

	if !ok {
		return nil, false
	}
	return c.(*Maintainer), true
}

func (m *Manager) ListMaintainers() []*Maintainer {
	maintainers := make([]*Maintainer, 0)
	m.maintainers.Range(func(key, value interface{}) bool {
		maintainers = append(maintainers, value.(*Maintainer))
		return true
	})
	return maintainers
}

func (m *Manager) isBootstrap() bool {
	return m.coordinatorVersion > 0
}
