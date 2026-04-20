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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// Manager is the manager of all changefeed maintainer in a ticdc server, each ticdc server will
// start a Manager when the ticdc server is startup. It responsible for:
// 1. Handle bootstrap command from coordinator and report all changefeed maintainer status.
// 2. Handle other commands from coordinator: like add or remove changefeed maintainer
// 3. Manage maintainers lifetime
type Manager struct {
<<<<<<< HEAD
	mc   messaging.MessageCenter
	conf *config.SchedulerConfig

	// changefeedID -> maintainer
	maintainers sync.Map
=======
	mc messaging.MessageCenter
>>>>>>> 0213a79b4 (maintainer,heartbeatpb: add drain target plumbing (#4759))

	coordinatorID      node.ID
	coordinatorVersion int64

	nodeInfo *node.Info

	// msgCh is used to cache messages from coordinator.
	msgCh chan *messaging.TargetMessage
	// node holds node-scoped liveness and drain state that applies to the whole capture.
	node *managerNodeState
	// maintainers holds changefeed-scoped state and lifecycle operations.
	maintainers *managerMaintainerSet
}

// NewMaintainerManager create a changefeed maintainer manager instance
// and register message handler to message center.
//
// liveness must not be nil because it is shared with the server-wide node
// liveness state for scheduling and election decisions.
func NewMaintainerManager(
	nodeInfo *node.Info,
	conf *config.SchedulerConfig,
) *Manager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &Manager{
<<<<<<< HEAD
		mc:            mc,
		conf:          conf,
		maintainers:   sync.Map{},
		nodeInfo:      nodeInfo,
		msgCh:         make(chan *messaging.TargetMessage, 1024),
		taskScheduler: threadpool.NewThreadPoolDefault(),
=======
		mc:          mc,
		nodeInfo:    nodeInfo,
		msgCh:       make(chan *messaging.TargetMessage, 1024),
		node:        newManagerNodeState(nodeLiveness),
		maintainers: newManagerMaintainerSet(conf, nodeInfo),
>>>>>>> 0213a79b4 (maintainer,heartbeatpb: add drain target plumbing (#4759))
	}

	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.recvMessages)
	mc.RegisterHandler(messaging.MaintainerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			req := msg.Message[0].(*heartbeatpb.MaintainerCloseResponse)
			return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
		})
	return m
}

// recvMessages is the message handler for maintainer manager.
func (m *Manager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// Coordinator related messages.
	case messaging.TypeAddMaintainerRequest,
		messaging.TypeRemoveMaintainerRequest,
<<<<<<< HEAD
		messaging.TypeCoordinatorBootstrapRequest:
=======
		messaging.TypeCoordinatorBootstrapRequest,
		messaging.TypeSetNodeLivenessRequest,
		messaging.TypeSetDispatcherDrainTargetRequest:
>>>>>>> 0213a79b4 (maintainer,heartbeatpb: add drain target plumbing (#4759))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	// Receive bootstrap response message from the dispatcher manager.
	case messaging.TypeMaintainerBootstrapResponse:
		req := msg.Message[0].(*heartbeatpb.MaintainerBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	case messaging.TypeMaintainerPostBootstrapResponse:
		req := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapResponse)
		return m.dispatcherMaintainerMessage(ctx, common.NewChangefeedIDFromPB(req.ChangefeedID), msg)
	// Receive heartbeat message from dispatchers.
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
<<<<<<< HEAD
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
=======
			m.sendNodeHeartbeat(false)
			m.sendHeartbeat()
			m.cleanupRemovedMaintainers()
>>>>>>> 0213a79b4 (maintainer,heartbeatpb: add drain target plumbing (#4759))
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

// Close blocks until all local maintainers stop.
func (m *Manager) Close(_ context.Context) error {
	m.maintainers.closeAll()
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

	response := m.maintainers.buildBootstrapResponse()
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
}

<<<<<<< HEAD
func (m *Manager) onAddMaintainerRequest(req *heartbeatpb.AddMaintainerRequest) *heartbeatpb.MaintainerStatus {
	changefeedID := common.NewChangefeedIDFromPB(req.Id)
	_, ok := m.maintainers.Load(changefeedID)
	if ok {
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

=======
>>>>>>> 0213a79b4 (maintainer,heartbeatpb: add drain target plumbing (#4759))
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
<<<<<<< HEAD
=======
	case messaging.TypeSetNodeLivenessRequest:
		m.onSetNodeLivenessRequest(msg)
	case messaging.TypeSetDispatcherDrainTargetRequest:
		m.onSetDispatcherDrainTargetRequest(msg)
>>>>>>> 0213a79b4 (maintainer,heartbeatpb: add drain target plumbing (#4759))
	default:
	}
}

func (m *Manager) GetMaintainerForChangefeed(changefeedID common.ChangeFeedID) (*Maintainer, bool) {
	return m.maintainers.getMaintainer(changefeedID)
}

func (m *Manager) ListMaintainers() []*Maintainer {
	return m.maintainers.listMaintainers()
}

func (m *Manager) isBootstrap() bool {
	return m.coordinatorVersion > 0
}
