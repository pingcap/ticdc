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
	"github.com/pingcap/ticdc/pkg/api"
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
	mc messaging.MessageCenter

	coordinatorID      node.ID
	coordinatorVersion int64

	nodeInfo *node.Info

	// msgCh is used to cache messages from coordinator.
	msgCh chan *messaging.TargetMessage
	// node holds node-scoped liveness and drain state that applies to the whole capture.
	node *managerNodePart
	// maintainers holds changefeed-scoped state and lifecycle operations.
	maintainers *managerMaintainersPart
}

// NewMaintainerManager create a changefeed maintainer manager instance
// and register message handler to message center.
//
// liveness must not be nil because it is shared with the server-wide node
// liveness state for scheduling and election decisions.
func NewMaintainerManager(
	nodeInfo *node.Info,
	conf *config.SchedulerConfig,
	liveness *api.Liveness,
) *Manager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &Manager{
		mc:          mc,
		nodeInfo:    nodeInfo,
		msgCh:       make(chan *messaging.TargetMessage, 1024),
		node:        newManagerNodePart(liveness),
		maintainers: newManagerMaintainersPart(conf, nodeInfo),
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
		messaging.TypeCoordinatorBootstrapRequest,
		messaging.TypeSetNodeLivenessRequest,
		messaging.TypeSetDispatcherDrainTargetRequest:
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
			m.sendNodeHeartbeat(false)
			m.sendHeartbeat()
			m.cleanupRemovedMaintainers()
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

// Close closes, it's a block call.
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

	m.sendNodeHeartbeat(true)
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
	case messaging.TypeSetDispatcherDrainTargetRequest:
		m.onSetDispatcherDrainTargetRequest(msg)
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
