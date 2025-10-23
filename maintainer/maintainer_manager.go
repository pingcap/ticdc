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
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

// Manager is the manager of all changefeed maintainer in a ticdc server, each ticdc server will
// start a Manager when the ticdc server is startup. It responsible for:
// 1. Handle bootstrap command from coordinator and report all changefeed maintainer status.
// 2. Handle other commands from coordinator: like add or remove changefeed maintainer
// 3. Manage maintainers lifetime
type Manager struct {
	mc   messaging.MessageCenter
	conf *config.SchedulerConfig

	// changefeedID -> maintainer
	maintainers sync.Map

	coordinatorID      node.ID
	coordinatorVersion int64

	nodeInfo *node.Info

	// msgCh is used to cache messages from coordinator
	msgCh chan *messaging.TargetMessage

	taskScheduler threadpool.ThreadPool
}

// NewMaintainerManager create a changefeed maintainer manager instance
// and register message handler to message center
func NewMaintainerManager(
	nodeInfo *node.Info,
	conf *config.SchedulerConfig,
) *Manager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &Manager{
		mc:            mc,
		conf:          conf,
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
		messaging.TypeCoordinatorBootstrapRequest:
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
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
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
			// 1.  try to send heartbeat to coordinator
			m.sendHeartbeat()
			// 2. cleanup removed maintainers
			m.maintainers.Range(func(key, value interface{}) bool {
				cf := value.(*Maintainer)
				if cf.removed.Load() {
					cf.Close()
					log.Info("maintainer removed, remove it from dynamic stream",
						zap.Stringer("changefeed", cf.id),
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

// Close closes, it's a block call
func (m *Manager) Close(_ context.Context) error {
	// cancel the maintainers, but keep the related metrics
	m.maintainers.Range(func(key, value interface{}) bool {
		maintainer := value.(*Maintainer)
		maintainer.cancel()
		for _, handler := range maintainer.controller.taskHandles {
			handler.Cancel()
		}
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
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("new coordinator online",
		zap.Int64("version", m.coordinatorVersion))
}

func (m *Manager) onAddMaintainerRequest(req *heartbeatpb.AddMaintainerRequest) *heartbeatpb.MaintainerStatus {
	cfID := common.NewChangefeedIDFromPB(req.Id)
	_, ok := m.maintainers.Load(cfID)
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
			zap.Stringer("changefeed", cfID),
			zap.Uint64("checkpointTs", req.CheckpointTs),
			zap.Any("info", info))
	}

	ctx := context.Background()
	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.LoadKeyspace(ctx, cfID.Keyspace())
	if err != nil {
		// BUG tenfyzhong 2025-09-11 17:29:08 how to process err
		log.Error("load keyspace meta fail", zap.String("keyspace", cfID.Keyspace()))
	}

	maintainer := NewMaintainer(cfID, m.conf, info, m.nodeInfo, m.taskScheduler, req.CheckpointTs, req.IsNewChangefeed, keyspaceMeta.Id)
	m.maintainers.Store(cfID, maintainer)
	maintainer.pushEvent(&Event{changefeedID: cfID, eventType: EventInit})
	return nil
}

func (m *Manager) onRemoveMaintainerRequest(msg *messaging.TargetMessage) *heartbeatpb.MaintainerStatus {
	req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	cfID := common.NewChangefeedIDFromPB(req.GetId())
	cf, ok := m.maintainers.Load(cfID)
	if !ok {
		if !req.Cascade {
			log.Warn("ignore remove maintainer request, "+
				"since the maintainer not found",
				zap.Stringer("changefeed", cfID),
				zap.Any("request", req))
			return &heartbeatpb.MaintainerStatus{
				ChangefeedID: req.GetId(),
				State:        heartbeatpb.ComponentState_Stopped,
			}
		}

		ctx := context.Background()
		keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
		keyspaceMeta, err := keyspaceManager.LoadKeyspace(ctx, cfID.Keyspace())
		if err != nil {
			// BUG tenfyzhong 2025-09-11 17:29:08 how to process err
			log.Error("load keyspace meta fail", zap.String("keyspace", cfID.Keyspace()))
		}

		// it's cascade remove, we should remove the dispatcher from all node
		// here we create a maintainer to run the remove the dispatcher logic
		cf = NewMaintainerForRemove(cfID, m.conf, m.nodeInfo, m.taskScheduler, keyspaceMeta.Id)
		m.maintainers.Store(cfID, cf)
	}
	cf.(*Maintainer).pushEvent(&Event{
		changefeedID: cfID,
		eventType:    EventMessage,
		message:      msg,
	})
	log.Info("received remove maintainer request",
		zap.Stringer("changefeed", cfID))
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
		log.Info("received coordinator bootstrap request", zap.String("from", msg.From.String()))
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
	default:
	}
}

func (m *Manager) dispatcherMaintainerMessage(
	ctx context.Context, changefeed common.ChangeFeedID, msg *messaging.TargetMessage,
) error {
	c, ok := m.maintainers.Load(changefeed)
	if !ok {
		log.Warn("maintainer is not found",
			zap.Stringer("changefeed", changefeed),
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
