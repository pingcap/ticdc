// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

type managerMaintainersPart struct {
	// conf is shared scheduler configuration for newly created maintainers.
	conf *config.SchedulerConfig

	// nodeInfo identifies the current capture for new maintainer instances.
	nodeInfo *node.Info
	// taskScheduler is shared by all local maintainers to run background tasks.
	taskScheduler threadpool.ThreadPool

	// registry is the in-memory changefeedID -> maintainer mapping.
	registry sync.Map
}

// newManagerMaintainersPart initializes the changefeed-scoped state owned by a manager.
func newManagerMaintainersPart(conf *config.SchedulerConfig, nodeInfo *node.Info) *managerMaintainersPart {
	return &managerMaintainersPart{
		conf:          conf,
		nodeInfo:      nodeInfo,
		taskScheduler: threadpool.NewThreadPoolDefault(),
	}
}

// onAddMaintainerRequest enforces node-scoped admission rules before creating
// a changefeed-scoped maintainer.
func (m *Manager) onAddMaintainerRequest(req *heartbeatpb.AddMaintainerRequest) *heartbeatpb.MaintainerStatus {
	// Intentionally allow AddMaintainer while the node is draining.
	// During liveness propagation and scheduler view convergence, in-flight operators can still
	// issue add requests to this node; rejecting them here may break drain convergence.
	// Only STOPPING performs a hard reject for new maintainer creation.
	currentLiveness := m.node.liveness.Load()
	if currentLiveness == api.LivenessCaptureStopping {
		changefeedID := common.NewChangefeedIDFromPB(req.Id)
		log.Info("reject add maintainer request because node is stopping",
			zap.Stringer("nodeID", m.nodeInfo.ID),
			zap.Stringer("changefeedID", changefeedID))
		return nil
	}

	target, epoch := m.getDispatcherDrainTarget()
	return m.maintainers.handleAddMaintainer(req, target, epoch)
}

// onRemoveMaintainerRequest delegates changefeed removal to the maintainer part.
func (m *Manager) onRemoveMaintainerRequest(msg *messaging.TargetMessage) *heartbeatpb.MaintainerStatus {
	return m.maintainers.handleRemoveMaintainer(msg)
}

// onDispatchMaintainerRequest validates coordinator ownership before handling
// changefeed-scoped add/remove requests.
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

// sendHeartbeat reports maintainer-scoped status updates to coordinator.
func (m *Manager) sendHeartbeat() {
	if !m.isBootstrap() {
		return
	}
	response := m.maintainers.buildHeartbeat()
	if response == nil || len(response.Statuses) == 0 {
		return
	}
	m.sendMessages(response)
}

// cleanupRemovedMaintainers closes and unregisters maintainers that have fully stopped.
func (m *Manager) cleanupRemovedMaintainers() {
	m.maintainers.cleanupRemovedMaintainers()
}

// dispatcherMaintainerMessage routes dispatcher-originated messages to the
// target maintainer if it still exists locally.
func (m *Manager) dispatcherMaintainerMessage(
	ctx context.Context, changefeed common.ChangeFeedID, msg *messaging.TargetMessage,
) error {
	return m.maintainers.dispatchMaintainerMessage(ctx, changefeed, msg)
}

// closeAll stops every local maintainer during manager shutdown.
func (p *managerMaintainersPart) closeAll() {
	p.registry.Range(func(_, value interface{}) bool {
		value.(*Maintainer).Close()
		return true
	})
}

// buildBootstrapResponse snapshots all local maintainer states for coordinator bootstrap.
func (p *managerMaintainersPart) buildBootstrapResponse() *heartbeatpb.CoordinatorBootstrapResponse {
	response := &heartbeatpb.CoordinatorBootstrapResponse{}
	p.registry.Range(func(_, value interface{}) bool {
		maintainer := value.(*Maintainer)
		status := maintainer.GetMaintainerStatus()
		response.Statuses = append(response.Statuses, status)
		maintainer.statusChanged.Store(false)
		maintainer.lastReportTime = time.Now()
		return true
	})
	return response
}

// handleAddMaintainer decodes the request, creates the maintainer, and seeds it
// with the latest node-scoped dispatcher drain target.
func (p *managerMaintainersPart) handleAddMaintainer(
	req *heartbeatpb.AddMaintainerRequest,
	target node.ID,
	epoch uint64,
) *heartbeatpb.MaintainerStatus {
	changefeedID := common.NewChangefeedIDFromPB(req.Id)
	_, ok := p.registry.Load(changefeedID)
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

	maintainer := NewMaintainer(changefeedID, p.conf, info, p.nodeInfo, p.taskScheduler, req.CheckpointTs, req.IsNewChangefeed, req.KeyspaceId)
	maintainer.SetDispatcherDrainTarget(target, epoch)
	p.registry.Store(changefeedID, maintainer)
	maintainer.pushEvent(&Event{changefeedID: changefeedID, eventType: EventInit})
	return nil
}

// handleRemoveMaintainer handles both normal remove and cascade-remove flows.
func (p *managerMaintainersPart) handleRemoveMaintainer(msg *messaging.TargetMessage) *heartbeatpb.MaintainerStatus {
	req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	changefeedID := common.NewChangefeedIDFromPB(req.GetId())
	maintainer, ok := p.registry.Load(changefeedID)
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

		// It's cascade remove, we should remove the dispatcher from all node.
		// Here we create a maintainer to run the remove dispatcher logic.
		maintainer = NewMaintainerForRemove(changefeedID, p.conf, p.nodeInfo, p.taskScheduler, req.KeyspaceId)
		p.registry.Store(changefeedID, maintainer)
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

// buildHeartbeat collects status changes and periodic reports from local maintainers.
func (p *managerMaintainersPart) buildHeartbeat() *heartbeatpb.MaintainerHeartbeat {
	response := &heartbeatpb.MaintainerHeartbeat{}
	p.registry.Range(func(_, value interface{}) bool {
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
	if len(response.Statuses) == 0 {
		return nil
	}
	return response
}

// cleanupRemovedMaintainers closes maintainers after their remove flow has finished.
func (p *managerMaintainersPart) cleanupRemovedMaintainers() {
	p.registry.Range(func(key, value interface{}) bool {
		cf := value.(*Maintainer)
		if cf.removed.Load() {
			cf.Close()
			log.Info("maintainer removed, remove it from dynamic stream",
				zap.Stringer("changefeedID", cf.changefeedID),
				zap.Uint64("checkpointTs", cf.getWatermark().CheckpointTs),
			)
			p.registry.Delete(key)
		}
		return true
	})
}

// applyDispatcherDrainTarget fans out the latest node-scoped drain target to
// every currently active maintainer.
func (p *managerMaintainersPart) applyDispatcherDrainTarget(target node.ID, epoch uint64) {
	p.registry.Range(func(_, value interface{}) bool {
		value.(*Maintainer).SetDispatcherDrainTarget(target, epoch)
		return true
	})
}

// dispatchMaintainerMessage pushes a dispatcher-originated message into the
// target maintainer event loop.
func (p *managerMaintainersPart) dispatchMaintainerMessage(
	ctx context.Context, changefeed common.ChangeFeedID, msg *messaging.TargetMessage,
) error {
	c, ok := p.registry.Load(changefeed)
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

// getMaintainer returns the local maintainer for the given changefeed, if any.
func (p *managerMaintainersPart) getMaintainer(changefeedID common.ChangeFeedID) (*Maintainer, bool) {
	c, ok := p.registry.Load(changefeedID)
	if !ok {
		return nil, false
	}
	return c.(*Maintainer), true
}

// listMaintainers returns a snapshot of all currently registered maintainers.
func (p *managerMaintainersPart) listMaintainers() []*Maintainer {
	maintainers := make([]*Maintainer, 0)
	p.registry.Range(func(_, value interface{}) bool {
		maintainers = append(maintainers, value.(*Maintainer))
		return true
	})
	return maintainers
}
