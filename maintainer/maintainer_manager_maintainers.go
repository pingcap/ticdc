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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

// managerMaintainerSet owns the changefeed-scoped part of a maintainer manager.
// It tracks the local changefeedID -> maintainer registry, creates and removes
// maintainers, routes maintainer-bound messages, and aggregates maintainer
// heartbeats back to coordinator.
//
// In contrast, managerNodeState owns node-scoped state shared by the whole
// capture, such as liveness, node epoch, and the latest manager-level drain
// target. The Manager combines both layers: managerNodeState is the single
// node-wide source of truth, while managerMaintainerSet fans that node-scoped
// state out to individual maintainers and manages their per-changefeed
// lifecycles.
type managerMaintainerSet struct {
	// conf is shared scheduler configuration for newly created maintainers.
	conf *config.SchedulerConfig

	// nodeInfo identifies the current capture for new maintainer instances.
	nodeInfo *node.Info
	// taskScheduler is shared by all local maintainers to run background tasks.
	taskScheduler threadpool.ThreadPool

	// registry is the in-memory changefeedID -> maintainer mapping.
	registry sync.Map
}

// newManagerMaintainerSet initializes the changefeed-scoped state owned by a manager.
func newManagerMaintainerSet(conf *config.SchedulerConfig, nodeInfo *node.Info) *managerMaintainerSet {
	return &managerMaintainerSet{
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
	currentLiveness := liveness.CaptureAlive
	if m.node.liveness != nil {
		currentLiveness = m.node.liveness.Load()
	}
	if currentLiveness == liveness.CaptureStopping {
		changefeedID := common.NewChangefeedIDFromPB(req.Id)
		log.Info("reject add maintainer request because node is stopping",
			zap.Stringer("nodeID", m.nodeInfo.ID),
			zap.Stringer("changefeedID", changefeedID))
		return nil
	}

	return m.maintainers.handleAddMaintainer(req, m.getDispatcherDrainTarget)
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
		fields := []zap.Field{
			zap.String("type", msg.Type.String()),
			zap.Stringer("coordinatorID", m.coordinatorID),
			zap.Stringer("from", msg.From),
		}
		switch msg.Type {
		case messaging.TypeAddMaintainerRequest:
			changefeedID := common.NewChangefeedIDFromPB(msg.Message[0].(*heartbeatpb.AddMaintainerRequest).Id)
			fields = append(fields, zap.Stringer("changefeedID", changefeedID))
		case messaging.TypeRemoveMaintainerRequest:
			changefeedID := common.NewChangefeedIDFromPB(msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest).Id)
			fields = append(fields, zap.Stringer("changefeedID", changefeedID))
		}
		log.Warn("ignore invalid coordinator id", fields...)
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
func (p *managerMaintainerSet) closeAll() {
	p.registry.Range(func(_, value interface{}) bool {
		value.(*Maintainer).Close()
		return true
	})
}

// buildBootstrapResponse snapshots all local maintainer states for coordinator bootstrap.
func (p *managerMaintainerSet) buildBootstrapResponse() *heartbeatpb.CoordinatorBootstrapResponse {
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
func (p *managerMaintainerSet) handleAddMaintainer(
	req *heartbeatpb.AddMaintainerRequest,
	getDrainTarget func() (node.ID, uint64),
) *heartbeatpb.MaintainerStatus {
	changefeedID := common.NewChangefeedIDFromPB(req.Id)
	if _, ok := p.registry.Load(changefeedID); ok {
		return nil
	}

	info := &config.ChangeFeedInfo{}
	if err := json.Unmarshal(req.Config, info); err != nil {
		log.Error("ignore add maintainer request with invalid config",
			zap.Stringer("changefeedID", changefeedID),
			zap.Int("configBytes", len(req.Config)),
			zap.Error(err))
		return nil
	}
	if req.CheckpointTs == 0 {
		log.Error("ignore add maintainer request with invalid checkpointTs",
			zap.Stringer("changefeedID", changefeedID),
			zap.Uint64("checkpointTs", req.CheckpointTs))
		return nil
	}
	maintainer := NewMaintainer(changefeedID, p.conf, info, p.nodeInfo, p.taskScheduler, req.CheckpointTs, req.IsNewChangefeed, req.KeyspaceId)
	registered, loaded := p.registry.LoadOrStore(changefeedID, maintainer)
	if loaded {
		// Duplicate add requests can race on the same changefeed. Drop the loser and
		// stop the redundant maintainer immediately so background goroutines do not leak.
		maintainer.Close()
		return nil
	}

	registeredMaintainer := registered.(*Maintainer)
	// Register the maintainer before seeding the drain snapshot so concurrent
	// manager-level drain fanout can always observe it in the registry.
	target, epoch := getDrainTarget()
	registeredMaintainer.SetDispatcherDrainTarget(target, epoch)
	registeredMaintainer.pushEvent(&Event{changefeedID: changefeedID, eventType: EventInit})
	return nil
}

// handleRemoveMaintainer handles both normal remove and cascade-remove flows.
func (p *managerMaintainerSet) handleRemoveMaintainer(msg *messaging.TargetMessage) *heartbeatpb.MaintainerStatus {
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
func (p *managerMaintainerSet) buildHeartbeat() *heartbeatpb.MaintainerHeartbeat {
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
func (p *managerMaintainerSet) cleanupRemovedMaintainers() {
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
func (p *managerMaintainerSet) applyDispatcherDrainTarget(target node.ID, epoch uint64) {
	p.registry.Range(func(_, value interface{}) bool {
		value.(*Maintainer).SetDispatcherDrainTarget(target, epoch)
		return true
	})
}

// dispatchMaintainerMessage pushes a dispatcher-originated message into the
// target maintainer event loop.
func (p *managerMaintainerSet) dispatchMaintainerMessage(
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
		return errors.Trace(ctx.Err())
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
func (p *managerMaintainerSet) getMaintainer(changefeedID common.ChangeFeedID) (*Maintainer, bool) {
	c, ok := p.registry.Load(changefeedID)
	if !ok {
		return nil, false
	}
	return c.(*Maintainer), true
}

// listMaintainers returns a snapshot of all currently registered maintainers.
func (p *managerMaintainerSet) listMaintainers() []*Maintainer {
	maintainers := make([]*Maintainer, 0)
	p.registry.Range(func(_, value interface{}) bool {
		maintainers = append(maintainers, value.(*Maintainer))
		return true
	})
	return maintainers
}
