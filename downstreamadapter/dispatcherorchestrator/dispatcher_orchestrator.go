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

package dispatcherorchestrator

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// DispatcherOrchestrator coordinates the creation, deletion, and management of event dispatcher managers
// for different change feeds based on maintainer bootstrap messages.
type DispatcherOrchestrator struct {
	mc                 messaging.MessageCenter
	mutex              sync.Mutex // protect dispatcherManagers and closedMaintainerEpochs
	dispatcherManagers map[common.ChangeFeedID]*dispatchermanager.DispatcherManager
	// closedMaintainerEpochs remembers the highest epoch that closed a manager.
	// Map presence is meaningful because epoch 0 is a valid compatibility epoch.
	// The tombstone prevents a delayed old bootstrap from recreating the manager after close.
	closedMaintainerEpochs map[common.ChangeFeedID]uint64

	// shards partition changefeed control messages by changefeed ID. Each shard keeps
	// the existing FIFO queue semantics, while different shards can process messages
	// concurrently.
	shards []*orchestratorShard

	// closed indicates Close has been invoked and no more messages should be enqueued.
	closed atomic.Bool
	// msgGuardWaitGroup waits for in-flight RecvMaintainerRequest handlers before shutdown.
	msgGuardWaitGroup util.GuardedWaitGroup
}

const (
	// dispatcherOrchestratorShardCount is intentionally fixed to a small value.
	// The goal is to break the global head-of-line blocking without introducing
	// many long-lived goroutines or another layer of tuning knobs.
	dispatcherOrchestratorShardCount = 8
)

func New() *DispatcherOrchestrator {
	m := &DispatcherOrchestrator{
		mc:                     appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		dispatcherManagers:     make(map[common.ChangeFeedID]*dispatchermanager.DispatcherManager),
		closedMaintainerEpochs: make(map[common.ChangeFeedID]uint64),
		shards:                 make([]*orchestratorShard, dispatcherOrchestratorShardCount),
	}
	for i := range m.shards {
		m.shards[i] = newOrchestratorShard(m.processMessage)
	}
	m.mc.RegisterHandler(messaging.DispatcherManagerManagerTopic, m.RecvMaintainerRequest)
	return m
}

// Run starts all shard workers for asynchronous message processing.
func (m *DispatcherOrchestrator) Run() {
	log.Info("dispatcher orchestrator is running")
	for _, shard := range m.shards {
		shard.Run()
	}
}

// RecvMaintainerRequest is the handler for the maintainer request message.
// It puts the message into a queue for asynchronous processing to avoid blocking the message center.
func (m *DispatcherOrchestrator) RecvMaintainerRequest(
	_ context.Context,
	msg *messaging.TargetMessage,
) error {
	if !m.msgGuardWaitGroup.AddIf(func() bool { return !m.closed.Load() }) {
		log.Debug("dispatcher orchestrator already closed, drop message", zap.Any("message", msg.Message))
		return nil
	}
	defer m.msgGuardWaitGroup.Done()

	key, ok := getPendingMessageKey(msg)
	if !ok {
		log.Warn("unknown message type, drop message",
			zap.String("type", msg.Type.String()),
			zap.Any("message", msg.Message))
		return nil
	}

	// De-duplicate by (changefeedID, messageType) to avoid floods of retry messages.
	_ = m.shardForChangefeedID(key.changefeedID).TryEnqueue(key, msg)
	return nil
}

// shardForChangefeedID returns the shard responsible for the changefeed. The
// routing key is the internal changefeed ID so retries always land on the same
// shard. Changefeed IDs are UUID-derived GIDs, and GID.Hash mixes the low and
// high halves before applying modulo, which is sufficient for this fixed shard count.
func (m *DispatcherOrchestrator) shardForChangefeedID(changefeedID common.ChangeFeedID) *orchestratorShard {
	return m.shards[m.shardIndexForChangefeedID(changefeedID)]
}

func (m *DispatcherOrchestrator) shardIndexForChangefeedID(changefeedID common.ChangeFeedID) int {
	return changefeedID.ID().Hash(uint64(len(m.shards)))
}

func getPendingMessageKey(msg *messaging.TargetMessage) (pendingMessageKey, bool) {
	var changefeedID *heartbeatpb.ChangefeedID
	switch req := msg.Message[0].(type) {
	case *heartbeatpb.MaintainerBootstrapRequest:
		changefeedID = req.ChangefeedID
	case *heartbeatpb.MaintainerPostBootstrapRequest:
		changefeedID = req.ChangefeedID
	case *heartbeatpb.MaintainerCloseRequest:
		changefeedID = req.ChangefeedID
	default:
		return pendingMessageKey{}, false
	}
	return pendingMessageKey{
		changefeedID: common.NewChangefeedIDFromPB(changefeedID),
		msgType:      msg.Type,
	}, true
}

// processMessage dispatches a queued control message to the existing handler
// implementation. Shards only change concurrency, not per-message behavior.
func (m *DispatcherOrchestrator) processMessage(msg *messaging.TargetMessage) {
	switch req := msg.Message[0].(type) {
	case *heartbeatpb.MaintainerBootstrapRequest:
		if err := m.handleBootstrapRequest(msg.From, req); err != nil {
			log.Error("failed to handle bootstrap request", zap.Error(err))
		}
	case *heartbeatpb.MaintainerPostBootstrapRequest:
		// Only the event dispatcher manager with table trigger event dispatcher will receive the post bootstrap request.
		if err := m.handlePostBootstrapRequest(msg.From, req); err != nil {
			log.Error("failed to handle post bootstrap request", zap.Error(err))
		}
	case *heartbeatpb.MaintainerCloseRequest:
		if err := m.handleCloseRequest(msg.From, req); err != nil {
			log.Error("failed to handle close request", zap.Error(err))
		}
	default:
		log.Warn("unknown message type, ignore it",
			zap.String("type", msg.Type.String()),
			zap.Any("message", msg.Message))
	}
}

func (m *DispatcherOrchestrator) handleBootstrapRequest(
	from node.ID,
	req *heartbeatpb.MaintainerBootstrapRequest,
) error {
	cfId := common.NewChangefeedIDFromPB(req.ChangefeedID)

	cfConfig := &config.ChangefeedConfig{}
	if err := json.Unmarshal(req.Config, cfConfig); err != nil {
		log.Panic("failed to unmarshal changefeed config",
			zap.String("changefeedID", cfId.Name()), zap.Any("data", req.Config), zap.Error(err))
	}
	maintainerEpoch := req.MaintainerEpoch

	// Keep the map lock scoped to dispatcherManagers lookups and updates only.
	// NewDispatcherManager may perform expensive downstream initialization, so it
	// must run outside the mutex to let unrelated shards progress concurrently.
	m.mutex.Lock()
	manager, exists := m.dispatcherManagers[cfId]
	closedEpoch, closed := m.closedMaintainerEpochs[cfId]
	m.mutex.Unlock()

	var err error
	if !exists {
		if closed && (maintainerEpoch == 0 || maintainerEpoch <= closedEpoch) {
			log.Warn("drop stale maintainer bootstrap request after close",
				zap.String("changefeed", cfId.Name()),
				zap.String("from", from.String()),
				zap.Uint64("requestMaintainerEpoch", maintainerEpoch),
				zap.Uint64("closedMaintainerEpoch", closedEpoch))
			return nil
		}
		start := time.Now()
		manager, err = dispatchermanager.NewDispatcherManager(
			req.KeyspaceId,
			cfId,
			cfConfig,
			req.TableTriggerEventDispatcherId,
			req.TableTriggerRedoDispatcherId,
			req.StartTs,
			from,
			maintainerEpoch,
			req.IsNewChangefeed,
		)
		// Fast return the error to maintainer.
		if err != nil {
			log.Error("failed to create new dispatcher manager",
				zap.Any("changefeedID", cfId.Name()), zap.Duration("duration", time.Since(start)), zap.Error(err))

			appcontext.GetService[*dispatchermanager.HeartBeatCollector](appcontext.HeartbeatCollector).RemoveDispatcherManager(cfId)

			response := &heartbeatpb.MaintainerBootstrapResponse{
				ChangefeedID:    req.ChangefeedID,
				MaintainerEpoch: maintainerEpoch,
				Err: &heartbeatpb.RunningError{
					Time:    time.Now().String(),
					Node:    from.String(),
					Code:    string(errors.ErrorCode(err)),
					Message: err.Error(),
				},
			}
			log.Error("create new dispatcher manager failed",
				zap.Any("changefeedID", cfId.Name()), zap.Duration("duration", time.Since(start)), zap.Error(err))

			return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
		}
		m.mutex.Lock()
		m.dispatcherManagers[cfId] = manager
		delete(m.closedMaintainerEpochs, cfId)
		m.mutex.Unlock()
		metrics.DispatcherManagerGauge.WithLabelValues(cfId.Keyspace(), cfId.Name()).Inc()
	}

	manager.MaintainerFenceMu.Lock()
	if !manager.TryUpdateMaintainer(from, maintainerEpoch) {
		log.Warn("drop stale maintainer bootstrap request",
			zap.String("changefeed", cfId.Name()),
			zap.String("from", from.String()),
			zap.Uint64("requestMaintainerEpoch", maintainerEpoch),
			zap.Uint64("currentMaintainerEpoch", manager.GetMaintainerEpoch()),
			zap.String("currentMaintainer", manager.GetMaintainerID().String()))
		manager.MaintainerFenceMu.Unlock()
		return nil
	}
	if exists {
		// Check and potentially add a table trigger event dispatcher.
		// This is necessary during maintainer node migration, as the existing
		// dispatcher manager on the new node may not have a table trigger
		// event dispatcher configured yet.
		if req.TableTriggerEventDispatcherId != nil {
			tableTriggerDispatcher := manager.GetTableTriggerEventDispatcher()
			if tableTriggerDispatcher == nil {
				err = manager.NewTableTriggerEventDispatcher(
					req.TableTriggerEventDispatcherId,
					req.StartTs,
					false,
				)
				if err != nil {
					log.Error("failed to create new table trigger event dispatcher",
						zap.Stringer("changefeedID", cfId), zap.Error(err))
					manager.MaintainerFenceMu.Unlock()
					return m.handleDispatcherError(from, req.ChangefeedID, maintainerEpoch, err)
				}
			}
		}
		if req.TableTriggerRedoDispatcherId != nil {
			tableTriggerRedoDispatcher := manager.GetTableTriggerRedoDispatcher()
			if tableTriggerRedoDispatcher == nil {
				err = manager.NewTableTriggerRedoDispatcher(
					req.TableTriggerRedoDispatcherId,
					req.StartTs,
					false,
				)
				if err != nil {
					log.Error("failed to create new table trigger redo dispatcher",
						zap.Stringer("changefeedID", cfId), zap.Error(err))
					manager.MaintainerFenceMu.Unlock()
					return m.handleDispatcherError(from, req.ChangefeedID, maintainerEpoch, err)
				}
			}
		}
	}

	var (
		startTs     uint64
		redoStartTs uint64
	)
	if tableTriggerDispatcher := manager.GetTableTriggerEventDispatcher(); tableTriggerDispatcher != nil {
		startTs = tableTriggerDispatcher.GetStartTs()
	}
	if manager.IsRedoReady() {
		if tableTriggerRedoDispatcher := manager.GetTableTriggerRedoDispatcher(); tableTriggerRedoDispatcher != nil {
			redoStartTs = tableTriggerRedoDispatcher.GetStartTs()
		}
	}
	response := createBootstrapResponse(req.ChangefeedID, manager, startTs, redoStartTs)
	manager.MaintainerFenceMu.Unlock()
	return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
}

// handlePostBootstrapRequest handles the maintainer post-bootstrap request message.
// It initializes the table trigger event dispatcher with table schema information,
// which serves as the initial state for the table schema store. After initialization,
// the dispatcher registers itself with the event collector to begin receiving events.
func (m *DispatcherOrchestrator) handlePostBootstrapRequest(
	from node.ID,
	req *heartbeatpb.MaintainerPostBootstrapRequest,
) error {
	cfId := common.NewChangefeedIDFromPB(req.ChangefeedID)

	m.mutex.Lock()
	manager, exists := m.dispatcherManagers[cfId]
	m.mutex.Unlock()

	if !exists || manager.GetTableTriggerEventDispatcher() == nil {
		log.Error("Receive post bootstrap request but there is no table trigger event dispatcher",
			zap.Any("changefeedID", cfId.Name()))
		return nil
	}
	manager.MaintainerFenceMu.Lock()
	if !manager.IsMaintainerRequestAllowed(from, req.MaintainerEpoch) {
		log.Warn("drop stale maintainer post bootstrap request",
			zap.String("changefeed", cfId.Name()),
			zap.String("from", from.String()),
			zap.Uint64("requestMaintainerEpoch", req.MaintainerEpoch),
			zap.Uint64("currentMaintainerEpoch", manager.GetMaintainerEpoch()),
			zap.String("currentMaintainer", manager.GetMaintainerID().String()))
		manager.MaintainerFenceMu.Unlock()
		return nil
	}
	if manager.GetTableTriggerEventDispatcher().GetId() !=
		common.NewDispatcherIDFromPB(req.TableTriggerEventDispatcherId) {
		log.Error("Receive post bootstrap request but the table trigger event dispatcher id is not match",
			zap.Any("changefeedID", cfId.Name()),
			zap.String("expectedDispatcherID",
				manager.GetTableTriggerEventDispatcher().GetId().String()),
			zap.String("actualDispatcherID",
				common.NewDispatcherIDFromPB(req.TableTriggerEventDispatcherId).String()))

		err := errors.ErrChangefeedInitTableTriggerDispatcherFailed.
			GenWithStackByArgs("Receive post bootstrap request but the table trigger event dispatcher id is not match")

		response := &heartbeatpb.MaintainerPostBootstrapResponse{
			ChangefeedID:    req.ChangefeedID,
			MaintainerEpoch: req.MaintainerEpoch,
			Err: &heartbeatpb.RunningError{
				Time:    time.Now().String(),
				Node:    from.String(),
				Code:    string(errors.ErrorCode(err)),
				Message: err.Error(),
			},
		}

		manager.MaintainerFenceMu.Unlock()
		return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
	}

	// init table schema store
	err := manager.InitalizeTableTriggerEventDispatcher(req.Schemas)
	if err != nil {
		log.Error("failed to initialize table trigger event dispatcher",
			zap.Any("changefeedID", cfId.Name()), zap.Error(err))
		manager.MaintainerFenceMu.Unlock()
		return m.handleDispatcherError(from, req.ChangefeedID, req.MaintainerEpoch, err)
	}
	if manager.IsRedoReady() {
		err := manager.InitalizeTableTriggerRedoDispatcher(req.RedoSchemas)
		if err != nil {
			log.Error("failed to initialize table trigger redo dispatcher",
				zap.Any("changefeedID", cfId.Name()), zap.Error(err))
			manager.MaintainerFenceMu.Unlock()
			return m.handleDispatcherError(from, req.ChangefeedID, req.MaintainerEpoch, err)
		}
	}

	response := &heartbeatpb.MaintainerPostBootstrapResponse{
		ChangefeedID:                  req.ChangefeedID,
		TableTriggerEventDispatcherId: req.TableTriggerEventDispatcherId,
		MaintainerEpoch:               req.MaintainerEpoch,
	}
	manager.MaintainerFenceMu.Unlock()
	return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
}

func (m *DispatcherOrchestrator) handleCloseRequest(
	from node.ID,
	req *heartbeatpb.MaintainerCloseRequest,
) error {
	cfId := common.NewChangefeedIDFromPB(req.ChangefeedID)
	response := &heartbeatpb.MaintainerCloseResponse{
		ChangefeedID:    req.ChangefeedID,
		Success:         true,
		MaintainerEpoch: req.MaintainerEpoch,
	}

	m.mutex.Lock()
	manager, ok := m.dispatcherManagers[cfId]
	if !ok {
		m.recordClosedMaintainerEpochLocked(cfId, req.MaintainerEpoch, req.Removed)
	}
	m.mutex.Unlock()

	if ok {
		// Do not hold the orchestrator-wide map lock while waiting for the
		// per-changefeed fence; a slow manager must not block unrelated changefeeds.
		decGauge := false
		manager.MaintainerFenceMu.Lock()
		if manager.IsMaintainerRequestAllowed(from, req.MaintainerEpoch) {
			if closed := manager.TryClose(req.Removed); closed {
				m.mutex.Lock()
				currentManager, stillExists := m.dispatcherManagers[cfId]
				switch {
				case stillExists && currentManager == manager:
					delete(m.dispatcherManagers, cfId)
					m.recordClosedMaintainerEpochLocked(cfId, req.MaintainerEpoch, req.Removed)
					decGauge = true
					response.Success = true
				case !stillExists:
					m.recordClosedMaintainerEpochLocked(cfId, req.MaintainerEpoch, req.Removed)
					response.Success = true
				default:
					response.Success = false
				}
				m.mutex.Unlock()
			} else {
				response.Success = false
			}
		} else {
			// The active manager belongs to a newer maintainer. Do not close it, but
			// acknowledge the stale sender so removal-only maintainers can stop retrying.
			response.Success = true
			log.Warn("drop stale maintainer close request",
				zap.String("changefeed", cfId.Name()),
				zap.String("from", from.String()),
				zap.Uint64("requestMaintainerEpoch", req.MaintainerEpoch),
				zap.Uint64("currentMaintainerEpoch", manager.GetMaintainerEpoch()),
				zap.String("currentMaintainer", manager.GetMaintainerID().String()))
		}
		manager.MaintainerFenceMu.Unlock()
		if decGauge {
			metrics.DispatcherManagerGauge.WithLabelValues(cfId.Keyspace(), cfId.Name()).Dec()
		}
	}

	log.Info("try close dispatcher manager",
		zap.String("changefeed", cfId.String()), zap.Bool("success", response.Success))
	return m.sendResponse(from, messaging.MaintainerTopic, response)
}

func (m *DispatcherOrchestrator) recordClosedMaintainerEpochLocked(cfID common.ChangeFeedID, maintainerEpoch uint64, removed bool) {
	if maintainerEpoch == 0 && !removed {
		// Epoch 0 has no ordering information. Keep permanent tombstones only
		// for removal so mixed-version resume can still bootstrap in compat mode.
		return
	}
	closedEpoch, ok := m.closedMaintainerEpochs[cfID]
	if ok && closedEpoch >= maintainerEpoch {
		return
	}
	m.closedMaintainerEpochs[cfID] = maintainerEpoch
}

func createBootstrapResponse(
	changefeedID *heartbeatpb.ChangefeedID,
	manager *dispatchermanager.DispatcherManager,
	startTs, redoStartTs uint64,
) *heartbeatpb.MaintainerBootstrapResponse {
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID:    changefeedID,
		Spans:           make([]*heartbeatpb.BootstrapTableSpan, 0, manager.GetDispatcherMap().Len()),
		MaintainerEpoch: manager.GetMaintainerEpoch(),
	}

	// table trigger event dispatcher startTs
	if startTs != 0 {
		response.CheckpointTs = startTs
	}

	retrieveDispatcherSpanForBootstrapResponse(manager, response)
	if manager.IsRedoReady() {
		// table trigger redo dispatcher startTs
		if redoStartTs != 0 {
			response.RedoCheckpointTs = redoStartTs
		}
		retrieveRedoDispatcherSpanForBootstrapResponse(manager, response)
	}
	retrieveOperatorsForBootstrapResponse(changefeedID, manager, response)

	return response
}

func (m *DispatcherOrchestrator) sendResponse(to node.ID, topic string, msg messaging.IOTypeT) error {
	message := messaging.NewSingleTargetMessage(to, topic, msg)
	if err := m.mc.SendCommand(message); err != nil {
		log.Error("failed to send response", zap.Error(err))
		return err
	}
	return nil
}

func (m *DispatcherOrchestrator) Close() {
	if !m.closed.CompareAndSwap(false, true) {
		return
	}
	log.Info("dispatcher orchestrator is closing")
	m.mc.DeRegisterHandler(messaging.DispatcherManagerManagerTopic)

	// Wait until all in-flight RecvMaintainerRequest calls finish before closing shards.
	m.msgGuardWaitGroup.Wait()

	for _, shard := range m.shards {
		shard.CloseAsync()
	}
	for _, shard := range m.shards {
		shard.Wait()
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	for len(m.dispatcherManagers) > 0 {
		for id, manager := range m.dispatcherManagers {
			ok := manager.TryClose(false)
			if ok {
				delete(m.dispatcherManagers, id)
			}
		}
	}
	log.Info("dispatcher orchestrator closed")
}

// handleDispatcherError creates and sends an error response for create dispatcher-related failures
func (m *DispatcherOrchestrator) handleDispatcherError(
	from node.ID,
	changefeedID *heartbeatpb.ChangefeedID,
	maintainerEpoch uint64,
	err error,
) error {
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID:    changefeedID,
		MaintainerEpoch: maintainerEpoch,
		Err: &heartbeatpb.RunningError{
			Time:    time.Now().String(),
			Node:    from.String(),
			Code:    string(errors.ErrorCode(err)),
			Message: err.Error(),
		},
	}
	return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
}

func retrieveRedoDispatcherSpanForBootstrapResponse(
	manager *dispatchermanager.DispatcherManager,
	response *heartbeatpb.MaintainerBootstrapResponse,
) {
	if !manager.IsRedoReady() {
		return
	}
	manager.GetRedoDispatcherMap().ForEach(func(id common.DispatcherID, d *dispatcher.RedoDispatcher) {
		response.Spans = append(response.Spans, &heartbeatpb.BootstrapTableSpan{
			ID:              id.ToPB(),
			SchemaID:        d.GetSchemaID(),
			Span:            d.GetTableSpan(),
			ComponentStatus: d.GetComponentStatus(),
			CheckpointTs:    d.GetCheckpointTs(),
			BlockState:      d.GetBlockEventStatus(),
			Mode:            d.GetMode(),
		})
	})
}

func retrieveDispatcherSpanForBootstrapResponse(
	manager *dispatchermanager.DispatcherManager,
	response *heartbeatpb.MaintainerBootstrapResponse,
) {
	manager.GetDispatcherMap().ForEach(func(id common.DispatcherID, d *dispatcher.EventDispatcher) {
		response.Spans = append(response.Spans, &heartbeatpb.BootstrapTableSpan{
			ID:              id.ToPB(),
			SchemaID:        d.GetSchemaID(),
			Span:            d.GetTableSpan(),
			ComponentStatus: d.GetComponentStatus(),
			CheckpointTs:    d.GetCheckpointTs(),
			BlockState:      d.GetBlockEventStatus(),
			Mode:            d.GetMode(),
		})
	})
}

func retrieveOperatorsForBootstrapResponse(
	changefeedID *heartbeatpb.ChangefeedID,
	manager *dispatchermanager.DispatcherManager,
	response *heartbeatpb.MaintainerBootstrapResponse,
) {
	reportedDispatchers := make(map[reportedDispatcherKey]struct{}, len(response.Spans))
	for _, span := range response.Spans {
		reportedDispatchers[reportedDispatcherKey{
			id:   common.NewDispatcherIDFromPB(span.ID),
			mode: span.Mode,
		}] = struct{}{}
	}

	manager.GetCurrentOperatorMap().Range(func(_, value any) bool {
		req := value.(dispatchermanager.SchedulerDispatcherRequest)
		requestAllowed := manager.IsMaintainerRequestAllowed(req.From, req.MaintainerEpoch)
		dispatcherID := common.NewDispatcherIDFromPB(req.Config.DispatcherID)
		dispatcherExistsKnown := !common.IsRedoMode(req.Config.Mode) || manager.IsRedoReady()
		_, dispatcherReported := reportedDispatchers[reportedDispatcherKey{
			id:   dispatcherID,
			mode: req.Config.Mode,
		}]
		if !requestAllowed {
			// Restore stale remove only when the same bootstrap snapshot reports the dispatcher.
			// This keeps the working span and cleanup intent consistent even if live maps change during cleanup.
			if req.ScheduleAction != heartbeatpb.ScheduleAction_Remove || !dispatcherReported {
				return true
			}
			log.Info("include stale remove operator in bootstrap response",
				zap.String("changefeed", changefeedID.String()),
				zap.String("dispatcherID", req.Config.DispatcherID.String()),
				zap.String("from", req.From.String()),
				zap.Uint64("requestMaintainerEpoch", req.MaintainerEpoch),
				zap.Uint64("currentMaintainerEpoch", manager.GetMaintainerEpoch()),
				zap.String("currentMaintainer", manager.GetMaintainerID().String()))
		}
		// Log error if dispatcher not found and action is not create.
		// It's possible that the dispatcher is not found when the action is create
		// because the dispatcher may be created after the operator is stored.
		if dispatcherExistsKnown && !dispatcherReported && req.ScheduleAction != heartbeatpb.ScheduleAction_Create {
			if common.IsRedoMode(req.Config.Mode) {
				log.Error("Redo dispatcher not found, this should not happen",
					zap.String("changefeed", changefeedID.String()),
					zap.String("dispatcherID", req.Config.DispatcherID.String()))
			} else {
				log.Error("Dispatcher not found, this should not happen",
					zap.String("changefeed", changefeedID.String()),
					zap.String("dispatcherID", req.Config.DispatcherID.String()))
			}
		}
		response.Operators = append(response.Operators, &heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID:    req.ChangefeedID,
			Config:          req.Config,
			ScheduleAction:  req.ScheduleAction,
			OperatorType:    req.OperatorType,
			MaintainerEpoch: req.MaintainerEpoch,
		})
		return true
	})
}

type reportedDispatcherKey struct {
	id   common.DispatcherID
	mode int64
}
