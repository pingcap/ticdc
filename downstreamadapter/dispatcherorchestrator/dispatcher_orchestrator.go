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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// DispatcherOrchestrator coordinates the creation, deletion, and management of event dispatcher managers
// for different change feeds based on maintainer bootstrap messages.
type DispatcherOrchestrator struct {
	mc                 messaging.MessageCenter
	dispatcherManagers map[common.ChangeFeedID]*dispatchermanager.EventDispatcherManager
}

func New() *DispatcherOrchestrator {
	m := &DispatcherOrchestrator{
		mc:                 appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		dispatcherManagers: make(map[common.ChangeFeedID]*dispatchermanager.EventDispatcherManager),
	}
	m.mc.RegisterHandler(messaging.DispatcherManagerManagerTopic, m.RecvMaintainerRequest)
	return m
}

// RecvMaintainerRequest is the handler for the maintainer request message.
// It will delegate the request to the corresponding handler based on the message type.
func (m *DispatcherOrchestrator) RecvMaintainerRequest(
	_ context.Context,
	msg *messaging.TargetMessage,
) error {
	switch req := msg.Message[0].(type) {
	case *heartbeatpb.MaintainerBootstrapRequest:
		return m.handleBootstrapRequest(msg.From, req)
	case *heartbeatpb.MaintainerPostBootstrapRequest:
		// Only the event dispatcher manager with table trigger event dispatcher will receive the post bootstrap request
		return m.handlePostBootstrapRequest(msg.From, req)
	case *heartbeatpb.MaintainerCloseRequest:
		return m.handleCloseRequest(msg.From, req)
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (m *DispatcherOrchestrator) handleBootstrapRequest(
	from node.ID,
	req *heartbeatpb.MaintainerBootstrapRequest,
) error {
	cfId := common.NewChangefeedIDFromPB(req.ChangefeedID)

	cfConfig := &config.ChangefeedConfig{}
	if err := json.Unmarshal(req.Config, cfConfig); err != nil {
		log.Panic("failed to unmarshal changefeed config",
			zap.String("changefeedID", cfId.Name()), zap.Error(err))
		return err
	}

	manager, exists := m.dispatcherManagers[cfId]
	var err error
	var startTs uint64
	if !exists {
		manager, startTs, err = dispatchermanager.
			NewEventDispatcherManager(
				cfId,
				cfConfig,
				req.TableTriggerEventDispatcherId,
				req.StartTs,
				from,
				req.IsNewChangefeed,
			)
			// Fast return the error to maintainer.
		if err != nil {
			log.Error("failed to create new dispatcher manager",
				zap.Any("changefeedID", cfId.Name()), zap.Error(err))

			appcontext.GetService[*dispatchermanager.HeartBeatCollector](appcontext.HeartbeatCollector).RemoveEventDispatcherManager(manager)

			response := &heartbeatpb.MaintainerBootstrapResponse{
				ChangefeedID: req.ChangefeedID,
				Err: &heartbeatpb.RunningError{
					Time:    time.Now().String(),
					Node:    from.String(),
					Code:    string(apperror.ErrorCode(err)),
					Message: err.Error(),
				},
			}
			return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
		}
		m.dispatcherManagers[cfId] = manager
		metrics.EventDispatcherManagerGauge.WithLabelValues(cfId.Namespace(), cfId.Name()).Inc()
	} else {
		// Check and potentially add a table trigger event dispatcher.
		// This is necessary during maintainer node migration, as the existing
		// dispatcher manager on the new node may not have a table trigger
		// event dispatcher configured yet.
		if req.TableTriggerEventDispatcherId != nil {
			tableTriggerDispatcher := manager.GetTableTriggerEventDispatcher()
			if tableTriggerDispatcher == nil {
				startTs, err = manager.NewTableTriggerEventDispatcher(
					req.TableTriggerEventDispatcherId,
					req.StartTs,
					false,
				)
				if err != nil {
					log.Error("failed to create new table trigger event dispatcher",
						zap.Stringer("changefeedID", cfId), zap.Error(err))
					return m.handleDispatcherError(from, req.ChangefeedID, err)
				}
			} else {
				startTs = tableTriggerDispatcher.GetStartTs()
			}
		}
	}

	if manager.GetMaintainerID() != from {
		manager.SetMaintainerID(from)
		log.Info("maintainer changed",
			zap.String("changefeed", cfId.Name()), zap.String("maintainer", from.String()))
	}

	// FIXME(fizz): This is a temporary check to ensure the maintainer epoch is consistent.
	// I will remove this after fully testing the new maintainer epoch mechanism.
	if manager.GetMaintainerEpoch() != cfConfig.Epoch {
		log.Error("maintainer epoch changed, this should not happen, please report this issue",
			zap.String("changefeed", cfId.Name()), zap.Uint64("epoch", cfConfig.Epoch))
	}

	response := createBootstrapResponse(req.ChangefeedID, manager, startTs)
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
	manager, exists := m.dispatcherManagers[cfId]
	if !exists || manager.GetTableTriggerEventDispatcher() == nil {
		log.Error("Receive post bootstrap request but there is no table trigger event dispatcher",
			zap.Any("changefeedID", cfId.Name()))
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

		err := apperror.ErrChangefeedInitTableTriggerEventDispatcherFailed.
			GenWithStackByArgs("Receive post bootstrap request but the table trigger event dispatcher id is not match")

		response := &heartbeatpb.MaintainerPostBootstrapResponse{
			ChangefeedID: req.ChangefeedID,
			Err: &heartbeatpb.RunningError{
				Time:    time.Now().String(),
				Node:    from.String(),
				Code:    string(apperror.ErrorCode(err)),
				Message: err.Error(),
			},
		}

		return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
	}

	// init table schema store
	err := manager.InitalizeTableTriggerEventDispatcher(req.Schemas)
	if err != nil {
		log.Error("failed to initialize table trigger event dispatcher",
			zap.Any("changefeedID", cfId.Name()), zap.Error(err))
		return m.handleDispatcherError(from, req.ChangefeedID, err)
	}

	response := &heartbeatpb.MaintainerPostBootstrapResponse{
		ChangefeedID:                  req.ChangefeedID,
		TableTriggerEventDispatcherId: req.TableTriggerEventDispatcherId,
	}
	return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
}

func (m *DispatcherOrchestrator) handleCloseRequest(
	from node.ID,
	req *heartbeatpb.MaintainerCloseRequest,
) error {
	cfId := common.NewChangefeedIDFromPB(req.ChangefeedID)
	response := &heartbeatpb.MaintainerCloseResponse{
		ChangefeedID: req.ChangefeedID,
		Success:      true,
	}

	if manager, ok := m.dispatcherManagers[cfId]; ok {
		if closed := manager.TryClose(req.Removed); closed {
			delete(m.dispatcherManagers, cfId)
			metrics.EventDispatcherManagerGauge.WithLabelValues(cfId.Namespace(), cfId.Name()).Dec()
			response.Success = true
		} else {
			response.Success = false
		}
	}

	log.Info("try close dispatcher manager",
		zap.String("changefeed", cfId.Name()), zap.Bool("success", response.Success))
	return m.sendResponse(from, messaging.MaintainerTopic, response)
}

func createBootstrapResponse(
	changefeedID *heartbeatpb.ChangefeedID,
	manager *dispatchermanager.EventDispatcherManager,
	startTs uint64,
) *heartbeatpb.MaintainerBootstrapResponse {
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: changefeedID,
		Spans:        make([]*heartbeatpb.BootstrapTableSpan, 0, manager.GetDispatcherMap().Len()),
	}

	if startTs != 0 {
		response.CheckpointTs = startTs
	}

	manager.GetDispatcherMap().ForEach(func(id common.DispatcherID, d *dispatcher.Dispatcher) {
		response.Spans = append(response.Spans, &heartbeatpb.BootstrapTableSpan{
			ID:              id.ToPB(),
			SchemaID:        d.GetSchemaID(),
			Span:            d.GetTableSpan(),
			ComponentStatus: d.GetComponentStatus(),
			CheckpointTs:    d.GetCheckpointTs(),
			BlockState:      d.GetBlockEventStatus(),
		})
	})

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
	m.mc.DeRegisterHandler(messaging.DispatcherManagerManagerTopic)
}

// handleDispatcherError creates and sends an error response for create dispatcher-related failures
func (m *DispatcherOrchestrator) handleDispatcherError(
	from node.ID,
	changefeedID *heartbeatpb.ChangefeedID,
	err error,
) error {
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: changefeedID,
		Err: &heartbeatpb.RunningError{
			Time:    time.Now().String(),
			Node:    from.String(),
			Code:    string(apperror.ErrorCode(err)),
			Message: err.Error(),
		},
	}
	return m.sendResponse(from, messaging.MaintainerManagerTopic, response)
}
