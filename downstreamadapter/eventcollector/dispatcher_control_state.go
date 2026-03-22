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

package eventcollector

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type pendingRegisterState struct {
	serverID  node.ID
	onlyReuse bool
}

type pendingResetState struct {
	serverID node.ID
	resetTs  uint64
	epoch    uint64
}

type dispatcherConnState struct {
	sync.RWMutex
	eventServiceID          node.ID
	eventServiceIncarnation uint64
	readyEventReceived      atomic.Bool
	remoteCandidates        []string
}

func (d *dispatcherConnState) clear() {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = ""
	d.eventServiceIncarnation = 0
	d.readyEventReceived.Store(false)
}

func (d *dispatcherConnState) setEventServiceID(serverID node.ID) {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = serverID
}

func (d *dispatcherConnState) setWaitingReady(serverID node.ID) {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = serverID
	d.eventServiceIncarnation = 0
	d.readyEventReceived.Store(false)
}

func (d *dispatcherConnState) setWaitingReadyWithIncarnation(serverID node.ID, incarnation uint64) {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = serverID
	d.eventServiceIncarnation = incarnation
	d.readyEventReceived.Store(false)
}

func (d *dispatcherConnState) setReady(serverID node.ID, incarnation uint64) {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = serverID
	d.eventServiceIncarnation = incarnation
	d.readyEventReceived.Store(true)
}

func (d *dispatcherConnState) getEventServiceID() node.ID {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID
}

func (d *dispatcherConnState) getEventServiceIncarnation() uint64 {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceIncarnation
}

func (d *dispatcherConnState) getBinding() (node.ID, uint64, bool) {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID, d.eventServiceIncarnation, d.readyEventReceived.Load()
}

func (d *dispatcherConnState) isCurrentEventService(serverID node.ID) bool {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID == serverID
}

func (d *dispatcherConnState) isStreamingFrom(serverID node.ID) bool {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID == serverID && d.readyEventReceived.Load()
}

func (d *dispatcherConnState) isReceivingDataEvent() bool {
	d.RLock()
	defer d.RUnlock()
	return !d.eventServiceID.IsEmpty() && d.readyEventReceived.Load()
}

func (d *dispatcherConnState) trySetRemoteCandidates(nodes []string) bool {
	d.Lock()
	defer d.Unlock()
	if d.eventServiceID != "" || len(nodes) == 0 {
		return false
	}
	d.remoteCandidates = nodes
	return true
}

func (d *dispatcherConnState) getNextRemoteCandidate() node.ID {
	d.Lock()
	defer d.Unlock()
	if len(d.remoteCandidates) == 0 {
		return ""
	}
	d.eventServiceID = node.ID(d.remoteCandidates[0])
	d.remoteCandidates = d.remoteCandidates[1:]
	return d.eventServiceID
}

func (d *dispatcherConnState) clearRemoteCandidates() {
	d.Lock()
	defer d.Unlock()
	d.remoteCandidates = nil
}

func (d *dispatcherConnState) clearIfCurrentEventService(serverID node.ID) {
	d.Lock()
	defer d.Unlock()
	if d.eventServiceID == serverID {
		d.eventServiceID = ""
		d.eventServiceIncarnation = 0
		d.readyEventReceived.Store(false)
	}
}

type dispatcherControlState struct {
	connState dispatcherConnState

	controlMu        sync.Mutex
	pendingRegisters map[node.ID]*pendingRegisterState
	pendingReset     *pendingResetState
}

func (d *dispatcherStat) initControlState() {
	d.pendingRegisters = make(map[node.ID]*pendingRegisterState)
}

func (d *dispatcherControlState) clearBinding() {
	d.connState.clear()
}

func (d *dispatcherStat) registerTo(serverID node.ID) {
	if serverID == "" {
		return
	}
	onlyReuse := serverID != d.eventCollector.getLocalServerID()
	d.controlMu.Lock()
	if d.pendingRegisters == nil {
		d.pendingRegisters = make(map[node.ID]*pendingRegisterState)
	}
	d.pendingRegisters[serverID] = &pendingRegisterState{
		serverID:  serverID,
		onlyReuse: onlyReuse,
	}
	d.controlMu.Unlock()
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, d.newDispatcherRegisterRequest(d.eventCollector.getLocalServerID().String(), onlyReuse))
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) reregisterTo(serverID node.ID, incarnation uint64) {
	d.connState.setWaitingReadyWithIncarnation(serverID, incarnation)
	d.registerTo(serverID)
}

func (d *dispatcherStat) commitReady(serverID node.ID) {
	if d.tryResendPendingReset(serverID) {
		return
	}
	d.doReset(serverID, d.getResetTs(), false)
}

func (d *dispatcherStat) reset(serverID node.ID) {
	d.doReset(serverID, d.getResetTs(), d.lastEventSeq.Load() == 0)
}

func (d *dispatcherStat) preparePendingReset(serverID node.ID, resetTs uint64, reuseExisting bool) pendingResetState {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	if reuseExisting && d.pendingReset != nil {
		d.pendingReset.serverID = serverID
		d.pendingReset.resetTs = resetTs
		return *d.pendingReset
	}
	epoch := d.epoch.Add(1)
	d.lastEventSeq.Store(0)
	d.pendingReset = &pendingResetState{
		serverID: serverID,
		resetTs:  resetTs,
		epoch:    epoch,
	}
	return *d.pendingReset
}

func (d *dispatcherStat) getPendingReset() *pendingResetState {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	if d.pendingReset == nil {
		return nil
	}
	pending := *d.pendingReset
	return &pending
}

func (d *dispatcherStat) clearPendingReset(epoch uint64) {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	if d.pendingReset != nil && (epoch == 0 || d.pendingReset.epoch == epoch) {
		d.pendingReset = nil
	}
}

func (d *dispatcherStat) doReset(serverID node.ID, resetTs uint64, reuseExisting bool) {
	pendingReset := d.preparePendingReset(serverID, resetTs, reuseExisting)
	resetRequest := d.newDispatcherResetRequest(d.eventCollector.getLocalServerID().String(), pendingReset.resetTs, pendingReset.epoch)
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, resetRequest)
	d.eventCollector.enqueueMessageForSend(msg)
	log.Info("send reset dispatcher request to event service",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Stringer("eventServiceID", serverID),
		zap.Uint64("epoch", pendingReset.epoch),
		zap.Uint64("resetTs", pendingReset.resetTs))
}

func (d *dispatcherStat) tryResendPendingReset(serverID node.ID) bool {
	pendingReset := d.getPendingReset()
	if pendingReset == nil || pendingReset.serverID != serverID {
		return false
	}
	d.doReset(serverID, pendingReset.resetTs, true)
	return true
}

func (d *dispatcherStat) prepareRebuild(serverID node.ID, incarnation uint64) {
	d.preparePendingReset(serverID, d.getResetTs(), true)
	d.reregisterTo(serverID, incarnation)
}

func (d *dispatcherStat) getResetTs() uint64 {
	return d.target.GetCheckpointTs()
}

func (d *dispatcherStat) remove() {
	d.removeFrom(d.eventCollector.getLocalServerID())
	eventServiceID := d.connState.getEventServiceID()
	if eventServiceID != "" && eventServiceID != d.eventCollector.getLocalServerID() {
		d.removeFrom(eventServiceID)
	}
}

func (d *dispatcherStat) removeFrom(serverID node.ID) {
	if serverID == "" {
		return
	}
	log.Info("send remove dispatcher request to event service",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Stringer("eventServiceID", serverID))
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, d.newDispatcherRemoveRequest(d.eventCollector.getLocalServerID().String()))
	d.eventCollector.addRemoveTombstone(d.getDispatcherID(), serverID, msg)
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) clearPendingRegister(serverID node.ID) {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	if d.pendingRegisters == nil {
		return
	}
	delete(d.pendingRegisters, serverID)
}

func (d *dispatcherStat) reconcileControlRequests() {
	d.controlMu.Lock()
	registers := make([]pendingRegisterState, 0, len(d.pendingRegisters))
	for _, pending := range d.pendingRegisters {
		registers = append(registers, *pending)
	}
	var pendingReset *pendingResetState
	if d.pendingReset != nil {
		tmp := *d.pendingReset
		pendingReset = &tmp
	}
	d.controlMu.Unlock()

	for _, pending := range registers {
		msg := messaging.NewSingleTargetMessage(
			pending.serverID,
			messaging.EventServiceTopic,
			d.newDispatcherRegisterRequest(d.eventCollector.getLocalServerID().String(), pending.onlyReuse),
		)
		d.eventCollector.enqueueMessageForSend(msg)
	}

	if pendingReset == nil || !d.connState.isStreamingFrom(pendingReset.serverID) {
		return
	}
	msg := messaging.NewSingleTargetMessage(
		pendingReset.serverID,
		messaging.EventServiceTopic,
		d.newDispatcherResetRequest(d.eventCollector.getLocalServerID().String(), pendingReset.resetTs, pendingReset.epoch),
	)
	d.eventCollector.enqueueMessageForSend(msg)
}

func isIncarnationStale(current uint64, incoming uint64) bool {
	return current != 0 && incoming != 0 && incoming < current
}

func (d *dispatcherStat) handleDispatcherControlEvent(from node.ID, controlEvent *commonEvent.DispatcherControlEvent) {
	currentServerID, currentIncarnation, ready := d.connState.getBinding()
	if currentServerID == from && isIncarnationStale(currentIncarnation, controlEvent.Incarnation) {
		return
	}

	switch controlEvent.Action {
	case commonEvent.DispatcherControlActionRegister:
		d.clearPendingRegister(from)
		if !ready && currentServerID == from && controlEvent.Incarnation != 0 &&
			(currentIncarnation == 0 || currentIncarnation <= controlEvent.Incarnation) {
			d.connState.setWaitingReadyWithIncarnation(from, controlEvent.Incarnation)
		}
		if controlEvent.Status != commonEvent.DispatcherControlStatusRejected {
			return
		}
		if controlEvent.ReasonCode == commonEvent.DispatcherControlReasonNotReusable {
			candidate := d.connState.getNextRemoteCandidate()
			if candidate != "" {
				d.registerTo(candidate)
				return
			}
			d.connState.clearIfCurrentEventService(from)
			return
		}
		log.Warn("dispatcher register request was rejected",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Stringer("eventServiceID", from),
			zap.Uint64("reasonCode", controlEvent.ReasonCode))
	case commonEvent.DispatcherControlActionReset:
		switch controlEvent.Status {
		case commonEvent.DispatcherControlStatusAccepted, commonEvent.DispatcherControlStatusStale:
			d.clearPendingReset(controlEvent.Epoch)
		case commonEvent.DispatcherControlStatusNotFound:
			d.connState.setWaitingReadyWithIncarnation(from, controlEvent.Incarnation)
			d.registerTo(from)
		case commonEvent.DispatcherControlStatusRejected:
			log.Warn("dispatcher reset request was rejected",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.Stringer("eventServiceID", from),
				zap.Uint64("epoch", controlEvent.Epoch),
				zap.Uint64("reasonCode", controlEvent.ReasonCode))
		}
	default:
		log.Panic("unexpected control action for dispatcher stat",
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Uint64("action", uint64(controlEvent.Action)))
	}
}

func (d *dispatcherStat) setRemoteCandidates(nodes []string) {
	if len(nodes) == 0 {
		return
	}
	if d.connState.trySetRemoteCandidates(nodes) {
		log.Info("set remote candidates",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcherID", d.getDispatcherID()),
			zap.Int64("tableID", d.target.GetTableSpan().TableID),
			zap.Strings("nodes", nodes))
		d.registerTo(d.connState.getNextRemoteCandidate())
	}
}

func (d *dispatcherStat) newDispatcherRegisterRequest(serverId string, onlyReuse bool) *messaging.DispatcherRequest {
	startTs := d.target.GetStartTs()
	syncPointInterval := d.target.GetSyncPointInterval()
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId:         d.target.GetChangefeedID().ToPB(),
			DispatcherId:         d.target.GetId().ToPB(),
			TableSpan:            d.target.GetTableSpan(),
			StartTs:              startTs,
			ServerId:             serverId,
			ActionType:           eventpb.ActionType_ACTION_TYPE_REGISTER,
			FilterConfig:         d.target.GetFilterConfig(),
			EnableSyncPoint:      d.target.EnableSyncPoint(),
			SyncPointInterval:    uint64(syncPointInterval.Seconds()),
			SyncPointTs:          syncpoint.CalculateStartSyncPointTs(startTs, syncPointInterval, d.target.GetSkipSyncpointAtStartTs()),
			OnlyReuse:            onlyReuse,
			BdrMode:              d.target.GetBDRMode(),
			Mode:                 d.target.GetMode(),
			Epoch:                0,
			Timezone:             d.target.GetTimezone(),
			Integrity:            d.target.GetIntegrityConfig(),
			OutputRawChangeEvent: d.target.IsOutputRawChangeEvent(),
			TxnAtomicity:         string(d.target.GetTxnAtomicity()),
		},
	}
}

func (d *dispatcherStat) newDispatcherResetRequest(serverId string, resetTs uint64, epoch uint64) *messaging.DispatcherRequest {
	syncPointInterval := d.target.GetSyncPointInterval()
	skipSyncpointSameAsResetTs := false
	if resetTs == d.target.GetStartTs() {
		skipSyncpointSameAsResetTs = d.target.GetSkipSyncpointAtStartTs()
	}
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId:         d.target.GetChangefeedID().ToPB(),
			DispatcherId:         d.target.GetId().ToPB(),
			TableSpan:            d.target.GetTableSpan(),
			StartTs:              resetTs,
			ServerId:             serverId,
			ActionType:           eventpb.ActionType_ACTION_TYPE_RESET,
			FilterConfig:         d.target.GetFilterConfig(),
			EnableSyncPoint:      d.target.EnableSyncPoint(),
			SyncPointInterval:    uint64(syncPointInterval.Seconds()),
			SyncPointTs:          syncpoint.CalculateStartSyncPointTs(resetTs, syncPointInterval, skipSyncpointSameAsResetTs),
			BdrMode:              d.target.GetBDRMode(),
			Mode:                 d.target.GetMode(),
			Epoch:                epoch,
			Timezone:             d.target.GetTimezone(),
			Integrity:            d.target.GetIntegrityConfig(),
			OutputRawChangeEvent: d.target.IsOutputRawChangeEvent(),
		},
	}
}

func (d *dispatcherStat) newDispatcherRemoveRequest(serverId string) *messaging.DispatcherRequest {
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			ServerId:     serverId,
			ActionType:   eventpb.ActionType_ACTION_TYPE_REMOVE,
			Epoch:        d.epoch.Load(),
			Mode:         d.target.GetMode(),
		},
	}
}
