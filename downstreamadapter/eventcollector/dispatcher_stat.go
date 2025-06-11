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

package eventcollector

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type dispatcherConnState struct {
	sync.RWMutex
	// the server this dispatcher currently talking to
	eventServiceID node.ID
	// whether has received ready signal from `serverID`
	readyEventReceived atomic.Bool
	// the remote event services which may contain data this dispatcher needed
	remoteCandidates []string
}

func (d *dispatcherConnState) setEventServiceID(serverID node.ID) {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = serverID
}

func (d *dispatcherConnState) getEventServiceID() node.ID {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID
}

func (d *dispatcherConnState) isCurrentEventService(serverID node.ID) bool {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID == serverID
}

// TODO: whether this check is need? Or whether this check is enough?
func (d *dispatcherConnState) isReceivingDataEvent() bool {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID != "" && d.readyEventReceived.Load()
}

func (d *dispatcherConnState) trySetRemoteCandidates(nodes []string) bool {
	d.Lock()
	defer d.Unlock()
	// reading from a event service or checking remotes already, ignore
	if d.eventServiceID != "" {
		return false
	}
	if len(nodes) == 0 {
		return false
	}
	d.remoteCandidates = nodes
	return true
}

func (d *dispatcherConnState) getNextRemoteCandidate() node.ID {
	d.Lock()
	defer d.Unlock()
	if len(d.remoteCandidates) > 0 {
		d.eventServiceID = node.ID(d.remoteCandidates[0])
		d.remoteCandidates = d.remoteCandidates[1:]
		return d.eventServiceID
	}
	return ""
}

func (d *dispatcherConnState) clearRemoteCandidates() {
	d.Lock()
	defer d.Unlock()
	d.remoteCandidates = nil
}

// dispatcherStat is a helper struct to manage the state of a dispatcher.
type dispatcherStat struct {
	target         dispatcher.EventDispatcher
	eventCollector *EventCollector
	readyCallback  func()

	connState dispatcherConnState

	// lastEventSeq is the sequence number of the last received DML/DDL/Handshake event.
	// It is used to ensure the order of events.
	lastEventSeq atomic.Uint64

	// waitHandshake is used to indicate whether the dispatcher is waiting for a handshake event.
	// Dispatcher will drop all data events before receiving a handshake event.
	// TODO: it will be removed after we add epoch to events.
	waitHandshake atomic.Bool

	// The largest commit ts that has been sent to the dispatcher.
	sentCommitTs atomic.Uint64

	// tableInfo is the latest table info of the dispatcher's corresponding table.
	tableInfo atomic.Value
}

func newDispatcherStat(
	target dispatcher.EventDispatcher,
	eventCollector *EventCollector,
	readyCallback func(),
) *dispatcherStat {
	stat := &dispatcherStat{
		target:         target,
		eventCollector: eventCollector,
		readyCallback:  readyCallback,
	}
	stat.lastEventSeq.Store(0)
	stat.waitHandshake.Store(true)
	stat.sentCommitTs.Store(target.GetStartTs())
	return stat
}

func (d *dispatcherStat) run() {
	msg := messaging.NewSingleTargetMessage(d.eventCollector.getLocalServerID(), eventServiceTopic, newDispatcherRegisterRequest(d.target, false))
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) reset() {
	log.Info("Send reset dispatcher request to event service",
		zap.Stringer("dispatcher", d.target.GetId()),
		zap.Uint64("startTs", d.sentCommitTs.Load()))
	d.lastEventSeq.Store(0)
	d.waitHandshake.Store(true)
	msg := messaging.NewSingleTargetMessage(d.eventCollector.getLocalServerID(), eventServiceTopic, newDispatcherResetRequest(d.target))
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) remove() {
	// unregister from local event service
	msg := messaging.NewSingleTargetMessage(d.eventCollector.getLocalServerID(), eventServiceTopic, newDispatcherRemoveRequest(d.target))
	d.eventCollector.enqueueMessageForSend(msg)

	// check if it is need to unregister from remote event service
	eventServiceID := d.connState.getEventServiceID()
	if eventServiceID != "" && eventServiceID != d.eventCollector.getLocalServerID() {
		msg := messaging.NewSingleTargetMessage(eventServiceID, eventServiceTopic, newDispatcherRemoveRequest(d.target))
		d.eventCollector.enqueueMessageForSend(msg)
	}
}

func (d *dispatcherStat) pause() {
	// Just ignore the request if the dispatcher is not ready.
	if !d.connState.isReceivingDataEvent() {
		log.Info("ignore pause dispatcher request because the eventService is not ready",
			zap.Stringer("dispatcherID", d.getDispatcherID()),
			zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
		)
		return
	}
	eventServiceID := d.connState.getEventServiceID()
	msg := messaging.NewSingleTargetMessage(eventServiceID, eventServiceTopic, newDispatcherPauseRequest(d.target))
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) resume() {
	// Just ignore the request if the dispatcher is not ready.
	if !d.connState.isReceivingDataEvent() {
		log.Info("ignore resume dispatcher request because the eventService is not ready",
			zap.Stringer("dispatcherID", d.getDispatcherID()),
			zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
		)
		return
	}
	eventServiceID := d.connState.getEventServiceID()
	msg := messaging.NewSingleTargetMessage(eventServiceID, eventServiceTopic, newDispatcherResumeRequest(d.target))
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) wake() {
	d.eventCollector.ds.Wake(d.getDispatcherID())
}

func (d *dispatcherStat) getDispatcherID() common.DispatcherID {
	return d.target.GetId()
}

// TODO: add epoch to event and use it to filter irrelevant events
func (d *dispatcherStat) isEventFromCurrentEventService(event dispatcher.DispatcherEvent) bool {
	if !d.connState.isCurrentEventService(*event.From) {
		log.Warn("Receive event from other event service, ignore it",
			zap.Stringer("dispatcher", d.target.GetId()))
		return false
	}
	// TODO: maybe we can remove this after add epoch?
	if d.waitHandshake.Load() {
		log.Warn("Receive event before handshake event, ignore it",
			zap.Stringer("dispatcher", d.target.GetId()))
		return false
	}
	return true
}

// isEventSeqValid check whether there are any events being dropped
func (d *dispatcherStat) isEventSeqValid(event dispatcher.DispatcherEvent) bool {
	switch event.GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeDDLEvent,
		commonEvent.TypeHandshakeEvent:
		log.Debug("check event sequence",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Int("eventType", event.GetType()),
			zap.Uint64("receivedSeq", event.GetSeq()),
			zap.Uint64("lastEventSeq", d.lastEventSeq.Load()),
			zap.Uint64("commitTs", event.GetCommitTs()))

		expectedSeq := d.lastEventSeq.Add(1)
		if event.GetSeq() != expectedSeq {
			log.Warn("Received an out-of-order event, reset the dispatcher",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Int("eventType", event.GetType()),
				zap.Uint64("receivedSeq", event.GetSeq()),
				zap.Uint64("expectedSeq", expectedSeq),
				zap.Uint64("commitTs", event.GetCommitTs()))
			return false
		}
	}
	return true
}

func (d *dispatcherStat) isEventCommitTsValid(event dispatcher.DispatcherEvent) bool {
	// Note: a commit ts may have multiple transactions.
	// it is ok to send the same txn multiple times?
	// (we just want to avoid send old dml after new ddl)
	if event.GetCommitTs() < d.sentCommitTs.Load() {
		log.Warn("Receive a event older than sendCommitTs, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Int64("tableID", d.target.GetTableSpan().TableID),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Any("event", event.Event),
			zap.Uint64("eventCommitTs", event.GetCommitTs()),
			zap.Uint64("sentCommitTs", d.sentCommitTs.Load()))
		return false
	}
	d.sentCommitTs.Store(event.GetCommitTs())
	return true
}

func (d *dispatcherStat) handleHandshakeEvent(event dispatcher.DispatcherEvent) {
	if event.GetType() != commonEvent.TypeHandshakeEvent {
		log.Panic("should not happen")
	}
	if !d.connState.isReceivingDataEvent() {
		log.Panic("should not happen: server ID is not set")
	}
	if !d.connState.isCurrentEventService(*event.From) {
		if !d.connState.isCurrentEventService(d.eventCollector.getLocalServerID()) {
			log.Panic("receive handshake event from remote event service, but current event service is not local event service",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("from", event.From))
		}
		log.Info("receive handshake event from remote event service, but current event service is local event service, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("from", event.From))
		return
	}
	if !d.isEventSeqValid(event) {
		d.reset()
		return
	}
	d.waitHandshake.Store(false)
}

func (d *dispatcherStat) setTableInfo(tableInfo *common.TableInfo) {
	if tableInfo == nil {
		return
	}
	d.tableInfo.Store(tableInfo)
}

func (d *dispatcherStat) handleReadyEvent(event dispatcher.DispatcherEvent) {
	if event.GetType() != commonEvent.TypeReadyEvent {
		log.Panic("should not happen")
	}

	if d.connState.isCurrentEventService(d.eventCollector.getLocalServerID()) {
		// already received ready signal from local event service
		return
	}

	// if a dispatcher's readyCallback is set, it will just register to local event service.
	if d.readyCallback != nil {
		d.connState.setEventServiceID(d.eventCollector.getLocalServerID())
		d.connState.readyEventReceived.Store(true)
		d.readyCallback()
		return
	}
	eventServiceID := *event.From
	if d.connState.isCurrentEventService(eventServiceID) {
		// case 1: already received ready signal from the same server
		if d.connState.readyEventReceived.Load() {
			log.Info("received ready signal from the same server again, ignore it",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("eventServiceID", eventServiceID))
			return
		}
		// case 2: first ready signal from the server
		// (must be a remote candidate, because we won't set d.eventServiceInfo.serverID to local event service until we receive ready signal)
		log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("eventServiceID", eventServiceID))

		d.connState.setEventServiceID(eventServiceID)
		d.connState.readyEventReceived.Store(true)
		d.reset()
	} else if eventServiceID == d.eventCollector.getLocalServerID() {
		// case 3: received first ready signal from local event service
		oldEventServiceID := d.connState.getEventServiceID()
		if oldEventServiceID != "" {
			msg := messaging.NewSingleTargetMessage(oldEventServiceID, eventServiceTopic, newDispatcherRemoveRequest(d.target))
			d.eventCollector.enqueueMessageForSend(msg)
		}
		log.Info("received ready signal from local event service, prepare to reset the dispatcher",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("eventServiceID", eventServiceID))

		d.connState.setEventServiceID(eventServiceID)
		d.connState.readyEventReceived.Store(true)
		d.connState.clearRemoteCandidates()
		d.reset()
	} else {
		log.Panic("should not happen: we have received ready signal from other remote server",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("newRemoteEventService", eventServiceID),
			zap.Stringer("oldRemoteEventService", d.connState.getEventServiceID()))
	}
}

func (d *dispatcherStat) handleNotReusableEvent(event dispatcher.DispatcherEvent) {
	if event.GetType() != commonEvent.TypeNotReusableEvent {
		log.Panic("should not happen")
	}

	if d.connState.isCurrentEventService(*event.From) {
		candidate := d.connState.getNextRemoteCandidate()
		if candidate != "" {
			msg := messaging.NewSingleTargetMessage(candidate, eventServiceTopic, newDispatcherRegisterRequest(d.target, true))
			d.eventCollector.enqueueMessageForSend(msg)
		}
	}
}

func (d *dispatcherStat) setRemoteCandidates(nodes []string) {
	log.Info("set remote candidates",
		zap.Strings("nodes", nodes),
		zap.Stringer("dispatcherID", d.target.GetId()))
	if len(nodes) == 0 {
		return
	}
	if d.connState.trySetRemoteCandidates(nodes) {
		candidate := d.connState.getNextRemoteCandidate()
		msg := messaging.NewSingleTargetMessage(candidate, eventServiceTopic, newDispatcherRegisterRequest(d.target, true))
		d.eventCollector.enqueueMessageForSend(msg)
	}
}

func newDispatcherRegisterRequest(target dispatcher.EventDispatcher, onlyReuse bool) *eventpb.DispatcherRequest {
	startTs := target.GetStartTs()
	syncPointInterval := target.GetSyncPointInterval()
	return &eventpb.DispatcherRequest{
		ChangefeedId:      target.GetChangefeedID().ToPB(),
		DispatcherId:      target.GetId().ToPB(),
		TableSpan:         target.GetTableSpan(),
		StartTs:           startTs,
		ActionType:        eventpb.ActionType_ACTION_TYPE_REGISTER,
		FilterConfig:      target.GetFilterConfig(),
		EnableSyncPoint:   target.EnableSyncPoint(),
		SyncPointInterval: uint64(syncPointInterval.Seconds()),
		SyncPointTs:       syncpoint.CalculateStartSyncPointTs(startTs, syncPointInterval, target.GetStartTsIsSyncpoint()),
		OnlyReuse:         onlyReuse,
		BdrMode:           target.GetBDRMode(),
	}
}

func newDispatcherResetRequest(target dispatcher.EventDispatcher) *eventpb.DispatcherRequest {
	startTs := target.GetStartTs()
	syncPointInterval := target.GetSyncPointInterval()
	return &eventpb.DispatcherRequest{
		ChangefeedId:      target.GetChangefeedID().ToPB(),
		DispatcherId:      target.GetId().ToPB(),
		StartTs:           startTs,
		ActionType:        eventpb.ActionType_ACTION_TYPE_RESET,
		FilterConfig:      target.GetFilterConfig(),
		EnableSyncPoint:   target.EnableSyncPoint(),
		SyncPointInterval: uint64(syncPointInterval.Seconds()),
		SyncPointTs:       syncpoint.CalculateStartSyncPointTs(startTs, syncPointInterval, target.GetStartTsIsSyncpoint()),
	}
}

func newDispatcherRemoveRequest(target dispatcher.EventDispatcher) *eventpb.DispatcherRequest {
	return &eventpb.DispatcherRequest{
		ChangefeedId: target.GetChangefeedID().ToPB(),
		DispatcherId: target.GetId().ToPB(),
		ActionType:   eventpb.ActionType_ACTION_TYPE_REMOVE,
	}
}

func newDispatcherPauseRequest(target dispatcher.EventDispatcher) *eventpb.DispatcherRequest {
	return &eventpb.DispatcherRequest{
		ChangefeedId: target.GetChangefeedID().ToPB(),
		DispatcherId: target.GetId().ToPB(),
		ActionType:   eventpb.ActionType_ACTION_TYPE_PAUSE,
	}
}

func newDispatcherResumeRequest(target dispatcher.EventDispatcher) *eventpb.DispatcherRequest {
	return &eventpb.DispatcherRequest{
		ChangefeedId: target.GetChangefeedID().ToPB(),
		DispatcherId: target.GetId().ToPB(),
		ActionType:   eventpb.ActionType_ACTION_TYPE_RESUME,
	}
}
