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
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type dispatcherConnState struct {
	sync.RWMutex
	// 1) if eventServiceID is set to a remote event service,
	//   it means the dispatcher is trying to register to the remote event service,
	//   but eventServiceID may be changed if registration failed.
	// 2) if eventServiceID is set to local event service,
	//   it means the dispatcher has received ready signal from local event service,
	//   and eventServiceID will never change.
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

	// sentCommitTs is the largest commit timestamp that has been sent to the dispatcher.
	sentCommitTs atomic.Uint64
	// gotDDLOnTS indicates whether a DDL event was received at the sentCommitTs.
	gotDDLOnTs atomic.Bool
	// gotSyncpointOnTS indicates whether a sync point was received at the sentCommitTs.
	gotSyncpointOnTS atomic.Bool

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
	stat.sentCommitTs.Store(target.GetStartTs())
	return stat
}

func (d *dispatcherStat) run() {
	d.registerTo(d.eventCollector.getLocalServerID())
}

func (d *dispatcherStat) registerTo(serverID node.ID) {
	msg := messaging.NewSingleTargetMessage(serverID, eventServiceTopic, d.newDispatcherRegisterRequest(false))
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) reset(serverID node.ID) {
	log.Info("Send reset dispatcher request to event service",
		zap.Stringer("dispatcher", d.target.GetId()),
		zap.Stringer("eventServiceID", serverID),
		zap.Uint64("startTs", d.sentCommitTs.Load()))
	d.lastEventSeq.Store(0)
	msg := messaging.NewSingleTargetMessage(serverID, eventServiceTopic, d.newDispatcherResetRequest())
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) remove() {
	// unregister from local event service
	d.removeFrom(d.eventCollector.getLocalServerID())

	// check if it is need to unregister from remote event service
	eventServiceID := d.connState.getEventServiceID()
	if eventServiceID != "" && eventServiceID != d.eventCollector.getLocalServerID() {
		d.removeFrom(eventServiceID)
	}
}

func (d *dispatcherStat) removeFrom(serverID node.ID) {
	log.Info("Send remove dispatcher request to event service",
		zap.Stringer("dispatcher", d.target.GetId()),
		zap.Stringer("eventServiceID", serverID))
	msg := messaging.NewSingleTargetMessage(serverID, eventServiceTopic, d.newDispatcherRemoveRequest())
	d.eventCollector.enqueueMessageForSend(msg)
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
	msg := messaging.NewSingleTargetMessage(eventServiceID, eventServiceTopic, d.newDispatcherPauseRequest())
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
	msg := messaging.NewSingleTargetMessage(eventServiceID, eventServiceTopic, d.newDispatcherResumeRequest())
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) wake() {
	d.eventCollector.ds.Wake(d.getDispatcherID())
}

func (d *dispatcherStat) getDispatcherID() common.DispatcherID {
	return d.target.GetId()
}

// verifyEventSequence verifies if the event's sequence number is continuous with previous events.
// Returns false if sequence is discontinuous (indicating dropped events), which requires dispatcher reset.
func (d *dispatcherStat) verifyEventSequence(event dispatcher.DispatcherEvent) bool {
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
				zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Int("eventType", event.GetType()),
				zap.Uint64("receivedSeq", event.GetSeq()),
				zap.Uint64("expectedSeq", expectedSeq),
				zap.Uint64("commitTs", event.GetCommitTs()))
			return false
		}
	case commonEvent.TypeBatchDMLEvent:
		for _, e := range event.Event.(*commonEvent.BatchDMLEvent).DMLEvents {
			log.Debug("check batch DML event sequence",
				zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Uint64("receivedSeq", e.Seq),
				zap.Uint64("lastEventSeq", d.lastEventSeq.Load()),
				zap.Uint64("commitTs", e.CommitTs))

			expectedSeq := d.lastEventSeq.Add(1)
			if e.Seq != expectedSeq {
				log.Warn("Received an out-of-order batch DML event, reset the dispatcher",
					zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
					zap.Stringer("dispatcher", d.target.GetId()),
					zap.Int("eventType", event.GetType()),
					zap.Uint64("receivedSeq", e.Seq),
					zap.Uint64("expectedSeq", expectedSeq),
					zap.Uint64("commitTs", e.CommitTs))
				return false
			}
		}
	}
	return true
}

// filterAndUpdateEventByCommitTs verifies if the event's commit timestamp is valid.
// Note: this function must be called on every event received.
func (d *dispatcherStat) filterAndUpdateEventByCommitTs(event dispatcher.DispatcherEvent) bool {
	shouldIgnore := false
	if event.GetCommitTs() < d.sentCommitTs.Load() {
		shouldIgnore = true
	} else if event.GetCommitTs() == d.sentCommitTs.Load() {
		// Avoid send the same DDL event or SyncPoint event multiple times.
		switch event.GetType() {
		case commonEvent.TypeDDLEvent:
			shouldIgnore = d.gotDDLOnTs.Load()
		case commonEvent.TypeSyncPointEvent:
			shouldIgnore = d.gotSyncpointOnTS.Load()
		default:
			// TODO: check whether it is ok for other types of events?
			// a commit ts may have multiple transactions, it is ok to send the same txn multiple times?
		}
	}
	if shouldIgnore {
		log.Warn("Receive a event older than sendCommitTs, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Int64("tableID", d.target.GetTableSpan().TableID),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Any("event", event.Event),
			zap.Uint64("eventCommitTs", event.GetCommitTs()),
			zap.Uint64("sentCommitTs", d.sentCommitTs.Load()))
		return false
	}
	if event.GetCommitTs() > d.sentCommitTs.Load() {
		// if the commit ts is larger than the last sent commit ts,
		// we need to reset the DDL and SyncPoint flags.
		d.gotDDLOnTs.Store(false)
		d.gotSyncpointOnTS.Store(false)
	}
	d.sentCommitTs.Store(event.GetCommitTs())
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		d.gotDDLOnTs.Store(true)
	case commonEvent.TypeSyncPointEvent:
		d.gotSyncpointOnTS.Store(true)
	}
	return true
}

func (d *dispatcherStat) handleDataEvents(events ...dispatcher.DispatcherEvent) bool {
	switch events[0].GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeResolvedEvent:
		// 1. filter out events from stale event services
		// 2. check if the event seq is valid, if not, discard all events in this batch and reset the dispatcher
		//    Note: this may do some unnecessary reset, but after add epoch it will be fixed.
		// 3. ignore event with commit ts less than sentCommitTs
		containsValidEvents := false
		containsStaleEvents := false
		for _, event := range events {
			if d.connState.isCurrentEventService(*event.From) {
				containsValidEvents = true
				if !d.verifyEventSequence(event) {
					// if event seq is invalid, there must be some events dropped
					// we need drop all events in this batch and reset the dispatcher
					d.reset(d.connState.getEventServiceID())
					return false
				}
			} else {
				containsStaleEvents = true
			}
		}
		if !containsValidEvents {
			return false
		}
		var validEvents []dispatcher.DispatcherEvent
		if containsStaleEvents {
			for _, event := range events {
				if d.connState.isCurrentEventService(*event.From) && d.filterAndUpdateEventByCommitTs(event) {
					validEvents = append(validEvents, event)
				}
			}
		} else {
			invalidEventCount := 0
			meetValidEvent := false
			for _, event := range events {
				if !d.filterAndUpdateEventByCommitTs(event) {
					if meetValidEvent {
						// event is sort by commitTs, so no invalid event should be after a valid event
						log.Panic("should not happen: invalid event after valid event",
							zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
							zap.Stringer("dispatcherID", d.target.GetId()))
					}
					events[invalidEventCount] = event
					invalidEventCount++
				} else {
					meetValidEvent = true
				}
			}
			validEvents = events[invalidEventCount:]
		}
		return d.target.HandleEvents(validEvents, func() {
			d.wake()
			log.Debug("wake dispatcher",
				zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
				zap.Stringer("dispatcher", d.target.GetId()))
		})
	case commonEvent.TypeDDLEvent,
		commonEvent.TypeSyncPointEvent,
		commonEvent.TypeHandshakeEvent,
		commonEvent.TypeBatchDMLEvent:
		if len(events) != 1 {
			log.Panic("should not happen: only one event should be sent for DDL/SyncPoint/Handshake event")
		}
		from := events[0].From
		if !d.connState.isCurrentEventService(*from) {
			log.Info("receive DDL/SyncPoint/Handshake event from a stale event service, ignore it",
				zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("staleEventService", *from),
				zap.Stringer("currentEventService", d.connState.getEventServiceID()))
			return false
		}
		if !d.verifyEventSequence(events[0]) {
			d.reset(d.connState.getEventServiceID())
			return false
		}
		if events[0].GetType() == commonEvent.TypeBatchDMLEvent {
			tableInfo := d.tableInfo.Load().(*common.TableInfo)
			if tableInfo == nil {
				log.Panic("should not happen: table info should be set before batch DML event",
					zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
					zap.Stringer("dispatcher", d.target.GetId()))
			}
			batchDML := events[0].Event.(*event.BatchDMLEvent)
			batchDML.AssembleRows(tableInfo)
			dmlEvents := make([]dispatcher.DispatcherEvent, 0, len(batchDML.DMLEvents))
			for _, dml := range batchDML.DMLEvents {
				dmlEvent := dispatcher.NewDispatcherEvent(from, dml)
				if d.filterAndUpdateEventByCommitTs(dmlEvent) {
					dmlEvents = append(dmlEvents, dmlEvent)
				}
			}
			return d.target.HandleEvents(dmlEvents, func() { d.wake() })
		} else if events[0].GetType() == commonEvent.TypeHandshakeEvent {
			tableInfo := events[0].Event.(*event.HandshakeEvent).TableInfo
			if tableInfo != nil {
				d.tableInfo.Store(tableInfo)
			}
		} else if events[0].GetType() == commonEvent.TypeDDLEvent {
			if !d.filterAndUpdateEventByCommitTs(events[0]) {
				return false
			}
			tableInfo := events[0].Event.(*event.DDLEvent).TableInfo
			if tableInfo != nil {
				d.tableInfo.Store(tableInfo)
			}
			return d.target.HandleEvents(events, func() { d.wake() })
		} else {
			// SyncPointEvent
			if !d.filterAndUpdateEventByCommitTs(events[0]) {
				return false
			}
			return d.target.HandleEvents(events, func() { d.wake() })
		}
	default:
		log.Panic("should not happen: unknown event type", zap.Int("eventType", events[0].GetType()))
	}
	return false
}

// "signalEvent" refers to the types of events that may modify the event service with which this dispatcher communicates.
// "signalEvent" includes TypeReadyEvent/TypeNotReusableEvent
func (d *dispatcherStat) handleSignalEvent(event dispatcher.DispatcherEvent) {
	localServerID := d.eventCollector.getLocalServerID()
	// if the dispatcher has received ready signal from local event service,
	// ignore all types of signal events.
	if d.connState.isCurrentEventService(localServerID) {
		return
	}

	// if the event is neither from local event service nor from the current event service, ignore it.
	if *event.From != localServerID && !d.connState.isCurrentEventService(*event.From) {
		return
	}

	switch event.GetType() {
	case commonEvent.TypeReadyEvent:
		if *event.From == localServerID {
			if d.readyCallback != nil {
				d.connState.setEventServiceID(localServerID)
				d.connState.readyEventReceived.Store(true)
				d.readyCallback()
				return
			}
			// note: this must be the first ready event from local event service
			oldEventServiceID := d.connState.getEventServiceID()
			if oldEventServiceID != "" {
				d.removeFrom(oldEventServiceID)
			}
			log.Info("received ready signal from local event service, prepare to reset the dispatcher",
				zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
				zap.Stringer("dispatcher", d.target.GetId()))

			d.connState.setEventServiceID(localServerID)
			d.connState.readyEventReceived.Store(true)
			d.connState.clearRemoteCandidates()
			d.reset(localServerID)
		} else {
			// note: this ready event must be from a remote event service which the dispatcher is trying to register to.
			// TODO: if receive too much redudant ready events from remote service, we may need reset again?
			if d.connState.readyEventReceived.Load() {
				log.Info("received ready signal from the same server again, ignore it",
					zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
					zap.Stringer("dispatcher", d.target.GetId()),
					zap.Stringer("eventServiceID", *event.From))
				return
			}
			log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
				zap.Stringer("changefeedID", d.target.GetChangefeedID().ID()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("eventServiceID", *event.From))
			d.connState.readyEventReceived.Store(true)
			d.reset(*event.From)
		}
	case commonEvent.TypeNotReusableEvent:
		if *event.From == localServerID {
			log.Panic("should not happen: local event service should not send not reusable event")
		}
		candidate := d.connState.getNextRemoteCandidate()
		if candidate != "" {
			d.registerTo(candidate)
		}
	default:
		log.Panic("should not happen: unknown signal event type")
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
		d.registerTo(candidate)
	}
}

func (d *dispatcherStat) newDispatcherRegisterRequest(onlyReuse bool) *messaging.DispatcherRequest {
	startTs := d.target.GetStartTs()
	syncPointInterval := d.target.GetSyncPointInterval()
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			StartTs:      startTs,
			// ServerId is the id of the request sender.
			ServerId:          d.eventCollector.getLocalServerID().String(),
			ActionType:        eventpb.ActionType_ACTION_TYPE_REGISTER,
			FilterConfig:      d.target.GetFilterConfig(),
			EnableSyncPoint:   d.target.EnableSyncPoint(),
			SyncPointInterval: uint64(syncPointInterval.Seconds()),
			SyncPointTs:       syncpoint.CalculateStartSyncPointTs(startTs, syncPointInterval, d.target.GetStartTsIsSyncpoint()),
			OnlyReuse:         onlyReuse,
			BdrMode:           d.target.GetBDRMode(),
			Timezone:          d.target.GetTimezone(),
			Integrity:         d.target.GetIntegrityConfig(),
		},
	}
}

func (d *dispatcherStat) newDispatcherResetRequest() *messaging.DispatcherRequest {
	startTs := d.target.GetStartTs()
	syncPointInterval := d.target.GetSyncPointInterval()
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			StartTs:      startTs,
			// ServerId is the id of the request sender.
			ServerId:          d.eventCollector.getLocalServerID().String(),
			ActionType:        eventpb.ActionType_ACTION_TYPE_RESET,
			FilterConfig:      d.target.GetFilterConfig(),
			EnableSyncPoint:   d.target.EnableSyncPoint(),
			SyncPointInterval: uint64(syncPointInterval.Seconds()),
			SyncPointTs:       syncpoint.CalculateStartSyncPointTs(startTs, syncPointInterval, d.target.GetStartTsIsSyncpoint()),
		},
	}
}

func (d *dispatcherStat) newDispatcherRemoveRequest() *messaging.DispatcherRequest {
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			// ServerId is the id of the request sender.
			ServerId:   d.eventCollector.getLocalServerID().String(),
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
		},
	}
}

func (d *dispatcherStat) newDispatcherPauseRequest() *messaging.DispatcherRequest {
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			// ServerId is the id of the request sender.
			ServerId:   d.eventCollector.getLocalServerID().String(),
			ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE,
		},
	}
}

func (d *dispatcherStat) newDispatcherResumeRequest() *messaging.DispatcherRequest {
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			// ServerId is the id of the request sender.
			ServerId:   d.eventCollector.getLocalServerID().String(),
			ActionType: eventpb.ActionType_ACTION_TYPE_RESUME,
		},
	}
}
