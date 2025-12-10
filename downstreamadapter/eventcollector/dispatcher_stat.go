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
	"github.com/pingcap/ticdc/pkg/metrics"
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

func (d *dispatcherConnState) clear() {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = ""
	d.readyEventReceived.Store(false)
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
	return !d.eventServiceID.IsEmpty() && d.readyEventReceived.Load()
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
	target         dispatcher.DispatcherService
	eventCollector *EventCollector
	readyCallback  func()

	connState dispatcherConnState

	// epoch is used to filter invalid events.
	// It is incremented when the dispatcher is reset.
	epoch atomic.Uint64
	// lastEventSeq is the sequence number of the last received DML/DDL/Handshake event.
	// It is used to ensure the order of events.
	lastEventSeq atomic.Uint64
	// lastEventCommitTs is the commitTs of the last received DDL/DML/SyncPoint events.
	lastEventCommitTs atomic.Uint64
	// gotDDLOnTS indicates whether a DDL event was received at the sentCommitTs.
	gotDDLOnTs atomic.Bool
	// gotSyncpointOnTS indicates whether a sync point was received at the sentCommitTs.
	gotSyncpointOnTS atomic.Bool
	// tableInfo is the latest table info of the dispatcher's corresponding table.
	tableInfo atomic.Value
	// tableInfoVersion is the latest table info version of the dispatcher's corresponding table.
	// It is updated by ddl event
	tableInfoVersion atomic.Uint64
}

func newDispatcherStat(
	target dispatcher.DispatcherService,
	eventCollector *EventCollector,
	readyCallback func(),
) *dispatcherStat {
	stat := &dispatcherStat{
		target:         target,
		eventCollector: eventCollector,
		readyCallback:  readyCallback,
	}
	stat.lastEventSeq.Store(0)
	stat.lastEventCommitTs.Store(target.GetStartTs())
	return stat
}

func (d *dispatcherStat) run() {
	d.registerTo(d.eventCollector.getLocalServerID())
}

func (d *dispatcherStat) clear() {
	// TODO: this design is bad because we may receive stale heartbeat response,
	// which make us call clear and register again. But the register may be ignore,
	// so we will not receive any ready event.
	d.connState.clear()
}

// registerTo register the dispatcher to the specified event service.
func (d *dispatcherStat) registerTo(serverID node.ID) {
	// `onlyReuse` is used to control the register behavior at logservice side
	// it should be set to `false` when register to a local event service,
	// and set to `true` when register to a remote event service.
	onlyReuse := serverID != d.eventCollector.getLocalServerID()
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, d.newDispatcherRegisterRequest(d.eventCollector.getLocalServerID().String(), onlyReuse))
	d.eventCollector.enqueueMessageForSend(msg)
}

// commitReady is used to notify the event service to start sending events.
func (d *dispatcherStat) commitReady(serverID node.ID) {
	d.doReset(serverID, d.getResetTs())
}

// reset is used to reset the dispatcher to the specified commitTs,
// it will remove the dispatcher from the dynamic stream and add it back.
func (d *dispatcherStat) reset(serverID node.ID) {
	d.doReset(serverID, d.getResetTs())
}

func (d *dispatcherStat) doReset(serverID node.ID, resetTs uint64) {
	epoch := d.epoch.Add(1)
	d.lastEventSeq.Store(0)
	// remove the dispatcher from the dynamic stream
	resetRequest := d.newDispatcherResetRequest(d.eventCollector.getLocalServerID().String(), resetTs, epoch)
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, resetRequest)
	d.eventCollector.enqueueMessageForSend(msg)
	log.Info("send reset dispatcher request to event service",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Stringer("eventServiceID", serverID),
		zap.Uint64("epoch", epoch),
		zap.Uint64("resetTs", resetTs))
}

// getResetTs is used to get the resetTs of the dispatcher.
// resetTs must be larger than the startTs, otherwise it will cause panic in eventStore.
func (d *dispatcherStat) getResetTs() uint64 {
	return d.target.GetCheckpointTs()
}

// remove is used to remove the dispatcher from the event service.
func (d *dispatcherStat) remove() {
	// unregister from local event service
	d.removeFrom(d.eventCollector.getLocalServerID())
	// check if it is need to unregister from remote event service
	eventServiceID := d.connState.getEventServiceID()
	if eventServiceID != "" && eventServiceID != d.eventCollector.getLocalServerID() {
		d.removeFrom(eventServiceID)
	}
}

// removeFrom is used to remove the dispatcher from the specified event service.
func (d *dispatcherStat) removeFrom(serverID node.ID) {
	log.Info("send remove dispatcher request to event service",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Stringer("eventServiceID", serverID))
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, d.newDispatcherRemoveRequest(d.eventCollector.getLocalServerID().String()))
	d.eventCollector.enqueueMessageForSend(msg)
}

func (d *dispatcherStat) wake() {
	if common.IsRedoMode(d.target.GetMode()) {
		d.eventCollector.redoDs.Wake(d.getDispatcherID())
	} else {
		d.eventCollector.ds.Wake(d.getDispatcherID())
	}
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
		commonEvent.TypeHandshakeEvent,
		commonEvent.TypeSyncPointEvent,
		commonEvent.TypeResolvedEvent:
		log.Debug("check event sequence",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.String("eventType", commonEvent.TypeToString(event.GetType())),
			zap.Uint64("receivedSeq", event.GetSeq()),
			zap.Uint64("lastEventSeq", d.lastEventSeq.Load()),
			zap.Uint64("commitTs", event.GetCommitTs()))

		lastEventSeq := d.lastEventSeq.Load()
		expectedSeq := uint64(0)

		// Resolved event's seq is the last concrete data event's seq.
		if event.GetType() == commonEvent.TypeResolvedEvent {
			expectedSeq = lastEventSeq
		} else {
			// Other events' seq is the next sequence number.
			expectedSeq = d.lastEventSeq.Add(1)
		}

		if event.GetSeq() != expectedSeq {
			log.Warn("receive an out-of-order event, reset the dispatcher",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.String("eventType", commonEvent.TypeToString(event.GetType())),
				zap.Uint64("lastEventSeq", lastEventSeq),
				zap.Uint64("lastEventCommitTs", d.lastEventCommitTs.Load()),
				zap.Uint64("receivedSeq", event.GetSeq()),
				zap.Uint64("expectedSeq", expectedSeq),
				zap.Uint64("commitTs", event.GetCommitTs()))
			return false
		}
	case commonEvent.TypeBatchDMLEvent:
		for _, e := range event.Event.(*commonEvent.BatchDMLEvent).DMLEvents {
			log.Debug("check batch DML event sequence",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.Uint64("receivedSeq", e.Seq),
				zap.Uint64("lastEventSeq", d.lastEventSeq.Load()),
				zap.Uint64("commitTs", e.CommitTs))

			expectedSeq := d.lastEventSeq.Add(1)
			if e.Seq != expectedSeq {
				log.Warn("receive an out-of-order batch DML event, reset the dispatcher",
					zap.Stringer("changefeedID", d.target.GetChangefeedID()),
					zap.Stringer("dispatcher", d.getDispatcherID()),
					zap.String("eventType", commonEvent.TypeToString(event.GetType())),
					zap.Uint64("lastEventSeq", d.lastEventSeq.Load()),
					zap.Uint64("lastEventCommitTs", d.lastEventCommitTs.Load()),
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
	if event.GetCommitTs() < d.lastEventCommitTs.Load() {
		shouldIgnore = true
	} else if event.GetCommitTs() == d.lastEventCommitTs.Load() {
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
		log.Warn("receive a event older than sendCommitTs, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Int64("tableID", d.target.GetTableSpan().TableID),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event),
			zap.Uint64("eventCommitTs", event.GetCommitTs()),
			zap.Uint64("sentCommitTs", d.lastEventCommitTs.Load()))
		return false
	}
	if event.GetCommitTs() > d.lastEventCommitTs.Load() {
		// if the commit ts is larger than the last sent commit ts,
		// we need to reset the DDL and SyncPoint flags.
		d.gotDDLOnTs.Store(false)
		d.gotSyncpointOnTS.Store(false)
	}

	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		d.gotDDLOnTs.Store(true)
	case commonEvent.TypeSyncPointEvent:
		d.gotSyncpointOnTS.Store(true)
	}

	switch event.GetType() {
	case commonEvent.TypeDDLEvent,
		commonEvent.TypeDMLEvent,
		commonEvent.TypeBatchDMLEvent,
		commonEvent.TypeSyncPointEvent:
		d.lastEventCommitTs.Store(event.GetCommitTs())
	}

	return true
}

func (d *dispatcherStat) isFromCurrentEpoch(event dispatcher.DispatcherEvent) bool {
	if event.GetType() == commonEvent.TypeBatchDMLEvent {
		batchDML := event.Event.(*commonEvent.BatchDMLEvent)
		for _, dml := range batchDML.DMLEvents {
			if dml.GetEpoch() != d.epoch.Load() {
				return false
			}
		}
	}
	if event.GetEpoch() != d.epoch.Load() {
		return false
	}
	// check the invariant that handshake event is the first event of every epoch
	if event.GetType() != commonEvent.TypeHandshakeEvent && d.lastEventSeq.Load() == 0 {
		log.Warn("receive non-handshake event before handshake event, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event))
		return false
	}
	return true
}

// handleBatchDataEvents processes a batch of DML and Resolved events with the following algorithm:
// 1. First pass: Check if there are any valid events from current epoch and if any events are from stale epoch
//   - Valid events must come from current epoch and have valid sequence numbers
//   - If any event has invalid sequence, reset dispatcher and return false
//
// 2. Second pass: Filter events based on whether there are stale events
//   - If contains stale events: Only keep events from current service that pass commitTs check
//   - If no stale events: Keep all events after the first valid event (events are sorted by commitTs)
//
// 3. Finally: Forward valid events to target with wake callback
func (d *dispatcherStat) handleBatchDataEvents(events []dispatcher.DispatcherEvent) bool {
	var validEvents []dispatcher.DispatcherEvent
	for _, event := range events {
		if !d.isFromCurrentEpoch(event) {
			log.Debug("receive DML/Resolved event from a stale epoch, ignore it",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.String("eventType", commonEvent.TypeToString(event.GetType())),
				zap.Any("event", event.Event))
			continue
		}
		if !d.verifyEventSequence(event) {
			d.reset(d.connState.getEventServiceID())
			return false
		}
		if event.GetType() == commonEvent.TypeResolvedEvent {
			validEvents = append(validEvents, event)
		} else if event.GetType() == commonEvent.TypeDMLEvent {
			if d.filterAndUpdateEventByCommitTs(event) {
				validEvents = append(validEvents, event)
			}
		} else if event.GetType() == commonEvent.TypeBatchDMLEvent {
			tableInfo := d.tableInfo.Load().(*common.TableInfo)
			if tableInfo == nil {
				log.Panic("should not happen: table info should be set before batch DML event",
					zap.Stringer("changefeedID", d.target.GetChangefeedID()),
					zap.Stringer("dispatcher", d.getDispatcherID()))
			}
			// The cloudstorage sink replicate different file according the table version.
			// If one table is just scheduled to a new processor, the tableInfoVersion should be
			// greater than or equal to the startTs of dispatcher.
			// FIXME: more elegant implementation
			tableInfoVersion := max(d.tableInfoVersion.Load(), d.target.GetStartTs())
			batchDML := event.Event.(*commonEvent.BatchDMLEvent)
			batchDML.AssembleRows(tableInfo)
			for _, dml := range batchDML.DMLEvents {
				// DMLs in the same batch share the same updateTs in their table info,
				// but they may reference different table info objects,
				// so each needs to be initialized separately.
				dml.TableInfo.InitPrivateFields()
				dml.TableInfoVersion = tableInfoVersion
				dmlEvent := dispatcher.NewDispatcherEvent(event.From, dml)
				if d.filterAndUpdateEventByCommitTs(dmlEvent) {
					validEvents = append(validEvents, dmlEvent)
				}
			}
		} else {
			log.Panic("should not happen: unknown event type in batch data events",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcherID", d.getDispatcherID()),
				zap.String("eventType", commonEvent.TypeToString(event.GetType())))
		}
	}
	if len(validEvents) == 0 {
		return false
	}
	return d.target.HandleEvents(validEvents, func() { d.wake() })
}

// handleSingleDataEvents processes single DDL, SyncPoint or BatchDML events with the following algorithm:
// 1. Validate event count (must be exactly 1)
// 2. Check if event comes from current epoch
// 3. Verify event sequence number
// 4. Process event based on type:
//   - BatchDML: Split into individual DML events
//   - DDL: Update table info if present
//   - SyncPoint: Forward directly
//
// 5. For all types: Filter by commitTs before forwarding
func (d *dispatcherStat) handleSingleDataEvents(events []dispatcher.DispatcherEvent) bool {
	if len(events) != 1 {
		log.Panic("should not happen: only one event should be sent for DDL/SyncPoint/Handshake event")
	}
	from := events[0].From
	if !d.isFromCurrentEpoch(events[0]) {
		log.Info("receive DDL/SyncPoint/Handshake event from a stale epoch, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.String("eventType", commonEvent.TypeToString(events[0].GetType())),
			zap.Any("event", events[0].Event),
			zap.Uint64("eventEpoch", events[0].GetEpoch()),
			zap.Uint64("dispatcherEpoch", d.epoch.Load()),
			zap.Stringer("staleEventService", *from),
			zap.Stringer("currentEventService", d.connState.getEventServiceID()))
		return false
	}
	if !d.verifyEventSequence(events[0]) {
		d.reset(d.connState.getEventServiceID())
		return false
	}
	if events[0].GetType() == commonEvent.TypeDDLEvent {
		if !d.filterAndUpdateEventByCommitTs(events[0]) {
			return false
		}
		ddl := events[0].Event.(*commonEvent.DDLEvent)
		d.tableInfoVersion.Store(ddl.FinishedTs)
		if ddl.TableInfo != nil {
			d.tableInfo.Store(ddl.TableInfo)
		}
		return d.target.HandleEvents(events, func() { d.wake() })
	} else {
		if !d.filterAndUpdateEventByCommitTs(events[0]) {
			return false
		}
		return d.target.HandleEvents(events, func() { d.wake() })
	}
}

func (d *dispatcherStat) handleDataEvents(events ...dispatcher.DispatcherEvent) bool {
	switch events[0].GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeResolvedEvent,
		commonEvent.TypeBatchDMLEvent:
		return d.handleBatchDataEvents(events)
	case commonEvent.TypeDDLEvent,
		commonEvent.TypeSyncPointEvent:
		return d.handleSingleDataEvents(events)
	default:
		log.Panic("should not happen: unknown event type", zap.Int("eventType", events[0].GetType()))
	}
	return false
}

// "signalEvent" refers to the types of events that may modify the event service with which this dispatcher communicates.
// "signalEvent" includes TypeReadyEvent/TypeNotReusableEvent
func (d *dispatcherStat) handleSignalEvent(event dispatcher.DispatcherEvent) {
	localServerID := d.eventCollector.getLocalServerID()

	switch event.GetType() {
	case commonEvent.TypeReadyEvent:
		// if the dispatcher has received ready signal from local event service,
		// ignore all types of signal events.
		if d.connState.isCurrentEventService(localServerID) {
			// If we receive a ready event from a remote service while connected to the local
			// service, it implies a stale registration. Send a remove request to clean it up.
			if event.From != nil && *event.From != localServerID {
				d.removeFrom(*event.From)
			}
			return
		}

		// if the event is neither from local event service nor from the current event service, ignore it.
		if *event.From != localServerID && !d.connState.isCurrentEventService(*event.From) {
			return
		}

		if *event.From == localServerID {
			if d.readyCallback != nil {
				// If readyCallback is set, this dispatcher is performing its initial
				// registration with the local event service. Therefore, no deregistration
				// from a previous service is necessary.
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
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()))

			d.connState.setEventServiceID(localServerID)
			d.connState.readyEventReceived.Store(true)
			d.connState.clearRemoteCandidates()
			d.commitReady(localServerID)
		} else {
			// note: this ready event must be from a remote event service which the dispatcher is trying to register to.
			// TODO: if receive too much redudant ready events from remote service, we may need reset again?
			if d.connState.readyEventReceived.Load() {
				log.Info("received ready signal from the same server again, ignore it",
					zap.Stringer("changefeedID", d.target.GetChangefeedID()),
					zap.Stringer("dispatcher", d.getDispatcherID()),
					zap.Stringer("eventServiceID", *event.From))
				return
			}
			log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.Stringer("eventServiceID", *event.From))
			d.connState.readyEventReceived.Store(true)
			d.commitReady(*event.From)
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
		log.Panic("should not happen: unknown signal event type", zap.Int("eventType", event.GetType()))
	}
}

func (d *dispatcherStat) handleDropEvent(event dispatcher.DispatcherEvent) {
	dropEvent, ok := event.Event.(*commonEvent.DropEvent)
	if !ok {
		log.Panic("drop event is not a drop event",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event))
	}

	if !d.isFromCurrentEpoch(event) {
		log.Debug("receive a drop event from a stale epoch, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event))
		return
	}

	log.Info("received a dropEvent, need to reset the dispatcher",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Uint64("commitTs", dropEvent.GetCommitTs()),
		zap.Uint64("sequence", dropEvent.GetSeq()),
		zap.Uint64("lastEventCommitTs", d.lastEventCommitTs.Load()))
	d.reset(d.connState.getEventServiceID())
	metrics.EventCollectorDroppedEventCount.Inc()
}

func (d *dispatcherStat) handleHandshakeEvent(event dispatcher.DispatcherEvent) {
	log.Info("handle handshake event",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Any("event", event))

	handshakeEvent, ok := event.Event.(*commonEvent.HandshakeEvent)
	if !ok {
		log.Panic("handshake event is not a handshake event", zap.Any("event", event))
	}
	if !d.isFromCurrentEpoch(event) {
		log.Info("receive a handshake event from a stale epoch, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event))
		return
	}
	tableInfo := handshakeEvent.TableInfo
	if tableInfo != nil {
		d.tableInfo.Store(tableInfo)
	}
	d.lastEventSeq.Store(handshakeEvent.Seq)
}

func (d *dispatcherStat) setRemoteCandidates(nodes []string) {
	if len(nodes) == 0 {
		return
	}
	if d.connState.trySetRemoteCandidates(nodes) {
		log.Info("set remote candidates",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcherID", d.getDispatcherID()),
			zap.Int64("tableID", d.target.GetTableSpan().TableID), zap.Strings("nodes", nodes))
		candidate := d.connState.getNextRemoteCandidate()
		d.registerTo(candidate)
	}
}

func (d *dispatcherStat) newDispatcherRegisterRequest(serverId string, onlyReuse bool) *messaging.DispatcherRequest {
	startTs := d.target.GetStartTs()
	syncPointInterval := d.target.GetSyncPointInterval()
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			StartTs:      startTs,
			// ServerId is the id of the request sender.
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

	// after reset during normal run time, we can filter reduduant syncpoint at event collector side
	// so we just take care of the case that resetTs is same as startTs
	skipSyncpointSameAsResetTs := false
	if resetTs == d.target.GetStartTs() {
		skipSyncpointSameAsResetTs = d.target.GetSkipSyncpointAtStartTs()
	}
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: d.target.GetChangefeedID().ToPB(),
			DispatcherId: d.target.GetId().ToPB(),
			TableSpan:    d.target.GetTableSpan(),
			StartTs:      resetTs,
			// ServerId is the id of the request sender.
			ServerId:          serverId,
			ActionType:        eventpb.ActionType_ACTION_TYPE_RESET,
			FilterConfig:      d.target.GetFilterConfig(),
			EnableSyncPoint:   d.target.EnableSyncPoint(),
			SyncPointInterval: uint64(syncPointInterval.Seconds()),
			SyncPointTs:       syncpoint.CalculateStartSyncPointTs(resetTs, syncPointInterval, skipSyncpointSameAsResetTs),
			// OnlyReuse:         false,
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
			// ServerId is the id of the request sender.
			ServerId:   serverId,
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
			Mode:       d.target.GetMode(),
		},
	}
}
