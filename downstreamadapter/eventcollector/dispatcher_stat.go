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
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type dispatcherEpochState struct {
	epoch uint64
	// lastEventSeq is the sequence number of the last received DML/DDL/Handshake
	// event in this epoch.
	lastEventSeq atomic.Uint64
	// maxEventTs tracks the largest ts that the event collector has safely
	// accepted in this epoch. Event service checkpoint heartbeats must not exceed
	// it, otherwise a sink-side checkpoint jump could make event service skip data
	// that has not reached the collector yet. Such checkpoint jumps are possible
	// because reset only changes the upstream epoch. Old in-flight events that
	// were already forwarded to the sink before reset may still finish
	// asynchronously and advance target.GetCheckpointTs() after reset/handshake,
	// even though the collector has not received the same progress from the new
	// epoch yet.
	maxEventTs atomic.Uint64
}

func newDispatcherEpochState(epoch uint64, lastEventSeq uint64, maxEventTs uint64) *dispatcherEpochState {
	state := &dispatcherEpochState{
		epoch: epoch,
	}
	state.lastEventSeq.Store(lastEventSeq)
	state.maxEventTs.Store(maxEventTs)
	return state
}

// dispatcherStat keeps per-dispatcher state used when handling events, including
// epoch, event sequence, heartbeat progress, commitTs filters, and cached table
// info. It uses dispatcherSession to manage the dispatcher's EventService
// connection lifecycle.
type dispatcherStat struct {
	target         dispatcher.DispatcherService
	eventCollector *EventCollector
	session        *dispatcherSession

	currentEpoch atomic.Pointer[dispatcherEpochState]
	// lastEventCommitTs is the commitTs of the last received DDL/DML/SyncPoint events.
	lastEventCommitTs atomic.Uint64
	// gotDDLOnTS indicates whether a DDL event was received at the sentCommitTs.
	gotDDLOnTs atomic.Bool
	// gotSyncpointOnTS indicates whether a sync point was received at the sentCommitTs.
	gotSyncpointOnTS atomic.Bool
	// tableInfo is the latest table info of the dispatcher's corresponding table.
	tableInfo atomic.Value
	// tableInfoVersion is the latest schema version delivered to this dispatcher.
	// It may advance even when tableInfo is not replaced.
	tableInfoVersion atomic.Uint64
}

func newDispatcherStat(
	target dispatcher.DispatcherService,
	eventCollector *EventCollector,
	readyCallback func(),
) *dispatcherStat {
	return newDispatcherStatInternal(
		target,
		eventCollector,
		eventCollector.getLocalServerID(),
		eventCollector.enqueueMessageForSend,
		readyCallback,
	)
}

func newDispatcherStatInternal(
	target dispatcher.DispatcherService,
	eventCollector *EventCollector,
	localServerID node.ID,
	sendMessage func(*messaging.TargetMessage),
	readyCallback func(),
) *dispatcherStat {
	stat := &dispatcherStat{
		target:         target,
		eventCollector: eventCollector,
	}
	stat.currentEpoch.Store(newDispatcherEpochState(0, 0, target.GetStartTs()))
	stat.lastEventCommitTs.Store(target.GetStartTs())
	stat.session = newDispatcherSession(
		target,
		localServerID,
		sendMessage,
		stat.advanceEpochForReset,
		readyCallback,
	)
	return stat
}

func (d *dispatcherStat) loadCurrentEpochState() *dispatcherEpochState {
	state := d.currentEpoch.Load()
	if state == nil {
		log.Panic("dispatcher epoch state is not initialized",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()))
	}
	return state
}

func (d *dispatcherStat) run() {
	d.session.startLocalRegistration()
}

func (d *dispatcherStat) startRemoteProbing(nodes []string) {
	d.session.startRemoteProbing(nodes)
}

func (d *dispatcherStat) advanceEpochForReset(resetTs uint64) uint64 {
	for {
		currentState := d.loadCurrentEpochState()
		nextState := newDispatcherEpochState(currentState.epoch+1, 0, resetTs)
		if d.currentEpoch.CompareAndSwap(currentState, nextState) {
			return nextState.epoch
		}
	}
}

// remove is used to remove the dispatcher from the event service.
func (d *dispatcherStat) remove() {
	d.session.remove()
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

func (d *dispatcherStat) verifyEventSequence(event dispatcher.DispatcherEvent, state *dispatcherEpochState) bool {
	// check the invariant that handshake event is the first event of every epoch
	if event.GetType() != commonEvent.TypeHandshakeEvent && state.lastEventSeq.Load() == 0 {
		log.Warn("receive non-handshake event before handshake event, reset the dispatcher",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event))
		return false
	}

	debugEnabled := logger.IsDebugEnabled()
	switch event.GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeDDLEvent,
		commonEvent.TypeHandshakeEvent,
		commonEvent.TypeSyncPointEvent,
		commonEvent.TypeResolvedEvent:
		if debugEnabled {
			log.Debug("check event sequence",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.String("eventType", commonEvent.TypeToString(event.GetType())),
				zap.Uint64("receivedSeq", event.GetSeq()),
				zap.Uint64("lastEventSeq", state.lastEventSeq.Load()),
				zap.Uint64("commitTs", event.GetCommitTs()))
		}

		lastEventSeq := state.lastEventSeq.Load()
		expectedSeq := uint64(0)

		// Resolved event's seq is the last concrete data event's seq.
		if event.GetType() == commonEvent.TypeResolvedEvent {
			expectedSeq = lastEventSeq
		} else {
			// Other events' seq is the next sequence number.
			expectedSeq = state.lastEventSeq.Add(1)
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
			if debugEnabled {
				log.Debug("check batch DML event sequence",
					zap.Stringer("changefeedID", d.target.GetChangefeedID()),
					zap.Stringer("dispatcher", d.getDispatcherID()),
					zap.Uint64("receivedSeq", e.Seq),
					zap.Uint64("lastEventSeq", state.lastEventSeq.Load()),
					zap.Uint64("commitTs", e.CommitTs))
			}

			expectedSeq := state.lastEventSeq.Add(1)
			if e.Seq != expectedSeq {
				log.Warn("receive an out-of-order batch DML event, reset the dispatcher",
					zap.Stringer("changefeedID", d.target.GetChangefeedID()),
					zap.Stringer("dispatcher", d.getDispatcherID()),
					zap.String("eventType", commonEvent.TypeToString(event.GetType())),
					zap.Uint64("lastEventSeq", state.lastEventSeq.Load()),
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

// shouldForwardEventByCommitTs verifies if the event's commit timestamp is valid.
// Note: this function must be called on every event received before forwarding it.
func (d *dispatcherStat) shouldForwardEventByCommitTs(event dispatcher.DispatcherEvent) bool {
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
			zap.Uint64("eventCommitTs", event.GetCommitTs()),
			zap.Uint64("sentCommitTs", d.lastEventCommitTs.Load()))
		return false
	}

	return true
}

func (d *dispatcherStat) observeCurrentEpochMaxEventTs(state *dispatcherEpochState, ts uint64) {
	util.MustCompareAndMonotonicIncrease(&state.maxEventTs, ts)
}

func (d *dispatcherStat) updateCommitTsStateByEvents(state *dispatcherEpochState, events []dispatcher.DispatcherEvent) {
	lastEventCommitTs := d.lastEventCommitTs.Load()
	gotDDLOnTs := d.gotDDLOnTs.Load()
	gotSyncpointOnTS := d.gotSyncpointOnTS.Load()

	for _, event := range events {
		d.observeCurrentEpochMaxEventTs(state, event.GetCommitTs())

		if event.GetCommitTs() > lastEventCommitTs {
			// if the commit ts is larger than the last sent commit ts,
			// we need to reset the DDL and SyncPoint flags.
			gotDDLOnTs = false
			gotSyncpointOnTS = false
		}

		switch event.GetType() {
		case commonEvent.TypeDDLEvent:
			gotDDLOnTs = true
		case commonEvent.TypeSyncPointEvent:
			gotSyncpointOnTS = true
		}

		switch event.GetType() {
		case commonEvent.TypeDDLEvent,
			commonEvent.TypeDMLEvent,
			commonEvent.TypeBatchDMLEvent,
			commonEvent.TypeSyncPointEvent:
			lastEventCommitTs = event.GetCommitTs()
		}
	}

	d.lastEventCommitTs.Store(lastEventCommitTs)
	d.gotDDLOnTs.Store(gotDDLOnTs)
	d.gotSyncpointOnTS.Store(gotSyncpointOnTS)
}

func (d *dispatcherStat) isFromCurrentEpoch(event dispatcher.DispatcherEvent, state *dispatcherEpochState) bool {
	if event.GetType() == commonEvent.TypeBatchDMLEvent {
		batchDML := event.Event.(*commonEvent.BatchDMLEvent)
		for _, dml := range batchDML.DMLEvents {
			if dml.GetEpoch() != state.epoch {
				return false
			}
		}
	}
	return event.GetEpoch() == state.epoch
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
	hasDML := false
	state := d.loadCurrentEpochState()
	for _, event := range events {
		if !d.isFromCurrentEpoch(event, state) {
			if logger.IsDebugEnabled() {
				log.Debug("receive DML/Resolved event from a stale epoch, ignore it",
					zap.Stringer("changefeedID", d.target.GetChangefeedID()),
					zap.Stringer("dispatcher", d.getDispatcherID()),
					zap.String("eventType", commonEvent.TypeToString(event.GetType())),
					zap.Any("event", event.Event))
			}
			continue
		}
		if !d.verifyEventSequence(event, state) {
			d.session.resetCurrentEventService()
			return false
		}
		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			validEvents = append(validEvents, event)
		case commonEvent.TypeDMLEvent:
			if d.shouldForwardEventByCommitTs(event) {
				hasDML = true
				validEvents = append(validEvents, event)
			}
		case commonEvent.TypeBatchDMLEvent:
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
				dml.TableInfoVersion = tableInfoVersion
				dmlEvent := dispatcher.NewDispatcherEvent(event.From, dml)
				if d.shouldForwardEventByCommitTs(dmlEvent) {
					hasDML = true
					validEvents = append(validEvents, dmlEvent)
				}
			}
		default:
			log.Panic("should not happen: unknown event type in batch data events",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcherID", d.getDispatcherID()),
				zap.String("eventType", commonEvent.TypeToString(event.GetType())))
		}
	}
	if len(validEvents) == 0 {
		return false
	}
	d.updateCommitTsStateByEvents(state, validEvents)
	handled := d.target.HandleEvents(validEvents, func() { d.wake() })
	if hasDML {
		failpoint.Inject("InjectResetDispatcherAfterBatchDataEvents", func() {
			log.Info("inject dispatcher reset after batch data events",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcherID", d.getDispatcherID()),
				zap.Uint64("checkpointTs", d.target.GetCheckpointTs()))
			d.session.resetCurrentEventService()
		})
	}
	return handled
}

// handleSingleDataEvents processes a single DDL or SyncPoint event with the following algorithm:
// 1. Validate event count (must be exactly 1)
// 2. Check if event comes from current epoch
// 3. Verify event sequence number
// 4. Process event based on type:
//   - DDL: Update table info if present
//   - SyncPoint: Forward directly
//
// 5. For all types: Filter by commitTs before forwarding
func (d *dispatcherStat) handleSingleDataEvents(events []dispatcher.DispatcherEvent) bool {
	if len(events) != 1 {
		log.Panic("should not happen: only one event should be sent for DDL/SyncPoint/Handshake event")
	}
	from := events[0].From
	state := d.loadCurrentEpochState()
	if !d.isFromCurrentEpoch(events[0], state) {
		log.Info("receive DDL/SyncPoint/Handshake event from a stale epoch, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.String("eventType", commonEvent.TypeToString(events[0].GetType())),
			zap.Uint64("eventEpoch", events[0].GetEpoch()),
			zap.Uint64("dispatcherEpoch", state.epoch),
			zap.Stringer("staleEventService", *from),
			zap.Stringer("currentEventService", d.session.getEventServiceID()))
		return false
	}
	if !d.verifyEventSequence(events[0], state) {
		d.session.resetCurrentEventService()
		return false
	}
	if !d.shouldForwardEventByCommitTs(events[0]) {
		return false
	}
	if events[0].GetType() == commonEvent.TypeDDLEvent {
		ddl := events[0].Event.(*commonEvent.DDLEvent)
		ddl, err := d.target.GetRouter().ApplyToDDLEvent(ddl)
		if err != nil {
			log.Error("failed to apply routing to DDL event",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.Error(err))
			if target, ok := d.target.(dispatcher.Dispatcher); ok {
				target.HandleError(err)
			}
			return false
		}
		events[0].Event = ddl
		d.updateTableInfoByDDL(ddl)
	}
	d.updateCommitTsStateByEvents(state, events)
	return d.target.HandleEvents(events, func() { d.wake() })
}

// updateTableInfoByDDL advances the table schema version and, when the DDL
// event carries a TableInfo matching the dispatcher's table, refreshes the
// cached TableInfo used for DML row assembly.
//
// Must be called from the per-dispatcher event loop (handleSingleDataEvents),
// which guarantees serial access to dispatcherStat fields for a given table.
func (d *dispatcherStat) updateTableInfoByDDL(ddl *commonEvent.DDLEvent) {
	tableSpan := d.target.GetTableSpan()
	if tableSpan == nil || tableSpan.TableID == common.DDLSpanTableID {
		return
	}

	// EXCHANGE PARTITION can change the schema version of a physical table dispatcher
	// while ddl.TableInfo carries another logical table. The storage sink uses
	// tableInfoVersion to decide whether a DML belongs to an old schema, so advance
	// it for every DDL delivered to this dispatcher.
	// TODO: Revisit whether the storage sink should discard DML solely by comparing
	// tableInfoVersion with existing schema files.
	d.tableInfoVersion.Store(ddl.FinishedTs)

	if ddl.TableInfo == nil {
		return
	}

	// A table dispatcher can receive DDLs unrelated to its own table for barrier
	// coordination, for example CREATE VIEW is tracked in every table's DDL history.
	// The cached table info is used to assemble subsequent DML rows. For partition
	// tables, the dispatcher span ID is a physical partition ID while TableInfo
	// carries the logical table ID, so compare with the cached table info first.
	expectedTableID := tableSpan.TableID
	current := d.tableInfo.Load()
	if current != nil {
		expectedTableID = current.(*common.TableInfo).TableName.TableID
	}
	if ddl.TableInfo.TableName.TableID != expectedTableID {
		return
	}

	d.tableInfo.Store(ddl.TableInfo)
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
	d.session.handleSignalEvent(event)
}

func (d *dispatcherStat) handleDropEvent(event dispatcher.DispatcherEvent) {
	dropEvent, ok := event.Event.(*commonEvent.DropEvent)
	if !ok {
		log.Panic("drop event is not a drop event",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event))
	}

	state := d.loadCurrentEpochState()
	if !d.isFromCurrentEpoch(event, state) {
		if logger.IsDebugEnabled() {
			log.Debug("receive a drop event from a stale epoch, ignore it",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.Any("event", event.Event))
		}
		return
	}

	log.Info("received a dropEvent, need to reset the dispatcher",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Uint64("commitTs", dropEvent.GetCommitTs()),
		zap.Uint64("sequence", dropEvent.GetSeq()),
		zap.Uint64("lastEventCommitTs", d.lastEventCommitTs.Load()))
	d.session.resetCurrentEventService()
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
	state := d.loadCurrentEpochState()
	if !d.isFromCurrentEpoch(event, state) {
		log.Info("receive a handshake event from a stale epoch, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event))
		return
	}
	if event.GetSeq() != 1 {
		log.Warn("should not happen: handshake event sequence number is not 1",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Uint64("sequence", event.GetSeq()))
		return
	}
	tableInfo := handshakeEvent.TableInfo
	if tableInfo != nil {
		tableInfo, err := d.target.GetRouter().ApplyToTableInfo(tableInfo)
		if err != nil {
			log.Error("failed to apply routing to handshake table info",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.Error(err))
			if target, ok := d.target.(dispatcher.Dispatcher); ok {
				target.HandleError(err)
			}
			return
		}
		d.tableInfo.Store(tableInfo)
	}
	state.lastEventSeq.Store(handshakeEvent.Seq)
	d.observeCurrentEpochMaxEventTs(state, handshakeEvent.GetCommitTs())
}

func (d *dispatcherStat) getHeartbeatReport() (node.ID, uint64, uint64, bool) {
	eventServiceID := d.session.getEventServiceID()
	if eventServiceID.IsEmpty() {
		return "", 0, 0, false
	}
	state := d.loadCurrentEpochState()
	checkpointTs := min(d.target.GetCheckpointTs(), state.maxEventTs.Load())
	return eventServiceID, checkpointTs, state.epoch, true
}
