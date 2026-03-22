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
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

type dispatcherDataState struct {
	epoch             atomic.Uint64
	lastEventSeq      atomic.Uint64
	lastEventCommitTs atomic.Uint64
	gotDDLOnTs        atomic.Bool
	gotSyncpointOnTS  atomic.Bool
	tableInfo         atomic.Value
	tableInfoVersion  atomic.Uint64
}

func (d *dispatcherStat) initDataState(startTs uint64) {
	d.lastEventSeq.Store(0)
	d.lastEventCommitTs.Store(startTs)
}

func (d *dispatcherStat) wake() {
	if common.IsRedoMode(d.target.GetMode()) {
		d.eventCollector.redoDs.Wake(d.getDispatcherID())
		return
	}
	d.eventCollector.ds.Wake(d.getDispatcherID())
}

func (d *dispatcherStat) getDispatcherID() common.DispatcherID {
	return d.target.GetId()
}

func (d *dispatcherStat) verifyEventSequence(event dispatcher.DispatcherEvent) bool {
	if event.GetType() != commonEvent.TypeHandshakeEvent && d.lastEventSeq.Load() == 0 {
		log.Warn("receive non-handshake event before handshake event, reset the dispatcher",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event))
		return false
	}

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
		expectedSeq := lastEventSeq
		if event.GetType() != commonEvent.TypeResolvedEvent {
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

func (d *dispatcherStat) filterAndUpdateEventByCommitTs(event dispatcher.DispatcherEvent) bool {
	shouldIgnore := false
	if event.GetCommitTs() < d.lastEventCommitTs.Load() {
		shouldIgnore = true
	} else if event.GetCommitTs() == d.lastEventCommitTs.Load() {
		switch event.GetType() {
		case commonEvent.TypeDDLEvent:
			shouldIgnore = d.gotDDLOnTs.Load()
		case commonEvent.TypeSyncPointEvent:
			shouldIgnore = d.gotSyncpointOnTS.Load()
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
	case commonEvent.TypeDDLEvent, commonEvent.TypeDMLEvent, commonEvent.TypeBatchDMLEvent, commonEvent.TypeSyncPointEvent:
		d.lastEventCommitTs.Store(event.GetCommitTs())
	}
	return true
}

func (d *dispatcherStat) isFromCurrentEpoch(event dispatcher.DispatcherEvent) bool {
	if event.GetType() == commonEvent.TypeBatchDMLEvent {
		for _, dml := range event.Event.(*commonEvent.BatchDMLEvent).DMLEvents {
			if dml.GetEpoch() != d.epoch.Load() {
				return false
			}
		}
	}
	return event.GetEpoch() == d.epoch.Load()
}

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
		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			validEvents = append(validEvents, event)
		case commonEvent.TypeDMLEvent:
			if d.filterAndUpdateEventByCommitTs(event) {
				validEvents = append(validEvents, event)
			}
		case commonEvent.TypeBatchDMLEvent:
			tableInfo := d.tableInfo.Load().(*common.TableInfo)
			if tableInfo == nil {
				log.Panic("should not happen: table info should be set before batch DML event",
					zap.Stringer("changefeedID", d.target.GetChangefeedID()),
					zap.Stringer("dispatcher", d.getDispatcherID()))
			}
			tableInfoVersion := max(d.tableInfoVersion.Load(), d.target.GetStartTs())
			batchDML := event.Event.(*commonEvent.BatchDMLEvent)
			batchDML.AssembleRows(tableInfo)
			for _, dml := range batchDML.DMLEvents {
				dml.TableInfo.InitPrivateFields()
				dml.TableInfoVersion = tableInfoVersion
				dmlEvent := dispatcher.NewDispatcherEvent(event.From, dml)
				if d.filterAndUpdateEventByCommitTs(dmlEvent) {
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
	return d.target.HandleEvents(validEvents, func() { d.wake() })
}

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
	if !d.filterAndUpdateEventByCommitTs(events[0]) {
		return false
	}
	if events[0].GetType() == commonEvent.TypeDDLEvent {
		ddl := events[0].Event.(*commonEvent.DDLEvent)
		d.tableInfoVersion.Store(ddl.FinishedTs)
		if ddl.TableInfo != nil {
			d.tableInfo.Store(ddl.TableInfo)
		}
	}
	return d.target.HandleEvents(events, func() { d.wake() })
}

func (d *dispatcherStat) handleDataEvents(events ...dispatcher.DispatcherEvent) bool {
	switch events[0].GetType() {
	case commonEvent.TypeDMLEvent, commonEvent.TypeResolvedEvent, commonEvent.TypeBatchDMLEvent:
		return d.handleBatchDataEvents(events)
	case commonEvent.TypeDDLEvent, commonEvent.TypeSyncPointEvent:
		return d.handleSingleDataEvents(events)
	default:
		log.Panic("should not happen: unknown event type", zap.Int("eventType", events[0].GetType()))
	}
	return false
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
