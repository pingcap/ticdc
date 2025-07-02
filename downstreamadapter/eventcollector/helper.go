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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

func NewEventDynamicStream(collector *EventCollector) dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler] {
	option := dynstream.NewOption()
	option.BatchCount = 67136
	option.UseBuffer = false
	// Enable memory control for dispatcher events dynamic stream.
	option.EnableMemoryControl = true
	if option.EnableMemoryControl {
		log.Info("New EventDynamicStream, memory control is enabled")
	} else {
		log.Info("New EventDynamicStream, memory control is disabled")
	}

	eventsHandler := &EventsHandler{}
	stream := dynstream.NewParallelDynamicStream(func(id common.DispatcherID) uint64 { return (common.GID)(id).FastHash() }, eventsHandler, option)
	stream.Start()
	return stream
}

// EventsHandler is used to dispatch the received events.
// If the event is a DML event, it will be added to the sink for writing to downstream.
// If the event is a resolved TS event, it will be update the resolvedTs of the dispatcher.
// If the event is a DDL event,
//  1. If it is a single table DDL, it will be added to the sink for writing to downstream(async).
//  2. If it is a multi-table DDL, We will generate a TableSpanBlockStatus message with ddl info to send to maintainer.
//     for the multi-table DDL, we will also generate a ResendTask to resend the TableSpanBlockStatus message with ddl info
//     to maintainer each 200ms to avoid message is missing.
//
// If the event is a Sync Point event, we deal it as a multi-table DDL event.
//
// We can handle multi events in batch if there only dml events and resovledTs events.
// For DDL event and Sync Point Event, we should handle them singlely.
// Thus, if a event is DDL event or Sync Point Event, we will only get one event at once.
// Otherwise, we can get a batch events.
// We always return block = true for Handle() except we only receive the resolvedTs events.
// So we only will reach next Handle() when previous events are all push downstream successfully.
type EventsHandler struct{}

func (h *EventsHandler) Path(event dispatcher.DispatcherEvent) common.DispatcherID {
	return event.GetDispatcherID()
}

// Invariant: at any times, we can receive events from at most two event service, and one of them must be local event service.
func (h *EventsHandler) Handle(stat *dispatcherStat, events ...dispatcher.DispatcherEvent) bool {
	log.Debug("handle events", zap.Any("dispatcher", stat.target.GetId()))
	if len(events) == 0 {
		return false
	}

	// log.Info("fizz handle events", zap.Any("events", events))
	// Only check the first event type, because all events in the same batch should be in the same type group.
	switch events[0].GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeResolvedEvent,
		commonEvent.TypeDDLEvent,
		commonEvent.TypeSyncPointEvent,
		commonEvent.TypeBatchDMLEvent:
		return stat.handleDataEvents(events...)
	case commonEvent.TypeReadyEvent,
		commonEvent.TypeNotReusableEvent:
		if len(events) > 1 {
			log.Panic("unexpected multiple events for TypeReadyEvent or TypeNotReusableEvent",
				zap.Int("count", len(events)))
		}
		stat.handleSignalEvent(events[0])
		return false
	case commonEvent.TypeDropEvent:
		if len(events) > 1 {
			log.Panic("unexpected multiple events for TypeDropEvent",
				zap.Int("count", len(events)))
		}
		stat.handleDropEvent(events[0])
		return false
	case commonEvent.TypeHandshakeEvent:
		if len(events) > 1 {
			log.Panic("unexpected multiple events for TypeHandshakeEvent",
				zap.Int("count", len(events)))
		}
		stat.handleHandshakeEvent(events[0])
		return false
	default:
		log.Panic("unknown event type", zap.Int("type", int(events[0].GetType())))
	}
	return false
}

const (
	// We ensure the resolvedTs and dml events can be in the same batch
	// To avoid the dml batch to be too small with frequent resolvedTs events.
	// If the dml event batch is too small, the sink may not be able to batch enough, leading to the performance degradation.
	DataGroupResolvedTsOrDML = iota + 1
	DataGroupDDL
	DataGroupSyncPoint
	DataGroupHandshake
	DataGroupReady
	DataGroupNotReusable
	DataGroupDrop
)

func (h *EventsHandler) GetType(event dispatcher.DispatcherEvent) dynstream.EventType {
	switch event.GetType() {
	case commonEvent.TypeResolvedEvent:
		return dynstream.EventType{DataGroup: DataGroupResolvedTsOrDML, Property: dynstream.PeriodicSignal, Droppable: true}
	case commonEvent.TypeDMLEvent:
		return dynstream.EventType{DataGroup: DataGroupResolvedTsOrDML, Property: dynstream.BatchableData, Droppable: true}
	case commonEvent.TypeBatchDMLEvent:
		return dynstream.EventType{DataGroup: DataGroupResolvedTsOrDML, Property: dynstream.BatchableData, Droppable: true}
	case commonEvent.TypeDDLEvent:
		return dynstream.EventType{DataGroup: DataGroupDDL, Property: dynstream.NonBatchable, Droppable: true}
	case commonEvent.TypeSyncPointEvent:
		return dynstream.EventType{DataGroup: DataGroupSyncPoint, Property: dynstream.NonBatchable, Droppable: true}
	case commonEvent.TypeHandshakeEvent:
		return dynstream.EventType{DataGroup: DataGroupHandshake, Property: dynstream.NonBatchable, Droppable: false}
	case commonEvent.TypeReadyEvent:
		return dynstream.EventType{DataGroup: DataGroupReady, Property: dynstream.NonBatchable, Droppable: false}
	case commonEvent.TypeNotReusableEvent:
		return dynstream.EventType{DataGroup: DataGroupNotReusable, Property: dynstream.NonBatchable, Droppable: false}
	case commonEvent.TypeDropEvent:
		return dynstream.EventType{DataGroup: DataGroupDrop, Property: dynstream.NonBatchable, Droppable: false}
	default:
		log.Panic("unknown event type", zap.Int("type", int(event.GetType())))
	}
	return dynstream.DefaultEventType
}

func (h *EventsHandler) GetSize(event dispatcher.DispatcherEvent) int { return int(event.GetSize()) }

func (h *EventsHandler) IsPaused(event dispatcher.DispatcherEvent) bool { return event.IsPaused() }

func (h *EventsHandler) GetArea(path common.DispatcherID, dest *dispatcherStat) common.GID {
	return dest.target.GetChangefeedID().ID()
}

func (h *EventsHandler) GetTimestamp(event dispatcher.DispatcherEvent) dynstream.Timestamp {
	return dynstream.Timestamp(event.GetCommitTs())
}

func (h *EventsHandler) OnDrop(event dispatcher.DispatcherEvent) interface{} {
	switch event.GetType() {
	case commonEvent.TypeDMLEvent, commonEvent.TypeHandshakeEvent, commonEvent.TypeDDLEvent:
		log.Info("Drop event", zap.String("dispatcher", event.GetDispatcherID().String()), zap.Any("event", event))
		dropEvent := commonEvent.NewDropEvent(event.GetDispatcherID(), event.GetSeq(), event.GetEpoch(), event.GetCommitTs())
		return dispatcher.NewDispatcherEvent(event.From, dropEvent)
	}
	return nil
}
