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

package maintainer

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/range_checker"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// Barrier manage the block events for the changefeed
// note: the dispatcher will guarantee the order of the block event.
// the block event processing logic:
// 1. dispatcher report an event to maintainer, like ddl, sync point
// 2. maintainer wait for all dispatchers reporting block event (all dispatchers must report the same event)
// 3. maintainer choose one dispatcher to write(tack an action) the event to downstream, (resend logic is needed)
// 4. maintainer wait for the selected dispatcher reporting event(write) done message (resend logic is needed)
// 5. maintainer send pass action to all other dispatchers. (resend logic is needed)
// 6. maintainer wait for all dispatchers reporting event(pass) done message
// 7. maintainer clear the event, and schedule block event? todo: what if we schedule first then wait for all dispatchers?
type Barrier struct {
	blockedEvents     *BlockedEventMap
	controller        *Controller
	splitTableEnabled bool
}

type BlockedEventMap struct {
	mutex sync.Mutex
	m     map[eventKey]*BarrierEvent
}

func NewBlockEventMap() *BlockedEventMap {
	return &BlockedEventMap{
		m: make(map[eventKey]*BarrierEvent),
	}
}

func (b *BlockedEventMap) Range(f func(key eventKey, value *BarrierEvent) bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for k, v := range b.m {
		if !f(k, v) {
			break
		}
	}
}

func (b *BlockedEventMap) Get(key eventKey) (*BarrierEvent, bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	event, ok := b.m[key]
	return event, ok
}

func (b *BlockedEventMap) Set(key eventKey, event *BarrierEvent) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.m[key] = event
}

func (b *BlockedEventMap) Delete(key eventKey) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	delete(b.m, key)
}

// eventKey is the key of the block event,
// the ddl and sync point are identified by the blockTs and isSyncPoint since they can share the same blockTs
type eventKey struct {
	blockTs     uint64
	isSyncPoint bool
}

// NewBarrier create a new barrier for the changefeed
func NewBarrier(controller *Controller, splitTableEnabled bool) *Barrier {
	return &Barrier{
		blockedEvents:     NewBlockEventMap(),
		controller:        controller,
		splitTableEnabled: splitTableEnabled,
	}
}

// HandleStatus handle the block status from dispatcher manager
func (b *Barrier) HandleStatus(from node.ID,
	request *heartbeatpb.BlockStatusRequest,
) *messaging.TargetMessage {
	log.Info("handle block status", zap.String("from", from.String()),
		zap.String("changefeed", request.ChangefeedID.GetName()),
		zap.Any("detail", request))
	eventDispatcherIDsMap := make(map[*BarrierEvent][]*heartbeatpb.DispatcherID)
	actions := []*heartbeatpb.DispatcherStatus{}
	var dispatcherStatus []*heartbeatpb.DispatcherStatus
	for _, status := range request.BlockStatuses {
		// deal with block status, and check whether need to return action.
		// we need to deal with the block status in order, otherwise scheduler may have problem
		// e.g. TODO（truncate + create table)
		event, action := b.handleOneStatus(request.ChangefeedID, status)
		if event == nil {
			// should not happen
			log.Error("handle block status failed, event is nil",
				zap.String("from", from.String()),
				zap.String("changefeed", request.ChangefeedID.GetName()),
				zap.String("detail", status.String()))
			continue
		}
		eventDispatcherIDsMap[event] = append(eventDispatcherIDsMap[event], status.ID)
		if action != nil {
			actions = append(actions, action)
		}
	}
	for event, dispatchers := range eventDispatcherIDsMap {
		dispatcherStatus = append(dispatcherStatus, &heartbeatpb.DispatcherStatus{
			InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
				InfluenceType: heartbeatpb.InfluenceType_Normal,
				DispatcherIDs: dispatchers,
			},
			Ack: ackEvent(event.commitTs, event.isSyncPoint),
		})
	}
	for action := range actions {
		dispatcherStatus = append(dispatcherStatus, actions[action])
	}

	if len(dispatcherStatus) <= 0 {
		log.Warn("no dispatcher status to send",
			zap.String("from", from.String()),
			zap.String("changefeed", request.ChangefeedID.String()))
		return nil
	}
	// send ack or write action message to dispatcher
	return messaging.NewSingleTargetMessage(from,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:       request.ChangefeedID,
			DispatcherStatuses: dispatcherStatus,
		})
}

// HandleBootstrapResponse rebuild the block event from the bootstrap response
func (b *Barrier) HandleBootstrapResponse(bootstrapRespMap map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	for _, resp := range bootstrapRespMap {
		for _, span := range resp.Spans {
			// we only care about the WAITING, WRITING and DONE stage
			if span.BlockState == nil || span.BlockState.Stage == heartbeatpb.BlockStage_NONE {
				continue
			}

			blockState := span.BlockState
			key := getEventKey(blockState.BlockTs, blockState.IsSyncPoint)
			event, ok := b.blockedEvents.Get(key)
			if !ok {
				event = NewBlockEvent(common.NewChangefeedIDFromPB(resp.ChangefeedID), common.NewDispatcherIDFromPB(span.ID), b.controller, blockState, b.splitTableEnabled)
				b.blockedEvents.Set(key, event)
			}
			switch blockState.Stage {
			case heartbeatpb.BlockStage_WAITING:
				// it's the dispatcher's responsibility to resend the block event
			case heartbeatpb.BlockStage_WRITING:
				// it's in writing stage, must be the writer dispatcher
				// it's the maintainer's responsibility to resend the write action
				event.selected.Store(true)
				event.writerDispatcher = common.NewDispatcherIDFromPB(span.ID)
			case heartbeatpb.BlockStage_DONE:
				// it's the maintainer's responsibility to resend the pass action
				event.selected.Store(true)
				event.writerDispatcherAdvanced = true
			}
			event.markDispatcherEventDone(common.NewDispatcherIDFromPB(span.ID))
		}
	}
	// Here we iter the block event, to check each whether each blockTable each the target state.
	//
	// Because the maintainer is restarted, some dispatcher may finish push forward the ddl state
	// For example, a rename table1 ddl, which block the NodeA's table trigger, and NodeB's table1,
	// and sink is mysql-class.
	// If NodeA offline when it just write the rename ddl, but not report to the maintainer.
	// Then the maintainer will be restarted, and due to the ddl is finished into mysql,
	// from ddl-ts, we can find the table trigger event dispatcher is reach the ddl's commit,
	// so it will not block by the ddl, and can continue to handle the following events.
	// While for the table1 in NodeB, it's still wait the pass action.
	// So we need to check the block event when the maintainer is restarted to help block event decide its state.
	// TODO:double check the logic here
	b.blockedEvents.Range(func(key eventKey, barrierEvent *BarrierEvent) bool {
		if barrierEvent.allDispatcherReported() {
			// it means the dispatchers involved in the block event are all in the cached resp, not restarted.
			// so we don't do speical check for this event
			// just use usual logic to handle it
			// Besides, is the dispatchers are all reported waiting status, it means at least one dispatcher
			// is not get acked, so it must be resend by dispatcher later.
			return true
		}
		switch barrierEvent.blockedDispatchers.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			for _, tableId := range barrierEvent.blockedDispatchers.TableIDs {
				replications := b.controller.replicationDB.GetTasksByTableID(tableId)
				for _, replication := range replications {
					if replication.GetStatus().CheckpointTs >= barrierEvent.commitTs {
						barrierEvent.rangeChecker.AddSubRange(replication.Span.TableID, replication.Span.StartKey, replication.Span.EndKey)
					}
				}
			}
		case heartbeatpb.InfluenceType_DB:
			schemaID := barrierEvent.blockedDispatchers.SchemaID
			replications := b.controller.replicationDB.GetTasksBySchemaID(schemaID)
			for _, replication := range replications {
				if replication.GetStatus().CheckpointTs >= barrierEvent.commitTs {
					barrierEvent.rangeChecker.AddSubRange(replication.Span.TableID, replication.Span.StartKey, replication.Span.EndKey)
				}
			}
		case heartbeatpb.InfluenceType_All:
			replications := b.controller.replicationDB.GetAllTasks()
			for _, replication := range replications {
				if replication.GetStatus().CheckpointTs >= barrierEvent.commitTs {
					barrierEvent.rangeChecker.AddSubRange(replication.Span.TableID, replication.Span.StartKey, replication.Span.EndKey)
				}
			}
		}
		// meet the target state(which means the ddl is writen), we need to send pass actions in resend
		if barrierEvent.allDispatcherReported() {
			barrierEvent.selected.Store(true)
			barrierEvent.writerDispatcherAdvanced = true
		}
		return true
	})
}

// Resend resends the message to the dispatcher manger, the pass action is handle here
func (b *Barrier) Resend() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage

	eventList := make([]*BarrierEvent, 0)
	b.blockedEvents.Range(func(key eventKey, barrierEvent *BarrierEvent) bool {
		// todo: we can limit the number of messages to send in one round here
		msgs = append(msgs, barrierEvent.resend()...)

		eventList = append(eventList, barrierEvent)
		return true
	})

	for _, event := range eventList {
		if event != nil {
			// check the event is finished or not
			b.checkEventFinish(event)
		}
	}
	return msgs
}

// ShouldBlockCheckpointTs returns ture there is a block event need block the checkpoint ts forwarding
// currently, when the block event is a create table event, we should block the checkpoint ts forwarding
// because on the
func (b *Barrier) ShouldBlockCheckpointTs() bool {
	flag := false
	b.blockedEvents.Range(func(key eventKey, barrierEvent *BarrierEvent) bool {
		if barrierEvent.hasNewTable {
			flag = true
			return false
		}
		return true
	})
	return flag
}

func (b *Barrier) handleOneStatus(changefeedID *heartbeatpb.ChangefeedID, status *heartbeatpb.TableSpanBlockStatus) (*BarrierEvent, *heartbeatpb.DispatcherStatus) {
	cfID := common.NewChangefeedIDFromPB(changefeedID)
	dispatcherID := common.NewDispatcherIDFromPB(status.ID)

	// when a span send a block event, its checkpint must reached status.State.BlockTs - 1,
	// so here we forward the span's checkpoint ts to status.State.BlockTs - 1
	span := b.controller.GetTask(dispatcherID)
	if span != nil {
		span.UpdateStatus(&heartbeatpb.TableSpanStatus{
			ID:              status.ID,
			CheckpointTs:    status.State.BlockTs - 1,
			ComponentStatus: heartbeatpb.ComponentState_Working,
		})
		if status.State != nil {
			span.UpdateBlockState(*status.State)
		}
	}
	if status.State.Stage == heartbeatpb.BlockStage_DONE {
		return b.handleEventDone(cfID, dispatcherID, status), nil
	}
	return b.handleBlockState(cfID, dispatcherID, status)
}

func (b *Barrier) handleEventDone(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID, status *heartbeatpb.TableSpanBlockStatus) *BarrierEvent {
	key := getEventKey(status.State.BlockTs, status.State.IsSyncPoint)
	event, ok := b.blockedEvents.Get(key)
	if !ok {
		// no block event found
		be := NewBlockEvent(changefeedID, dispatcherID, b.controller, status.State, b.splitTableEnabled)
		// the event is a fake event, the dispatcher will not send the block event
		be.rangeChecker = range_checker.NewBoolRangeChecker(false)
		return be
	}

	// there is a block event and the dispatcher write or pass action already
	// which means we have sent pass or write action to it
	// the writer already synced ddl to downstream
	if event.writerDispatcher == dispatcherID {
		// the pass action will be sent periodically in resend logic if not acked
		// todo: schedule the block event here?
		event.writerDispatcherAdvanced = true
	}

	// checkpoint ts is advanced, clear the map, so do not need to resend message anymore
	event.markDispatcherEventDone(dispatcherID)
	b.checkEventFinish(event)
	return event
}

func (b *Barrier) handleBlockState(changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	status *heartbeatpb.TableSpanBlockStatus,
) (*BarrierEvent, *heartbeatpb.DispatcherStatus) {
	blockState := status.State
	if blockState.IsBlocked {
		key := getEventKey(blockState.BlockTs, blockState.IsSyncPoint)
		// insert an event, or get the old one event check if the event is already tracked
		event := b.getOrInsertNewEvent(changefeedID, dispatcherID, key, blockState)
		if dispatcherID == b.controller.ddlDispatcherID {
			log.Info("the block event is sent by ddl dispatcher",
				zap.String("changefeed", changefeedID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", blockState.BlockTs))
			event.tableTriggerDispatcherRelated = true
		}
		if event.selected.Load() {
			// the event already in the selected state, ignore the block event just sent ack
			log.Warn("the block event already selected, ignore the block event",
				zap.String("changefeed", changefeedID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", blockState.BlockTs),
			)
			// check whether the event can be finished.
			b.checkEventFinish(event)
			return event, nil
		}
		// the block event, and check whether we need to send write action
		event.markDispatcherEventDone(dispatcherID)
		return event, event.checkEventAction(dispatcherID)
	}
	// it's not a blocked event, it must be sent by table event trigger dispatcher, just for doing scheduler
	// and the ddl already synced to downstream , e.g.: create table
	// if ack failed, dispatcher will send a heartbeat again, so we do not need to care about resend message here
	event := NewBlockEvent(changefeedID, dispatcherID, b.controller, blockState, b.splitTableEnabled)
	event.scheduleBlockEvent()
	return event, nil
}

// getOrInsertNewEvent get the block event from the map, if not found, create a new one
func (b *Barrier) getOrInsertNewEvent(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID,
	key eventKey, blockState *heartbeatpb.State,
) *BarrierEvent {
	event, ok := b.blockedEvents.Get(key)
	if !ok {
		event = NewBlockEvent(changefeedID, dispatcherID, b.controller, blockState, b.splitTableEnabled)
		b.blockedEvents.Set(key, event)
	}
	return event
}

// check whether the event is get all the done message from dispatchers
// if so, remove the event from blockedTs, not need to resend message anymore
func (b *Barrier) checkEventFinish(be *BarrierEvent) {
	if !be.allDispatcherReported() {
		return
	}
	if be.selected.Load() {
		log.Info("the all dispatchers reported event done, remove event",
			zap.String("changefeed", be.cfID.Name()),
			zap.Uint64("committs", be.commitTs))
		// already selected a dispatcher to write, now all dispatchers reported the block event
		b.blockedEvents.Delete(getEventKey(be.commitTs, be.isSyncPoint))
	}
}

// ackEvent creates an ack event
func ackEvent(commitTs uint64, isSyncPoint bool) *heartbeatpb.ACK {
	return &heartbeatpb.ACK{
		CommitTs:    commitTs,
		IsSyncPoint: isSyncPoint,
	}
}

// getEventKey returns the key of the block event
func getEventKey(blockTs uint64, isSyncPoint bool) eventKey {
	return eventKey{
		blockTs:     blockTs,
		isSyncPoint: isSyncPoint,
	}
}
