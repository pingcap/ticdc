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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/span"
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
	blockedEvents      *BlockedEventMap         // tracks all block events that still wait for dispatcher progress
	pendingEvents      *pendingScheduleEventMap // pending DDL events that require scheduling order
	spanController     *span.Controller
	operatorController *operator.Controller
	splitTableEnabled  bool
	mode               int64
}

// NewBarrier create a new barrier for the changefeed
func NewBarrier(spanController *span.Controller,
	operatorController *operator.Controller,
	splitTableEnabled bool,
	bootstrapRespMap map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	mode int64,
) *Barrier {
	barrier := Barrier{
		blockedEvents:      NewBlockEventMap(),
		pendingEvents:      newPendingScheduleEventMap(),
		spanController:     spanController,
		operatorController: operatorController,
		splitTableEnabled:  splitTableEnabled,
		mode:               mode,
	}
	barrier.handleBootstrapResponse(bootstrapRespMap)
	return &barrier
}

// HandleStatus handle the block status from dispatcher manager
func (b *Barrier) HandleStatus(from node.ID,
	request *heartbeatpb.BlockStatusRequest,
) []*messaging.TargetMessage {
	log.Debug("handle block status", zap.String("from", from.String()),
		zap.String("changefeed", request.ChangefeedID.GetName()),
		zap.Any("detail", request), zap.Int64("mode", b.mode))
	eventDispatcherIDsMap := make(map[*BarrierEvent][]*heartbeatpb.DispatcherID)
	actions := map[node.ID][]*heartbeatpb.DispatcherStatus{}
	var dispatcherStatus []*heartbeatpb.DispatcherStatus
	for _, status := range request.BlockStatuses {
		// only receive block status from the replicating dispatcher
		dispatcherID := common.NewDispatcherIDFromPB(status.ID)
		if dispatcherID != b.spanController.GetDDLDispatcherID() {
			task := b.spanController.GetTaskByID(dispatcherID)
			if task == nil {
				log.Info("Get block status from unexisted dispatcher, ignore it", zap.String("changefeed", request.ChangefeedID.GetName()), zap.String("dispatcher", dispatcherID.String()), zap.Uint64("commitTs", status.State.BlockTs))
				continue
			} else {
				if !b.spanController.IsReplicating(task) {
					log.Info("Get block status from unreplicating dispatcher, ignore it", zap.String("changefeed", request.ChangefeedID.GetName()), zap.String("dispatcher", dispatcherID.String()), zap.Uint64("commitTs", status.State.BlockTs))
					continue
				}
			}
		}

		// deal with block status, and check whether need to return action.
		// we need to deal with the block status in order, otherwise scheduler may have problem
		// e.g. TODO（truncate + create table)
		event, action, targetID, needACK := b.handleOneStatus(request.ChangefeedID, status)
		if event == nil {
			// should not happen
			log.Error("handle block status failed, event is nil",
				zap.String("from", from.String()),
				zap.String("changefeed", request.ChangefeedID.GetName()),
				zap.String("detail", status.String()),
				zap.Uint64("commitTs", status.State.BlockTs))
			continue
		}
		if needACK {
			eventDispatcherIDsMap[event] = append(eventDispatcherIDsMap[event], status.ID)
			if action != nil && targetID != "" {
				actions[targetID] = append(actions[targetID], action)
			}
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
	dispatcherStatus = append(dispatcherStatus, actions[from]...)
	if len(dispatcherStatus) <= 0 {
		log.Warn("no dispatcher status to send",
			zap.String("from", from.String()),
			zap.String("changefeed", request.ChangefeedID.String()))
	}

	// send ack or write action message to dispatcher
	msg := messaging.NewSingleTargetMessage(from,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:       request.ChangefeedID,
			DispatcherStatuses: dispatcherStatus,
			Mode:               b.mode,
		})
	msgs := []*messaging.TargetMessage{msg}

	for id, action := range actions {
		if id != from && len(action) != 0 {
			msg := messaging.NewSingleTargetMessage(id,
				messaging.HeartbeatCollectorTopic,
				&heartbeatpb.HeartBeatResponse{
					ChangefeedID:       request.ChangefeedID,
					DispatcherStatuses: action,
					Mode:               b.mode,
				})
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

// handleBootstrapResponse rebuild the block event from the bootstrap response
func (b *Barrier) handleBootstrapResponse(bootstrapRespMap map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	for _, resp := range bootstrapRespMap {
		for _, span := range resp.Spans {
			if b.mode != span.Mode {
				continue
			}
			// we only care about the WAITING, WRITING and DONE stage
			if span.BlockState == nil || span.BlockState.Stage == heartbeatpb.BlockStage_NONE {
				continue
			}

			blockState := span.BlockState
			key := getEventKey(blockState.BlockTs, blockState.IsSyncPoint)
			event, ok := b.blockedEvents.Get(key)
			if !ok {
				event = NewBlockEvent(common.NewChangefeedIDFromPB(resp.ChangefeedID), common.NewDispatcherIDFromPB(span.ID), b.spanController, b.operatorController, blockState, b.splitTableEnabled)
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
	b.blockedEvents.Range(func(key eventKey, barrierEvent *BarrierEvent) bool {
		if barrierEvent.allDispatcherReported() {
			// it means the dispatchers involved in the block event are all in the cached resp, not restarted.
			// so we don't do speical check for this event
			// just use usual logic to handle it
			// Besides, is the dispatchers are all reported waiting status, it means at least one dispatcher
			// is not get acked, so it must be resent by dispatcher later.
			return true
		}
		barrierEvent.checkBlockedDispatchers()
		return true
	})
}

// Resend resends the message to the dispatcher manger, the pass action is handle here
func (b *Barrier) Resend() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage

	eventList := make([]*BarrierEvent, 0)
	b.blockedEvents.Range(func(key eventKey, barrierEvent *BarrierEvent) bool {
		// todo: we can limit the number of messages to send in one round here
		msgs = append(msgs, barrierEvent.resend(b.mode)...)

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

// ShouldBlockCheckpointTs returns ture if there is a block event need block the checkpoint ts forwarding
// currently, when the block event is a create table event, we should block the checkpoint ts forwarding
// because on the complete checkpointTs calculation should consider the new dispatcher.
func (b *Barrier) ShouldBlockCheckpointTs() bool {
	flag := false
	b.blockedEvents.RangeWoLock(func(key eventKey, barrierEvent *BarrierEvent) bool {
		if barrierEvent.hasNewTable {
			flag = true
			return false
		}
		return true
	})
	return flag
}

// GetMinBlockedCheckpointTsForNewTables returns the minimum checkpoint ts for the new tables
func (b *Barrier) GetMinBlockedCheckpointTsForNewTables(minCheckpointTs uint64) uint64 {
	b.blockedEvents.Range(func(key eventKey, barrierEvent *BarrierEvent) bool {
		if barrierEvent.hasNewTable && minCheckpointTs > barrierEvent.commitTs {
			minCheckpointTs = barrierEvent.commitTs
		}
		return true
	})
	return minCheckpointTs
}

func (b *Barrier) handleOneStatus(changefeedID *heartbeatpb.ChangefeedID, status *heartbeatpb.TableSpanBlockStatus) (*BarrierEvent, *heartbeatpb.DispatcherStatus, node.ID, bool) {
	cfID := common.NewChangefeedIDFromPB(changefeedID)
	dispatcherID := common.NewDispatcherIDFromPB(status.ID)

	// when a span send a block event, its checkpint must reached status.State.BlockTs - 1,
	// so here we forward the span's checkpoint ts to status.State.BlockTs - 1
	span := b.spanController.GetTaskByID(dispatcherID)
	if span != nil {
		span.UpdateStatus(&heartbeatpb.TableSpanStatus{
			ID:              status.ID,
			CheckpointTs:    status.State.BlockTs - 1,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			Mode:            status.Mode,
		})
		if status.State != nil {
			span.UpdateBlockState(*status.State)
		}
	}
	if status.State.Stage == heartbeatpb.BlockStage_DONE {
		return b.handleEventDone(cfID, dispatcherID, status), nil, "", true
	}
	return b.handleBlockState(cfID, dispatcherID, status)
}

func (b *Barrier) handleEventDone(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID, status *heartbeatpb.TableSpanBlockStatus) *BarrierEvent {
	key := getEventKey(status.State.BlockTs, status.State.IsSyncPoint)
	event, ok := b.blockedEvents.Get(key)
	if !ok {
		log.Debug("No block event found, ignore the event done message",
			zap.String("changefeed", changefeedID.Name()),
			zap.String("dispatcher", dispatcherID.String()),
			zap.Uint64("commitTs", status.State.BlockTs),
			zap.Any("state", status.State),
			zap.Bool("isSyncPoint", status.State.IsSyncPoint),
			zap.Int64("mode", b.mode),
		)
		return nil
	}

	// there is a block event and the dispatcher write or pass action already
	// which means we have sent pass or write action to it
	// the writer already synced ddl to downstream
	if event.writerDispatcher == dispatcherID {
		if event.needSchedule {
			// we need do schedule when writerDispatcherAdvanced
			// Otherwise, if we do schedule when just selected = true, then ask dispatcher execute ddl
			// when meeting truncate table,
			// there is possible that dml for the new table will arrive before truncate ddl executed.
			// that will lead to data loss
			scheduled := b.tryScheduleEvent(event)
			if !scheduled {
				// not scheduled yet, just return, wait for next resend
				return event
			}
		} else {
			// the pass action will be sent periodically in resend logic if not acked
			event.writerDispatcherAdvanced = true
			event.lastResendTime = time.Now().Add(-20 * time.Second)
		}
	}

	// checkpoint ts is advanced, clear the map, so do not need to resend message anymore
	event.markDispatcherEventDone(dispatcherID)
	b.checkEventFinish(event)
	return event
}

func (b *Barrier) handleBlockState(changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	status *heartbeatpb.TableSpanBlockStatus,
) (*BarrierEvent, *heartbeatpb.DispatcherStatus, node.ID, bool) {
	blockState := status.State
	if blockState.IsBlocked {
		key := getEventKey(blockState.BlockTs, blockState.IsSyncPoint)
		// insert an event, or get the old one event check if the event is already tracked
		event := b.getOrInsertNewEvent(changefeedID, dispatcherID, key, blockState)
		if dispatcherID == b.spanController.GetDDLDispatcherID() {
			log.Info("the block event is sent by ddl dispatcher",
				zap.String("changefeed", changefeedID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", blockState.BlockTs))
			event.tableTriggerDispatcherRelated = true
		}
		if event.selected.Load() {
			// the event already in the selected state, ignore the block event just sent ack
			log.Debug("the block event already selected, ignore the block event",
				zap.String("changefeed", changefeedID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", blockState.BlockTs),
			)
			// check whether the event can be finished.
			b.checkEventFinish(event)
			return event, nil, "", true
		}
		// the block event, and check whether we need to send write action
		event.markDispatcherEventDone(dispatcherID)
		status, targetID := event.checkEventAction(dispatcherID)
		if status != nil && event.needSchedule {
			// scheduling is only required for ddl that changes tables, enqueue the event
			b.pendingEvents.add(event)
		}
		return event, status, targetID, true
	}
	// it's not a blocked event, it must be sent by table event trigger event dispatcher, just for doing scheduler
	// and the ddl already synced to downstream , e.g.: create table
	// if ack failed, dispatcher will send a heartbeat again, so we do not need to care about resend message here
	//
	// Besides, we need to add the event into the blockedEvents map first, and then delete after finish scheduler
	// that make scheduleBlockEvent can calculate correctly.
	key := getEventKey(blockState.BlockTs, blockState.IsSyncPoint)
	event := b.getOrInsertNewEvent(changefeedID, dispatcherID, key, blockState)
	event.writerDispatcher = dispatcherID
	if !event.needSchedule {
		b.blockedEvents.Delete(getEventKey(event.commitTs, event.isSyncPoint))
		return event, nil, "", true
	}
	// enqueue ddl that needs scheduling so the table trigger dispatcher can process in order
	// otherwise the barrier may receive the first status of "recover table t_a" before it sees the
	// "truncate table t_a" done status when intermediate messages are lost, and the recover ddl would
	// be scheduled before truncate finishes, re-adding the table before drop completes and risking data loss.
	b.pendingEvents.add(event)
	scheduled := b.tryScheduleEvent(event)
	if !scheduled {
		b.blockedEvents.Delete(getEventKey(event.commitTs, event.isSyncPoint))
		return event, nil, "", false
	}
	b.blockedEvents.Delete(getEventKey(event.commitTs, event.isSyncPoint))
	return event, nil, "", true
}

// getOrInsertNewEvent get the block event from the map, if not found, create a new one
func (b *Barrier) getOrInsertNewEvent(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID,
	key eventKey, blockState *heartbeatpb.State,
) *BarrierEvent {
	event, ok := b.blockedEvents.Get(key)
	if !ok {
		event = NewBlockEvent(changefeedID, dispatcherID, b.spanController, b.operatorController, blockState, b.splitTableEnabled)
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
		log.Info("all dispatchers reported event done, remove event",
			zap.String("changefeed", be.cfID.Name()),
			zap.Uint64("committs", be.commitTs))
		// already selected a dispatcher to write, now all dispatchers reported the block event
		b.blockedEvents.Delete(getEventKey(be.commitTs, be.isSyncPoint))
	}
}

func (b *Barrier) tryScheduleEvent(event *BarrierEvent) bool {
	if !event.needSchedule {
		return true
	}
	log.Info("event trySchedule",
		zap.String("changefeed", event.cfID.Name()),
		zap.String("writerDispatcher", event.writerDispatcher.String()),
		zap.Uint64("EventCommitTs", event.commitTs))
	// pending queue ensures ddl with the same eventKey only schedules once and in order
	ready, candidate := b.pendingEvents.popIfHead(event)
	if !ready {
		if candidate == nil {
			log.Info("no candidate here, skip",
				zap.String("changefeed", event.cfID.Name()),
				zap.String("writerDispatcher", event.writerDispatcher.String()),
				zap.Uint64("EventCommitTs", event.commitTs),
				zap.Bool("isSyncPoint", event.isSyncPoint))
		} else {
			log.Info("event waits for a smaller commitTs before scheduling",
				zap.String("changefeed", event.cfID.Name()),
				zap.String("writerDispatcher", event.writerDispatcher.String()),
				zap.Uint64("EventCommitTs", event.commitTs),
				zap.Bool("isSyncPoint", event.isSyncPoint),
				zap.Uint64("blockingEventCommitTs", candidate.commitTs),
				zap.Bool("blockingEventIsSyncPoint", candidate.isSyncPoint))
		}
		return false
	}
	event.scheduleBlockEvent()
	event.writerDispatcherAdvanced = true
	event.lastResendTime = time.Now().Add(-20 * time.Second)
	return true
}

// ackEvent creates an ack event
func ackEvent(commitTs uint64, isSyncPoint bool) *heartbeatpb.ACK {
	return &heartbeatpb.ACK{
		CommitTs:    commitTs,
		IsSyncPoint: isSyncPoint,
	}
}
