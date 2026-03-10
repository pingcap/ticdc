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
	flushEnabled       bool
	mode               int64
}

// NewBarrier create a new barrier for the changefeed
func NewBarrier(spanController *span.Controller,
	operatorController *operator.Controller,
	splitTableEnabled bool,
	bootstrapRespMap map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	mode int64,
) *Barrier {
	return NewBarrierWithFlush(spanController, operatorController, splitTableEnabled, true, bootstrapRespMap, mode)
}

// NewBarrierWithFlush creates a barrier with an explicit flush phase switch.
// flushEnabled must be decided before restoring bootstrap events so restored
// events use the right phase machine from the beginning.
func NewBarrierWithFlush(spanController *span.Controller,
	operatorController *operator.Controller,
	splitTableEnabled bool,
	flushEnabled bool,
	bootstrapRespMap map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	mode int64,
) *Barrier {
	barrier := Barrier{
		blockedEvents:      NewBlockEventMap(),
		pendingEvents:      newPendingScheduleEventMap(),
		spanController:     spanController,
		operatorController: operatorController,
		splitTableEnabled:  splitTableEnabled,
		flushEnabled:       flushEnabled,
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
				log.Info("Get block status from unexisted dispatcher, ignore it", zap.String("changefeed", request.ChangefeedID.GetName()), zap.String("dispatcher", dispatcherID.String()), zap.Uint64("commitTs", status.State.BlockTs), zap.Int64("mode", b.mode))
				continue
			} else {
				if !b.spanController.IsReplicating(task) {
					log.Info("Get block status from unreplicating dispatcher, ignore it", zap.String("changefeed", request.ChangefeedID.GetName()), zap.String("dispatcher", dispatcherID.String()), zap.Uint64("commitTs", status.State.BlockTs), zap.Int64("mode", b.mode))
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
				zap.Uint64("commitTs", status.State.BlockTs),
				zap.Int64("mode", b.mode))
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
			zap.String("changefeed", request.ChangefeedID.String()),
			zap.Int64("mode", b.mode))
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
				event = NewBlockEvent(common.NewChangefeedIDFromPB(resp.ChangefeedID), common.NewDispatcherIDFromPB(span.ID), b.spanController, b.operatorController, blockState, b.splitTableEnabled, b.mode)
				event.flushEnabled = b.flushEnabled
				event.flushDispatcherAdvanced = !b.flushEnabled
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
	b.blockedEvents.RangeWithoutLock(func(key eventKey, barrierEvent *BarrierEvent) bool {
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

	// We should only see DONE after the event has been selected.
	if !event.selected.Load() {
		return event
	}

	// Phase 1 (Flush, storage split-table only): all influenced dispatchers must flush pre-barrier DML first.
	// This prevents pre-DDL data from overtaking the barrier write when a table is
	// split into multiple dispatchers across nodes.
	// DONE from all influenced dispatchers advances flushDispatcherAdvanced = true.
	if !event.flushDispatcherAdvanced {
		event.markDispatcherEventDone(dispatcherID)
		if !event.allDispatcherReported() {
			return event
		}

		event.flushDispatcherAdvanced = true
		event.rangeChecker.Reset()
		event.reportedDispatchers = make(map[common.DispatcherID]struct{})
		event.lastResendTime = time.Now().Add(-20 * time.Second)
		return event
	}

	// Phase 2 (Write): only writerDispatcher executes Action_Write.
	// DONE from writerDispatcher advances writerDispatcherAdvanced = true.
	if !event.writerDispatcherAdvanced {
		if event.writerDispatcher != dispatcherID {
			// Ignore stale DONE from non-writer dispatchers while waiting writer Action_Write.
			return event
		}

		if event.needSchedule {
			// We should schedule only after writer finished Action_Write.
			// Otherwise truncate/create like ddl may expose new table dml before old table cleanup.
			scheduled := b.tryScheduleEvent(event)
			if !scheduled {
				// Not scheduled yet, keep waiting and resend later.
				return event
			}
		} else {
			event.writerDispatcherAdvanced = true
			event.lastResendTime = time.Now().Add(-20 * time.Second)
		}

		// Start Phase 3 from a clean coverage window.
		event.rangeChecker.Reset()
		event.reportedDispatchers = make(map[common.DispatcherID]struct{})
		event.lastResendTime = time.Now().Add(-20 * time.Second)
		return event
	}

	// Phase 3 (Pass): all influenced dispatchers report DONE for Action_Pass,
	// then checkEventFinish removes the barrier from blockedEvents.
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
				zap.Uint64("commitTs", blockState.BlockTs),
				zap.Int64("mode", b.mode))
			event.tableTriggerDispatcherRelated = true
		}
		if event.selected.Load() {
			// the event already in the selected state, ignore the block event just sent ack
			log.Debug("the block event already selected, ignore the block event",
				zap.String("changefeed", changefeedID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", blockState.BlockTs),
				zap.Int64("mode", b.mode),
			)
			// check whether the event can be finished.
			b.checkEventFinish(event)
			return event, nil, "", true
		}

		// For DB/All block events (including syncpoint), maintainer creates the range checker based on
		// spanController's current task snapshot when it receives the table trigger dispatcher's report
		// (see BarrierEvent.markDispatcherEventDone -> createRangeCheckerForTypeDB/All).
		//
		// If there are pending schedule-required events (e.g. TRUNCATE TABLE) that have finished writing
		// but not finished scheduling yet, this snapshot may miss newly added tables and lead to an
		// incomplete range checker, allowing the DB/All event to advance too early.
		//
		// To avoid this race, when the table trigger dispatcher reports a DB/All block event and there
		// are still pending schedule-required events, we intentionally do NOT ack or act on this report.
		// The table trigger dispatcher will resend the block status later, after scheduling is done.
		if dispatcherID == b.spanController.GetDDLDispatcherID() &&
			blockState.BlockTables != nil &&
			(blockState.BlockTables.InfluenceType != heartbeatpb.InfluenceType_Normal) {
			if pending := b.pendingEvents.Len(); pending > 0 {
				log.Debug("discard db/all block event from ddl dispatcher due to pending schedule events, wait next resend",
					zap.String("changefeed", changefeedID.Name()),
					zap.String("dispatcher", dispatcherID.String()),
					zap.Uint64("commitTs", blockState.BlockTs),
					zap.Bool("isSyncPoint", blockState.IsSyncPoint),
					zap.Int("pendingScheduleEvents", pending),
					zap.Int64("mode", b.mode))
				return event, nil, "", false
			}
		}

		// the block event, and check whether we need to send write action
		event.markDispatcherEventDone(dispatcherID)
		status, targetID := event.checkEventAction(dispatcherID)
		if event.selected.Load() && event.needSchedule {
			// scheduling is only required for ddl that changes tables. enqueue once the
			// barrier is selected, regardless of whether the action is sent immediately.
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
		event = NewBlockEvent(changefeedID, dispatcherID, b.spanController, b.operatorController, blockState, b.splitTableEnabled, b.mode)
		event.flushEnabled = b.flushEnabled
		event.flushDispatcherAdvanced = !b.flushEnabled
		b.blockedEvents.Set(key, event)
	}
	return event
}

// check whether the event is get all the done message from dispatchers
// if so, remove the event from blockedTs, not need to resend message anymore
func (b *Barrier) checkEventFinish(be *BarrierEvent) {
	if !be.selected.Load() {
		return
	}
	if !be.flushDispatcherAdvanced || !be.writerDispatcherAdvanced {
		return
	}
	if !be.allDispatcherReported() {
		return
	}

	log.Info("all dispatchers reported event done, remove event",
		zap.String("changefeed", be.cfID.Name()),
		zap.Uint64("committs", be.commitTs),
		zap.Int64("mode", b.mode))
	b.blockedEvents.Delete(getEventKey(be.commitTs, be.isSyncPoint))
}

func (b *Barrier) tryScheduleEvent(event *BarrierEvent) bool {
	if !event.needSchedule {
		return true
	}
	log.Info("event trySchedule",
		zap.String("changefeed", event.cfID.Name()),
		zap.String("writerDispatcher", event.writerDispatcher.String()),
		zap.Uint64("EventCommitTs", event.commitTs),
		zap.Int64("mode", b.mode))
	// pending queue ensures ddl with the same eventKey only schedules once and in order
	ready, candidate := b.pendingEvents.popIfHead(event)
	if !ready {
		if candidate == nil {
			log.Info("no candidate here, skip",
				zap.String("changefeed", event.cfID.Name()),
				zap.String("writerDispatcher", event.writerDispatcher.String()),
				zap.Uint64("EventCommitTs", event.commitTs),
				zap.Bool("isSyncPoint", event.isSyncPoint),
				zap.Int64("mode", b.mode))
		} else {
			log.Info("event waits for a smaller commitTs before scheduling",
				zap.String("changefeed", event.cfID.Name()),
				zap.String("writerDispatcher", event.writerDispatcher.String()),
				zap.Uint64("EventCommitTs", event.commitTs),
				zap.Bool("isSyncPoint", event.isSyncPoint),
				zap.Uint64("blockingEventCommitTs", candidate.commitTs),
				zap.Bool("blockingEventIsSyncPoint", candidate.isSyncPoint),
				zap.Int64("mode", b.mode))
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
