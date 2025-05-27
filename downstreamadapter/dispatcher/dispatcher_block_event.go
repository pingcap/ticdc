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

package dispatcher

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

// =================== functions to handle block event ===================

// HandleDispatcherStatus is used to handle the dispatcher status from the Maintainer to deal with the block event.
// Each dispatcher status may contain an ACK info or a dispatcher action or both.
// If we get an ack info, we need to check whether the ack is for the ddl event in resend task map. If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event. If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream.
// 2. If the action is a pass, we just need to pass the event
func (d *Dispatcher) HandleDispatcherStatus(dispatcherStatus *heartbeatpb.DispatcherStatus) {
	log.Info("dispatcher handle dispatcher status",
		zap.Any("dispatcherStatus", dispatcherStatus),
		zap.Stringer("dispatcher", d.id),
		zap.Any("action", dispatcherStatus.GetAction()),
		zap.Any("ack", dispatcherStatus.GetAck()))
	// Step1: deal with the ack info
	ack := dispatcherStatus.GetAck()
	if ack != nil {
		identifier := BlockEventIdentifier{
			CommitTs:    ack.CommitTs,
			IsSyncPoint: ack.IsSyncPoint,
		}
		d.cancelResendTask(identifier)
	}

	// Step2: deal with the dispatcher action
	action := dispatcherStatus.GetAction()
	if action != nil {
		pendingEvent := d.blockEventStatus.getEvent()
		if pendingEvent == nil && action.CommitTs > d.GetResolvedTs() {
			// we have not received the block event, and the action is for the future event, so just ignore
			log.Info("pending event is nil, and the action's commit is larger than dispatchers resolvedTs",
				zap.Uint64("resolvedTs", d.GetResolvedTs()),
				zap.Uint64("actionCommitTs", action.CommitTs),
				zap.Stringer("dispatcher", d.id))
			// we have not received the block event, and the action is for the future event, so just ignore
			return
		}
		if d.blockEventStatus.actionMatchs(action) {
			log.Info("pending event get the action",
				zap.Any("action", action),
				zap.Any("innerAction", int(action.Action)),
				zap.Stringer("dispatcher", d.id),
				zap.Uint64("pendingEventCommitTs", pendingEvent.GetCommitTs()))
			d.blockEventStatus.updateBlockStage(heartbeatpb.BlockStage_WRITING)
			pendingEvent.PushFrontFlushFunc(func() {
				// clear blockEventStatus should be before wake ds.
				// otherwise, there may happen:
				// 1. wake ds
				// 2. get new ds and set new pending event
				// 3. clear blockEventStatus(should be the old pending event, but clear the new one)
				d.blockEventStatus.clear()
			})
			if action.Action == heartbeatpb.Action_Write {
				failpoint.Inject("BlockOrWaitBeforeWrite", nil)
				err := d.AddBlockEventToSink(pendingEvent)
				if err != nil {
					select {
					case d.errCh <- err:
					default:
						log.Error("error channel is full, discard error",
							zap.Stringer("changefeedID", d.changefeedID),
							zap.Stringer("dispatcherID", d.id),
							zap.Error(err))
					}
					return
				}
				failpoint.Inject("BlockOrWaitReportAfterWrite", nil)
			} else {
				failpoint.Inject("BlockOrWaitBeforePass", nil)
				d.PassBlockEventToSink(pendingEvent)
				failpoint.Inject("BlockAfterPass", nil)
			}
		}

		// Step3: whether the outdate message or not, we need to return message show we have finished the event.
		d.blockStatusesChan <- &heartbeatpb.TableSpanBlockStatus{
			ID: d.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:   true,
				BlockTs:     dispatcherStatus.GetAction().CommitTs,
				IsSyncPoint: dispatcherStatus.GetAction().IsSyncPoint,
				Stage:       heartbeatpb.BlockStage_DONE,
			},
		}
	}
}

// shouldBlock check whether the event should be blocked(to wait maintainer response)
// For the ddl event with more than one blockedTable, it should block.
// For the ddl event with only one blockedTable, it should block only if the table is not complete span.
// Sync point event should always block.
func (d *Dispatcher) shouldBlock(event commonEvent.BlockEvent) bool {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		ddlEvent := event.(*commonEvent.DDLEvent)
		if ddlEvent.BlockedTables == nil {
			return false
		}
		switch ddlEvent.GetBlockedTables().InfluenceType {
		case commonEvent.InfluenceTypeNormal:
			if len(ddlEvent.GetBlockedTables().TableIDs) > 1 {
				return true
			}
			if !common.IsCompleteSpan(d.tableSpan) {
				// if the table is split, even the blockTable only itself, it should block
				return true
			}
			return false
		case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
			return true
		}
	case commonEvent.TypeSyncPointEvent:
		return true
	default:
		log.Error("invalid event type", zap.Any("eventType", event.GetType()))
	}
	return false
}

// 1.If the event is a single table DDL, it will be added to the sink for writing to downstream.
// If the ddl leads to add new tables or drop tables, it should send heartbeat to maintainer
// 2. If the event is a multi-table DDL / sync point Event, it will generate a TableSpanBlockStatus message with ddl info to send to maintainer.
func (d *Dispatcher) dealWithBlockEvent(event commonEvent.BlockEvent) {
	if !d.shouldBlock(event) {
		ddl, ok := event.(*commonEvent.DDLEvent)
		// a BDR mode cluster, TiCDC can receive DDLs from all roles of TiDB.
		// However, CDC only executes the DDLs from the TiDB that has BDRRolePrimary role.
		if ok && d.bdrMode && ddl.BDRMode != string(ast.BDRRolePrimary) {
			d.PassBlockEventToSink(event)
		} else {
			err := d.AddBlockEventToSink(event)
			if err != nil {
				select {
				case d.errCh <- err:
				default:
					log.Error("error channel is full, discard error",
						zap.Stringer("changefeedID", d.changefeedID),
						zap.Stringer("dispatcherID", d.id),
						zap.Error(err))
				}
				return
			}
		}
		if event.GetNeedAddedTables() != nil || event.GetNeedDroppedTables() != nil {
			message := &heartbeatpb.TableSpanBlockStatus{
				ID: d.id.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:         false,
					BlockTs:           event.GetCommitTs(),
					NeedDroppedTables: event.GetNeedDroppedTables().ToPB(),
					NeedAddedTables:   commonEvent.ToTablesPB(event.GetNeedAddedTables()),
					IsSyncPoint:       false, // sync point event must should block
					Stage:             heartbeatpb.BlockStage_NONE,
				},
			}
			identifier := BlockEventIdentifier{
				CommitTs:    event.GetCommitTs(),
				IsSyncPoint: false,
			}

			if event.GetNeedAddedTables() != nil {
				// When the ddl need add tables, we need the maintainer to block the forwarding of checkpointTs
				// Because the the new add table should join the calculation of checkpointTs
				// So the forwarding of checkpointTs should be blocked until the new dispatcher is created.
				// While there is a time gap between dispatcher send the block status and
				// maintainer begin to create dispatcher(and block the forwaring checkpoint)
				// in order to avoid the checkpointTs forward unexceptedly,
				// we need to block the checkpoint forwarding in this dispatcher until receive the ack from maintainer.
				//
				//     |----> block checkpointTs forwaring of this dispatcher ------>|-----> forwarding checkpointTs normally
				//     |        send block stauts                 send ack           |
				// dispatcher -------------------> maintainer ----------------> dispatcher
				//                                     |
				//                                     |----------> Block CheckpointTs Forwarding and create new dispatcher
				// Thus, we add the event to tableProgress again, and call event postFunc when the ack is received from maintainer.
				event.ClearPostFlushFunc()
				d.tableProgress.Add(event)
				d.resendTaskMap.Set(identifier, newResendTask(message, d, event.PostFlush))
			} else {
				d.resendTaskMap.Set(identifier, newResendTask(message, d, nil))
			}
			d.blockStatusesChan <- message
		}
	} else {
		d.blockEventStatus.setBlockEvent(event, heartbeatpb.BlockStage_WAITING)

		if event.GetType() == commonEvent.TypeSyncPointEvent {
			// deal with multi sync point commit ts in one Sync Point Event
			// make each commitTs as a single message for maintainer
			// Because the batch commitTs in different dispatchers can be different.
			commitTsList := event.(*commonEvent.SyncPointEvent).GetCommitTsList()
			blockTables := event.GetBlockedTables().ToPB()
			needDroppedTables := event.GetNeedDroppedTables().ToPB()
			needAddedTables := commonEvent.ToTablesPB(event.GetNeedAddedTables())
			for _, commitTs := range commitTsList {
				message := &heartbeatpb.TableSpanBlockStatus{
					ID: d.id.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked:         true,
						BlockTs:           commitTs,
						BlockTables:       blockTables,
						NeedDroppedTables: needDroppedTables,
						NeedAddedTables:   needAddedTables,
						UpdatedSchemas:    nil,
						IsSyncPoint:       true,
						Stage:             heartbeatpb.BlockStage_WAITING,
					},
				}
				identifier := BlockEventIdentifier{
					CommitTs:    commitTs,
					IsSyncPoint: true,
				}
				d.resendTaskMap.Set(identifier, newResendTask(message, d, nil))
				d.blockStatusesChan <- message
			}
		} else {
			message := &heartbeatpb.TableSpanBlockStatus{
				ID: d.id.ToPB(),
				State: &heartbeatpb.State{
					IsBlocked:         true,
					BlockTs:           event.GetCommitTs(),
					BlockTables:       event.GetBlockedTables().ToPB(),
					NeedDroppedTables: event.GetNeedDroppedTables().ToPB(),
					NeedAddedTables:   commonEvent.ToTablesPB(event.GetNeedAddedTables()),
					UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(event.GetUpdatedSchemas()), // only exists for rename table and rename tables
					IsSyncPoint:       false,
					Stage:             heartbeatpb.BlockStage_WAITING,
				},
			}
			identifier := BlockEventIdentifier{
				CommitTs:    event.GetCommitTs(),
				IsSyncPoint: false,
			}
			d.resendTaskMap.Set(identifier, newResendTask(message, d, nil))
			d.blockStatusesChan <- message
		}
	}

	// dealing with events which update schema ids
	// Only rename table and rename tables may update schema ids(rename db1.table1 to db2.table2)
	// Here we directly update schema id of dispatcher when we begin to handle the ddl event,
	// but not waiting maintainer response for ready to write/pass the ddl event.
	// Because the schemaID of each dispatcher is only use to dealing with the db-level ddl event(like drop db) or drop table.
	// Both the rename table/rename tables, drop table and db-level ddl event will be send to the table trigger event dispatcher in order.
	// So there won't be a related db-level ddl event is in dealing when we get update schema id events.
	// Thus, whether to update schema id before or after current ddl event is not important.
	// To make it easier, we choose to directly update schema id here.
	if event.GetUpdatedSchemas() != nil && d.tableSpan != common.DDLSpan {
		for _, schemaIDChange := range event.GetUpdatedSchemas() {
			if schemaIDChange.TableID == d.tableSpan.TableID {
				if schemaIDChange.OldSchemaID != d.schemaID {
					log.Error("Wrong Schema ID",
						zap.Stringer("dispatcherID", d.id),
						zap.Int64("exceptSchemaID", schemaIDChange.OldSchemaID),
						zap.Int64("actualSchemaID", d.schemaID),
						zap.String("tableSpan", common.FormatTableSpan(d.tableSpan)))
					return
				} else {
					d.schemaID = schemaIDChange.NewSchemaID
					d.schemaIDToDispatchers.Update(schemaIDChange.OldSchemaID, schemaIDChange.NewSchemaID)
					return
				}
			}
		}
	}
}

func (d *Dispatcher) cancelResendTask(identifier BlockEventIdentifier) {
	task := d.resendTaskMap.Get(identifier)
	if task == nil {
		return
	}

	task.Cancel()
	d.resendTaskMap.Delete(identifier)
}

func (d *Dispatcher) GetBlockEventStatus() *heartbeatpb.State {
	pendingEvent, blockStage := d.blockEventStatus.getEventAndStage()

	// we only need to report the block status for the ddl that block others and not finished.
	if pendingEvent == nil || !d.shouldBlock(pendingEvent) {
		return nil
	}

	// we only need to report the block status of these block ddls when maintainer is restarted.
	// For the non-block but with needDroppedTables and needAddTables ddls,
	// we don't need to report it when maintainer is restarted, because:
	// 1. the ddl not block other dispatchers
	// 2. maintainer can get current available tables based on table trigger event dispatcher's startTs,
	//    so don't need to do extra add and drop actions.

	return &heartbeatpb.State{
		IsBlocked:         true,
		BlockTs:           pendingEvent.GetCommitTs(),
		BlockTables:       pendingEvent.GetBlockedTables().ToPB(),
		NeedDroppedTables: pendingEvent.GetNeedDroppedTables().ToPB(),
		NeedAddedTables:   commonEvent.ToTablesPB(pendingEvent.GetNeedAddedTables()),
		UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(pendingEvent.GetUpdatedSchemas()), // only exists for rename table and rename tables
		IsSyncPoint:       pendingEvent.GetType() == commonEvent.TypeSyncPointEvent,         // sync point event must should block
		Stage:             blockStage,
	}
}
