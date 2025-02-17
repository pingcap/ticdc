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
	"github.com/pingcap/ticdc/maintainer/range_checker"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// BarrierEvent is a barrier event that reported by dispatchers, note is a block multiple dispatchers
// all of these dispatchers should report the same event
type BarrierEvent struct {
	cfID        common.ChangeFeedID
	commitTs    uint64
	controller  *Controller
	selected    atomic.Bool
	hasNewTable bool
	// table trigger event dispatcher reported the block event, we should use it as the writer
	tableTriggerDispatcherRelated bool
	writerDispatcher              common.DispatcherID
	writerDispatcherAdvanced      bool

	blockedDispatchers *heartbeatpb.InfluencedTables
	dropDispatchers    *heartbeatpb.InfluencedTables
	newTables          []*heartbeatpb.Table
	schemaIDChange     []*heartbeatpb.SchemaIDChange
	isSyncPoint        bool
	// if the split table is enable for this changefeeed, if not we can use table id to check coverage
	dynamicSplitEnabled bool

	// used to store report waiting status dispatchers before rangeChecker is created
	// when BlockTables.InfluenceType is not Normal, we should store reported dispatchers first
	// and wait get the reported from table trigger event dispatcher(all/db type must have table trigger event dispatcher)
	// then create the rangeChecker and update the reported dispatchers.
	// Why we need to wait table trigger event dispatcher?
	// because we need to consider the add/drop tables in the other ddls.
	// so only we use table trigger to create rangeChecker can ensure the coverage is correct.
	reportedDispatchers map[common.DispatcherID]struct{}
	// rangeChecker is used to check if all the dispatchers reported the block events
	rangeChecker   range_checker.RangeChecker
	lastResendTime time.Time

	lastWarningLogTime time.Time
}

func NewBlockEvent(cfID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	controller *Controller,
	status *heartbeatpb.State,
	dynamicSplitEnabled bool,
) *BarrierEvent {
	event := &BarrierEvent{
		controller:          controller,
		selected:            atomic.Bool{},
		hasNewTable:         len(status.NeedAddedTables) > 0,
		cfID:                cfID,
		commitTs:            status.BlockTs,
		blockedDispatchers:  status.BlockTables,
		newTables:           status.NeedAddedTables,
		dropDispatchers:     status.NeedDroppedTables,
		schemaIDChange:      status.UpdatedSchemas,
		lastResendTime:      time.Time{},
		reportedDispatchers: make(map[common.DispatcherID]struct{}),
		isSyncPoint:         status.IsSyncPoint,
		dynamicSplitEnabled: dynamicSplitEnabled,
		lastWarningLogTime:  time.Now(),
	}
	if status.BlockTables != nil {
		switch status.BlockTables.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			if dynamicSplitEnabled {
				event.rangeChecker = range_checker.NewTableSpanRangeChecker(status.BlockTables.TableIDs)
			} else {
				event.rangeChecker = range_checker.NewTableCountChecker(len(status.BlockTables.TableIDs))
			}
		case heartbeatpb.InfluenceType_DB:
			// TODO:clean code
			// create range checker if dispatcher is ddl dispatcher
			// otherwise store dispatcherID in reportedDispatchers, and not create rangeChecker
			if dispatcherID == controller.ddlDispatcherID {
				event.createRangeCheckerForTypeDB()
			} else {
				event.reportedDispatchers[dispatcherID] = struct{}{}
			}
		case heartbeatpb.InfluenceType_All:
			// create range checker if dispatcher is ddl dispatcher
			// otherwise store dispatcherID in reportedDispatchers, and not create rangeChecker
			if dispatcherID == controller.ddlDispatcherID {
				event.createRangeCheckerForTypeAll()
			} else {
				event.reportedDispatchers[dispatcherID] = struct{}{}
			}
		}
	}
	log.Info("new block event is created",
		zap.String("changefeedID", cfID.Name()),
		zap.Uint64("blockTs", event.commitTs),
		zap.Bool("syncPoint", event.isSyncPoint),
		zap.Any("detail", status))
	return event
}

func (be *BarrierEvent) createRangeCheckerForTypeAll() {
	if be.dynamicSplitEnabled {
		reps := be.controller.GetAllTasks()
		tbls := make([]int64, 0, len(reps))
		for _, rep := range reps {
			tbls = append(tbls, rep.Span.TableID)
		}
		tbls = append(tbls, heartbeatpb.DDLSpan.TableID)
		be.rangeChecker = range_checker.NewTableSpanRangeChecker(tbls)
	} else {
		be.rangeChecker = range_checker.NewTableCountChecker(be.controller.TaskSize())
	}
	log.Info("create range checker for block event", zap.Any("influcenceType", be.blockedDispatchers.InfluenceType), zap.Any("commitTs", be.commitTs))
}

func (be *BarrierEvent) createRangeCheckerForTypeDB() {
	if be.dynamicSplitEnabled {
		reps := be.controller.GetTasksBySchemaID(be.blockedDispatchers.SchemaID)
		tbls := make([]int64, 0, len(reps))
		for _, rep := range reps {
			tbls = append(tbls, rep.Span.TableID)
		}

		tbls = append(tbls, heartbeatpb.DDLSpan.TableID)
		be.rangeChecker = range_checker.NewTableSpanRangeChecker(tbls)
	} else {
		be.rangeChecker = range_checker.NewTableCountChecker(
			be.controller.GetTaskSizeBySchemaID(be.blockedDispatchers.SchemaID) + 1 /*table trigger event dispatcher*/)
	}
	log.Info("create range checker for block event", zap.Any("influcenceType", be.blockedDispatchers.InfluenceType), zap.Any("commitTs", be.commitTs))
}

func (be *BarrierEvent) checkEventAction(dispatcherID common.DispatcherID) *heartbeatpb.DispatcherStatus {
	if !be.allDispatcherReported() {
		return nil
	}
	return be.onAllDispatcherReportedBlockEvent(dispatcherID)
}

// onAllDispatcherReportedBlockEvent is called when all dispatcher reported the block event
// it will select a dispatcher as the writer, reset the range checker ,and move the event to the selected state
// returns the dispatcher status to the dispatcher manager
func (be *BarrierEvent) onAllDispatcherReportedBlockEvent(dispatcherID common.DispatcherID) *heartbeatpb.DispatcherStatus {
	var dispatcher common.DispatcherID
	switch be.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_DB, heartbeatpb.InfluenceType_All:
		// for all and db type, we always use the table trigger event dispatcher as the writer
		log.Info("use table trigger event as the writer dispatcher",
			zap.String("changefeed", be.cfID.Name()),
			zap.String("dispatcher", be.controller.ddlDispatcherID.String()),
			zap.Uint64("commitTs", be.commitTs))
		dispatcher = be.controller.ddlDispatcherID
	default:
		selected := dispatcherID.ToPB()
		if be.tableTriggerDispatcherRelated {
			// select the last one as the writer
			// or the table trigger event dispatcher if it's one of the blocked dispatcher
			selected = be.controller.ddlDispatcherID.ToPB()
			log.Info("use table trigger event as the writer dispatcher",
				zap.String("changefeed", be.cfID.Name()),
				zap.String("dispatcher", selected.String()),
				zap.Uint64("commitTs", be.commitTs))
		}
		dispatcher = common.NewDispatcherIDFromPB(selected)
	}

	// reset ranger checkers
	be.rangeChecker.Reset()
	be.selected.Store(true)
	be.writerDispatcher = dispatcher
	log.Info("all dispatcher reported heartbeat, schedule it, and select one to write",
		zap.String("changefeed", be.cfID.Name()),
		zap.String("dispatcher", be.writerDispatcher.String()),
		zap.Uint64("commitTs", be.commitTs),
		zap.String("barrierType", be.blockedDispatchers.InfluenceType.String()))
	be.scheduleBlockEvent()
	return &heartbeatpb.DispatcherStatus{
		InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			DispatcherIDs: []*heartbeatpb.DispatcherID{be.writerDispatcher.ToPB()},
		},
		Action: be.action(heartbeatpb.Action_Write),
	}
}

func (be *BarrierEvent) scheduleBlockEvent() {
	log.Info("schedule block event", zap.Uint64("commitTs", be.commitTs))
	// dispatcher notify us to drop some tables, by dispatcher ID or schema ID
	if be.dropDispatchers != nil {
		switch be.dropDispatchers.InfluenceType {
		case heartbeatpb.InfluenceType_DB:
			be.controller.RemoveTasksBySchemaID(be.dropDispatchers.SchemaID)
			log.Info(" remove table",
				zap.String("changefeed", be.cfID.Name()),
				zap.Uint64("commitTs", be.commitTs),
				zap.Int64("schema", be.dropDispatchers.SchemaID))
		case heartbeatpb.InfluenceType_Normal:
			be.controller.RemoveTasksByTableIDs(be.dropDispatchers.TableIDs...)
			log.Info(" remove table",
				zap.String("changefeed", be.cfID.Name()),
				zap.Uint64("commitTs", be.commitTs),
				zap.Int64s("table", be.dropDispatchers.TableIDs))
		case heartbeatpb.InfluenceType_All:
			be.controller.RemoveAllTasks()
			log.Info("remove all tables by barrier",
				zap.Uint64("commitTs", be.commitTs),
				zap.String("changefeed", be.cfID.Name()))
		}
	}
	for _, add := range be.newTables {
		log.Info(" add new table",
			zap.Uint64("commitTs", be.commitTs),
			zap.String("changefeed", be.cfID.Name()),
			zap.Int64("schema", add.SchemaID),
			zap.Int64("table", add.TableID))
		be.controller.AddNewTable(commonEvent.Table{
			SchemaID: add.SchemaID,
			TableID:  add.TableID,
		}, be.commitTs)
	}

	for _, change := range be.schemaIDChange {
		log.Info("update schema id",
			zap.String("changefeed", be.cfID.Name()),
			zap.Uint64("commitTs", be.commitTs),
			zap.Int64("newSchema", change.OldSchemaID),
			zap.Int64("oldSchema", change.NewSchemaID),
			zap.Int64("table", change.TableID))
		be.controller.UpdateSchemaID(change.TableID, change.NewSchemaID)
	}
}

func (be *BarrierEvent) markTableDone(tableID int64) {
	be.rangeChecker.AddSubRange(tableID, nil, nil)
}

func (be *BarrierEvent) markDispatcherEventDone(dispatcherID common.DispatcherID) {
	replicaSpan := be.controller.GetTask(dispatcherID)
	if replicaSpan == nil {
		log.Warn("dispatcher not found, ignore",
			zap.String("changefeed", be.cfID.Name()),
			zap.String("dispatcher", dispatcherID.String()))
		return
	}
	if be.rangeChecker == nil {
		// rangeChecker is not created
		if dispatcherID == be.controller.ddlDispatcherID {
			// create rangeChecker
			switch be.blockedDispatchers.InfluenceType {
			case heartbeatpb.InfluenceType_Normal:
				log.Panic("influence type should not be normal when range checker is nil")
			case heartbeatpb.InfluenceType_DB:
				// create range checker first
				be.createRangeCheckerForTypeDB()
				// add reported dispatchers sub range to range checker
				be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
				for dispatcher := range be.reportedDispatchers {
					replicaSpan := be.controller.GetTask(dispatcher)
					if replicaSpan == nil {
						log.Warn("dispatcher not found, ignore",
							zap.String("changefeed", be.cfID.Name()),
							zap.String("dispatcher", dispatcherID.String()))
						return
					}
					be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
				}
			case heartbeatpb.InfluenceType_All:
				// create range checker first
				be.createRangeCheckerForTypeAll()
				// add reported dispatchers sub range to range checker
				be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
				for dispatcher := range be.reportedDispatchers {
					replicaSpan := be.controller.GetTask(dispatcher)
					if replicaSpan == nil {
						log.Warn("dispatcher not found, ignore",
							zap.String("changefeed", be.cfID.Name()),
							zap.String("dispatcher", dispatcherID.String()))
						return
					}
					be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
				}
			}
		} else {
			be.reportedDispatchers[dispatcherID] = struct{}{}
		}
	} else {
		be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
	}
}

func (be *BarrierEvent) allDispatcherReported() bool {
	if be.rangeChecker == nil {
		return false
	}
	return be.rangeChecker.IsFullyCovered()
}

// send pass action to the related dispatchers, if find the related dispatchers are all removed, mark rangeCheck done
// else return pass action messages
func (be *BarrierEvent) sendPassAction() []*messaging.TargetMessage {
	if be.blockedDispatchers == nil {
		return []*messaging.TargetMessage{}
	}
	msgMap := make(map[node.ID]*messaging.TargetMessage)
	switch be.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_DB:
		spans := be.controller.GetTasksBySchemaID(be.blockedDispatchers.SchemaID)
		if len(spans) == 0 {
			// means tables are removed, mark the event done
			be.rangeChecker.MarkCovered()
			return nil
		} else {
			for _, stm := range spans {
				nodeID := stm.GetNodeID()
				if nodeID == "" {
					continue
				}
				_, ok := msgMap[nodeID]
				if !ok {
					msgMap[nodeID] = be.newPassActionMessage(nodeID)
				}
			}
		}
	case heartbeatpb.InfluenceType_All:
		// all type will not have drop-type ddl.
		for _, n := range be.controller.GetAllNodes() {
			msgMap[n] = be.newPassActionMessage(n)
		}
	case heartbeatpb.InfluenceType_Normal:
		for _, tableID := range be.blockedDispatchers.TableIDs {
			spans := be.controller.GetTasksByTableID(tableID)
			if len(spans) == 0 {
				be.markTableDone(tableID)
			} else {
				for _, stm := range spans {
					nodeID := stm.GetNodeID()
					dispatcherID := stm.ID
					if dispatcherID == be.writerDispatcher {
						continue
					}
					msg, ok := msgMap[nodeID]
					if !ok {
						msg = be.newPassActionMessage(nodeID)
						msgMap[nodeID] = msg
					}
					influencedDispatchers := msg.Message[0].(*heartbeatpb.HeartBeatResponse).DispatcherStatuses[0].InfluencedDispatchers
					influencedDispatchers.DispatcherIDs = append(influencedDispatchers.DispatcherIDs, dispatcherID.ToPB())
				}
			}
		}
	}
	msgs := make([]*messaging.TargetMessage, 0, len(msgMap))
	for _, msg := range msgMap {
		msgs = append(msgs, msg)
	}
	return msgs
}

func (be *BarrierEvent) resend() []*messaging.TargetMessage {
	if time.Since(be.lastResendTime) < time.Second {
		return nil
	}
	var msgs []*messaging.TargetMessage
	defer func() {
		if time.Since(be.lastWarningLogTime) > time.Second*10 {
			if be.rangeChecker != nil {
				log.Warn("barrier event is not resolved",
					zap.String("changefeed", be.cfID.Name()),
					zap.Uint64("commitTs", be.commitTs),
					zap.Bool("isSyncPoint", be.isSyncPoint),
					zap.Bool("selected", be.selected.Load()),
					zap.Bool("writerDispatcherAdvanced", be.writerDispatcherAdvanced),
					zap.String("coverage", be.rangeChecker.Detail()),
					zap.Any("blocker", be.blockedDispatchers),
					zap.Any("resend", msgs),
				)
			} else {
				log.Warn("barrier event is not resolved",
					zap.String("changefeed", be.cfID.Name()),
					zap.Uint64("commitTs", be.commitTs),
					zap.Bool("isSyncPoint", be.isSyncPoint),
					zap.Bool("selected", be.selected.Load()),
					zap.Bool("writerDispatcherAdvanced", be.writerDispatcherAdvanced),
					zap.Any("blocker", be.blockedDispatchers),
					zap.Any("resend", msgs),
				)
			}
			be.lastWarningLogTime = time.Now()
		}
	}()

	// still waiting for all dispatcher to reach the block commit ts
	if !be.selected.Load() {
		return nil
	}
	be.lastResendTime = time.Now()
	// we select a dispatcher as the writer, still waiting for that dispatcher advance its checkpoint ts
	if !be.writerDispatcherAdvanced {
		// resend write action
		stm := be.controller.GetTask(be.writerDispatcher)
		if stm == nil || stm.GetNodeID() == "" {
			log.Warn("writer dispatcher not found",
				zap.String("changefeed", be.cfID.Name()),
				zap.Uint64("commitTs", be.commitTs),
				zap.Bool("isSyncPoint", be.isSyncPoint))
			// todo: select a new writer
			return nil
		}
		msgs = []*messaging.TargetMessage{be.newWriterActionMessage(stm.GetNodeID())}
	} else {
		// the writer dispatcher is advanced, resend pass action
		return be.sendPassAction()
	}
	return msgs
}

func (be *BarrierEvent) newWriterActionMessage(capture node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(capture, messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID: be.cfID.ToPB(),
			DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
				{
					Action: be.action(heartbeatpb.Action_Write),
					InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						DispatcherIDs: []*heartbeatpb.DispatcherID{
							be.writerDispatcher.ToPB(),
						},
					},
				},
			},
		})
}

func (be *BarrierEvent) newPassActionMessage(capture node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(capture, messaging.HeartbeatCollectorTopic,
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID: be.cfID.ToPB(),
			DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
				{
					Action: be.action(heartbeatpb.Action_Pass),
					InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
						InfluenceType:       be.blockedDispatchers.InfluenceType,
						SchemaID:            be.blockedDispatchers.SchemaID,
						ExcludeDispatcherId: be.writerDispatcher.ToPB(),
					},
				},
			},
		})
}

func (be *BarrierEvent) action(action heartbeatpb.Action) *heartbeatpb.DispatcherAction {
	return &heartbeatpb.DispatcherAction{
		Action:      action,
		CommitTs:    be.commitTs,
		IsSyncPoint: be.isSyncPoint,
	}
}
