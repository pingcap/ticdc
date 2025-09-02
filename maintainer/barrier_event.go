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
	"github.com/pingcap/ticdc/maintainer/range_checker"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// BarrierEvent is a barrier event that reported by dispatchers, note is a block multiple dispatchers
// all of these dispatchers should report the same event
type BarrierEvent struct {
	cfID               common.ChangeFeedID
	commitTs           uint64
	spanController     *span.Controller
	operatorController *operator.Controller
	nodeManager        *watcher.NodeManager
	selected           atomic.Bool
	hasNewTable        bool
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

	// Used to record reported dispatchers and has two main functions:
	// 1. To facilitate subsequent verification of the dispatcher's existence (refer allDispatcherReported())
	// 2. When BlockTables.InfluenceType is not Normal, we should store reported dispatchers first
	//    and wait get the reported from table trigger event dispatcher(all/db type must have table trigger event dispatcher)
	//    then create the rangeChecker and update the reported dispatchers.
	//    Why we need to wait table trigger event dispatcher?
	//    because we need to consider the add/drop tables in the other ddls.
	//    so only we use table trigger to create rangeChecker can ensure the coverage is correct.
	reportedDispatchers map[common.DispatcherID]struct{}
	// rangeChecker is used to check if all the dispatchers reported the block events
	rangeChecker   range_checker.RangeChecker
	lastResendTime time.Time

	lastWarningLogTime time.Time
}

func NewBlockEvent(cfID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	spanController *span.Controller,
	operatorController *operator.Controller,
	status *heartbeatpb.State,
	dynamicSplitEnabled bool,
) *BarrierEvent {
	event := &BarrierEvent{
		cfID:               cfID,
		commitTs:           status.BlockTs,
		spanController:     spanController,
		operatorController: operatorController,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		selected:           atomic.Bool{},
		hasNewTable:        len(status.NeedAddedTables) > 0,

		blockedDispatchers: status.BlockTables,
		dropDispatchers:    status.NeedDroppedTables,
		newTables:          status.NeedAddedTables,
		schemaIDChange:     status.UpdatedSchemas,
		isSyncPoint:        status.IsSyncPoint,
		// if the split table is enable for this changefeeed, if not we can use table id to check coverage
		dynamicSplitEnabled: dynamicSplitEnabled,

		reportedDispatchers: make(map[common.DispatcherID]struct{}),
		lastResendTime:      time.Time{},

		lastWarningLogTime: time.Now(),
	}

	if status.BlockTables != nil {
		switch status.BlockTables.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			if dynamicSplitEnabled {
				event.rangeChecker = range_checker.NewTableSpanRangeChecker(status.BlockTables.TableIDs)
			} else {
				event.rangeChecker = range_checker.NewTableCountChecker(len(status.BlockTables.TableIDs))
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
		reps := be.spanController.GetAllTasks()
		tbls := make([]int64, 0, len(reps))
		for _, rep := range reps {
			tbls = append(tbls, rep.Span.TableID)
		}
		tbls = append(tbls, common.DDLSpan.TableID)
		be.rangeChecker = range_checker.NewTableSpanRangeChecker(tbls)
	} else {
		be.rangeChecker = range_checker.NewTableCountChecker(be.spanController.TaskSize())
	}
	log.Info("create range checker for block event", zap.Any("influcenceType", be.blockedDispatchers.InfluenceType), zap.Any("commitTs", be.commitTs))
}

func (be *BarrierEvent) createRangeCheckerForTypeDB() {
	if be.dynamicSplitEnabled {
		reps := be.spanController.GetTasksBySchemaID(be.blockedDispatchers.SchemaID)
		tbls := make([]int64, 0, len(reps))
		for _, rep := range reps {
			tbls = append(tbls, rep.Span.TableID)
		}

		tbls = append(tbls, common.DDLSpan.TableID)
		be.rangeChecker = range_checker.NewTableSpanRangeChecker(tbls)
	} else {
		be.rangeChecker = range_checker.NewTableCountChecker(
			be.spanController.GetTaskSizeBySchemaID(be.blockedDispatchers.SchemaID) + 1 /*table trigger event dispatcher*/)
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
			zap.String("dispatcher", be.spanController.GetDDLDispatcherID().String()),
			zap.Uint64("commitTs", be.commitTs))
		dispatcher = be.spanController.GetDDLDispatcherID()
	default:
		selected := dispatcherID.ToPB()
		if be.tableTriggerDispatcherRelated {
			// select the last one as the writer
			// or the table trigger event dispatcher if it's one of the blocked dispatcher
			selected = be.spanController.GetDDLDispatcherID().ToPB()
			log.Info("use table trigger event as the writer dispatcher",
				zap.String("changefeed", be.cfID.Name()),
				zap.String("dispatcher", selected.String()),
				zap.Uint64("commitTs", be.commitTs))
		}
		dispatcher = common.NewDispatcherIDFromPB(selected)
	}

	// reset ranger checkers and reportedDispatchers
	be.rangeChecker.Reset()
	be.reportedDispatchers = make(map[common.DispatcherID]struct{})

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
			be.operatorController.RemoveTasksBySchemaID(be.dropDispatchers.SchemaID)
			log.Info("remove table",
				zap.String("changefeed", be.cfID.Name()),
				zap.Uint64("commitTs", be.commitTs),
				zap.Int64("schema", be.dropDispatchers.SchemaID))
		case heartbeatpb.InfluenceType_Normal:
			be.operatorController.RemoveTasksByTableIDs(be.dropDispatchers.TableIDs...)
			log.Info("remove table",
				zap.String("changefeed", be.cfID.Name()),
				zap.Uint64("commitTs", be.commitTs),
				zap.Int64s("table", be.dropDispatchers.TableIDs))
		case heartbeatpb.InfluenceType_All:
			log.Panic("invalid influence type meet drop dispatchers",
				zap.Any("blockedDispatchers", be.blockedDispatchers),
				zap.Any("changefeed", be.cfID.Name()),
				zap.Any("commitTs", be.commitTs),
			)
		}
	}
	for _, add := range be.newTables {
		log.Info("add new table",
			zap.Uint64("commitTs", be.commitTs),
			zap.String("changefeed", be.cfID.Name()),
			zap.Int64("schema", add.SchemaID),
			zap.Int64("table", add.TableID))
		be.spanController.AddNewTable(commonEvent.Table{
			SchemaID:  add.SchemaID,
			TableID:   add.TableID,
			Splitable: add.Splitable,
		}, be.commitTs)
	}

	for _, change := range be.schemaIDChange {
		log.Info("update schema id",
			zap.String("changefeed", be.cfID.Name()),
			zap.Uint64("commitTs", be.commitTs),
			zap.Int64("newSchema", change.OldSchemaID),
			zap.Int64("oldSchema", change.NewSchemaID),
			zap.Int64("table", change.TableID))
		be.spanController.UpdateSchemaID(change.TableID, change.NewSchemaID)
	}
}

func (be *BarrierEvent) markTableDone(tableID int64) {
	be.rangeChecker.AddSubRange(tableID, nil, nil)
}

func (be *BarrierEvent) addDispatchersToRangeChecker() {
	for dispatcher := range be.reportedDispatchers {
		replicaSpan := be.spanController.GetTaskByID(dispatcher)
		if replicaSpan == nil {
			log.Info("dispatcher not found, ignore",
				zap.String("changefeed", be.cfID.Name()),
				zap.String("dispatcher", dispatcher.String()))
			continue
		}
		be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
	}
}

func (be *BarrierEvent) markDispatcherEventDone(dispatcherID common.DispatcherID) {
	replicaSpan := be.spanController.GetTaskByID(dispatcherID)
	if replicaSpan == nil {
		log.Warn("dispatcher not found, ignore",
			zap.String("changefeed", be.cfID.Name()),
			zap.String("dispatcher", dispatcherID.String()))
		return
	}

	be.reportedDispatchers[dispatcherID] = struct{}{}
	if be.rangeChecker == nil {
		// rangeChecker is not created
		if be.spanController.IsDDLDispatcher(dispatcherID) {
			// create rangeChecker
			switch be.blockedDispatchers.InfluenceType {
			case heartbeatpb.InfluenceType_Normal:
				log.Panic("influence type should not be normal when range checker is nil")
			case heartbeatpb.InfluenceType_DB:
				// create range checker first
				be.createRangeCheckerForTypeDB()
				be.addDispatchersToRangeChecker()
			case heartbeatpb.InfluenceType_All:
				// create range checker first
				be.createRangeCheckerForTypeAll()
				be.addDispatchersToRangeChecker()
			}
		}
	} else {
		be.rangeChecker.AddSubRange(replicaSpan.Span.TableID, replicaSpan.Span.StartKey, replicaSpan.Span.EndKey)
	}
}

func (be *BarrierEvent) allDispatcherReported() bool {
	if be.rangeChecker == nil {
		return false
	}

	if !be.rangeChecker.IsFullyCovered() {
		return false
	}

	needDoubleCheck := false

	// we need to double check whether there are some unexisted dispatcherID in reported Dispatchers
	// There is a example to show the necessary for the double check
	// 1. Table A was first split into dispatchers A, B, C, and D.
	//    A received the DDL (ts=10) first and reported it to the maintainer.
	// 2. At this point, table A underwent some merge and split operations,
	//    becoming dispatchers E, F, G, and H, which continued to synchronize.
	// 3. Meanwhile, dispatchers E, F, and G also received the DDL and reported it to the maintainer.
	//    The spans of A, E, F, and G met the spanChecker, causing the DDL to begin execution.
	// 4. However, H had not yet received the corresponding DDL and was still executing the preceding DML.
	//    Because the DDL was executed before the DML, the DML execution failed.
	// 5. Therefore, when checking the reported status, we need to check for expired dispatchers
	//    to avoid this situation.
	for dispatcherID := range be.reportedDispatchers {
		if dispatcherID == be.spanController.GetDDLDispatcherID() {
			continue
		}
		task := be.spanController.GetTaskByID(dispatcherID)
		if task == nil {
			log.Info("unexisted dispatcher, remove it from barrier event",
				zap.String("changefeed", be.cfID.Name()),
				zap.String("dispatcher", dispatcherID.String()),
				zap.Uint64("commitTs", be.commitTs),
			)
			needDoubleCheck = true
			delete(be.reportedDispatchers, dispatcherID)
		} else {
			if !be.spanController.IsReplicating(task) {
				log.Info("unreplicating dispatcher, remove it from barrier event",
					zap.String("changefeed", be.cfID.Name()),
					zap.String("dispatcher", dispatcherID.String()),
					zap.Uint64("commitTs", be.commitTs),
				)
				needDoubleCheck = true
				delete(be.reportedDispatchers, dispatcherID)
			}
		}
	}

	if needDoubleCheck {
		be.rangeChecker.Reset()

		switch be.blockedDispatchers.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			if be.dynamicSplitEnabled {
				be.rangeChecker = range_checker.NewTableSpanRangeChecker(be.blockedDispatchers.TableIDs)
			} else {
				be.rangeChecker = range_checker.NewTableCountChecker(len(be.blockedDispatchers.TableIDs))
			}
		case heartbeatpb.InfluenceType_DB:
			be.createRangeCheckerForTypeDB()
		case heartbeatpb.InfluenceType_All:
			be.createRangeCheckerForTypeAll()
		}

		be.addDispatchersToRangeChecker()

		return be.rangeChecker.IsFullyCovered()
	}

	return true
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
		spans := be.spanController.GetTasksBySchemaID(be.blockedDispatchers.SchemaID)
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
		for _, n := range getAllNodes(be.nodeManager) {
			msgMap[n] = be.newPassActionMessage(n)
		}
	case heartbeatpb.InfluenceType_Normal:
		for _, tableID := range be.blockedDispatchers.TableIDs {
			spans := be.spanController.GetTasksByTableID(tableID)
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

// check all related blocked dispatchers progress, to forward the progress of some block event,
// to avoid the corner case that some dispatcher has forward checkpointTs.
// If the dispatcher's checkpointTs >= commitTs of this event, means the block event is writen to the sink.
//
// For example, there are two nodes A and B, and there are two dispatchers A1 and B1, maintainer is also running on A.
// One ddl event E need the evolve of A1 and B1, and A1 finish flushing the event E downstream.
// While before A1 report the checkpointTs, node A crash.
// Then maintainer transfer to node B, and B1 report the block event.
// And new A1 will be created as startTs = E.commitTs, because the ddl_ts in sink is E.commitTs.
// while in HandleBootstrapResponse, the replication checkpointTs of A1 is still smaller than E.commitTs.(not finish reporting new checkpointTs of A1)
// so we will still have a block event, waiting for the report of A1.
//
// So we add this check in resend, to provide a safety check for ddl event, avoid a block event is always blocked.
func (be *BarrierEvent) checkBlockedDispatchers() {
	switch be.blockedDispatchers.InfluenceType {
	case heartbeatpb.InfluenceType_Normal:
		for _, tableId := range be.blockedDispatchers.TableIDs {
			replications := be.spanController.GetTasksByTableID(tableId)
			for _, replication := range replications {
				if replication.GetStatus().CheckpointTs >= be.commitTs {
					// one related table has forward checkpointTs, means the block event can be advanced
					be.selected.Store(true)
					be.writerDispatcherAdvanced = true
					return
				}
			}
		}
	case heartbeatpb.InfluenceType_DB:
		schemaID := be.blockedDispatchers.SchemaID
		replications := be.spanController.GetTasksBySchemaID(schemaID)
		for _, replication := range replications {
			if replication.GetStatus().CheckpointTs >= be.commitTs {
				// one related table has forward checkpointTs, means the block event can be advanced
				be.selected.Store(true)
				be.writerDispatcherAdvanced = true
				return
			}
		}
	case heartbeatpb.InfluenceType_All:
		replications := be.spanController.GetAllTasks()
		for _, replication := range replications {
			if replication.GetStatus().CheckpointTs >= be.commitTs {
				// one related table has forward checkpointTs, means the block event can be advanced
				be.selected.Store(true)
				be.writerDispatcherAdvanced = true
				return
			}
		}
	}
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
		if time.Since(be.lastResendTime) > 30*time.Second {
			log.Info("barrier event is not being selected",
				zap.String("changefeed", be.cfID.Name()),
				zap.Uint64("commitTs", be.commitTs),
				zap.Bool("isSyncPoint", be.isSyncPoint),
				zap.Bool("selected", be.selected.Load()),
				zap.Bool("writerDispatcherAdvanced", be.writerDispatcherAdvanced),
				zap.Any("blocker", be.blockedDispatchers))
		}
		be.checkBlockedDispatchers()
		return nil
	}
	be.lastResendTime = time.Now()
	// we select a dispatcher as the writer, still waiting for that dispatcher advance its checkpoint ts
	if !be.writerDispatcherAdvanced {
		// resend write action
		stm := be.spanController.GetTaskByID(be.writerDispatcher)
		if stm == nil || stm.GetNodeID() == "" {
			log.Warn("writer dispatcher not found",
				zap.String("changefeed", be.cfID.Name()),
				zap.String("dispatcher", be.writerDispatcher.String()),
				zap.Uint64("commitTs", be.commitTs),
				zap.Bool("isSyncPoint", be.isSyncPoint))

			// choose a new one as the writer
			// it only can happen then the split and merge happens to a table, and the writeDispatcher is not the table trigger event dispatcher
			// So the block event influence type is must normal, we just need to select one dispatcher in the block dispatchers
			if be.blockedDispatchers.InfluenceType != heartbeatpb.InfluenceType_Normal || len(be.blockedDispatchers.TableIDs) == 0 {
				log.Panic("influence type should be normal when writer dispatcher not found",
					zap.String("changefeed", be.cfID.Name()),
					zap.Any("event", be),
					zap.String("dispatcher", be.writerDispatcher.String()),
					zap.Uint64("commitTs", be.commitTs),
					zap.Bool("isSyncPoint", be.isSyncPoint))
			}

			tableID := be.blockedDispatchers.TableIDs[0]
			replications := be.spanController.GetTasksByTableID(tableID)

			if len(replications) == 0 {
				log.Panic("replications for this block event should not be empty",
					zap.String("changefeed", be.cfID.Name()),
					zap.Int64("tableID", tableID),
					zap.Any("event", be),
					zap.String("dispatcher", be.writerDispatcher.String()),
					zap.Uint64("commitTs", be.commitTs),
					zap.Bool("isSyncPoint", be.isSyncPoint))
			}

			be.writerDispatcher = replications[0].ID
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

// GetAllNodes returns all alive nodes
func getAllNodes(nodeManager *watcher.NodeManager) []node.ID {
	aliveNodes := nodeManager.GetAliveNodes()
	nodes := make([]node.ID, 0, len(aliveNodes))
	for id := range aliveNodes {
		nodes = append(nodes, id)
	}
	return nodes
}
