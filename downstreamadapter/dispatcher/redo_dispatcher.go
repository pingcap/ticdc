// Copyright 2025 PingCAP, Inc.
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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/redo"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

var _ EventDispatcher = (*RedoDispatcher)(nil)

type RedoDispatcher struct {
	changefeedID common.ChangeFeedID
	id           common.DispatcherID
	schemaID     int64
	tableSpan    *heartbeatpb.TableSpan
	// startTs is the timestamp that the dispatcher need to receive and flush events.
	startTs            uint64
	startTsIsSyncpoint bool
	// The ts from pd when the dispatcher is createrd.
	// when downstream is mysql-class, for dml event we need to compare the commitTs with this ts
	// to determine whether the insert event should use `Replace` or just `Insert`
	// Because when the dispatcher scheduled or the node restarts, there may be some dml events to receive twice.
	// So we need to use `Replace` to avoid duplicate key error.
	// Table Trigger Event Dispatcher doesn't need this, because it doesn't deal with dml events.
	creationPDTs uint64
	// componentStatus is the status of the dispatcher, such as working, removing, stopperd.
	componentStatus *ComponentStateWithMutex
	// the config of filter
	filterConfig *eventpb.FilterConfig

	// shared by the event dispatcher manager
	redoSink *redo.Sink

	// statusesChan is used to store the status of dispatchers when status changed
	// and push to heartbeatRequestQueue
	statusesChan chan TableSpanStatusWithSeq
	// blockStatusesChan use to collector block status of ddl/sync point event to Maintainer
	// shared by the event dispatcher manager
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus

	// schemaIDToDispatchers is shared in the eventDispatcherManager,
	// it store all the infos about schemaID->Dispatchers
	// Dispatchers may change the schemaID when meets some special events, such as rename ddl
	// we use schemaIDToDispatchers to calculate the dispatchers that need to receive the dispatcher status
	schemaIDToDispatchers *SchemaIDToDispatchers

	// the max resolvedTs received by the dispatcher
	resolvedTs uint64

	// blockEventStatus is used to store the current pending ddl/sync point event and its block status.
	blockEventStatus BlockEventStatus

	// tableProgress is used to calculate the checkpointTs of the dispatcher
	tableProgress *TableProgress

	// resendTaskMap is store all the resend task of ddl/sync point event current.
	// When we meet a block event that need to report to maintainer, we will create a resend task and store it in the map(avoid message lost)
	// When we receive the ack from maintainer, we will cancel the resend task.
	resendTaskMap *ResendTaskMap

	isRemoving atomic.Bool

	// errCh is used to collect the errors that need to report to maintainer
	// such as error of flush ddl events
	// errCh is shared in the eventDispatcherManager
	errCh chan error

	bdrMode bool
	seq     uint64

	BootstrapState bootstrapState
}

func NewRedoDispatcher(
	changefeedID common.ChangeFeedID,
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	redoSink *redo.Sink,
	startTs uint64,
	statusesChan chan TableSpanStatusWithSeq,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	schemaID int64,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	filterConfig *eventpb.FilterConfig,
	currentPdTs uint64,
	errCh chan error,
	bdrMode bool,
) *RedoDispatcher {
	dispatcher := &RedoDispatcher{
		changefeedID:          changefeedID,
		id:                    id,
		tableSpan:             tableSpan,
		redoSink:              redoSink,
		startTs:               startTs,
		startTsIsSyncpoint:    false,
		statusesChan:          statusesChan,
		blockStatusesChan:     blockStatusesChan,
		componentStatus:       newComponentStateWithMutex(heartbeatpb.ComponentState_Initializing),
		resolvedTs:            startTs,
		filterConfig:          filterConfig,
		isRemoving:            atomic.Bool{},
		blockEventStatus:      BlockEventStatus{blockPendingEvent: nil},
		tableProgress:         NewTableProgress(),
		schemaID:              schemaID,
		schemaIDToDispatchers: schemaIDToDispatchers,
		resendTaskMap:         newResendTaskMap(),
		creationPDTs:          currentPdTs,
		errCh:                 errCh,
		bdrMode:               bdrMode,
		BootstrapState:        BootstrapFinished,
	}

	addToStatusDynamicStream(dispatcher)
	return dispatcher
}

// HandleDispatcherStatus is used to handle the dispatcher status from the Maintainer to deal with the block event.
// Each dispatcher status may contain an ACK info or a dispatcher action or both.
// If we get an ack info, we need to check whether the ack is for the ddl event in resend task map. If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event. If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream.
// 2. If the action is a pass, we just need to pass the event
func (rd *RedoDispatcher) HandleDispatcherStatus(dispatcherStatus *heartbeatpb.DispatcherStatus) {
	log.Info("dispatcher handle dispatcher status",
		zap.Any("dispatcherStatus", dispatcherStatus),
		zap.Stringer("dispatcher", rd.id),
		zap.Any("action", dispatcherStatus.GetAction()),
		zap.Any("ack", dispatcherStatus.GetAck()))
	// Step1: deal with the ack info
	ack := dispatcherStatus.GetAck()
	if ack != nil {
		identifier := BlockEventIdentifier{
			CommitTs:    ack.CommitTs,
			IsSyncPoint: ack.IsSyncPoint,
		}
		rd.cancelResendTask(identifier)
	}

	// Step2: deal with the dispatcher action
	action := dispatcherStatus.GetAction()
	if action != nil {
		pendingEvent := rd.blockEventStatus.getEvent()
		if pendingEvent == nil && action.CommitTs > rd.GetResolvedTs() {
			// we have not received the block event, and the action is for the future event, so just ignore
			log.Info("pending event is nil, and the action's commit is larger than dispatchers resolvedTs",
				zap.Uint64("resolvedTs", rd.GetResolvedTs()),
				zap.Uint64("actionCommitTs", action.CommitTs),
				zap.Stringer("dispatcher", rd.id))
			// we have not received the block event, and the action is for the future event, so just ignore
			return
		}
		if rd.blockEventStatus.actionMatchs(action) {
			log.Info("pending event get the action",
				zap.Any("action", action),
				zap.Any("innerAction", int(action.Action)),
				zap.Stringer("dispatcher", rd.id),
				zap.Uint64("pendingEventCommitTs", pendingEvent.GetCommitTs()))
			rd.blockEventStatus.updateBlockStage(heartbeatpb.BlockStage_WRITING)
			pendingEvent.PushFrontFlushFunc(func() {
				// clear blockEventStatus should be before wake ds.
				// otherwise, there may happen:
				// 1. wake ds
				// 2. get new ds and set new pending event
				// 3. clear blockEventStatus(should be the old pending event, but clear the new one)
				rd.blockEventStatus.clear()
			})
			if action.Action == heartbeatpb.Action_Write {
				failpoint.Inject("BlockOrWaitBeforeWrite", nil)
				err := rd.AddBlockEventToSink(pendingEvent)
				if err != nil {
					select {
					case rd.errCh <- err:
					default:
						log.Error("error channel is full, discard error",
							zap.Stringer("changefeedID", rd.changefeedID),
							zap.Stringer("dispatcherID", rd.id),
							zap.Error(err))
					}
					return
				}
				failpoint.Inject("BlockOrWaitReportAfterWrite", nil)
			} else {
				failpoint.Inject("BlockOrWaitBeforePass", nil)
				rd.PassBlockEventToSink(pendingEvent)
				failpoint.Inject("BlockAfterPass", nil)
			}
		}

		// Step3: whether the outdate message or not, we need to return message show we have finished the event.
		rd.blockStatusesChan <- &heartbeatpb.TableSpanBlockStatus{
			ID: rd.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:   true,
				BlockTs:     dispatcherStatus.GetAction().CommitTs,
				IsSyncPoint: dispatcherStatus.GetAction().IsSyncPoint,
				Stage:       heartbeatpb.BlockStage_DONE,
			},
		}
	}
}

// HandleEvents can batch handle events about resolvedTs Event and DML Event.
// While for DDLEvent and SyncPointEvent, they should be handled separately,
// because they are block events.
// We ensure we only will receive one event when it's ddl event or sync point event
// by setting them with different event types in DispatcherEventsHandler.GetType
// When we handle events, we don't have any previous events still in sink.
func (rd *RedoDispatcher) HandleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	// if m.redoMetaManager.Enabled() {
	// 	flushed := m.redoMetaManager.GetFlushedMeta()
	// 	// Use the same example as above, let say there are some events are replicated by cdc:
	// 	// [dml-1(ts=5), dml-2(ts=8), dml-3(ts=11), ddl-1(ts=11), ddl-2(ts=12)].
	// 	// Suppose redoCheckpointTs=10 and ddl-1(ts=11) is executed, the redo apply operation
	// 	// would fail when applying the old data dml-3(ts=11) to a new schmea. Therefore, We
	// 	// need to wait `redoCheckpointTs == ddlCommitTs(ts=11)` before executing ddl-1.
	// 	redoCheckpointReachBarrier = flushed.CheckpointTs == nextDDL.CommitTs

	// 	// If redo is enabled, m.ddlResolvedTs == redoDDLManager.GetResolvedTs(), so we need to
	// 	// wait nextDDL to be written to redo log before executing this DDL.
	// 	redoDDLResolvedTsExceedBarrier = m.ddlResolvedTs >= nextDDL.CommitTs
	// }

	// Only return false when all events are resolvedTs Event.
	block = false
	dmlWakeOnce := &sync.Once{}
	// Dispatcher is ready, handle the events
	for _, dispatcherEvent := range dispatcherEvents {
		log.Debug("redo dispatcher receive all event",
			zap.Stringer("dispatcher", rd.id),
			zap.Any("event", dispatcherEvent.Event))
		failpoint.Inject("HandleEventsSlowly", func() {
			lag := time.Duration(rand.Intn(5000)) * time.Millisecond
			log.Warn("handle events slowly", zap.Duration("lag", lag))
			time.Sleep(lag)
		})

		event := dispatcherEvent.Event
		// Pre-check, make sure the event is not stale
		if event.GetCommitTs() < rd.GetResolvedTs() {
			log.Warn("Received a stale event, should ignore it",
				zap.Uint64("dispatcherResolvedTs", rd.GetResolvedTs()),
				zap.Uint64("eventCommitTs", event.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()),
				zap.Int("eventType", event.GetType()),
				zap.Stringer("dispatcher", rd.id))
			continue
		}

		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			atomic.StoreUint64(&rd.resolvedTs, event.(commonEvent.ResolvedEvent).ResolvedTs)
		case commonEvent.TypeDMLEvent:
			dml := event.(*commonEvent.DMLEvent)
			if dml.Len() == 0 {
				return block
			}
			block = true
			dml.ReplicatingTs = rd.creationPDTs
			dml.AddPostFlushFunc(func() {
				// Considering dml event in sink may be written to downstream not in order,
				// thus, we use tableProgress.Empty() to ensure these events are flushed to downstream completely
				// and wake dynamic stream to handle the next events.
				if rd.tableProgress.Empty() {
					dmlWakeOnce.Do(wakeCallback)
				}
			})
			rd.AddDMLEventToSink(dml)
		case commonEvent.TypeDDLEvent:
			if len(dispatcherEvents) != 1 {
				log.Panic("ddl event should only be singly handled",
					zap.Stringer("dispatcherID", rd.id))
			}
			failpoint.Inject("BlockOrWaitBeforeDealWithDDL", nil)
			block = true
			ddl := event.(*commonEvent.DDLEvent)

			// Some DDL have some problem to sync to downstream, such as rename table with inappropriate filter
			// such as https://docs.pingcap.com/zh/tidb/stable/ticdc-ddl#rename-table-%E7%B1%BB%E5%9E%8B%E7%9A%84-ddl-%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9
			// so we need report the error to maintainer.
			err := ddl.GetError()
			if err != nil {
				select {
				case rd.errCh <- err:
				default:
					log.Error("error channel is full, discard error",
						zap.Stringer("changefeedID", rd.changefeedID),
						zap.Stringer("dispatcherID", rd.id),
						zap.Error(err))
				}
				return
			}
			log.Info("redo dispatcher receive ddl event",
				zap.Stringer("dispatcher", rd.id),
				zap.String("query", ddl.Query),
				zap.Int64("table", ddl.TableID),
				zap.Uint64("commitTs", event.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()))
			ddl.AddPostFlushFunc(func() {
				wakeCallback()
			})
			rd.dealWithBlockEvent(ddl)
		case commonEvent.TypeSyncPointEvent:
		case commonEvent.TypeHandshakeEvent:
			log.Warn("Receive handshake event unexpectedly",
				zap.Stringer("dispatcher", rd.id),
				zap.Any("event", event))
		default:
			log.Panic("Unexpected event type",
				zap.Int("eventType", event.GetType()),
				zap.Stringer("dispatcher", rd.id),
				zap.Uint64("commitTs", event.GetCommitTs()))
		}
	}
	return block
}

func (rd *RedoDispatcher) shouldBlock(event commonEvent.BlockEvent) bool {
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
			if !common.IsCompleteSpan(rd.tableSpan) {
				// if the table is split, even the blockTable only itself, it should block
				return true
			}
			return false
		case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
			return true
		}
	default:
		log.Error("invalid event type", zap.Any("eventType", event.GetType()))
	}
	return false
}

func (rd *RedoDispatcher) dealWithBlockEvent(event commonEvent.BlockEvent) {
	if !rd.shouldBlock(event) {
		ddl, ok := event.(*commonEvent.DDLEvent)
		// a BDR mode cluster, TiCDC can receive DDLs from all roles of TiDB.
		// However, CDC only executes the DDLs from the TiDB that has BDRRolePrimary role.
		if ok && rd.bdrMode && ddl.BDRMode != string(ast.BDRRolePrimary) {
			rd.PassBlockEventToSink(event)
		} else {
			err := rd.AddBlockEventToSink(event)
			if err != nil {
				select {
				case rd.errCh <- err:
				default:
					log.Error("error channel is full, discard error",
						zap.Stringer("changefeedID", rd.changefeedID),
						zap.Stringer("dispatcherID", rd.id),
						zap.Error(err))
				}
				return
			}
		}
		if event.GetNeedAddedTables() != nil || event.GetNeedDroppedTables() != nil {
			message := &heartbeatpb.TableSpanBlockStatus{
				ID: rd.id.ToPB(),
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
				rd.tableProgress.Add(event)
				rd.resendTaskMap.Set(identifier, newResendTask(message, rd, event.PostFlush))
			} else {
				rd.resendTaskMap.Set(identifier, newResendTask(message, rd, nil))
			}
			rd.blockStatusesChan <- message
		}
	} else {
		rd.blockEventStatus.setBlockEvent(event, heartbeatpb.BlockStage_WAITING)
		message := &heartbeatpb.TableSpanBlockStatus{
			ID: rd.id.ToPB(),
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
		rd.resendTaskMap.Set(identifier, newResendTask(message, rd, nil))
		rd.blockStatusesChan <- message
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
	if event.GetUpdatedSchemas() != nil && rd.tableSpan != common.DDLSpan {
		for _, schemaIDChange := range event.GetUpdatedSchemas() {
			if schemaIDChange.TableID == rd.tableSpan.TableID {
				if schemaIDChange.OldSchemaID != rd.schemaID {
					log.Error("Wrong Schema ID",
						zap.Stringer("dispatcherID", rd.id),
						zap.Int64("exceptSchemaID", schemaIDChange.OldSchemaID),
						zap.Int64("actualSchemaID", rd.schemaID),
						zap.String("tableSpan", common.FormatTableSpan(rd.tableSpan)))
					return
				} else {
					rd.schemaID = schemaIDChange.NewSchemaID
					rd.schemaIDToDispatchers.Update(schemaIDChange.OldSchemaID, schemaIDChange.NewSchemaID)
					return
				}
			}
		}
	}
}

func (rd *RedoDispatcher) cancelResendTask(identifier BlockEventIdentifier) {
	task := rd.resendTaskMap.Get(identifier)
	if task == nil {
		return
	}

	task.Cancel()
	rd.resendTaskMap.Delete(identifier)
}

func (rd *RedoDispatcher) AddDMLEventToSink(event *commonEvent.DMLEvent) {
	rd.tableProgress.Add(event)
	rd.redoSink.AddDMLEvent(event)
}

func (rd *RedoDispatcher) AddBlockEventToSink(event commonEvent.BlockEvent) error {
	rd.tableProgress.Add(event)
	return rd.redoSink.WriteBlockEvent(event)
}

func (rd *RedoDispatcher) PassBlockEventToSink(event commonEvent.BlockEvent) {
	rd.tableProgress.Pass(event)
	event.PostFlush()
}

func (rd *RedoDispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return rd.tableSpan
}

func (rd *RedoDispatcher) GetStartTs() uint64 {
	return rd.startTs
}

func (rd *RedoDispatcher) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&rd.resolvedTs)
}

func (rd *RedoDispatcher) GetCheckpointTs() uint64 {
	checkpointTs, isEmpty := rd.tableProgress.GetCheckpointTs()
	if checkpointTs == 0 {
		// This means the dispatcher has never send events to the sink,
		// so we use resolvedTs as checkpointTs
		return rd.GetResolvedTs()
	}
	if isEmpty {
		return max(checkpointTs, rd.GetResolvedTs())
	}
	return checkpointTs
}

func (rd *RedoDispatcher) GetId() common.DispatcherID {
	return rd.id
}

func (rd *RedoDispatcher) GetChangefeedID() common.ChangeFeedID {
	return rd.changefeedID
}

func (rd *RedoDispatcher) GetSchemaID() int64 {
	return rd.schemaID
}

func (rd *RedoDispatcher) GetFilterConfig() *eventpb.FilterConfig {
	return rd.filterConfig
}

func (rd *RedoDispatcher) Remove() {
	log.Info("redo table event dispatcher component status changed to stopping",
		zap.Stringer("changefeedID", rd.changefeedID),
		zap.Stringer("dispatcher", rd.id),
		zap.String("table", common.FormatTableSpan(rd.tableSpan)),
		zap.Uint64("checkpointTs", rd.GetCheckpointTs()),
		zap.Uint64("resolvedTs", rd.GetResolvedTs()),
	)
	rd.isRemoving.Store(true)
}

func (rd *RedoDispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	// If sink is normal(not meet error), we need to wait all the events in sink to flushed downstream successfully.
	// If sink is not normal, we can close the dispatcher immediately.
	if rd.tableProgress.Empty() {
		w.CheckpointTs = rd.GetCheckpointTs()
		w.ResolvedTs = rd.GetResolvedTs()

		rd.componentStatus.Set(heartbeatpb.ComponentState_Stopped)
		return w, true
	}
	return w, false
}

func (rd *RedoDispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return rd.componentStatus.Get()
}

func (rd *RedoDispatcher) GetRemovingStatus() bool {
	return rd.isRemoving.Load()
}

func (rd *RedoDispatcher) GetBlockEventStatus() *heartbeatpb.State {
	return nil
}

func (rd *RedoDispatcher) GetEventSizePerSecond() float32 {
	return rd.tableProgress.GetEventSizePerSecond()
}

func (rd *RedoDispatcher) IsTableTriggerEventDispatcher() bool {
	return rd.tableSpan == common.DDLSpan
}

func (rd *RedoDispatcher) SetSeq(seq uint64) {
	rd.seq = seq
}

func (rd *RedoDispatcher) GetBDRMode() bool {
	return rd.bdrMode
}

func (rd *RedoDispatcher) EnableSyncPoint() bool {
	return false
}

func (rd *RedoDispatcher) GetStartTsIsSyncpoint() bool {
	return false
}

func (rd *RedoDispatcher) GetSyncPointInterval() time.Duration {
	return 0
}

func (rd *RedoDispatcher) GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus {
	return rd.blockStatusesChan
}

func (rd *RedoDispatcher) GetType() int {
	return TypeDispatcherRedo
}
