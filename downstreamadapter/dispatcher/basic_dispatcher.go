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
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DispatcherService defines the interface for providing dispatcher information and basic event handling.
type DispatcherService interface {
	GetId() common.DispatcherID
	GetMode() int64
	GetStartTs() uint64
	GetBDRMode() bool
	GetChangefeedID() common.ChangeFeedID
	GetTableSpan() *heartbeatpb.TableSpan
	GetTimezone() string
	GetIntegrityConfig() *eventpb.IntegrityConfig
	GetFilterConfig() *eventpb.FilterConfig
	EnableSyncPoint() bool
	GetSyncPointInterval() time.Duration
	GetSkipSyncpointAtStartTs() bool
	GetTxnAtomicity() config.AtomicityLevel
	GetResolvedTs() uint64
	GetCheckpointTs() uint64
	HandleEvents(events []DispatcherEvent, wakeCallback func()) (block bool)
	IsOutputRawChangeEvent() bool
}

// Dispatcher defines the interface for event dispatchers that are responsible for receiving events
// from EventCollector and dispatching them to Sink components. It extends DispatcherService with
// additional lifecycle management and capabilities for handling block events (DDL/SyncPoint)
type Dispatcher interface {
	DispatcherService
	GetSchemaID() int64
	HandleDispatcherStatus(*heartbeatpb.DispatcherStatus)
	HandleError(err error)
	SetSeq(seq uint64)
	SetStartTs(startTs uint64)
	SetCurrentPDTs(currentPDTs uint64)
	SetSkipSyncpointAtStartTs(skipSyncpointAtStartTs bool)
	SetSkipDMLAsStartTs(skipDMLAsStartTs bool)
	SetComponentStatus(status heartbeatpb.ComponentState)
	GetRemovingStatus() bool
	GetTryRemoving() bool
	SetTryRemoving()
	GetHeartBeatInfo(h *HeartBeatInfo)
	GetComponentStatus() heartbeatpb.ComponentState
	GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus
	GetEventSizePerSecond() float32
	IsTableTriggerEventDispatcher() bool
	DealWithBlockEvent(event commonEvent.BlockEvent)
	TryClose() (w heartbeatpb.Watermark, ok bool)
	Remove()
}

/*
BasicDispatcher is responsible for getting events from Event Service and sending them to Sink in appropriate order.
Each dispatcher only deal with the events of one tableSpan in one changefeed.
Each DispatcherManager will have multiple dispatchers.

All dispatchers will communicate with the Maintainer about self progress and whether can push down the blocked ddl event.

Because Sink does not flush events to the downstream in strict order.
the dispatcher can't send event to Sink continuously all the time,
1. The ddl event/sync point event can be sent to Sink only when the previous event has been flushed to downstream successfully.
2. Only when the ddl event/sync point event is flushed to downstream successfully, the dispatcher can send the following event to Sink.
3. For the cross table ddl event/sync point event, dispatcher needs to negotiate with the maintainer to decide whether and when send it to Sink.

The workflow related to the dispatcher is as follows:

	+--------------+       +----------------+       +------------+       +--------+        +------------+
	| EventService |  -->  | EventCollector |  -->  | Dispatcher |  -->  |  Sink  |  -->   | Downstream |
	+--------------+       +----------------+       +------------+       +--------+        +------------+
	                                                        |
										  HeartBeatResponse | HeartBeatRequest
										   DispatcherStatus | BlockStatus
	                                              +--------------------+
	                                              | HeartBeatCollector |
												  +--------------------+
												            |
															|
												      +------------+
	                                                  | Maintainer |
												      +------------+
*/

type BasicDispatcher struct {
	id       common.DispatcherID
	schemaID int64

	tableSpan *heartbeatpb.TableSpan
	// isCompleteTable indicates whether this dispatcher is responsible for a complete table
	// or just a part of the table (span). When true, the dispatcher handles the entire table;
	// when false, it only handles a portion of the table.
	isCompleteTable bool

	// startTs is the timestamp that the dispatcher need to receive and flush events.
	startTs uint64

	// skipSyncpointAtStartTs is used to determine whether we need to skip the syncpoint event which is same as the startTs
	// skipSyncpointAtStartTs only maybe true in MysqlSink.
	// it's used to deal with the corner case when ddl commitTs is same as the syncpointTs commitTs
	// For example, syncpointInterval = 10, ddl commitTs = 20, syncpointTs = 20
	// case 1: ddl and syncpoint is flushed successfully, and then restart --> startTs = 20, skipSyncpointAtStartTs = true
	// case 2: ddl is flushed successfully, syncpointTs not and then restart --> startTs = 20, skipSyncpointAtStartTs = false --> receive syncpoint first
	skipSyncpointAtStartTs bool
	// skipDMLAsStartTs indicates whether to skip DML events at startTs+1 timestamp.
	// When true, the dispatcher should filter out DML events with commitTs == startTs+1, but keep DDL events.
	// This flag is set to true ONLY when is_syncpoint=false AND finished=0 in ddl-ts table (non-syncpoint DDL not finished).
	// In this case, we return startTs = ddlTs-1 to replay the DDL, and skip the already-written DML at ddlTs
	// to avoid duplicate writes while ensuring the DDL is replayed.
	// Note: When is_syncpoint=true AND finished=0 (DDL finished but syncpoint not finished),
	// skipDMLAsStartTs is false because the DDL is already completed and DML should be processed normally.
	skipDMLAsStartTs bool
	// The ts from pdClock when the dispatcher is created.
	// when downstream is mysql-class, for dml event we need to compare the commitTs with this ts
	// to determine whether the insert event should use `Replace` or just `Insert`
	// Because when the dispatcher scheduled or the node restarts, there may be some dml events to receive twice.
	// So we need to use `Replace` to avoid duplicate key error.
	// Table Trigger Event Dispatcher doesn't need this, because it doesn't deal with dml events.
	creationPDTs uint64
	// componentStatus is the status of the dispatcher, such as working, removing, stopped.
	componentStatus *ComponentStateWithMutex

	// schemaIDToDispatchers is shared in the DispatcherManager
	schemaIDToDispatchers *SchemaIDToDispatchers

	// Shared info containing all common configuration and resources
	sharedInfo *SharedInfo

	// sink is the sink for this dispatcher
	sink sink.Sink

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

	// tableSchemaStore only exist when the dispatcher is a table trigger event dispatcher
	// tableSchemaStore store the schema infos for all the table in the event dispatcher manager
	// it's used for sink to calculate the tableNames or TableIds
	tableSchemaStore *util.TableSchemaStore

	// try to remove the dispatcher, but dispatcher may not able to be removed now
	tryRemoving atomic.Bool
	// is able to remove, and removing now
	isRemoving atomic.Bool
	// duringHandleEvents is used to indicate whether the dispatcher is currently handling events.
	// This field prevents a race condition where TryClose is called while events are being processed.
	// In this corner case, `tableProgress` might be empty, which could lead to the dispatcher being removed prematurely.
	duringHandleEvents atomic.Bool

	seq  uint64
	mode int64

	BootstrapState bootstrapState
}

func NewBasicDispatcher(
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	startTs uint64,
	schemaID int64,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	skipSyncpointAtStartTs bool,
	skipDMLAsStartTs bool,
	currentPDTs uint64,
	mode int64,
	sink sink.Sink,
	sharedInfo *SharedInfo,
) *BasicDispatcher {
	dispatcher := &BasicDispatcher{
		id:                     id,
		tableSpan:              tableSpan,
		isCompleteTable:        common.IsCompleteSpan(tableSpan),
		startTs:                startTs,
		skipSyncpointAtStartTs: skipSyncpointAtStartTs,
		skipDMLAsStartTs:       skipDMLAsStartTs,
		sharedInfo:             sharedInfo,
		sink:                   sink,
		componentStatus:        newComponentStateWithMutex(heartbeatpb.ComponentState_Initializing),
		resolvedTs:             startTs,
		isRemoving:             atomic.Bool{},
		duringHandleEvents:     atomic.Bool{},
		blockEventStatus:       BlockEventStatus{blockPendingEvent: nil},
		tableProgress:          NewTableProgress(),
		schemaID:               schemaID,
		schemaIDToDispatchers:  schemaIDToDispatchers,
		resendTaskMap:          newResendTaskMap(),
		creationPDTs:           currentPDTs,
		mode:                   mode,
		BootstrapState:         BootstrapFinished,
	}

	return dispatcher
}

func (d *BasicDispatcher) AddDMLEventsToSink(events []*commonEvent.DMLEvent) {
	// for one batch events, we need to add all them in table progress first, then add them to sink
	// because we need to ensure the wakeCallback only will be called when
	// all these events are flushed to downstream successfully
	for _, event := range events {
		d.tableProgress.Add(event)
	}
	for _, event := range events {
		d.sink.AddDMLEvent(event)
		failpoint.Inject("BlockAddDMLEvents", nil)
	}
}

func (d *BasicDispatcher) AddBlockEventToSink(event commonEvent.BlockEvent) error {
	// For ddl event, we need to check whether it should be sent to downstream.
	// It may be marked as not sync by filter when building the event.
	if event.GetType() == commonEvent.TypeDDLEvent {
		ddl := event.(*commonEvent.DDLEvent)
		// If NotSync is true, it means the DDL should not be sent to downstream.
		// So we just call PassBlockEventToSink to update the table progress and call the postFlush func.
		if ddl.NotSync {
			log.Info("ignore DDL by NotSync", zap.Stringer("dispatcher", d.id), zap.Any("ddl", ddl))
			d.PassBlockEventToSink(event)
			return nil
		}
	}
	d.tableProgress.Add(event)
	return d.sink.WriteBlockEvent(event)
}

func (d *BasicDispatcher) PassBlockEventToSink(event commonEvent.BlockEvent) {
	d.tableProgress.Pass(event)
	event.PostFlush()
}

func (d *BasicDispatcher) isFirstEvent(event commonEvent.Event) bool {
	if d.componentStatus.Get() == heartbeatpb.ComponentState_Initializing {
		switch event.GetType() {
		case commonEvent.TypeResolvedEvent, commonEvent.TypeDMLEvent, commonEvent.TypeDDLEvent:
			if event.GetCommitTs() > d.startTs {
				return true
			}
		// the first syncpoint event can be same as startTs
		case commonEvent.TypeSyncPointEvent:
			if event.GetCommitTs() >= d.startTs {
				return true
			}
		}
	}
	return false
}

func (d *BasicDispatcher) GetHeartBeatInfo(h *HeartBeatInfo) {
	h.Watermark.CheckpointTs = d.GetCheckpointTs()
	h.Watermark.ResolvedTs = d.GetResolvedTs()
	h.Watermark.LastSyncedTs = d.GetLastSyncedTs()
	h.Id = d.GetId()
	h.ComponentStatus = d.GetComponentStatus()
	h.IsRemoving = d.GetRemovingStatus()
}

func (d *BasicDispatcher) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&d.resolvedTs)
}

func (d *BasicDispatcher) GetLastSyncedTs() uint64 {
	return d.tableProgress.GetLastSyncedTs()
}

func (d *BasicDispatcher) GetCheckpointTs() uint64 {
	checkpointTs, isEmpty := d.tableProgress.GetCheckpointTs()
	if checkpointTs == 0 {
		// This means the dispatcher has never send events to the sink,
		// so we use resolvedTs as checkpointTs
		return d.GetResolvedTs()
	}

	if isEmpty {
		return max(checkpointTs, d.GetResolvedTs())
	}
	return checkpointTs
}

// updateDispatcherStatusToWorking updates the dispatcher status to working and adds it to status dynamic stream
func (d *BasicDispatcher) updateDispatcherStatusToWorking() {
	log.Info("update dispatcher status to working",
		zap.Stringer("dispatcher", d.id),
		zap.Stringer("changefeedID", d.sharedInfo.changefeedID),
		zap.String("table", common.FormatTableSpan(d.tableSpan)),
		zap.Uint64("checkpointTs", d.GetCheckpointTs()),
		zap.Uint64("resolvedTs", d.GetResolvedTs()),
	)
	// only when we receive the first event, we can regard the dispatcher begin syncing data
	// then add it to status dynamic stream to receive dispatcher status from maintainer
	addToStatusDynamicStream(d)
	// set the dispatcher to working status
	d.componentStatus.Set(heartbeatpb.ComponentState_Working)
	d.sharedInfo.statusesChan <- TableSpanStatusWithSeq{
		TableSpanStatus: &heartbeatpb.TableSpanStatus{
			ID:              d.id.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    d.GetCheckpointTs(),
			Mode:            d.GetMode(),
		},
		Seq: d.seq,
	}
}

func (d *BasicDispatcher) HandleError(err error) {
	select {
	case d.sharedInfo.errCh <- err:
	default:
		log.Error("error channel is full, discard error",
			zap.Stringer("changefeedID", d.sharedInfo.changefeedID),
			zap.Stringer("dispatcherID", d.id),
			zap.Error(err))
	}
}

func (d *BasicDispatcher) HandleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	log.Panic("should not call this")
	return false
}

// handleEvents can batch handle events about resolvedTs Event and DML Event.
// While for DDLEvent and SyncPointEvent, they should be handled separately,
// because they are block events.
// We ensure we only will receive one event when it's ddl event or sync point event
// by setting them with different event types in DispatcherEventsHandler.GetType
// When we handle events, we don't have any previous events still in sink.
//
// wakeCallback is used to wake the dynamic stream to handle the next batch events.
// It will be called when all the events are flushed to downstream successfully.
func (d *BasicDispatcher) handleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) bool {
	if d.GetRemovingStatus() {
		log.Warn("dispatcher is removing", zap.Any("id", d.id))
		return true
	}

	d.duringHandleEvents.Store(true)
	defer d.duringHandleEvents.Store(false)

	// Only return false when all events are resolvedTs Event.
	block := false
	dmlWakeOnce := &sync.Once{}
	dmlEvents := make([]*commonEvent.DMLEvent, 0, len(dispatcherEvents))
	latestResolvedTs := uint64(0)
	// Dispatcher is ready, handle the events
	for _, dispatcherEvent := range dispatcherEvents {
		if log.GetLevel() == zapcore.DebugLevel {
			log.Debug("dispatcher receive all event",
				zap.Stringer("dispatcher", d.id), zap.Int64("mode", d.mode),
				zap.String("eventType", commonEvent.TypeToString(dispatcherEvent.Event.GetType())),
				zap.Any("event", dispatcherEvent.Event))
		}

		failpoint.Inject("HandleEventsSlowly", func() {
			lag := time.Duration(rand.Intn(5000)) * time.Millisecond
			log.Warn("handle events slowly", zap.Duration("lag", lag))
			time.Sleep(lag)
		})

		event := dispatcherEvent.Event
		// Pre-check, make sure the event is not stale
		if event.GetCommitTs() < d.GetResolvedTs() {
			log.Warn("Received a stale event, should ignore it",
				zap.Uint64("dispatcherResolvedTs", d.GetResolvedTs()),
				zap.Uint64("eventCommitTs", event.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()),
				zap.Int("eventType", event.GetType()),
				zap.Stringer("dispatcher", d.id))
			continue
		}

		// only when we receive the first event, we can regard the dispatcher begin syncing data
		// then turning into working status.
		if d.isFirstEvent(event) {
			d.updateDispatcherStatusToWorking()
		}

		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			latestResolvedTs = event.(commonEvent.ResolvedEvent).ResolvedTs
		case commonEvent.TypeDMLEvent:
			dml := event.(*commonEvent.DMLEvent)
			if dml.Len() == 0 {
				continue
			}

			// Skip DML events at startTs+1 when skipDMLAsStartTs is true.
			// This handles the corner case where a DDL at ts=X crashed after writing DML but before marking finished.
			// We return startTs=X-1 to replay the DDL, but need to skip the already-written DML at ts=X (startTs+1).
			if d.skipDMLAsStartTs && event.GetCommitTs() == d.startTs+1 {
				log.Info("skip DML event at startTs+1 due to DDL crash recovery",
					zap.Stringer("dispatcher", d.id),
					zap.Uint64("startTs", d.startTs),
					zap.Uint64("dmlCommitTs", event.GetCommitTs()),
					zap.Uint64("seq", event.GetSeq()))
				continue
			}

			block = true
			dml.ReplicatingTs = d.creationPDTs
			dml.AddPostFlushFunc(func() {
				// Considering dml event in sink may be written to downstream not in order,
				// thus, we use tableProgress.Empty() to ensure these events are flushed to downstream completely
				// and wake dynamic stream to handle the next events.
				if d.tableProgress.Empty() {
					dmlWakeOnce.Do(wakeCallback)
				}
			})
			dmlEvents = append(dmlEvents, dml)
		case commonEvent.TypeDDLEvent:
			if len(dispatcherEvents) != 1 {
				log.Panic("ddl event should only be singly handled",
					zap.Stringer("dispatcherID", d.id))
			}
			failpoint.Inject("BlockOrWaitBeforeDealWithDDL", nil)
			block = true
			ddl := event.(*commonEvent.DDLEvent)

			// Some DDL have some problem to sync to downstream, such as rename table with inappropriate filter
			// such as https://docs.pingcap.com/zh/tidb/stable/ticdc-ddl#rename-table-%E7%B1%BB%E5%9E%8B%E7%9A%84-ddl-%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9
			// so we need report the error to maintainer.
			err := ddl.GetError()
			if err != nil {
				d.HandleError(err)
				return block
			}
			// if the dispatcher is not for a complete table,
			// we need to check whether the ddl event will break the splittability of this table
			// if it breaks, we need to report the error to the maintainer.
			if !d.isCompleteTable {
				if !commonEvent.IsSplitable(ddl.TableInfo) && d.sharedInfo.enableSplittableCheck {
					d.HandleError(errors.ErrTableAfterDDLNotSplitable.GenWithStackByArgs("unexpected ddl event; This ddl event will break splitable of this table. Only table with pk and no uk can be split."))
					return block
				}
			}

			log.Info("dispatcher receive ddl event",
				zap.Stringer("dispatcher", d.id),
				zap.String("query", ddl.Query),
				zap.Int64("table", ddl.GetTableID()),
				zap.Uint64("commitTs", event.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()))
			ddl.AddPostFlushFunc(func() {
				if d.tableSchemaStore != nil {
					d.tableSchemaStore.AddEvent(ddl)
				}
				wakeCallback()
			})
			d.sharedInfo.GetBlockEventExecutor().Submit(d, ddl)
		case commonEvent.TypeSyncPointEvent:
			if common.IsRedoMode(d.GetMode()) {
				continue
			}
			if len(dispatcherEvents) != 1 {
				log.Panic("sync point event should only be singly handled",
					zap.Stringer("dispatcherID", d.id))
			}
			block = true
			syncPoint := event.(*commonEvent.SyncPointEvent)
			log.Info("dispatcher receive sync point event",
				zap.Stringer("dispatcher", d.id),
				zap.Uint64("commitTs", syncPoint.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()))

			syncPoint.AddPostFlushFunc(func() {
				wakeCallback()
			})
			d.sharedInfo.GetBlockEventExecutor().Submit(d, syncPoint)
		case commonEvent.TypeHandshakeEvent:
			log.Warn("Receive handshake event unexpectedly",
				zap.Stringer("dispatcher", d.id),
				zap.Any("event", event))
		default:
			log.Panic("Unexpected event type",
				zap.Int("eventType", event.GetType()),
				zap.Stringer("dispatcher", d.id),
				zap.Uint64("commitTs", event.GetCommitTs()))
		}
	}
	// resolvedTs and dml events can be in the same batch,
	// We need to update the resolvedTs after all the dml events are added to sink.
	//
	// If resolvedTs updated first, and then dml events are added to sink,
	// the checkpointTs may be incorrect set as the new resolvedTs,
	// due to the tableProgress is empty before dml events add into sink.
	if len(dmlEvents) > 0 {
		d.AddDMLEventsToSink(dmlEvents)
	}
	if latestResolvedTs > 0 {
		atomic.StoreUint64(&d.resolvedTs, latestResolvedTs)
	}
	return block
}

// HandleDispatcherStatus is used to handle the dispatcher status from the Maintainer to deal with the block event.
// Each dispatcher status may contain an ACK info or a dispatcher action or both.
// If we get an ack info, we need to check whether the ack is for the ddl event in resend task map. If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event. If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream.
// 2. If the action is a pass, we just need to pass the event
func (d *BasicDispatcher) HandleDispatcherStatus(dispatcherStatus *heartbeatpb.DispatcherStatus) {
	log.Debug("dispatcher handle dispatcher status",
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
			log.Debug("pending event is nil, and the action's commit is larger than dispatchers resolvedTs",
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
					d.HandleError(err)
					return
				}
				failpoint.Inject("BlockOrWaitReportAfterWrite", nil)
			} else {
				failpoint.Inject("BlockOrWaitBeforePass", nil)
				d.PassBlockEventToSink(pendingEvent)
				failpoint.Inject("BlockAfterPass", nil)
			}
		} else {
			ts, ok := d.blockEventStatus.getEventCommitTs()
			if ok && action.CommitTs > ts {
				log.Debug("pending event's commitTs is smaller than the action's commitTs, just ignore it",
					zap.Uint64("pendingEventCommitTs", ts),
					zap.Uint64("actionCommitTs", action.CommitTs),
					zap.Stringer("dispatcher", d.id))
				return
			}
		}

		// Step3: whether the outdate message or not, we need to return message show we have finished the event.
		d.sharedInfo.blockStatusesChan <- &heartbeatpb.TableSpanBlockStatus{
			ID: d.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:   true,
				BlockTs:     dispatcherStatus.GetAction().CommitTs,
				IsSyncPoint: dispatcherStatus.GetAction().IsSyncPoint,
				Stage:       heartbeatpb.BlockStage_DONE,
			},
			Mode: d.GetMode(),
		}
	}
}

// shouldBlock check whether the event should be blocked(to wait maintainer response)
// For the ddl event with more than one blockedTable, it should block.
// For the ddl event with only one blockedTable, it should block only if the table is not complete span.
// Sync point event should always block.
func (d *BasicDispatcher) shouldBlock(event commonEvent.BlockEvent) bool {
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
			if !d.isCompleteTable {
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
func (d *BasicDispatcher) DealWithBlockEvent(event commonEvent.BlockEvent) {
	if !d.shouldBlock(event) {
		err := d.AddBlockEventToSink(event)
		if err != nil {
			d.HandleError(err)
			return
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
				Mode: d.GetMode(),
			}
			identifier := BlockEventIdentifier{
				CommitTs:    event.GetCommitTs(),
				IsSyncPoint: false,
			}

			if event.GetNeedAddedTables() != nil {
				// When the ddl need add tables, we need the maintainer to block the forwarding of checkpointTs
				// Because the new add table should join the calculation of checkpointTs
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
			d.sharedInfo.blockStatusesChan <- message
		}
	} else {
		d.blockEventStatus.setBlockEvent(event, heartbeatpb.BlockStage_WAITING)

		message := &heartbeatpb.TableSpanBlockStatus{
			ID: d.id.ToPB(),
			State: &heartbeatpb.State{
				IsBlocked:         true,
				BlockTs:           event.GetCommitTs(),
				BlockTables:       event.GetBlockedTables().ToPB(),
				NeedDroppedTables: event.GetNeedDroppedTables().ToPB(),
				NeedAddedTables:   commonEvent.ToTablesPB(event.GetNeedAddedTables()),
				UpdatedSchemas:    commonEvent.ToSchemaIDChangePB(event.GetUpdatedSchemas()),
				IsSyncPoint:       event.GetType() == commonEvent.TypeSyncPointEvent,
				Stage:             heartbeatpb.BlockStage_WAITING,
			},
			Mode: d.GetMode(),
		}
		identifier := BlockEventIdentifier{
			CommitTs:    event.GetCommitTs(),
			IsSyncPoint: event.GetType() == commonEvent.TypeSyncPointEvent,
		}
		d.resendTaskMap.Set(identifier, newResendTask(message, d, nil))
		d.sharedInfo.blockStatusesChan <- message
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
	if event.GetUpdatedSchemas() != nil && !d.IsTableTriggerEventDispatcher() {
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

func (d *BasicDispatcher) cancelResendTask(identifier BlockEventIdentifier) {
	task := d.resendTaskMap.Get(identifier)
	if task == nil {
		return
	}

	task.Cancel()
	d.resendTaskMap.Delete(identifier)
}

func (d *BasicDispatcher) GetBlockEventStatus() *heartbeatpb.State {
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

func (d *BasicDispatcher) Remove() {
	log.Panic("should not call this")
}

// TryClose should be called before Remove(), because the dispatcher may still wait the dispatcher status from maintainer.
// TryClose will return the watermark of current dispatcher, and return true when the dispatcher finished sending events to sink.
// DispatcherManager will clean the dispatcher info after Remove() is called.
func (d *BasicDispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	// If sink is normal(not meet error), we need to wait all the events in sink to flushed downstream successfully
	// If sink is not normal, we can close the dispatcher immediately.
	if !d.sink.IsNormal() || (d.tableProgress.Empty() && !d.duringHandleEvents.Load()) {
		w.CheckpointTs = d.GetCheckpointTs()
		w.ResolvedTs = d.GetResolvedTs()

		if d.IsTableTriggerEventDispatcher() && d.tableSchemaStore != nil {
			d.tableSchemaStore.Clear()
		}
		log.Info("dispatcher component has stopped and is ready for cleanup",
			zap.Stringer("changefeedID", d.sharedInfo.changefeedID),
			zap.Stringer("dispatcher", d.id),
			zap.Int64("mode", d.mode),
			zap.String("table", common.FormatTableSpan(d.tableSpan)),
			zap.Uint64("checkpointTs", d.GetCheckpointTs()),
			zap.Uint64("resolvedTs", d.GetResolvedTs()),
		)
		return w, true
	}
	log.Info("dispatcher is not ready to close",
		zap.Stringer("dispatcher", d.id),
		zap.Int64("mode", d.mode),
		zap.Bool("sinkIsNormal", d.sink.IsNormal()),
		zap.Bool("tableProgressEmpty", d.tableProgress.Empty()),
		zap.Int("tableProgressLen", d.tableProgress.Len()),
		zap.Uint64("tableProgressMaxCommitTs", d.tableProgress.MaxCommitTs())) // check whether continue receive new events.
	return w, false
}

// It removes the dispatcher from status dynamic stream to stop receiving status info from maintainer.
func (d *BasicDispatcher) removeDispatcher() {
	log.Info("remove dispatcher",
		zap.Stringer("dispatcher", d.id),
		zap.Int64("mode", d.mode),
		zap.Stringer("changefeedID", d.sharedInfo.changefeedID),
		zap.String("table", common.FormatTableSpan(d.tableSpan)))
	dispatcherStatusDS := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDS.RemovePath(d.id)
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed",
			zap.Stringer("changefeedID", d.sharedInfo.changefeedID),
			zap.Stringer("dispatcher", d.id),
			zap.String("table", common.FormatTableSpan(d.tableSpan)),
			zap.Uint64("checkpointTs", d.GetCheckpointTs()),
			zap.Uint64("resolvedTs", d.GetResolvedTs()),
			zap.Error(err))
	}

	// remove unfinished resend task
	identifiers := d.resendTaskMap.Keys()
	for _, identifier := range identifiers {
		d.cancelResendTask(identifier)
		log.Info("cancel resend task before remove dispatcher",
			zap.Any("identifier", identifier),
			zap.Stringer("dispatcherID", d.id))
	}
}
