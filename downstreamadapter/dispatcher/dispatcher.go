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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

var _ EventDispatcher = (*Dispatcher)(nil)

// EventDispatcher is the interface that responsible for receiving events from Event Service
type EventDispatcher interface {
	GetId() common.DispatcherID
	GetStartTs() uint64
	GetBDRMode() bool
	GetChangefeedID() common.ChangeFeedID
	GetTableSpan() *heartbeatpb.TableSpan
	GetTimezone() string
	GetIntegrityConfig() *eventpb.IntegrityConfig
	GetFilterConfig() *eventpb.FilterConfig
	EnableSyncPoint() bool
	GetSyncPointInterval() time.Duration
	GetStartTsIsSyncpoint() bool
	GetResolvedTs() uint64
	GetCheckpointTs() uint64
	HandleEvents(events []DispatcherEvent, wakeCallback func()) (block bool)
}

/*
Dispatcher is responsible for getting events from Event Service and sending them to Sink in appropriate order.
Each dispatcher only deal with the events of one tableSpan in one changefeed.
Here is a special dispatcher will deal with the events of the DDLSpan in one changefeed, we call it TableTriggerEventDispatcher
One changefeed across multiple nodes only will have one TableTriggerEventDispatcher.
Each EventDispatcherManager will have multiple dispatchers.

All dispatchers in the changefeed will share the same Sink.
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

type Dispatcher struct {
	changefeedID common.ChangeFeedID
	id           common.DispatcherID
	schemaID     int64
	tableSpan    *heartbeatpb.TableSpan
	// startTs is the timestamp that the dispatcher need to receive and flush events.
	startTs            uint64
	startTsIsSyncpoint bool
	// The ts from pdClock when the dispatcher is created.
	// when downstream is mysql-class, for dml event we need to compare the commitTs with this ts
	// to determine whether the insert event should use `Replace` or just `Insert`
	// Because when the dispatcher scheduled or the node restarts, there may be some dml events to receive twice.
	// So we need to use `Replace` to avoid duplicate key error.
	// Table Trigger Event Dispatcher doesn't need this, because it doesn't deal with dml events.
	creationPDTs uint64
	// componentStatus is the status of the dispatcher, such as working, removing, stopped.
	componentStatus *ComponentStateWithMutex
	// the config of filter
	filterConfig *eventpb.FilterConfig

	// shared by the event dispatcher manager
	sink sink.Sink

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

	timezone        string
	integrityConfig *eventpb.IntegrityConfig

	// if syncPointInfo is not nil, means enable Sync Point feature,
	syncPointConfig *syncpoint.SyncPointConfig

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

	isRemoving atomic.Bool

	// errCh is used to collect the errors that need to report to maintainer
	// such as error of flush ddl events
	// errCh is shared in the eventDispatcherManager
	errCh chan error

	bdrMode bool
	seq     uint64

	BootstrapState bootstrapState
}

func NewDispatcher(
	changefeedID common.ChangeFeedID,
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	sink sink.Sink,
	startTs uint64,
	statusesChan chan TableSpanStatusWithSeq,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	schemaID int64,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	timezone string,
	integrityConfig *eventpb.IntegrityConfig,
	syncPointConfig *syncpoint.SyncPointConfig,
	startTsIsSyncpoint bool,
	filterConfig *eventpb.FilterConfig,
	currentPdTs uint64,
	errCh chan error,
	bdrMode bool,
) *Dispatcher {
	dispatcher := &Dispatcher{
		changefeedID:          changefeedID,
		id:                    id,
		tableSpan:             tableSpan,
		sink:                  sink,
		startTs:               startTs,
		startTsIsSyncpoint:    startTsIsSyncpoint,
		statusesChan:          statusesChan,
		blockStatusesChan:     blockStatusesChan,
		timezone:              timezone,
		integrityConfig:       integrityConfig,
		syncPointConfig:       syncPointConfig,
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

	return dispatcher
}

// InitializeTableSchemaStore initializes the tableSchemaStore for the table trigger event dispatcher.
// It returns true if the tableSchemaStore is initialized successfully, otherwise returns false.
func (d *Dispatcher) InitializeTableSchemaStore(schemaInfo []*heartbeatpb.SchemaInfo) (ok bool, err error) {
	// Only the table trigger event dispatcher need to create a tableSchemaStore
	// Because we only need to calculate the tableNames or TableIds in the sink
	// when the event dispatcher manager have table trigger event dispatcher
	if !d.tableSpan.Equal(common.DDLSpan) {
		log.Error("InitializeTableSchemaStore should only be received by table trigger event dispatcher", zap.Any("dispatcher", d.id))
		return false, apperror.ErrChangefeedInitTableTriggerEventDispatcherFailed.
			GenWithStackByArgs("InitializeTableSchemaStore should only be received by table trigger event dispatcher")
	}

	if d.tableSchemaStore != nil {
		log.Info("tableSchemaStore has already been initialized", zap.Stringer("dispatcher", d.id))
		return false, nil
	}

	d.tableSchemaStore = util.NewTableSchemaStore(schemaInfo, d.sink.SinkType())
	d.sink.SetTableSchemaStore(d.tableSchemaStore)
	return true, nil
}

// HandleEvents can batch handle events about resolvedTs Event and DML Event.
// While for DDLEvent and SyncPointEvent, they should be handled separately,
// because they are block events.
// We ensure we only will receive one event when it's ddl event or sync point event
// by setting them with different event types in DispatcherEventsHandler.GetType
// When we handle events, we don't have any previous events still in sink.
//
// wakeCallback is used to wake the dynamic stream to handle the next batch events.
// It will be called when all the events are flushed to downstream successfully.
func (d *Dispatcher) HandleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	// Only return false when all events are resolvedTs Event.
	block = false
	dmlWakeOnce := &sync.Once{}
	dmlEvents := make([]*commonEvent.DMLEvent, 0, len(dispatcherEvents))
	latestResolvedTs := uint64(0)
	// Dispatcher is ready, handle the events
	for _, dispatcherEvent := range dispatcherEvents {
		log.Debug("dispatcher receive all event",
			zap.Stringer("dispatcher", d.id),
			zap.String("eventType", commonEvent.TypeToString(dispatcherEvent.Event.GetType())),
			zap.Any("event", dispatcherEvent.Event))
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
				return
			}
			log.Info("dispatcher receive ddl event",
				zap.Stringer("dispatcher", d.id),
				zap.String("query", ddl.Query),
				zap.Int64("table", ddl.TableID),
				zap.Uint64("commitTs", event.GetCommitTs()),
				zap.Uint64("seq", event.GetSeq()))
			ddl.AddPostFlushFunc(func() {
				if d.tableSchemaStore != nil {
					d.tableSchemaStore.AddEvent(ddl)
				}
				wakeCallback()
			})
			d.dealWithBlockEvent(ddl)
		case commonEvent.TypeSyncPointEvent:
			if len(dispatcherEvents) != 1 {
				log.Panic("sync point event should only be singly handled",
					zap.Stringer("dispatcherID", d.id))
			}
			block = true
			syncPoint := event.(*commonEvent.SyncPointEvent)
			log.Info("dispatcher receive sync point event",
				zap.Stringer("dispatcher", d.id),
				zap.Any("commitTsList", syncPoint.GetCommitTsList()),
				zap.Uint64("seq", event.GetSeq()))

			syncPoint.AddPostFlushFunc(func() {
				wakeCallback()
			})
			d.dealWithBlockEvent(syncPoint)
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

func (d *Dispatcher) AddDMLEventsToSink(events []*commonEvent.DMLEvent) {
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

func (d *Dispatcher) AddBlockEventToSink(event commonEvent.BlockEvent) error {
	d.tableProgress.Add(event)
	return d.sink.WriteBlockEvent(event)
}

func (d *Dispatcher) PassBlockEventToSink(event commonEvent.BlockEvent) {
	d.tableProgress.Pass(event)
	event.PostFlush()
}

func (d *Dispatcher) isFirstEvent(event commonEvent.Event) bool {
	if d.componentStatus.Get() == heartbeatpb.ComponentState_Initializing {
		switch event.GetType() {
		case commonEvent.TypeResolvedEvent, commonEvent.TypeDMLEvent, commonEvent.TypeDDLEvent, commonEvent.TypeSyncPointEvent:
			if event.GetCommitTs() > d.startTs {
				return true
			}
		}
	}
	return false
}

// TryClose should be called before Remove(), because the dispatcher may still wait the dispatcher status from maintainer.
// TryClose will return the watermark of current dispatcher, and return true when the dispatcher finished sending events to sink.
// EventDispatcherManager will clean the dispatcher info after Remove() is called.
func (d *Dispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	// If sink is normal(not meet error), we need to wait all the events in sink to flushed downstream successfully
	// If sink is not normal, we can close the dispatcher immediately.
	if !d.sink.IsNormal() || d.tableProgress.Empty() {
		w.CheckpointTs = d.GetCheckpointTs()
		w.ResolvedTs = d.GetResolvedTs()

		d.componentStatus.Set(heartbeatpb.ComponentState_Stopped)
		if d.IsTableTriggerEventDispatcher() {
			d.tableSchemaStore.Clear()
		}
		log.Info("dispatcher component has stopped and is ready for cleanup",
			zap.Stringer("changefeedID", d.changefeedID),
			zap.Stringer("dispatcher", d.id),
			zap.String("table", common.FormatTableSpan(d.tableSpan)),
			zap.Uint64("checkpointTs", d.GetCheckpointTs()),
			zap.Uint64("resolvedTs", d.GetResolvedTs()),
		)
		return w, true
	}
	log.Info("dispatcher is not ready to close",
		zap.Stringer("dispatcher", d.id),
		zap.Bool("sinkIsNormal", d.sink.IsNormal()),
		zap.Bool("tableProgressEmpty", d.tableProgress.Empty()))
	return w, false
}

// Remove is called when TryClose returns true,
// It set isRemoving to true, to make the dispatcher can be clean by the eventDispatcherManager.
// it also remove the dispatcher from status dynamic stream to stop receiving status info from maintainer.
func (d *Dispatcher) Remove() {
	log.Info("Remove dispatcher",
		zap.Stringer("dispatcher", d.id),
		zap.Stringer("changefeedID", d.changefeedID),
		zap.String("table", common.FormatTableSpan(d.tableSpan)))
	d.isRemoving.Store(true)

	dispatcherStatusDS := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDS.RemovePath(d.id)
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed",
			zap.Stringer("changefeedID", d.changefeedID),
			zap.Stringer("dispatcher", d.id),
			zap.String("table", common.FormatTableSpan(d.tableSpan)),
			zap.Uint64("checkpointTs", d.GetCheckpointTs()),
			zap.Uint64("resolvedTs", d.GetResolvedTs()),
			zap.Error(err))
	}
}

func (d *Dispatcher) GetHeartBeatInfo(h *HeartBeatInfo) {
	h.Watermark.CheckpointTs = d.GetCheckpointTs()
	h.Watermark.ResolvedTs = d.GetResolvedTs()
	h.Id = d.GetId()
	h.ComponentStatus = d.GetComponentStatus()
	h.IsRemoving = d.GetRemovingStatus()
}

func (d *Dispatcher) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&d.resolvedTs)
}

func (d *Dispatcher) GetCheckpointTs() uint64 {
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

// EmitBootstrap emits the table bootstrap event in a blocking way after changefeed started
// It will return after the bootstrap event is sent.
func (d *Dispatcher) EmitBootstrap() bool {
	bootstrap := loadBootstrapState(&d.BootstrapState)
	switch bootstrap {
	case BootstrapFinished:
		return true
	case BootstrapInProgress:
		return false
	case BootstrapNotStarted:
	}
	storeBootstrapState(&d.BootstrapState, BootstrapInProgress)
	tables := d.tableSchemaStore.GetAllNormalTableIds()
	if len(tables) == 0 {
		storeBootstrapState(&d.BootstrapState, BootstrapFinished)
		return true
	}
	start := time.Now()
	ts := d.GetStartTs()
	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	currentTables := make([]*common.TableInfo, 0, len(tables))
	for i := 0; i < len(tables); i++ {
		err := schemaStore.RegisterTable(tables[i], ts)
		if err != nil {
			log.Warn("register table to schemaStore failed",
				zap.Int64("tableID", tables[i]),
				zap.Uint64("startTs", ts),
				zap.Error(err),
			)
			continue
		}
		tableInfo, err := schemaStore.GetTableInfo(tables[i], ts)
		if err != nil {
			log.Warn("get table info failed, just ignore",
				zap.Stringer("changefeed", d.changefeedID),
				zap.Error(err))
			continue
		}
		currentTables = append(currentTables, tableInfo)
	}

	log.Info("start to send bootstrap messages",
		zap.Stringer("changefeed", d.changefeedID),
		zap.Int("tables", len(currentTables)))
	for idx, table := range currentTables {
		if table.IsView() {
			continue
		}
		ddlEvent := codec.NewBootstrapDDLEvent(table)
		err := d.sink.WriteBlockEvent(ddlEvent)
		if err != nil {
			log.Error("send bootstrap message failed",
				zap.Stringer("changefeed", d.changefeedID),
				zap.Int("tables", len(currentTables)),
				zap.Int("emitted", idx+1),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
			d.HandleError(errors.ErrExecDDLFailed.GenWithStackByArgs())
			return true
		}
	}
	storeBootstrapState(&d.BootstrapState, BootstrapFinished)
	log.Info("send bootstrap messages finished",
		zap.Stringer("changefeed", d.changefeedID),
		zap.Int("tables", len(currentTables)),
		zap.Duration("cost", time.Since(start)))
	return true
}

// updateDispatcherStatusToWorking updates the dispatcher status to working and adds it to status dynamic stream
func (d *Dispatcher) updateDispatcherStatusToWorking() {
	// only when we receive the first event, we can regard the dispatcher begin syncing data
	// then add it to status dynamic stream to receive dispatcher status from maintainer
	d.addToStatusDynamicStream()
	// set the dispatcher to working status
	d.componentStatus.Set(heartbeatpb.ComponentState_Working)
	d.statusesChan <- TableSpanStatusWithSeq{
		TableSpanStatus: &heartbeatpb.TableSpanStatus{
			ID:              d.id.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    d.GetCheckpointTs(),
		},
		Seq: d.seq,
	}
}

func (d *Dispatcher) HandleError(err error) {
	select {
	case d.errCh <- err:
	default:
		log.Error("error channel is full, discard error",
			zap.Stringer("changefeedID", d.changefeedID),
			zap.Stringer("dispatcherID", d.id),
			zap.Error(err))
	}
}
