// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the Licens
// You may obtain a copy of the License at
//
//     http://www.apachorg/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the Licens

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

// var _ EventDispatcher = (*EventDispatcher)(nil)

// EventDispatcher is the interface that responsible for receiving events from Event Service
// type EventDispatcher interface {
// 	GetId() common.DispatcherID
// 	GetStartTs() uint64
// 	GetBDRMode() bool
// 	GetChangefeedID() common.ChangeFeedID
// 	GetTableSpan() *heartbeatpb.TableSpan
// 	GetTimezone() string
// 	GetIntegrityConfig() *eventpb.IntegrityConfig
// 	GetFilterConfig() *eventpb.FilterConfig
// 	EnableSyncPoint() bool
// 	GetSyncPointInterval() timDuration
// 	GetStartTsIsSyncpoint() bool
// 	GetResolvedTs() uint64
// 	HandleEvents(events []DispatcherEvent, wakeCallback func()) bool
// 	GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus
// 	HandleDispatcherStatus(*heartbeatpb.DispatcherStatus)
// 	GetComponentStatus() heartbeatpb.ComponentState
// 	SetComponentStatus(heartbeatpb.ComponentState)
// 	SetStartTs(uint64)
// 	SetCurrentPDTs(uint64)
// 	HandleError(error)
// 	SetStartTsIsSyncpoint(bool)
// 	// GetType returns the dispatcher type
// 	GetType() int
// }

type EventDispatcher struct {
	*BasicDispatcher
	BootstrapState bootstrapState
	redo           bool
	redoGlobalTs   *atomic.Uint64
	cacheEvents    struct {
		sync.Mutex
		events chan cacheEvents
	}
}

func NewEventDispatcher(
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
	redo bool,
	redoGlobalTs *atomic.Uint64,
) *EventDispatcher {
	basicDispatcher := NewBasicDispatcher(
		changefeedID,
		id, tableSpan, sink,
		startTs,
		statusesChan,
		blockStatusesChan,
		schemaID,
		schemaIDToDispatchers,
		timezone,
		integrityConfig,
		syncPointConfig,
		startTsIsSyncpoint,
		filterConfig,
		currentPdTs,
		errCh,
		bdrMode,
		TypeDispatcherEvent,
	)
	dispatcher := &EventDispatcher{
		BasicDispatcher: basicDispatcher,
		BootstrapState:  BootstrapFinished,
		// redo
		redo:         redo,
		redoGlobalTs: redoGlobalTs,
	}
	dispatcher.cacheEvents.events = make(chan cacheEvents, 1)

	return dispatcher
}

// InitializeTableSchemaStore initializes the tableSchemaStore for the table trigger event dispatcher.
// It returns true if the tableSchemaStore is initialized successfully, otherwise returns fals
func (d *EventDispatcher) InitializeTableSchemaStore(schemaInfo []*heartbeatpb.SchemaInfo) (ok bool, err error) {
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

func (d *EventDispatcher) HandleCacheEvents() {
	select {
	case cacheEvents, ok := <-d.cacheEvents.events:
		if !ok {
			return
		}
		block := d.HandleEvents(cacheEvents.events, cacheEvents.wakeCallback)
		if !block {
			cacheEvents.wakeCallback()
		}
	default:
	}
}

func (d *EventDispatcher) cache(dispatcherEvents []DispatcherEvent, wakeCallback func()) {
	d.cacheEvents.Lock()
	defer d.cacheEvents.Unlock()
	if d.GetRemovingStatus() {
		log.Warn("dispatcher has removed", zap.Any("id", d.id))
		return
	}
	// cache here
	cacheEvents := newCacheEvents(dispatcherEvents, wakeCallback)
	select {
	case d.cacheEvents.events <- cacheEvents:
		log.Warn("cache event",
			zap.Stringer("dispatcher", d.id),
			zap.Uint64("dispatcherResolvedTs", d.GetResolvedTs()),
			zap.Int("length", len(dispatcherEvents)),
			zap.Int("eventType", dispatcherEvents[len(dispatcherEvents)-1].Event.GetType()),
			zap.Uint64("commitTs", dispatcherEvents[len(dispatcherEvents)-1].Event.GetCommitTs()),
			zap.Uint64("redoGlobalTs", d.redoGlobalTs.Load()),
		)
	default:
		log.Panic("dispatcher cache events is full", zap.Stringer("dispatcher", d.id), zap.Int("len", len(d.cacheEvents.events)))
	}
}

func (d *EventDispatcher) HandleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	// redo check
	if d.redo && len(dispatcherEvents) > 0 && d.redoGlobalTs.Load() < dispatcherEvents[len(dispatcherEvents)-1].Event.GetCommitTs() {
		d.cache(dispatcherEvents, wakeCallback)
		return true
	}
	return d.handleEvents(dispatcherEvents, wakeCallback)
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
func (d *EventDispatcher) handleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	// Only return false when all events are resolvedTs Event.
	block = false
	dmlWakeOnce := &sync.Once{}
	dmlEvents := make([]*commonEvent.DMLEvent, 0, len(dispatcherEvents))
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

		// only when we receive the first event, we can regard the dispatcher begin syncing data
		// then turning into working status.
		if d.isFirstEvent(event) {
			d.updateDispatcherStatusToWorking()
		}

		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			atomic.StoreUint64(&d.resolvedTs, event.(commonEvent.ResolvedEvent).ResolvedTs)
		case commonEvent.TypeDMLEvent:
			dml := event.(*commonEvent.DMLEvent)
			if dml.Len() == 0 {
				return block
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
	if len(dmlEvents) > 0 {
		d.AddDMLEventsToSink(dmlEvents)
	}
	return block
}

func (d *EventDispatcher) AddDMLEventsToSink(events []*commonEvent.DMLEvent) {
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

func (d *EventDispatcher) AddBlockEventToSink(event commonEvent.BlockEvent) error {
	d.tableProgress.Add(event)
	return d.sink.WriteBlockEvent(event)
}

func (d *EventDispatcher) PassBlockEventToSink(event commonEvent.BlockEvent) {
	d.tableProgress.Pass(event)
	event.PostFlush()
}

func (d *EventDispatcher) isFirstEvent(event commonEvent.Event) bool {
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
func (d *EventDispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
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
func (d *EventDispatcher) Remove() {
	if d.isRemoving.CompareAndSwap(false, true) {
		d.cacheEvents.Lock()
		defer d.cacheEvents.Unlock()
		close(d.cacheEvents.events)
	}
	log.Info("remove dispatcher",
		zap.Stringer("dispatcher", d.id),
		zap.Stringer("changefeedID", d.changefeedID),
		zap.String("table", common.FormatTableSpan(d.tableSpan)))
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

func (d *EventDispatcher) GetHeartBeatInfo(h *HeartBeatInfo) {
	h.Watermark.CheckpointTs = d.GetCheckpointTs()
	h.Watermark.ResolvedTs = d.GetResolvedTs()
	h.Id = d.GetId()
	h.ComponentStatus = d.GetComponentStatus()
	h.IsRemoving = d.GetRemovingStatus()
}

func (d *EventDispatcher) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&d.resolvedTs)
}

func (d *EventDispatcher) GetCheckpointTs() uint64 {
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
func (d *EventDispatcher) EmitBootstrap() bool {
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
