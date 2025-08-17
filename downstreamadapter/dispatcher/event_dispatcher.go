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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

var _ Dispatcher = (*EventDispatcher)(nil)

// EventDispatcher is the dispatcher to flush events to the downstream
type EventDispatcher struct {
	*BasicDispatcher
	// BootstrapState stores a bootstrap state
	// when state is BootstrapNotStarted, it will send bootstrap messages
	// only for simple protocol
	BootstrapState bootstrapState
	redoEnable     bool
	// redoGlobalTs is updated by the maintainer. Events with a commit-ts greater than redoGlobalTs are cached until the redo log catches up.
	redoGlobalTs *atomic.Uint64
	// cacheEvents is used to store events with a commit-ts greater than redoGlobalTs
	cacheEvents struct {
		sync.Mutex
		events chan cacheEvents
	}
}

// All event dispatchers in the changefeed will share the same Sink.
// Here is a special dispatcher will deal with the events of the DDLSpan in one changefeed, we call it TableTriggerEventDispatcher
// One changefeed across multiple nodes only will have one TableTriggerEventDispatcher.
func NewEventDispatcher(
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	startTs uint64,
	schemaID int64,
	startTsIsSyncpoint bool,
	currentPdTs uint64,
	dispatcherType int,
	sink sink.Sink,
	sharedInfo *SharedInfo,
	redoEnable bool,
	redoGlobalTs *atomic.Uint64,
) *EventDispatcher {
	basicDispatcher := NewBasicDispatcher(
		id,
		tableSpan,
		startTs,
		schemaID,
		startTsIsSyncpoint,
		currentPdTs,
		dispatcherType,
		sink,
		sharedInfo,
	)
	dispatcher := &EventDispatcher{
		BasicDispatcher: basicDispatcher,
		BootstrapState:  BootstrapFinished,
		// redo related
		redoEnable:   redoEnable,
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

// HandleCacheEvents called when redoGlobalTs is updated
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

// cache will send the dispatcherEvents to the cacheEvents channel
func (d *EventDispatcher) cache(dispatcherEvents []DispatcherEvent, wakeCallback func()) {
	d.cacheEvents.Lock()
	defer d.cacheEvents.Unlock()
	if d.GetRemovingStatus() {
		log.Warn("dispatcher has removed", zap.Any("id", d.id))
		return
	}
	// Here we have to create a new event slice, because dispatcherEvents will be cleaned up in dynamic stream
	cacheEvents := cacheEvents{
		events:       append(make([]DispatcherEvent, 0, len(dispatcherEvents)), dispatcherEvents...),
		wakeCallback: wakeCallback,
	}
	select {
	case d.cacheEvents.events <- cacheEvents:
		log.Info("cache events",
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
	// if the commit-ts of last event of dispatcherEvents is greater than redoGlobalTs,
	// the dispatcherEvents will be cached util the redoGlobalTs is updated.
	if d.redoEnable && len(dispatcherEvents) > 0 && d.redoGlobalTs.Load() < dispatcherEvents[len(dispatcherEvents)-1].Event.GetCommitTs() {
		d.cache(dispatcherEvents, wakeCallback)
		return true
	}
	return d.handleEvents(dispatcherEvents, wakeCallback)
}

// Remove is called when TryClose returns true
// It set isRemoving to true, to make the dispatcher can be clean by the DispatcherManager.
func (d *EventDispatcher) Remove() {
	if d.isRemoving.CompareAndSwap(false, true) {
		d.cacheEvents.Lock()
		defer d.cacheEvents.Unlock()
		close(d.cacheEvents.events)
	}
	d.removeDispatcher()
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
				zap.Stringer("changefeed", d.sharedInfo.changefeedID),
				zap.Error(err))
			continue
		}
		currentTables = append(currentTables, tableInfo)
	}

	log.Info("start to send bootstrap messages",
		zap.Stringer("changefeed", d.sharedInfo.changefeedID),
		zap.Int("tables", len(currentTables)))
	for idx, table := range currentTables {
		if table.IsView() {
			continue
		}
		ddlEvent := codec.NewBootstrapDDLEvent(table)
		err := d.sink.WriteBlockEvent(ddlEvent)
		if err != nil {
			log.Error("send bootstrap message failed",
				zap.Stringer("changefeed", d.sharedInfo.changefeedID),
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
		zap.Stringer("changefeed", d.sharedInfo.changefeedID),
		zap.Int("tables", len(currentTables)),
		zap.Duration("cost", time.Since(start)))
	return true
}

// cacheEvents cache the events which commit-ts is less than or equal the redoGlobalTs
// it will be used when redoEnable is true
type cacheEvents struct {
	events       []DispatcherEvent
	wakeCallback func()
}
