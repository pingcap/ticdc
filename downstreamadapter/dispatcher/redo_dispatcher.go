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
	"go.uber.org/zap"
)

type RedoDispatcher struct {
	*BasicDispatcher
	isRemoving atomic.Bool
}

func NewRedoDispatcher(
	changefeedID common.ChangeFeedID,
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	redoSink sink.Sink,
	startTs uint64,
	statusesChan chan TableSpanStatusWithSeq,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	schemaID int64,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	timezone string,
	integrityConfig *eventpb.IntegrityConfig,
	filterConfig *eventpb.FilterConfig,
	errCh chan error,
	bdrMode bool,
) *RedoDispatcher {
	basicDispatcher := NewBasicDispatcher(
		changefeedID,
		id, tableSpan, redoSink,
		startTs,
		statusesChan,
		blockStatusesChan,
		schemaID,
		schemaIDToDispatchers,
		timezone,
		integrityConfig,
		nil,
		false,
		filterConfig,
		0,
		errCh,
		bdrMode,
		TypeDispatcherRedo,
	)
	dispatcher := &RedoDispatcher{
		BasicDispatcher: basicDispatcher,
	}

	return dispatcher
}

// HandleEvents can batch handle events about resolvedTs Event and DML Event.
// While for DDLEvent and SyncPointEvent, they should be handled separately,
// because they are block events.
// We ensure we only will receive one event when it's ddl event or sync point event
// by setting them with different event types in DispatcherEventsHandler.GetType
// When we handle events, we don't have any previous events still in sink.
func (rd *RedoDispatcher) HandleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	if rd.GetRemovingStatus() {
		log.Warn("redo dispatcher has removed", zap.Any("id", rd.id))
		return true
	}
	// Only return false when all events are resolvedTs Event.
	block = false
	dmlWakeOnce := &sync.Once{}
	dmlEvents := make([]*commonEvent.DMLEvent, 0, len(dispatcherEvents))
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

		// only when we receive the first event, we can regard the dispatcher begin syncing data
		// then turning into working status.
		if rd.isFirstEvent(event) {
			rd.updateDispatcherStatusToWorking()
		}

		switch event.GetType() {
		case commonEvent.TypeResolvedEvent:
			atomic.StoreUint64(&rd.resolvedTs, event.GetCommitTs())
		case commonEvent.TypeDMLEvent:
			dml := event.(*commonEvent.DMLEvent)
			if dml.Len() == 0 {
				return block
			}
			block = true
			dml.AddPostFlushFunc(func() {
				// Considering dml event in sink may be written to downstream not in order,
				// thus, we use tableProgress.Empty() to ensure these events are flushed to downstream completely
				// and wake dynamic stream to handle the next events.
				if rd.tableProgress.Empty() {
					dmlWakeOnce.Do(wakeCallback)
				}
			})
			dmlEvents = append(dmlEvents, dml)
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
				rd.HandleError(err)
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
	if len(dmlEvents) > 0 {
		rd.AddDMLEventsToSink(dmlEvents)
	}
	return block
}

func (rd *RedoDispatcher) Remove() {
	rd.isRemoving.Store(true)
	log.Info("remove redo dispatcher",
		zap.Stringer("dispatcher", rd.id),
		zap.Stringer("changefeedID", rd.changefeedID),
		zap.String("table", common.FormatTableSpan(rd.tableSpan)))
	dispatcherStatusDS := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDS.RemovePath(rd.id)
	if err != nil {
		log.Error("remove redo dispatcher from dynamic stream failed",
			zap.Stringer("changefeedID", rd.changefeedID),
			zap.Stringer("dispatcher", rd.id),
			zap.String("table", common.FormatTableSpan(rd.tableSpan)),
			zap.Uint64("checkpointTs", rd.GetCheckpointTs()),
			zap.Uint64("resolvedTs", rd.GetResolvedTs()),
			zap.Error(err))
	}
}

func (rd *RedoDispatcher) TryClose() (w heartbeatpb.Watermark, ok bool) {
	// If redoSink is normal(not meet error), we need to wait all the events in redoSink to flushed downstream successfully.
	// If redoSink is not normal, we can close the dispatcher immediately.
	if !rd.sink.IsNormal() || rd.tableProgress.Empty() {
		w.CheckpointTs = rd.GetCheckpointTs()
		w.ResolvedTs = rd.GetResolvedTs()

		rd.componentStatus.Set(heartbeatpb.ComponentState_Stopped)
		return w, true
	}
	log.Info("redo dispatcher is not ready to close",
		zap.Stringer("dispatcher", rd.id),
		zap.Bool("sinkIsNormal", rd.sink.IsNormal()),
		zap.Bool("tableProgressEmpty", rd.tableProgress.Empty()))
	return w, false
}
