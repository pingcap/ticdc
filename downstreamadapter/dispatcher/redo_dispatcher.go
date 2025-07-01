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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

var _ Dispatcher = (*RedoDispatcher)(nil)

type RedoDispatcher struct {
	*BasicDispatcher
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

func (rd *RedoDispatcher) HandleEvents(dispatcherEvents []DispatcherEvent, wakeCallback func()) (block bool) {
	if rd.GetRemovingStatus() {
		log.Warn("redo dispatcher has removed", zap.Any("id", rd.id))
		return true
	}
	return rd.handleEvents(dispatcherEvents, wakeCallback)
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
