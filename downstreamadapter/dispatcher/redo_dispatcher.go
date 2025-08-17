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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/redo"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)

var _ Dispatcher = (*RedoDispatcher)(nil)

// RedoDispatcher is the dispatcher to flush events to the redo log
type RedoDispatcher struct {
	*BasicDispatcher
	// redoMeta stores the redo meta log
	redoMeta *redo.RedoMeta
	cancel   context.CancelFunc
}

// RedoDispatcher is similar with BasicDispatcher.
func NewRedoDispatcher(
	id common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	startTs uint64,
	schemaID int64,
	startTsIsSyncpoint bool,
	sink sink.Sink,
	sharedInfo *SharedInfo,
) *RedoDispatcher {
	basicDispatcher := NewBasicDispatcher(
		id,
		tableSpan,
		startTs,
		schemaID,
		startTsIsSyncpoint,
		0,
		TypeDispatcherRedo,
		sink,
		sharedInfo,
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

// Remove is called when TryClose returns true
// It set isRemoving to true, to make the dispatcher can be clean by the DispatcherManager.
func (rd *RedoDispatcher) Remove() {
	rd.isRemoving.Store(true)
	rd.removeDispatcher()
	if rd.cancel != nil {
		rd.cancel()
	}
}

// SetRedoMeta used to init redo meta
// only for redo table trigger event dispatcher
func (rd *RedoDispatcher) SetRedoMeta(cfg *config.ConsistentConfig) {
	if !rd.IsTableTriggerEventDispatcher() {
		log.Error("SetRedoMeta should be called by redo table trigger event dispatcher", zap.Any("id", rd.GetId()))
	}
	ctx := context.Background()
	ctx, rd.cancel = context.WithCancel(ctx)
	rd.redoMeta = redo.NewRedoMeta(rd.sharedInfo.changefeedID, rd.startTs, cfg)
	go func() {
		err := rd.redoMeta.PreStart(ctx)
		if err != nil {
			rd.HandleError(err)
		}
		err = rd.redoMeta.Run(ctx)
		if err != nil {
			rd.HandleError(err)
		}
	}()
}

// UpdateMeta used to update redo meta log
// only for redo table trigger event dispatcher
func (rd *RedoDispatcher) UpdateMeta(checkpointTs, resolvedTs common.Ts) {
	if !rd.IsTableTriggerEventDispatcher() {
		log.Error("UpdateMeta should be called by redo table trigger event dispatcher", zap.Any("id", rd.GetId()))
	}
	if rd.redoMeta.Running() {
		log.Debug("update redo meta", zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("checkpointTs", checkpointTs))
		rd.redoMeta.UpdateMeta(checkpointTs, resolvedTs)
	}
}
