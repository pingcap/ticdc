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

package dispatchermanager

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const redoHeartBeatInterval = time.Second * 1

func (e *EventDispatcherManager) newRedoDispatchers(infos []dispatcherCreateInfo, removeDDLTs bool) error {
	start := time.Now()

	dispatcherIds := make([]common.DispatcherID, 0, len(infos))
	tableIds := make([]int64, 0, len(infos))
	startTsList := make([]int64, 0, len(infos))
	tableSpans := make([]*heartbeatpb.TableSpan, 0, len(infos))
	schemaIds := make([]int64, 0, len(infos))
	for _, info := range infos {
		id := info.Id
		if _, ok := e.redoDispatcherMap.Get(id); ok {
			continue
		}
		dispatcherIds = append(dispatcherIds, id)
		tableIds = append(tableIds, info.TableSpan.TableID)
		startTsList = append(startTsList, int64(info.StartTs))
		tableSpans = append(tableSpans, info.TableSpan)
		schemaIds = append(schemaIds, info.SchemaID)
	}

	if len(dispatcherIds) == 0 {
		return nil
	}

	// When initializing the dispatcher manager, both the redo dispatcher and the common dispatcher exist.
	// The common dispatcher obtains the true start timestamp (start-ts) if the sink type is MySQL,
	// this start-ts is always greater than or equal to the global start-ts.
	// However, the redo dispatcher must receive data before the common dispatcher, and the common dispatcher replicates based on the global redo timestamp.
	// If the redo dispatcher’s start-ts is less than that of the common dispatcher,
	// we will encounter a checkpoint-ts greater than the resolved-ts in the redo metadata.
	// This results in the redo metadata recording an incorrect log, which can cause a panic if no additional redo metadata logs are flushed.
	// Therefore, we must ensure that the start-ts remains consistent with the common dispatcher by querying the start-ts from the MySQL sink.
	var newStartTsList []int64
	var err error
	if e.sink.SinkType() == common.MysqlSinkType {
		newStartTsList, _, err = e.sink.(*mysql.Sink).GetStartTsList(tableIds, startTsList, removeDDLTs)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("calculate real startTs for redo dispatchers",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Any("receiveStartTs", startTsList),
			zap.Any("realStartTs", newStartTsList),
			zap.Bool("removeDDLTs", removeDDLTs),
		)
	} else {
		newStartTsList = startTsList
	}

	for idx, id := range dispatcherIds {
		rd := dispatcher.NewRedoDispatcher(
			e.changefeedID,
			id, tableSpans[idx], e.redoSink,
			uint64(newStartTsList[idx]),
			e.statusesChan,
			e.blockStatusesChan,
			schemaIds[idx],
			e.redoSchemaIDToDispatchers,
			e.config.TimeZone,
			e.integrityConfig,
			e.filterConfig,
			e.errCh,
			e.config.BDRMode)
		if e.heartBeatTask == nil {
			e.heartBeatTask = newHeartBeatTask(e)
		}

		if rd.IsTableTriggerEventDispatcher() {
			e.redoTableTriggerEventDispatcher = rd
		} else {
			e.redoSchemaIDToDispatchers.Set(schemaIds[idx], id)
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(rd, e.redoQuota)
		}

		redoSeq := e.redoDispatcherMap.Set(rd.GetId(), rd)
		rd.SetSeq(redoSeq)

		if rd.IsTableTriggerEventDispatcher() {
			e.metricRedoTableTriggerEventDispatcherCount.Inc()
		} else {
			e.metricRedoEventDispatcherCount.Inc()
		}

		log.Info("new redo dispatcher created",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("tableSpan", common.FormatTableSpan(tableSpans[idx])),
			zap.Int64("startTs", newStartTsList[idx]))
	}
	e.metricRedoCreateDispatcherDuration.Observe(time.Since(start).Seconds() / float64(len(dispatcherIds)))
	log.Info("batch create new redo dispatchers",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Int("count", len(dispatcherIds)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

func (e *EventDispatcherManager) setRedoMeta() {
	if !e.redoMeta.Enabled() || e.redoMeta.Running() {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		err := e.redoMeta.PreStart(e.ctx)
		if err != nil && !errors.Is(errors.Cause(err), context.Canceled) {
			select {
			case <-e.ctx.Done():
				return
			case e.errCh <- err:
			default:
				log.Error("error channel is full, discard error",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Error(err),
				)
			}
		}
		err = e.redoMeta.Run(e.ctx)
		if err != nil && !errors.Is(errors.Cause(err), context.Canceled) {
			select {
			case <-e.ctx.Done():
				return
			case e.errCh <- err:
			default:
				log.Error("error channel is full, discard error",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Error(err),
				)
			}
		}
	}()
}

func (e *EventDispatcherManager) collectRedoTs(ctx context.Context) error {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	ticker := time.NewTicker(redoHeartBeatInterval)
	defer ticker.Stop()
	var previousCheckpointTs uint64
	var previousResolvedTs uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var checkpointTs uint64 = math.MaxUint64
			var resolvedTs uint64 = math.MaxUint64
			e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.EventDispatcher) {
				checkpointTs = min(checkpointTs, dispatcher.GetCheckpointTs())
			})
			e.redoDispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.RedoDispatcher) {
				resolvedTs = min(resolvedTs, dispatcher.GetCheckpointTs())
			})
			// Avoid invalid message
			if previousCheckpointTs >= checkpointTs && previousResolvedTs >= resolvedTs {
				continue
			}
			// The length of dispatcher map is zero, we should not update the previous ts.
			if checkpointTs != math.MaxUint64 {
				previousCheckpointTs = max(previousCheckpointTs, checkpointTs)
			}
			if resolvedTs != math.MaxUint64 {
				previousResolvedTs = max(previousResolvedTs, resolvedTs)
			}
			message := new(heartbeatpb.RedoTsMessage)
			message.ChangefeedID = e.changefeedID.ToPB()
			message.CheckpointTs = checkpointTs
			message.ResolvedTs = resolvedTs
			err := mc.SendCommand(
				messaging.NewSingleTargetMessage(
					e.GetMaintainerID(),
					messaging.MaintainerManagerTopic,
					message,
				))
			if err != nil {
				log.Error("failed to send redoTs request message", zap.Error(err))
			}
		}
	}
}

func (e *EventDispatcherManager) MergeRedoDispatcher(dispatcherIDs []common.DispatcherID, mergedDispatcherID common.DispatcherID) *MergeCheckTask {
	// Step 1: check the dispatcherIDs and mergedDispatcherID are valid:
	//         1. whether the mergedDispatcherID is not exist in the dispatcherMap
	//         2. whether the dispatcherIDs exist in the dispatcherMap
	//         3. whether the dispatcherIDs belong to the same table
	//         4. whether the dispatcherIDs have consecutive ranges
	//         5. whether the dispatcher in working status.

	if len(dispatcherIDs) < 2 {
		log.Error("merge redo dispatcher failed, invalid dispatcherIDs",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Any("dispatcherIDs", dispatcherIDs))
		return nil
	}
	if dispatcherItem, ok := e.redoDispatcherMap.Get(mergedDispatcherID); ok {
		// if the status is working, means the mergeDispatcher is outdated, return the latest status info
		if dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_Working {
			e.statusesChan <- dispatcher.TableSpanStatusWithSeq{
				TableSpanStatus: &heartbeatpb.TableSpanStatus{
					ID:              mergedDispatcherID.ToPB(),
					CheckpointTs:    dispatcherItem.GetCheckpointTs(),
					ComponentStatus: heartbeatpb.ComponentState_Working,
					Redo:            true,
				},
				Seq: e.redoDispatcherMap.GetSeq(),
			}
		}
		// otherwise, merge is in process, just return.
		return nil
	}

	var prevTableSpan *heartbeatpb.TableSpan
	var startKey []byte
	var endKey []byte
	var schemaID int64
	var fakeStartTs uint64 = math.MaxUint64 // we calculate the fake startTs as the min-checkpointTs of these dispatchers
	for idx, id := range dispatcherIDs {
		dispatcher, ok := e.redoDispatcherMap.Get(id)
		if !ok {
			log.Error("merge redo dispatcher failed, the dispatcher is not found",
				zap.Stringer("changefeedID", e.changefeedID),
				zap.Any("dispatcherID", id))
			return nil
		}
		if dispatcher.GetComponentStatus() != heartbeatpb.ComponentState_Working {
			log.Error("merge redo dispatcher failed, the dispatcher is not working",
				zap.Stringer("changefeedID", e.changefeedID),
				zap.Any("dispatcherID", id),
				zap.Any("componentStatus", dispatcher.GetComponentStatus()))
			return nil
		}
		if dispatcher.GetCheckpointTs() < fakeStartTs {
			fakeStartTs = dispatcher.GetCheckpointTs()
		}
		if idx == 0 {
			prevTableSpan = dispatcher.GetTableSpan()
			startKey = prevTableSpan.StartKey
			schemaID = dispatcher.GetSchemaID()
		} else {
			currentTableSpan := dispatcher.GetTableSpan()
			if !common.IsTableSpanConsecutive(prevTableSpan, currentTableSpan) {
				log.Error("merge redo dispatcher failed, the dispatcherIDs are not consecutive",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Any("dispatcherIDs", dispatcherIDs),
					zap.Any("prevTableSpan", prevTableSpan),
					zap.Any("currentTableSpan", currentTableSpan),
				)
				return nil
			}
			prevTableSpan = currentTableSpan
			endKey = currentTableSpan.EndKey
		}
	}

	// Step 2: create a new dispatcher with the merged ranges, and set it to preparing state;
	//         set the old dispatchers to waiting merge state.
	//         now, we just create a non-working dispatcher, we will make the dispatcher into work when DoMerge() called
	mergedSpan := &heartbeatpb.TableSpan{
		TableID:  prevTableSpan.TableID,
		StartKey: startKey,
		EndKey:   endKey,
	}

	mergedDispatcher := dispatcher.NewRedoDispatcher(
		e.changefeedID,
		mergedDispatcherID,
		mergedSpan,
		e.redoSink,
		fakeStartTs, // real startTs will be calculated later.
		e.statusesChan,
		e.blockStatusesChan,
		schemaID,
		e.redoSchemaIDToDispatchers,
		e.config.TimeZone,
		e.integrityConfig,
		e.filterConfig,
		e.errCh,
		e.config.BDRMode,
	)

	log.Info("new redo dispatcher created(merge dispatcher)",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", mergedDispatcherID),
		zap.String("tableSpan", common.FormatTableSpan(mergedSpan)))

	mergedDispatcher.SetComponentStatus(heartbeatpb.ComponentState_Preparing)
	seq := e.redoDispatcherMap.Set(mergedDispatcherID, mergedDispatcher)
	mergedDispatcher.SetSeq(seq)
	e.redoSchemaIDToDispatchers.Set(mergedDispatcher.GetSchemaID(), mergedDispatcherID)
	e.metricRedoEventDispatcherCount.Inc()

	for _, id := range dispatcherIDs {
		dispatcher, ok := e.redoDispatcherMap.Get(id)
		if ok {
			dispatcher.SetComponentStatus(heartbeatpb.ComponentState_WaitingMerge)
		}
	}

	// Step 3: register mergeDispatcher into event collector, and generate a task to check the merged dispatcher status
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).PrepareAddDispatcher(
		mergedDispatcher,
		e.redoQuota,
		func() {
			mergedDispatcher.SetComponentStatus(heartbeatpb.ComponentState_MergeReady)
			log.Info("merge redo dispatcher is ready",
				zap.Stringer("changefeedID", e.changefeedID),
				zap.Stringer("dispatcherID", mergedDispatcher.GetId()),
				zap.Any("tableSpan", common.FormatTableSpan(mergedDispatcher.GetTableSpan())),
			)
		})
	return newMergeCheckTask(e, mergedDispatcher, dispatcherIDs)
}

func (e *EventDispatcherManager) RedoDoMerge(t *MergeCheckTask) {
	log.Info("redo do merge",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Any("dispatcherIDs", t.dispatcherIDs),
		zap.Any("mergedDispatcher", t.mergedDispatcher.GetId()),
	)
	// Step1: close all dispatchers to be merged, calculate the min checkpointTs of the merged dispatcher
	minCheckpointTs := uint64(math.MaxUint64)
	closedList := make([]bool, len(t.dispatcherIDs)) // record whether the dispatcher is closed successfully
	closedCount := 0
	count := 0
	for closedCount < len(t.dispatcherIDs) {
		for idx, id := range t.dispatcherIDs {
			if closedList[idx] {
				continue
			}
			dispatcher, ok := e.redoDispatcherMap.Get(id)
			if !ok {
				log.Panic("redo dispatcher not found when do merge", zap.Stringer("dispatcherID", id))
			}
			if count == 0 {
				appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)
			}

			watermark, ok := dispatcher.TryClose()
			if ok {
				if watermark.CheckpointTs < minCheckpointTs {
					minCheckpointTs = watermark.CheckpointTs
				}
				closedList[idx] = true
				closedCount++
			} else {
				log.Info("redo dispatcher is still not closed", zap.Stringer("dispatcherID", id))
			}
		}
		time.Sleep(10 * time.Millisecond)
		count += 1
		log.Info("event dispatcher manager is doing merge, waiting for redo dispatchers to be closed",
			zap.Int("closedCount", closedCount),
			zap.Int("total", len(t.dispatcherIDs)),
			zap.Int("count", count),
			zap.Any("mergedDispatcher", t.mergedDispatcher.GetId()),
		)
	}

	// We don't need to calculate the true start timestamp (start-ts) because the redo metadata records the minimum checkpoint timestamp and resolved timestamp.
	// The merger dispatcher operates by first creating a dispatcher and then removing it.
	// Even if the redo dispatcher’s start-ts is less than that of the common dispatcher, we still record the correct redo metadata log.
	t.mergedDispatcher.SetStartTs(minCheckpointTs)
	t.mergedDispatcher.SetCurrentPDTs(e.pdClock.CurrentTS())
	t.mergedDispatcher.SetComponentStatus(heartbeatpb.ComponentState_Initializing)
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).CommitAddDispatcher(t.mergedDispatcher, minCheckpointTs)
	log.Info("merge redo dispatcher commit add dispatcher",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", t.mergedDispatcher.GetId()),
		zap.Any("tableSpan", common.FormatTableSpan(t.mergedDispatcher.GetTableSpan())),
		zap.Uint64("startTs", minCheckpointTs),
	)

	// Step3: cancel the merge task
	t.Cancel()

	// Step4: remove all the dispatchers to be merged
	// we set dispatcher removing status to true after we set the merged dispatcher into dispatcherMap and change its status to Initializing.
	// so that we can ensure the calculate of checkpointTs of the event dispatcher manager will include the merged dispatcher of the dispatchers to be merged
	// to avoid the fallback of the checkpointTs
	for _, id := range t.dispatcherIDs {
		dispatcher, ok := e.redoDispatcherMap.Get(id)
		if !ok {
			log.Panic("dispatcher not found when do merge", zap.Stringer("dispatcherID", id))
		}
		dispatcher.Remove()
	}
}

func (e *EventDispatcherManager) closeRedoAllDispatchers() {
	leftToCloseDispatchers := make([]*dispatcher.RedoDispatcher, 0)
	e.redoDispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.RedoDispatcher) {
		// Remove dispatcher from eventService
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)

		_, ok := dispatcher.TryClose()
		if !ok {
			leftToCloseDispatchers = append(leftToCloseDispatchers, dispatcher)
		} else {
			dispatcher.Remove()
		}
	})
	// wait all dispatchers finish syncing the data to sink
	for _, dispatcher := range leftToCloseDispatchers {
		log.Info("closing redo dispatcher",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Stringer("dispatcherID", dispatcher.GetId()),
			zap.Any("tableSpan", common.FormatTableSpan(dispatcher.GetTableSpan())),
		)
		ok := false
		count := 0
		for !ok {
			_, ok = dispatcher.TryClose()
			time.Sleep(10 * time.Millisecond)
			count += 1
			if count%100 == 0 {
				log.Info("waiting for redo dispatcher to close",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Stringer("dispatcherID", dispatcher.GetId()),
					zap.Any("tableSpan", common.FormatTableSpan(dispatcher.GetTableSpan())),
					zap.Int("count", count),
				)
			}
		}
		// Remove should be called after dispatcher is closed
		dispatcher.Remove()
	}
}

func (e *EventDispatcherManager) removeRedoDispatcher(id common.DispatcherID) {
	dispatcherItem, ok := e.redoDispatcherMap.Get(id)
	if ok {
		if dispatcherItem.GetRemovingStatus() {
			return
		}
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcherItem)

		count := 0
		ok := false
		// We don't want to block the ds handle function, so we just try 10 times.
		// If the dispatcher is not closed, we can wait for the next message to check it again
		for !ok && count < 10 {
			_, ok = dispatcherItem.TryClose()
			time.Sleep(10 * time.Millisecond)
			count += 1
			if count%5 == 0 {
				log.Info("waiting for dispatcher to close",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Stringer("dispatcherID", dispatcherItem.GetId()),
					zap.Any("tableSpan", common.FormatTableSpan(dispatcherItem.GetTableSpan())),
					zap.Int("count", count),
				)
			}
		}
		if ok {
			dispatcherItem.Remove()
		}
	} else {
		e.statusesChan <- dispatcher.TableSpanStatusWithSeq{
			TableSpanStatus: &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
				Redo:            true,
			},
			Seq: e.dispatcherMap.GetSeq(),
		}
	}
}

func (e *EventDispatcherManager) cleanRedoDispatcher(id common.DispatcherID, schemaID int64) {
	e.redoDispatcherMap.Delete(id)
	e.redoSchemaIDToDispatchers.Delete(schemaID, id)
	if e.redoTableTriggerEventDispatcher != nil && e.redoTableTriggerEventDispatcher.GetId() == id {
		e.redoTableTriggerEventDispatcher = nil
		e.metricRedoTableTriggerEventDispatcherCount.Dec()
	} else {
		e.metricRedoEventDispatcherCount.Dec()
	}
	log.Info("redo table event dispatcher completely stopped, and delete it from event dispatcher manager",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", id),
	)
}

func (e *EventDispatcherManager) SetGlobalRedoTs(checkpointTs, resolvedTs uint64) bool {
	// only update meta on the one node
	if e.tableTriggerEventDispatcher != nil && e.redoMeta.Running() {
		log.Info("update redo meta", zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("checkpointTs", checkpointTs), zap.Any("redoGlobalTs", e.redoGlobalTs.Load()))
		e.redoMeta.UpdateMeta(checkpointTs, resolvedTs)
	}
	return util.CompareAndMonotonicIncrease(&e.redoGlobalTs, resolvedTs)
}

func (e *EventDispatcherManager) GetRedoDispatcherMap() *DispatcherMap[*dispatcher.RedoDispatcher] {
	return e.redoDispatcherMap
}

func (e *EventDispatcherManager) GetRedoTableTriggerEventDispatcher() *dispatcher.RedoDispatcher {
	return e.redoTableTriggerEventDispatcher
}

func (e *EventDispatcherManager) GetAllRedoDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.redoSchemaIDToDispatchers.GetDispatcherIDs(schemaID)
	if e.redoTableTriggerEventDispatcher != nil {
		dispatcherIDs = append(dispatcherIDs, e.redoTableTriggerEventDispatcher.GetId())
	}
	return dispatcherIDs
}
