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
	"math"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func getDispatcherStatus(id common.DispatcherID, dispatcherItem dispatcher.Dispatcher, needCompleteStatus bool) (*heartbeatpb.TableSpanStatus, *cleanMap, *heartbeatpb.Watermark) {
	heartBeatInfo := &dispatcher.HeartBeatInfo{}
	// the merged dispatcher in preparing state, don't need to join the calculation of the heartbeat
	// the dispatcher still not know the startTs of it, and the dispatchers to be merged are still in the calculation of the checkpointTs
	if dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_Preparing || dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_MergeReady {
		return nil, nil, nil
	}
	dispatcherItem.GetHeartBeatInfo(heartBeatInfo)
	// If the dispatcher is in removing state, we need to check if it's closed successfully.
	// If it's closed successfully, we could clean it up.
	// TODO: we need to consider how to deal with the checkpointTs of the removed dispatcher if the message will be discarded.
	if heartBeatInfo.IsRemoving {
		watermark, ok := dispatcherItem.TryClose()
		if ok {
			// If the dispatcher is removed successfully, we need to add the tableSpan into message whether needCompleteStatus is true or not.
			return &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
				CheckpointTs:    watermark.CheckpointTs,
				Mode:            dispatcherItem.GetMode(),
			}, &cleanMap{dispatcherItem.GetId(), dispatcherItem.GetSchemaID(), dispatcherItem.GetMode()}, &watermark
		}
	}
	if needCompleteStatus {
		if dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_Initializing {
			log.Debug("dispatcher is initializing",
				zap.Stringer("changefeedID", dispatcherItem.GetChangefeedID()),
				zap.Stringer("dispatcherID", id),
				zap.String("tableSpan", common.FormatTableSpan(dispatcherItem.GetTableSpan())),
				zap.Any("componentStatus", dispatcherItem.GetComponentStatus()),
			)
			return nil, nil, &heartBeatInfo.Watermark
		}
		return &heartbeatpb.TableSpanStatus{
			ID:                 id.ToPB(),
			ComponentStatus:    heartBeatInfo.ComponentStatus,
			CheckpointTs:       heartBeatInfo.Watermark.CheckpointTs,
			EventSizePerSecond: dispatcherItem.GetEventSizePerSecond(),
			Mode:               dispatcherItem.GetMode(),
		}, nil, &heartBeatInfo.Watermark
	}
	return nil, nil, &heartBeatInfo.Watermark
}

func prepareCreateDispatcher[T dispatcher.Dispatcher](infos map[common.DispatcherID]dispatcherCreateInfo, dispatcherMap *DispatcherMap[T]) (
	[]common.DispatcherID, []int64, []int64, []*heartbeatpb.TableSpan, []int64,
) {
	dispatcherIds := make([]common.DispatcherID, 0, len(infos))
	tableIds := make([]int64, 0, len(infos))
	startTsList := make([]int64, 0, len(infos))
	tableSpans := make([]*heartbeatpb.TableSpan, 0, len(infos))
	schemaIds := make([]int64, 0, len(infos))
	for _, info := range infos {
		id := info.Id
		if _, ok := dispatcherMap.Get(id); ok {
			continue
		}
		dispatcherIds = append(dispatcherIds, id)
		tableIds = append(tableIds, info.TableSpan.TableID)
		startTsList = append(startTsList, int64(info.StartTs))
		tableSpans = append(tableSpans, info.TableSpan)
		schemaIds = append(schemaIds, info.SchemaID)
	}
	return dispatcherIds, tableIds, startTsList, tableSpans, schemaIds
}

func prepareMergeDispatcher[T dispatcher.Dispatcher](changefeedID common.ChangeFeedID,
	dispatcherIDs []common.DispatcherID,
	dispatcherMap *DispatcherMap[T],
	mergedDispatcherID common.DispatcherID,
	statusesChan chan dispatcher.TableSpanStatusWithSeq,
) bool {
	if len(dispatcherIDs) < 2 {
		log.Error("merge dispatcher failed, invalid dispatcherIDs",
			zap.Stringer("changefeedID", changefeedID),
			zap.Any("dispatcherIDs", dispatcherIDs))
		return false
	}
	if dispatcherItem, ok := dispatcherMap.Get(mergedDispatcherID); ok {
		// if the status is working, means the mergeDispatcher is outdated, return the latest status info
		if dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_Working {
			statusesChan <- dispatcher.TableSpanStatusWithSeq{
				TableSpanStatus: &heartbeatpb.TableSpanStatus{
					ID:              mergedDispatcherID.ToPB(),
					CheckpointTs:    dispatcherItem.GetCheckpointTs(),
					ComponentStatus: heartbeatpb.ComponentState_Working,
					Mode:            dispatcherItem.GetMode(),
				},
				Seq: dispatcherMap.GetSeq(),
			}
		}
		// otherwise, merge is in process, just return.
		return false
	}
	return true
}

func createMergedSpan[T dispatcher.Dispatcher](changefeedID common.ChangeFeedID,
	dispatcherIDs []common.DispatcherID,
	dispatcherMap *DispatcherMap[T],
) (*heartbeatpb.TableSpan, uint64, int64) {
	var prevTableSpan *heartbeatpb.TableSpan
	var startKey []byte
	var endKey []byte
	var schemaID int64
	var fakeStartTs uint64 = math.MaxUint64 // we calculate the fake startTs as the min-checkpointTs of these dispatchers

	for idx, id := range dispatcherIDs {
		dispatcherItem, ok := dispatcherMap.Get(id)
		if !ok {
			log.Error("merge dispatcher failed, the dispatcher is not found",
				zap.Stringer("changefeedID", changefeedID),
				zap.Any("dispatcherID", id))
			return nil, 0, 0
		}
		if dispatcherItem.GetComponentStatus() != heartbeatpb.ComponentState_Working {
			log.Error("merge dispatcher failed, the dispatcher is not working",
				zap.Stringer("changefeedID", changefeedID),
				zap.Any("dispatcherID", id),
				zap.Int64("mode", dispatcherItem.GetMode()),
				zap.Any("componentStatus", dispatcherItem.GetComponentStatus()))
			return nil, 0, 0
		}
		if dispatcherItem.GetCheckpointTs() < fakeStartTs {
			fakeStartTs = dispatcherItem.GetCheckpointTs()
		}
		if idx == 0 {
			prevTableSpan = dispatcherItem.GetTableSpan()
			startKey = prevTableSpan.StartKey
			schemaID = dispatcherItem.GetSchemaID()
		} else {
			currentTableSpan := dispatcherItem.GetTableSpan()
			if !common.IsTableSpanConsecutive(prevTableSpan, currentTableSpan) {
				log.Error("merge dispatcher failed, the dispatcherIDs are not consecutive",
					zap.Stringer("changefeedID", changefeedID),
					zap.Any("dispatcherIDs", dispatcherIDs),
					zap.Int64("mode", dispatcherItem.GetMode()),
					zap.Any("prevTableSpan", prevTableSpan),
					zap.Any("currentTableSpan", currentTableSpan),
				)
				return nil, 0, 0
			}
			prevTableSpan = currentTableSpan
			endKey = currentTableSpan.EndKey
		}
	}
	// Step 2: create a new dispatcher with the merged ranges, and set it to preparing state;
	//
	//	set the old dispatchers to waiting merge state.
	//	now, we just create a non-working dispatcher, we will make the dispatcher into work when DoMerge() called
	return &heartbeatpb.TableSpan{
		TableID:  prevTableSpan.TableID,
		StartKey: startKey,
		EndKey:   endKey,
	}, fakeStartTs, schemaID
}

func registerMergeDispatcher[T dispatcher.Dispatcher](changefeedID common.ChangeFeedID,
	dispatcherIDs []common.DispatcherID,
	dispatcherMap *DispatcherMap[T],
	mergedDispatcherID common.DispatcherID,
	mergedDispatcher T,
	schemaIDToDispatchers *dispatcher.SchemaIDToDispatchers,
	metricDispatcherCount prometheus.Gauge,
	memQuota uint64,
) {
	mergedDispatcher.SetComponentStatus(heartbeatpb.ComponentState_Preparing)
	seq := dispatcherMap.Set(mergedDispatcherID, mergedDispatcher)
	mergedDispatcher.SetSeq(seq)
	schemaIDToDispatchers.Set(mergedDispatcher.GetSchemaID(), mergedDispatcherID)
	metricDispatcherCount.Inc()

	for _, id := range dispatcherIDs {
		dispatcherItem, ok := dispatcherMap.Get(id)
		if ok {
			dispatcherItem.SetComponentStatus(heartbeatpb.ComponentState_WaitingMerge)
		}
	}
	// Step 3: register mergeDispatcher into event collector, and generate a task to check the merged dispatcher status
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).PrepareAddDispatcher(
		mergedDispatcher,
		memQuota,
		func() {
			mergedDispatcher.SetComponentStatus(heartbeatpb.ComponentState_MergeReady)
			log.Info("merge dispatcher is ready",
				zap.Stringer("changefeedID", changefeedID),
				zap.Stringer("dispatcherID", mergedDispatcher.GetId()),
				zap.Int64("mode", mergedDispatcher.GetMode()),
				zap.Any("tableSpan", common.FormatTableSpan(mergedDispatcher.GetTableSpan())),
			)
		})
}

func removeDispatcher[T dispatcher.Dispatcher](e *DispatcherManager,
	id common.DispatcherID,
	dispatcherMap *DispatcherMap[T],
	sinkType common.SinkType,
) {
	changefeedID := e.changefeedID
	statusesChan := e.sharedInfo.GetStatusesChan()

	dispatcherItem, ok := dispatcherMap.Get(id)
	if ok {
		if dispatcherItem.GetRemovingStatus() {
			return
		}

		// Perform fast synchronous cleanup operations
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcherItem)

		// for non-mysql class sink, only the event dispatcher manager with table trigger event dispatcher need to receive the checkpointTs message.
		if common.IsDefaultMode(dispatcherItem.GetMode()) && dispatcherItem.IsTableTriggerEventDispatcher() && sinkType != common.MysqlSinkType {
			err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveCheckpointTsMessage(changefeedID)
			if err != nil {
				log.Error("remove checkpointTs message failed",
					zap.Stringer("changefeedID", changefeedID),
					zap.Error(err),
				)
			}
		}

		// Submit async remove task to thread pool
		task := &RemoveDispatcherTask{
			manager:        e,
			dispatcherItem: dispatcherItem,
			retryCount:     0,
		}
		scheduler := GetRemoveDispatcherTaskScheduler()
		taskHandle := scheduler.Submit(task, time.Now())

		// Save taskHandle for later cancellation
		e.removeTaskHandles.Store(id, taskHandle)

		log.Info("submitted async remove task",
			zap.Stringer("changefeedID", changefeedID),
			zap.Stringer("dispatcherID", id))
	} else {
		statusesChan <- dispatcher.TableSpanStatusWithSeq{
			TableSpanStatus: &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
				// If the dispatcherItem is not existed, we use sinkType to check
				Mode: common.GetModeBySinkType(sinkType),
			},
			Seq: dispatcherMap.GetSeq(),
		}
	}
}

// closeAllDispatchers is called when the event dispatcher manager is closing
func closeAllDispatchers[T dispatcher.Dispatcher](changefeedID common.ChangeFeedID,
	dispatcherMap *DispatcherMap[T],
	sinkType common.SinkType,
) {
	dispatcherMap.ForEach(func(id common.DispatcherID, dispatcherItem T) {
		// Remove dispatcher from eventService
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcherItem)

		if common.IsDefaultMode(dispatcherItem.GetMode()) && dispatcherItem.IsTableTriggerEventDispatcher() && sinkType != common.MysqlSinkType {
			err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveCheckpointTsMessage(changefeedID)
			if err != nil {
				log.Error("remove checkpointTs message failed",
					zap.Stringer("changefeedID", changefeedID),
					zap.Error(err),
				)
			}
		}
		dispatcherItem.TryClose()
		dispatcherItem.Remove()
	})
}
