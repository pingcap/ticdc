// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dispatchermanager

import (
	"math"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

// HeartbeatTask is a perioic task to collect the heartbeat status from event dispatcher manager and push to heartbeatRequestQueue
type HeartBeatTask struct {
	taskHandle *threadpool.TaskHandle
	manager    *EventDispatcherManager
	// Used to determine when to collect complete status
	statusTick int
}

func newHeartBeatTask(manager *EventDispatcherManager) *HeartBeatTask {
	taskScheduler := GetHeartBeatTaskScheduler()
	t := &HeartBeatTask{
		manager:    manager,
		statusTick: 0,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(time.Second*1))
	return t
}

func (t *HeartBeatTask) Execute() time.Time {
	if t.manager.closed.Load() {
		return time.Time{}
	}
	executeInterval := time.Millisecond * 200
	// 10s / 200ms = 50
	completeStatusInterval := int(time.Second * 10 / executeInterval)
	t.statusTick++
	needCompleteStatus := (t.statusTick)%completeStatusInterval == 0
	message := t.manager.aggregateDispatcherHeartbeats(needCompleteStatus)
	t.manager.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: t.manager.GetMaintainerID(), Request: message})
	return time.Now().Add(executeInterval)
}

func (t *HeartBeatTask) Cancel() {
	t.taskHandle.Cancel()
}

var (
	heartBeatTaskSchedulerOnce sync.Once
	heartBeatTaskScheduler     threadpool.ThreadPool
)

func GetHeartBeatTaskScheduler() threadpool.ThreadPool {
	heartBeatTaskSchedulerOnce.Do(func() {
		heartBeatTaskScheduler = threadpool.NewThreadPoolDefault()
	})
	return heartBeatTaskScheduler
}

func SetHeartBeatTaskScheduler(taskScheduler threadpool.ThreadPool) {
	heartBeatTaskScheduler = taskScheduler
}

var (
	mergeCheckTaskSchedulerOnce sync.Once
	mergeCheckTaskScheduler     threadpool.ThreadPool
)

func GetMergeCheckTaskScheduler() threadpool.ThreadPool {
	mergeCheckTaskSchedulerOnce.Do(func() {
		mergeCheckTaskScheduler = threadpool.NewThreadPoolDefault()
	})
	return mergeCheckTaskScheduler
}

func SetMergeCheckTaskScheduler(taskScheduler threadpool.ThreadPool) {
	mergeCheckTaskScheduler = taskScheduler
}

// MergeCheckTask is a task to check the status of the merged dispatcher.
type MergeCheckTask struct {
	taskHandle       *threadpool.TaskHandle
	manager          *EventDispatcherManager
	mergedDispatcher dispatcher.Dispatcher
	dispatcherIDs    []common.DispatcherID // the ids of dispatchers to be merged
}

func newMergeCheckTask(manager *EventDispatcherManager, mergedDispatcher dispatcher.Dispatcher, dispatcherIDs []common.DispatcherID) *MergeCheckTask {
	taskScheduler := GetMergeCheckTaskScheduler()
	t := &MergeCheckTask{
		manager:          manager,
		mergedDispatcher: mergedDispatcher,
		dispatcherIDs:    dispatcherIDs,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(time.Second*1))
	return t
}

func (t *MergeCheckTask) Execute() time.Time {
	if t.manager.closed.Load() {
		return time.Time{}
	}

	if t.mergedDispatcher.GetComponentStatus() != heartbeatpb.ComponentState_MergeReady {
		return time.Now().Add(time.Second * 1)
	}

	if dispatcher.IsRedoDispatcher(t.mergedDispatcher) {
		doMerge(t, t.manager.redoDispatcherMap)
	} else {
		doMerge(t, t.manager.dispatcherMap)
	}
	return time.Now().Add(time.Second * 1)
}

func (t *MergeCheckTask) Cancel() {
	t.taskHandle.Cancel()
}

func doMerge[T dispatcher.Dispatcher](t *MergeCheckTask, dispatcherMap *DispatcherMap[T]) {
	log.Info("do merge",
		zap.Stringer("changefeedID", t.manager.changefeedID),
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
			dispatcher, ok := dispatcherMap.Get(id)
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
	t.mergedDispatcher.SetCurrentPDTs(t.manager.pdClock.CurrentTS())
	t.mergedDispatcher.SetComponentStatus(heartbeatpb.ComponentState_Initializing)
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).CommitAddDispatcher(t.mergedDispatcher, minCheckpointTs)
	log.Info("merge redo dispatcher commit add dispatcher",
		zap.Stringer("changefeedID", t.manager.changefeedID),
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
		dispatcher, ok := dispatcherMap.Get(id)
		if !ok {
			log.Panic("dispatcher not found when do merge", zap.Stringer("dispatcherID", id))
		}
		dispatcher.Remove()
	}
}
